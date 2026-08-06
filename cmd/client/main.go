package main

import (
    "bufio"
    "bytes"
    "context"
    "crypto/tls"
    "encoding/base64"
    "encoding/binary"
    "errors"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
    "net/url"
    "os"
    "os/signal"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
    "syscall"
    "time"

    "github.com/gorilla/websocket"
    "github.com/refraction-networking/utls"
    "github.com/xtaci/smux"

    "proxy/internal/config"
    "proxy/internal/constants"
    "proxy/internal/protocol"
    "proxy/internal/relay"
    "proxy/internal/session"
    "proxy/internal/transport"
)

var (
    currentSession *session.ClientSession
    sessionMu      sync.Mutex
    conf           *config.ClientConfig
    pool           *ECHPool
)

type ECHPool struct {
    wsServerAddr  string
    connectionNum int
    targetIPs     []string
    clientID      string

    wsConnsMu     sync.RWMutex
    smuxConns     []*smux.Session
    channelRTT    []int64
    selectCounter uint64
}

func main() {
    if len(os.Args) < 2 {
        log.Fatalf("Usage: %s <config.json>", os.Args[0])
    }
    if err := config.LoadClientConfig(os.Args[1]); err != nil {
        log.Fatalf("load config: %v", err)
    }
    conf = config.ClientConf

    if err := transport.InitUUID(conf.UUID); err != nil {
        log.Fatalf("init uuid: %v", err)
    }

    if err := transport.InitECH(conf.DNSServer, conf.ECHDomain, conf.Fallback); err != nil {
        log.Printf("[ECH] 初始化失败: %v", err)
    }

    go runSocks5(conf.LocalSocks5)
    if conf.LocalHTTP != "" {
        go runHTTP(conf.LocalHTTP)
    }

    startPool()
    connectLoop()

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh
    if currentSession != nil {
        currentSession.Close()
    }
}

func startPool() {
    total := conf.Concur
    pool = &ECHPool{
        wsServerAddr:  conf.ServerAddr,
        connectionNum: conf.Concur,
        targetIPs:     []string{},
        clientID:      conf.UUID,
        smuxConns:     make([]*smux.Session, total),
        channelRTT:    make([]int64, total),
    }
    for i := 0; i < len(pool.smuxConns); i++ {
        go pool.dialAndServe(i, "")
    }
}

func (p *ECHPool) dialAndServe(idx int, ip string) {
    chID := idx + 1
    ipLabel := ip
    if strings.TrimSpace(ipLabel) == "" {
        ipLabel = "自动解析"
    }
    for {
        wsConn, err := dialWS(p.wsServerAddr, ip, p.clientID, chID)
        if err != nil {
            log.Printf("[客户端] 通道 %d (IP:%s) 连接失败: %v", chID, ipLabel, err)
            time.Sleep(3 * time.Second)
            continue
        }
        wsNet := transport.NewWSNetConn(wsConn, conf.UpPack, conf.ChunkMs, conf.MaxQueueBytes)
        sess, err := smux.Client(wsNet, nil)
        if err != nil {
            _ = wsConn.Close()
            log.Printf("[客户端] 通道 %d (IP:%s) smux 初始化失败: %v", chID, ipLabel, err)
            time.Sleep(3 * time.Second)
            continue
        }
        p.wsConnsMu.Lock()
        p.smuxConns[idx] = sess
        p.channelRTT[idx] = 0
        p.wsConnsMu.Unlock()
        log.Printf("[客户端] 通道 %d (IP:%s) 就绪", chID, ipLabel)
        if rtt, err := p.probeChannelRTTOnce(sess, conf.DialTimeout); err == nil {
            p.channelRTT[idx] = rtt
        }

        done := make(chan error, 1)
        go p.probeChannelRTT(sess, idx, done)
        var probeErr error
        select {
        case probeErr = <-done:
        case <-wsNet.Dead():
            _ = sess.Close()
            <-done
            probeErr = wsNet.DeadErr()
            if probeErr == nil {
                probeErr = io.EOF
            }
        }

        _ = sess.Close()
        _ = wsConn.Close()

        p.wsConnsMu.Lock()
        p.smuxConns[idx] = nil
        p.channelRTT[idx] = 0
        p.wsConnsMu.Unlock()
        if probeErr != nil {
            log.Printf("[客户端] 通道 %d 断开原因: %v", chID, probeErr)
        }
        log.Printf("[客户端] 通道 %d 断开，重连中...", chID)
        time.Sleep(3 * time.Second)
    }
}

func (p *ECHPool) probeChannelRTT(sess *smux.Session, idx int, done chan error) {
    var exitErr error
    defer func() {
        done <- exitErr
        close(done)
    }()
    ticker := time.NewTicker(conf.KeepAlive)
    defer ticker.Stop()
    for {
        rtt, err := p.probeChannelRTTOnce(sess, conf.DialTimeout)
        if err != nil {
            p.channelRTT[idx] = int64(conf.DialTimeout.Nanoseconds())
            if sess.IsClosed() {
                exitErr = err
                return
            }
            <-ticker.C
            continue
        }
        p.channelRTT[idx] = rtt
        <-ticker.C
    }
}

func (p *ECHPool) probeChannelRTTOnce(sess *smux.Session, timeout time.Duration) (int64, error) {
    start := time.Now()
    s, err := sess.OpenStream()
    if err != nil {
        return 0, err
    }
    defer s.Close()
    _ = s.SetDeadline(time.Now().Add(timeout))
    if err := writeSmuxOpenHeader(s, constants.StreamKindPing, 0, ""); err != nil {
        return 0, err
    }
    payload := make([]byte, 8)
    binary.BigEndian.PutUint64(payload, uint64(start.UnixNano()))
    if _, err := s.Write(payload); err != nil {
        return 0, err
    }
    ack := make([]byte, 8)
    if _, err := io.ReadFull(s, ack); err != nil {
        return 0, err
    }
    if !bytes.Equal(ack, payload) {
        return 0, fmt.Errorf("ping ack mismatch")
    }
    return time.Since(start).Nanoseconds(), nil
}

func (p *ECHPool) openBestStream() (*smux.Stream, int, int, error) {
    p.wsConnsMu.RLock()
    type candidate struct {
        idx int
        rtt int64
    }
    cands := make([]candidate, 0, len(p.smuxConns))
    for i, sess := range p.smuxConns {
        if sess == nil || sess.IsClosed() {
            continue
        }
        rtt := p.channelRTT[i]
        if rtt <= 0 {
            rtt = int64(conf.DialTimeout.Nanoseconds())
        }
        cands = append(cands, candidate{idx: i, rtt: rtt})
    }
    p.wsConnsMu.RUnlock()
    if len(cands) == 0 {
        return nil, 0, 0, fmt.Errorf("无可用通道")
    }
    minRTT := cands[0].rtt
    for _, c := range cands[1:] {
        if c.rtt < minRTT {
            minRTT = c.rtt
        }
    }
    tieWindow := int64((10 * time.Millisecond).Nanoseconds())
    near := make([]candidate, 0, len(cands))
    for _, c := range cands {
        if c.rtt <= minRTT+tieWindow {
            near = append(near, c)
        }
    }
    pick := int(atomic.AddUint64(&p.selectCounter, 1)-1) % len(near)
    best := near[pick]
    p.wsConnsMu.RLock()
    sess := p.smuxConns[best.idx]
    p.wsConnsMu.RUnlock()
    if sess == nil || sess.IsClosed() {
        return nil, 0, 0, fmt.Errorf("通道不可用")
    }
    decision := best.idx + 1
    s, err := sess.OpenStream()
    if err != nil {
        return nil, 0, 0, err
    }
    return s, best.idx + 1, decision, nil
}

func (p *ECHPool) openTCPStream(target string) (*smux.Stream, int, int, error) {
    s, chID, decision, err := p.openBestStream()
    if err != nil {
        return nil, 0, 0, err
    }
    if err := writeSmuxOpenHeader(s, constants.StreamKindTCP, conf.IPStrategy, target); err != nil {
        _ = s.Close()
        return nil, 0, 0, err
    }
    return s, chID, decision, nil
}

func (p *ECHPool) openUDPStream(target string) (*smux.Stream, int, int, error) {
    s, chID, decision, err := p.openBestStream()
    if err != nil {
        return nil, 0, 0, err
    }
    if err := writeSmuxOpenHeader(s, constants.StreamKindUDP, conf.IPStrategy, target); err != nil {
        _ = s.Close()
        return nil, 0, 0, err
    }
    return s, chID, decision, nil
}

func writeSmuxOpenHeader(w io.Writer, kind byte, strategy byte, target string) error {
    if len(target) > 65535 {
        return fmt.Errorf("目标地址过长")
    }
    head := make([]byte, 4)
    head[0] = kind
    head[1] = strategy
    binary.BigEndian.PutUint16(head[2:4], uint16(len(target)))
    if _, err := w.Write(head); err != nil {
        return err
    }
    if len(target) == 0 {
        return nil
    }
    _, err := w.Write([]byte(target))
    return err
}

func writeChunk(w io.Writer, b []byte) error {
    if len(b) > 65535 {
        return fmt.Errorf("数据块过大")
    }
    h := make([]byte, 2)
    binary.BigEndian.PutUint16(h, uint16(len(b)))
    if _, err := w.Write(h); err != nil {
        return err
    }
    if len(b) == 0 {
        return nil
    }
    _, err := w.Write(b)
    return err
}

func readChunk(r io.Reader) ([]byte, error) {
    h := make([]byte, 2)
    if _, err := io.ReadFull(r, h); err != nil {
        return nil, err
    }
    n := int(binary.BigEndian.Uint16(h))
    if n == 0 {
        return nil, nil
    }
    b := make([]byte, n)
    _, err := io.ReadFull(r, b)
    return b, err
}

func writeUDPReply(w io.Writer, addr string, payload []byte) error {
    if len(addr) > 65535 {
        return fmt.Errorf("地址过长")
    }
    head := make([]byte, 4)
    binary.BigEndian.PutUint16(head[0:2], uint16(len(addr)))
    binary.BigEndian.PutUint16(head[2:4], uint16(len(payload)))
    if _, err := w.Write(head); err != nil {
        return err
    }
    if len(addr) > 0 {
        if _, err := w.Write([]byte(addr)); err != nil {
            return err
        }
    }
    if len(payload) > 0 {
        if _, err := w.Write(payload); err != nil {
            return err
        }
    }
    return nil
}

func readUDPReply(r io.Reader) (string, []byte, error) {
    head := make([]byte, 4)
    if _, err := io.ReadFull(r, head); err != nil {
        return "", nil, err
    }
    addrLen := int(binary.BigEndian.Uint16(head[0:2]))
    dataLen := int(binary.BigEndian.Uint16(head[2:4]))
    addrRaw := make([]byte, addrLen)
    if addrLen > 0 {
        if _, err := io.ReadFull(r, addrRaw); err != nil {
            return "", nil, err
        }
    }
    data := make([]byte, dataLen)
    if dataLen > 0 {
        if _, err := io.ReadFull(r, data); err != nil {
            return "", nil, err
        }
    }
    return string(addrRaw), data, nil
}

func dialWS(addr string, ip string, clientID string, channelID int) (*websocket.Conn, error) {
    u, err := url.Parse(addr)
    if err != nil {
        return nil, err
    }
    scheme := strings.ToLower(u.Scheme)
    if scheme != "ws" && scheme != "wss" {
        return nil, fmt.Errorf("仅支持 ws 或 wss")
    }

    q := u.Query()
    if clientID != "" {
        q.Set("client_id", clientID)
    }
    if channelID > 0 {
        q.Set("channel_id", strconv.Itoa(channelID))
    }
    u.RawQuery = q.Encode()
    dialAddr := u.String()

    dialer := websocket.Dialer{
        HandshakeTimeout: conf.DialTimeout,
        ReadBufferSize:   conf.ReadBuf,
        WriteBufferSize:  conf.ReadBuf,
    }
    if conf.Token != "" {
        dialer.Subprotocols = []string{conf.Token}
    }

    if scheme == "wss" {
        serverName := u.Hostname()
        var echList []byte
        if !conf.Fallback {
            echList, _ = transport.GetECHList()
        }
        utlsConfig := &utls.Config{
            ServerName:         serverName,
            InsecureSkipVerify: conf.Insecure,
            MinVersion:         tls.VersionTLS13,
        }
        if len(echList) > 0 {
            utlsConfig.EncryptedClientHelloConfigList = echList
        }
        clientHelloID := transport.GetClientHelloID(conf.UtlsFingerprint)
        if clientHelloID == nil {
            clientHelloID = &utls.HelloChrome_120
        }

        u.Scheme = "ws"
        dialAddr = u.String()

        dialer.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
            host, port, _ := net.SplitHostPort(address)
            if ip != "" {
                host = ip
            } else if conf.IPOverride != "" {
                host = conf.IPOverride
            }
            tcpConn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), conf.DialTimeout)
            if err != nil {
                return nil, err
            }
            uConn := utls.UClient(tcpConn, utlsConfig, *clientHelloID)
            if err := uConn.Handshake(); err != nil {
                tcpConn.Close()
                return nil, err
            }
            return uConn, nil
        }

        conn, resp, err := dialer.Dial(dialAddr, nil)
        if err != nil {
            if resp != nil && resp.StatusCode == http.StatusUnauthorized {
                return nil, fmt.Errorf("认证失败：Token 不匹配")
            }
            return nil, err
        }
        return conn, nil
    }

    return dialer.Dial(dialAddr, nil)
}

func connectLoop() {
    for {
        sess := currentSession
        if sess != nil && sess.GetID() == conf.UUID {
            time.Sleep(5 * time.Second)
            continue
        }
        if pool == nil {
            time.Sleep(1 * time.Second)
            continue
        }
        wsConn, err := dialWS(conf.ServerAddr, "", conf.UUID, 0)
        if err != nil {
            log.Printf("[客户端] 主连接失败: %v", err)
            time.Sleep(5 * time.Second)
            continue
        }
        wsNet := transport.NewWSNetConn(wsConn, conf.UpPack, conf.ChunkMs, conf.MaxQueueBytes)
        smuxSess, err := smux.Client(wsNet, nil)
        if err != nil {
            wsConn.Close()
            log.Printf("[客户端] smux 初始化失败: %v", err)
            time.Sleep(5 * time.Second)
            continue
        }
        if err := doHandshake(smuxSess); err != nil {
            smuxSess.Close()
            wsConn.Close()
            log.Printf("[客户端] 握手失败: %v", err)
            time.Sleep(5 * time.Second)
            continue
        }
        sess = session.NewClientSession(wsConn, smuxSess, conf.UUID)
        setClientSession(sess)
        log.Printf("[客户端] 主会话建立")
        go keepAlive(sess)
        go acceptStreams(sess)
        <-sess.Done()
        setClientSession(nil)
        log.Printf("[客户端] 主会话断开，重连...")
        time.Sleep(3 * time.Second)
    }
}

func doHandshake(sess *smux.Session) error {
    stream, err := sess.OpenStream()
    if err != nil {
        return err
    }
    defer stream.Close()
    uuid := transport.GetUUIDBytes()
    if err := protocol.WriteHandshakeRequest(stream, uuid, conf.DialTimeout); err != nil {
        return err
    }
    code, err := protocol.ReadHandshakeResponse(stream, conf.DialTimeout)
    if err != nil {
        return err
    }
    if code != constants.HandshakeOK {
        return fmt.Errorf("handshake failed: code %d", code)
    }
    return nil
}

func keepAlive(sess *session.ClientSession) {
    ticker := time.NewTicker(conf.KeepAlive)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            stream, err := sess.smuxSess.OpenStream()
            if err != nil {
                sess.Close()
                return
            }
            header := []byte{constants.StreamKindPing, 0, 0, 0}
            stream.Write(header)
            stream.Write(make([]byte, 8))
            stream.Close()
        case <-sess.Done():
            return
        }
    }
}

func acceptStreams(sess *session.ClientSession) {
    for {
        stream, err := sess.smuxSess.AcceptStream()
        if err != nil {
            sess.Close()
            return
        }
        ch := session.NewWSChannel(sess, stream, uint32(time.Now().UnixNano()))
        sess.AddChannel(ch)
        go func() {
            relayCfg := &relay.Config{
                DialTimeout: conf.DialTimeout,
                ReadBuf:     conf.ReadBuf,
                Concur:      conf.Concur,
            }
            relay.HandleSmuxStream(sess, ch, stream, relayCfg)
            ch.Close()
        }()
    }
}

func setClientSession(s *session.ClientSession) {
    sessionMu.Lock()
    defer sessionMu.Unlock()
    currentSession = s
}

func getClientSession() *session.ClientSession {
    sessionMu.Lock()
    defer sessionMu.Unlock()
    return currentSession
}

func clientSourceAddr(c net.Conn) string {
    if ra := c.RemoteAddr(); ra != nil {
        return ra.String()
    }
    return "-"
}

func logClientConnEvent(c net.Conn, reqType, target string, chID int, opened bool) {
    arrow := "关闭"
    if opened {
        arrow = "打开"
    }
    log.Printf("[客户端] %s %s %s %s 通道 %d", clientSourceAddr(c), reqType, arrow, target, chID)
}

// ---- TCP Listener ----
func runTCPListener(rule string) {
    rule = strings.TrimPrefix(rule, "tcp://")
    parts := strings.Split(rule, "/")
    if len(parts) != 2 {
        return
    }
    lAddr, tAddr := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
    l, err := net.Listen("tcp", lAddr)
    if err != nil {
        log.Fatalf("[客户端] TCP监听失败: %v", err)
    }
    log.Printf("[客户端] TCP转发: %s -> %s", lAddr, tAddr)
    for {
        c, err := l.Accept()
        if err != nil {
            continue
        }
        go handleLocalTCP(c, tAddr)
    }
}

func handleLocalTCP(c net.Conn, target string) {
    stream, chID, decision, err := pool.openTCPStream(target)
    if err != nil {
        _ = c.Close()
        return
    }
    logClientConnEvent(c, "TCP转发", target, decision, true)
    defer logClientConnEvent(c, "TCP转发", target, decision, false)
    proxyConn(c, stream)
}

func proxyConn(local net.Conn, remote net.Conn) {
    done := make(chan struct{}, 2)
    go func() {
        _, _ = io.Copy(remote, local)
        remote.Close()
        local.Close()
        done <- struct{}{}
    }()
    go func() {
        _, _ = io.Copy(local, remote)
        remote.Close()
        local.Close()
        done <- struct{}{}
    }()
    <-done
    <-done
}

// ---- SOCKS5 ----
type ProxyConfig struct {
    Username, Password, Host string
}

func runSOCKS5(addr string) {
    if addr == "" {
        return
    }
    h, u, p, err := parseAuthAndAddr(strings.TrimPrefix(addr, "socks5://"))
    if err != nil {
        log.Fatalf("[客户端] SOCKS5地址解析失败: %v", err)
    }
    l, err := net.Listen("tcp", h)
    if err != nil {
        log.Fatalf("[客户端] SOCKS5监听失败: %v", err)
    }
    log.Printf("[客户端] SOCKS5 代理: %s", h)
    cfgp := &ProxyConfig{u, p, h}
    for {
        c, err := l.Accept()
        if err != nil {
            continue
        }
        go handleSOCKS5(c, cfgp)
    }
}

func parseAuthAndAddr(full string) (string, string, string, error) {
    u, p, h := "", "", full
    if strings.Contains(full, "@") {
        parts := strings.SplitN(full, "@", 2)
        if len(parts) != 2 {
            return "", "", "", fmt.Errorf("格式错误")
        }
        auth := parts[0]
        if strings.Contains(auth, ":") {
            ap := strings.SplitN(auth, ":", 2)
            u, p = ap[0], ap[1]
        }
        h = parts[1]
    }
    return h, u, p, nil
}

func handleSOCKS5(c net.Conn, cfgp *ProxyConfig) {
    defer c.Close()
    _ = c.SetDeadline(time.Now().Add(conf.DialTimeout))
    buf := make([]byte, 2)
    if _, err := io.ReadFull(c, buf); err != nil || buf[0] != 0x05 {
        return
    }
    methods := make([]byte, buf[1])
    _, _ = io.ReadFull(c, methods)
    if cfgp.Username != "" {
        _, _ = c.Write([]byte{0x05, 0x02})
        if err := handleSOCKS5UserPassAuth(c, cfgp); err != nil {
            return
        }
    } else {
        _, _ = c.Write([]byte{0x05, 0x00})
    }

    head := make([]byte, 4)
    if _, err := io.ReadFull(c, head); err != nil {
        return
    }
    var target string
    switch head[3] {
    case 0x01:
        b := make([]byte, 4)
        _, _ = io.ReadFull(c, b)
        target = net.IP(b).String()
    case 0x03:
        b := make([]byte, 1)
        _, _ = io.ReadFull(c, b)
        addr := make([]byte, b[0])
        _, _ = io.ReadFull(c, addr)
        target = string(addr)
    case 0x04:
        b := make([]byte, 16)
        _, _ = io.ReadFull(c, b)
        target = net.IP(b).String()
    }
    pb := make([]byte, 2)
    _, _ = io.ReadFull(c, pb)
    port := int(pb[0])<<8 | int(pb[1])
    target = net.JoinHostPort(target, fmt.Sprintf("%d", port))

    // IP策略过滤
    host, _, _ := net.SplitHostPort(target)
    ip := net.ParseIP(host)
    if conf.IPStrategy == constants.IPStrategyIPv4Only {
        if head[3] == 0x04 || (ip != nil && ip.To4() == nil) {
            _, _ = c.Write([]byte{0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
            return
        }
    }
    if conf.IPStrategy == constants.IPStrategyIPv6Only {
        if head[3] == 0x01 || (ip != nil && ip.To4() != nil) {
            _, _ = c.Write([]byte{0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
            return
        }
    }

    _ = c.SetDeadline(time.Time{})

    switch head[1] {
    case 0x01:
        handleSOCKS5Connect(c, target)
    case 0x03:
        handleSOCKS5UDP(c, cfgp)
    }
}

func handleSOCKS5UserPassAuth(c net.Conn, cfgp *ProxyConfig) error {
    b := make([]byte, 2)
    _, _ = io.ReadFull(c, b)
    u := make([]byte, b[1])
    _, _ = io.ReadFull(c, u)
    _, _ = io.ReadFull(c, b[:1])
    p := make([]byte, b[0])
    _, _ = io.ReadFull(c, p)
    if string(u) == cfgp.Username && string(p) == cfgp.Password {
        _, _ = c.Write([]byte{0x01, 0x00})
        return nil
    }
    _, _ = c.Write([]byte{0x01, 0x01})
    return errors.New("认证失败")
}

func handleSOCKS5Connect(c net.Conn, target string) {
    _, err := c.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
    if err != nil {
        _ = c.Close()
        return
    }
    stream, _, decision, err := pool.openTCPStream(target)
    if err != nil {
        _ = c.Close()
        return
    }
    logClientConnEvent(c, "SOCKS5", target, decision, true)
    defer logClientConnEvent(c, "SOCKS5", target, decision, false)
    proxyConn(c, stream)
}

func handleSOCKS5UDP(c net.Conn, cfgp *ProxyConfig) {
    host, _, _ := net.SplitHostPort(cfgp.Host)
    uAddr, _ := net.ResolveUDPAddr("udp", net.JoinHostPort(host, "0"))
    ul, err := net.ListenUDP("udp", uAddr)
    if err != nil {
        _ = c.Close()
        return
    }
    defer ul.Close()

    actual, ok := ul.LocalAddr().(*net.UDPAddr)
    if !ok || actual == nil {
        _ = c.Close()
        return
    }
    resp := []byte{0x05, 0x00, 0x00}
    if ip4 := actual.IP.To4(); ip4 != nil {
        resp = append(resp, 0x01)
        resp = append(resp, ip4...)
    } else {
        resp = append(resp, 0x04)
        resp = append(resp, actual.IP...)
    }
    resp = append(resp, byte(actual.Port>>8), byte(actual.Port))
    if _, err := c.Write(resp); err != nil {
        _ = c.Close()
        return
    }

    assoc := &UDPAssociation{
        tcpConn:     c,
        udpListener: ul,
        pool:        pool,
        channelID:   -1,
    }
    go assoc.loop()
    b := make([]byte, 1)
    for {
        if _, err := c.Read(b); err != nil {
            assoc.Close()
            return
        }
    }
}

type UDPAssociation struct {
    tcpConn       net.Conn
    udpListener   *net.UDPConn
    clientUDPAddr *net.UDPAddr
    pool          *ECHPool

    mu        sync.Mutex
    closed    bool
    receiving bool
    channelID int
    target    string
    stream    *smux.Stream
}

func (a *UDPAssociation) loop() {
    bufPtr := transport.BufPool.Get().(*[]byte)
    buf := *bufPtr
    defer transport.BufPool.Put(bufPtr)

    for {
        n, addr, err := a.udpListener.ReadFromUDP(buf)
        if err != nil {
            return
        }
        a.mu.Lock()
        if a.clientUDPAddr == nil {
            a.clientUDPAddr = addr
        } else if a.clientUDPAddr.String() != addr.String() {
            a.mu.Unlock()
            continue
        }
        a.mu.Unlock()

        tgt, data, err := parseSOCKS5UDPPacket(buf[:n])
        if err == nil {
            h, ps, _ := net.SplitHostPort(tgt)
            if ip := net.ParseIP(h); ip != nil {
                if conf.IPStrategy == constants.IPStrategyIPv4Only && ip.To4() == nil {
                    continue
                }
                if conf.IPStrategy == constants.IPStrategyIPv6Only && ip.To4() != nil {
                    continue
                }
            }
            a.send(tgt, data)
        }
    }
}

func (a *UDPAssociation) send(target string, data []byte) {
    a.mu.Lock()
    if a.closed {
        a.mu.Unlock()
        return
    }
    needStart := !a.receiving
    if needStart {
        a.receiving = true
        a.target = target
    }
    stream := a.stream
    a.mu.Unlock()

    if needStart {
        s, id, decision, err := a.pool.openUDPStream(target)
        if err != nil {
            a.Close()
            return
        }
        a.mu.Lock()
        a.stream = s
        a.channelID = id
        stream = s
        a.mu.Unlock()
        logClientConnEvent(a.tcpConn, "SOCKS5-UDP", target, decision, true)
        go func() {
            for {
                addrStr, payload, e := readUDPReply(s)
                if e != nil {
                    a.Close()
                    return
                }
                a.handleUDPResponse(addrStr, payload)
            }
        }()
    } else {
        if target != "" && target != a.target {
            a.mu.Lock()
            a.target = target
            a.mu.Unlock()
        }
    }
    if stream == nil {
        a.Close()
        return
    }
    if err := writeChunk(stream, data); err != nil {
        a.Close()
    }
}

func (a *UDPAssociation) handleUDPResponse(addrStr string, data []byte) {
    host, portStr, _ := net.SplitHostPort(addrStr)
    port := 0
    fmt.Sscanf(portStr, "%d", &port)
    pkt, _ := buildSOCKS5UDPPacket(host, port, data)
    a.mu.Lock()
    defer a.mu.Unlock()
    if a.clientUDPAddr != nil {
        _, _ = a.udpListener.WriteToUDP(pkt, a.clientUDPAddr)
    }
}

func (a *UDPAssociation) Close() {
    a.mu.Lock()
    if a.closed {
        a.mu.Unlock()
        return
    }
    stream := a.stream
    target := a.target
    chID := a.channelID
    a.closed = true
    a.stream = nil
    a.mu.Unlock()
    if stream != nil {
        _ = stream.Close()
    }
    if chID > 0 && target != "" {
        logClientConnEvent(a.tcpConn, "SOCKS5-UDP", target, chID, false)
    }
    _ = a.udpListener.Close()
    if a.tcpConn != nil {
        _ = a.tcpConn.Close()
    }
}

func parseSOCKS5UDPPacket(b []byte) (string, []byte, error) {
    if len(b) < 10 || b[2] != 0 {
        return "", nil, errors.New("数据不合法")
    }
    off := 4
    var h string
    switch b[3] {
    case 0x01:
        if off+4 > len(b) {
            return "", nil, errors.New("IPv4地址长度过短")
        }
        h = net.IP(b[off : off+4]).String()
        off += 4
    case 0x03:
        if off+1 > len(b) {
            return "", nil, errors.New("域名长度不足")
        }
        l := int(b[off])
        off++
        if off+l > len(b) {
            return "", nil, errors.New("域名长度不足")
        }
        h = string(b[off : off+l])
        off += l
    case 0x04:
        if off+16 > len(b) {
            return "", nil, errors.New("IPv6地址长度过短")
        }
        h = net.IP(b[off : off+16]).String()
        off += 16
    default:
        return "", nil, errors.New("地址类型无效")
    }
    if off+2 > len(b) {
        return "", nil, errors.New("端口字段过短")
    }
    p := int(b[off])<<8 | int(b[off+1])
    off += 2
    t := fmt.Sprintf("%s:%d", h, p)
    if b[3] == 0x04 {
        t = fmt.Sprintf("[%s]:%d", h, p)
    }
    return t, b[off:], nil
}

func buildSOCKS5UDPPacket(h string, p int, d []byte) ([]byte, error) {
    buf := []byte{0, 0, 0}
    ip := net.ParseIP(h)
    if ip4 := ip.To4(); ip4 != nil {
        buf = append(buf, 0x01)
        buf = append(buf, ip4...)
    } else if ip != nil {
        buf = append(buf, 0x04)
        buf = append(buf, ip...)
    } else {
        buf = append(buf, 0x03, byte(len(h)))
        buf = append(buf, h...)
    }
    buf = append(buf, byte(p>>8), byte(p))
    buf = append(buf, d...)
    return buf, nil
}

// ---- HTTP ----
func runHTTP(addr string) {
    if addr == "" {
        return
    }
    h, u, p, _ := parseAuthAndAddr(strings.TrimPrefix(addr, "http://"))
    l, err := net.Listen("tcp", h)
    if err != nil {
        log.Fatalf("[客户端] HTTP监听失败: %v", err)
    }
    log.Printf("[客户端] HTTP 代理: %s", h)
    cfgp := &ProxyConfig{u, p, h}
    for {
        c, err := l.Accept()
        if err != nil {
            continue
        }
        go handleHTTP(c, cfgp)
    }
}

func handleHTTP(c net.Conn, cfgp *ProxyConfig) {
    defer c.Close()
    _ = c.SetDeadline(time.Now().Add(conf.DialTimeout))
    br := bufio.NewReader(c)
    req, err := http.ReadRequest(br)
    if err != nil {
        return
    }
    _ = c.SetDeadline(time.Time{})
    if cfgp.Username != "" {
        auth := req.Header.Get("Proxy-Authorization")
        ok := false
        if strings.HasPrefix(auth, "Basic ") {
            p, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(auth, "Basic "))
            pair := strings.SplitN(string(p), ":", 2)
            if len(pair) == 2 && pair[0] == cfgp.Username && pair[1] == cfgp.Password {
                ok = true
            }
        }
        if !ok {
            _, _ = c.Write([]byte("HTTP/1.1 407 需要认证\r\nProxy-Authenticate: Basic realm=\"代理\"\r\n\r\n"))
            return
        }
    }

    target := req.Host
    if !strings.Contains(target, ":") {
        if req.Method == "CONNECT" {
            target += ":443"
        } else {
            target += ":80"
        }
    }

    var first []byte

    if req.Method == "CONNECT" {
        _, _ = c.Write([]byte("HTTP/1.1 200 连接已建立\r\n\r\n"))
    } else {
        req.RequestURI = ""
        req.URL.Scheme = ""
        req.URL.Host = ""
        var buf bytes.Buffer
        _ = req.Write(&buf)
        first = buf.Bytes()
    }

    stream, _, decision, err := pool.openTCPStream(target)
    if err != nil {
        return
    }
    if len(first) > 0 {
        if _, err := stream.Write(first); err != nil {
            _ = stream.Close()
            return
        }
    }
    logClientConnEvent(c, "HTTP", target, decision, true)
    defer logClientConnEvent(c, "HTTP", target, decision, false)
    proxyConn(c, stream)
}
