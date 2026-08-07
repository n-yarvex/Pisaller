package main

import (
    "bytes"
    "context"
    "log"
    "net/http"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "time"

    "github.com/gorilla/websocket"
    "github.com/xtaci/smux"

    "proxy/internal/config"
    "proxy/internal/constants"
    "proxy/internal/protocol"
    "proxy/internal/relay"
    "proxy/internal/session"
    "proxy/internal/transport"
)

var upgrader = websocket.Upgrader{
    CheckOrigin:     func(r *http.Request) bool {
        origin := r.Header.Get("Origin")
        return origin == "" || origin == "http://"+r.Host || origin == "https://"+r.Host
    },
    ReadBufferSize:  config.ServerConf.ReadBuf,
    WriteBufferSize: config.ServerConf.ReadBuf,
}

var sessions sync.Map

func main() {
    if len(os.Args) < 2 {
        log.Fatalf("Usage: %s <config.json>", os.Args[0])
    }
    if err := config.LoadServerConfig(os.Args[1]); err != nil {
        log.Fatalf("load config: %v", err)
    }
    conf := config.ServerConf

    for _, uuid := range conf.AuthUUIDs {
        if err := transport.InitUUID(uuid); err != nil {
            log.Printf("init uuid %s: %v", uuid, err)
        }
    }

    http.HandleFunc("/ws", handleWebSocket)

    srv := &http.Server{
        Addr:         conf.ListenAddr,
        ReadTimeout:  conf.DialTimeout,
        WriteTimeout: conf.DialTimeout,
    }

    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        log.Printf("server listening on %s", conf.ListenAddr)
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("http server: %v", err)
        }
    }()

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    if err := srv.Shutdown(ctx); err != nil {
        log.Printf("server shutdown error: %v", err)
    }
    wg.Wait()
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
    ws, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        log.Printf("upgrade: %v", err)
        return
    }
    wsNetConn := transport.NewWSNetConn(ws, config.ServerConf.UpPack, config.ServerConf.ChunkMs, config.ServerConf.MaxQueueBytes)
    smuxSess, err := smux.Server(wsNetConn, nil)
    if err != nil {
        ws.Close()
        return
    }

    stream, err := smuxSess.AcceptStream()
    if err != nil {
        smuxSess.Close()
        ws.Close()
        return
    }
    defer stream.Close()

    uuid, err := protocol.ReadHandshakeRequest(stream, config.ServerConf.DialTimeout)
    if err != nil {
        smuxSess.Close()
        ws.Close()
        return
    }
    allowed := false
    for _, bin := range config.ServerConf.AuthUUIDBin {
        if bytes.Equal(uuid, bin) {
            allowed = true
            break
        }
    }
    if !allowed {
        if err := protocol.WriteHandshakeResponse(stream, constants.HandshakeFail, config.ServerConf.DialTimeout); err != nil {
            log.Printf("write handshake response: %v", err)
        }
        smuxSess.Close()
        ws.Close()
        return
    }
    if err := protocol.WriteHandshakeResponse(stream, constants.HandshakeOK, config.ServerConf.DialTimeout); err != nil {
        log.Printf("write handshake response: %v", err)
        smuxSess.Close()
        ws.Close()
        return
    }

    clientID := string(uuid)
    sess := session.NewClientSession(ws, smuxSess, clientID)
    sessions.Store(clientID, sess)

    log.Printf("client %s connected", clientID)

    go acceptStreams(sess)

    <-sess.Done()
    sessions.Delete(clientID)
    log.Printf("client %s disconnected", clientID)
}

func acceptStreams(sess *session.ClientSession) {
    relayCfg := &relay.Config{
        DialTimeout: config.ServerConf.DialTimeout,
        ReadBuf:     config.ServerConf.ReadBuf,
        Concur:      config.ServerConf.Concur,
    }
    for {
        stream, err := sess.AcceptStream()
        if err != nil {
            sess.Close()
            return
        }
        ch := session.NewWSChannel(sess, stream, uint32(time.Now().UnixNano()))
        sess.AddChannel(ch)
        go func() {
            defer ch.Close()
            relay.HandleSmuxStream(sess, ch, stream, relayCfg)
        }()
    }
}
