package relay

import (
    "io"
    "net"
    "time"

    "github.com/xtaci/smux"

    "proxy/internal/constants"
    "proxy/internal/pool"
    "proxy/internal/protocol"
    "proxy/internal/session"
    "proxy/internal/transport"
)

func HandleSmuxStream(sess *session.ClientSession, ch *session.WSChannel, stream *smux.Stream, cfg *Config) {
    defer stream.Close()

    kind, strategy, target, err := protocol.ReadSmuxOpenHeader(stream)
    if err != nil {
        return
    }

    switch kind {
    case constants.StreamKindPing:
        payload := make([]byte, 8)
        if _, err := io.ReadFull(stream, payload); err != nil {
            return
        }
        _, _ = stream.Write(payload)

    case constants.StreamKindTCP:
        dataCh := make(chan []byte, 256)
        errCh := make(chan error, 2)
        doneCh := make(chan struct{})

        go func() {
            defer close(doneCh)
            buf := make([]byte, cfg.ReadBuf)
            for {
                n, err := stream.Read(buf)
                if err != nil {
                    errCh <- err
                    return
                }
                if n > 0 {
                    data := make([]byte, n)
                    copy(data, buf[:n])
                    select {
                    case dataCh <- data:
                    case <-time.After(5 * time.Second):
                        errCh <- io.ErrTimeout
                        return
                    }
                }
            }
        }()

        dialCh := make(chan net.Conn, 1)
        go func() {
            conn, err := DialTCPWithRace(target, strategy, cfg.Concur, cfg.DialTimeout)
            if err != nil {
                errCh <- err
                return
            }
            dialCh <- conn
        }()

        var targetConn net.Conn
        var dialErr error

        select {
        case conn := <-dialCh:
            targetConn = conn
        case err := <-errCh:
            dialErr = err
        case <-time.After(cfg.DialTimeout):
            dialErr = io.ErrTimeout
        }

        if dialErr != nil {
            stream.Close()
            return
        }
        defer targetConn.Close()

        for {
            select {
            case data := <-dataCh:
                if _, err := targetConn.Write(data); err != nil {
                    return
                }
            default:
                goto forward
            }
        }

    forward:
        go func() {
            defer stream.Close()
            buf := make([]byte, cfg.ReadBuf)
            for {
                n, err := targetConn.Read(buf)
                if err != nil {
                    return
                }
                if n > 0 {
                    if _, err := stream.Write(buf[:n]); err != nil {
                        return
                    }
                }
            }
        }()

        for {
            select {
            case data, ok := <-dataCh:
                if !ok {
                    return
                }
                if _, err := targetConn.Write(data); err != nil {
                    return
                }
            case err := <-errCh:
                if err != nil {
                    return
                }
            case <-doneCh:
                return
            }
        }

    case constants.StreamKindUDP:
        relay, err := NewDirectUDPRelayer()
        if err != nil {
            return
        }
        defer relay.Close()
        done := make(chan struct{})
        go func() {
            defer close(done)
            for {
                addr, data, e := protocol.ReadUDPChunk(stream)
                if e != nil {
                    return
                }
                if len(data) == 0 {
                    continue
                }
                udpAddr, err := resolveUDPAddr(addr, strategy, cfg.DialTimeout)
                if err != nil {
                    continue
                }
                if _, e = relay.WriteTo(data, udpAddr); e != nil {
                    return
                }
            }
        }()
        bufPtr := pool.BufPool.Get().(*[]byte)
        buf := *bufPtr
        defer pool.BufPool.Put(bufPtr)
        for {
            relay.SetReadDeadline(time.Now().Add(1 * time.Second))
            n, addr, e := relay.Read(buf)
            if e != nil {
                if netErr, ok := e.(net.Error); ok && netErr.Timeout() {
                    select {
                    case <-done:
                        return
                    default:
                        continue
                    }
                }
                return
            }
            if err := protocol.WriteUDPChunk(stream, addr.String(), buf[:n]); err != nil {
                return
            }
        }
    }
}

type Config struct {
    DialTimeout time.Duration
    ReadBuf     int
    Concur      int
}
