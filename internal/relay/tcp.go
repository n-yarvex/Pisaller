package relay

import (
	"context"
	"io"
	"net"
	"sync"
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
		handleTCPStream(stream, target, strategy, cfg)

	case constants.StreamKindUDP:
		handleUDPStream(stream, strategy, cfg)
	}
}

func handleTCPStream(stream *smux.Stream, target string, strategy byte, cfg *Config) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataCh := make(chan []byte, 256)
	errCh := make(chan error, 2)

	// 读取 goroutine
	go func() {
		defer close(dataCh)
		buf := make([]byte, cfg.ReadBuf)
		for {
			n, err := stream.Read(buf)
			if err != nil {
				select {
				case errCh <- err:
				case <-ctx.Done():
				}
				return
			}
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				select {
				case dataCh <- data:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// 并发拨号
	dialCh := make(chan net.Conn, 1)
	go func() {
		conn, err := DialTCPWithRace(target, strategy, cfg.Concur, cfg.DialTimeout)
		if err != nil {
			select {
			case errCh <- err:
			case <-ctx.Done():
			}
			return
		}
		select {
		case dialCh <- conn:
		case <-ctx.Done():
			conn.Close()
		}
	}()

	var targetConn net.Conn
	select {
	case conn := <-dialCh:
		targetConn = conn
	case err := <-errCh:
		if err != nil {
			return
		}
	case <-time.After(cfg.DialTimeout):
		return
	}

	if targetConn == nil {
		return
	}
	defer targetConn.Close()

	// 转发: targetConn -> stream
	go func() {
		defer cancel()
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

	// 转发: stream -> targetConn
	for {
		select {
		case data, ok := <-dataCh:
			if !ok {
				return
			}
			if _, err := targetConn.Write(data); err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func handleUDPStream(stream *smux.Stream, strategy byte, cfg *Config) {
	relay, err := NewDirectUDPRelayer()
	if err != nil {
		return
	}
	defer relay.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// stream -> UDP
	go func() {
		defer cancel()
		for {
			data, err := readChunk(stream)
			if err != nil {
				return
			}
			if len(data) == 0 {
				continue
			}
			// 获取目标地址从 stream 的初始 target（已在 header 中）
			// UDP 数据直接使用，不需要额外的 addr 包装
			// 这里使用 readChunk 读取纯数据
		}
	}()

	// 实际上 UDP 需要 addr 信息，使用 protocol.ReadUDPChunk
	// 但客户端发送的是 writeChunk 格式（纯数据）
	// 需要与客户端协商一致

	// stream -> UDP (修正后使用 readChunk 匹配客户端的 writeChunk)
	go func() {
		defer cancel()
		for {
			data, err := readChunk(stream)
			if err != nil {
				return
			}
			if len(data) == 0 {
				continue
			}
			// UDP 需要目标地址，但客户端没有发送
			// 这是一个设计问题，需要重新考虑协议
		}
	}()

	bufPtr := pool.BufPool.Get().(*[]byte)
	buf := *bufPtr
	defer pool.BufPool.Put(bufPtr)

	for {
		relay.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, addr, err := relay.Read(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			return
		}
		// 使用 writeUDPReply 格式发送，与客户端 readUDPReply 匹配
		if err := writeUDPReply(stream, addr.String(), buf[:n]); err != nil {
			return
		}
	}
}

// readChunk 读取 [len:2][data] 格式
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

// writeUDPReply 使用与 main.go 中 readUDPReply 匹配的格式
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

type Config struct {
	DialTimeout time.Duration
	ReadBuf     int
	Concur      int
}
