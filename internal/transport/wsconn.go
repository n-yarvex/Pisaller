package transport

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type WSNetConn struct {
	ws       *websocket.Conn
	readMu   sync.Mutex
	writeMu  sync.Mutex
	reader   io.Reader
	deadCh   chan struct{}
	deadOnce sync.Once
	deadErr  error
	deadMu   sync.Mutex
	closed   int32

	upMu     sync.Mutex
	upQueue  [][]byte
	upSize   int
	upPack   int
	chunkMs  int
	timer    *time.Timer
	maxQueue int
}

func NewWSNetConn(ws *websocket.Conn, upPack, chunkMs, maxQueue int) *WSNetConn {
	if upPack <= 0 {
		upPack = 20 * 1024
	}
	if chunkMs <= 0 {
		chunkMs = 20
	}
	if maxQueue <= 0 {
		maxQueue = 128 * 1024
	}
	return &WSNetConn{
		ws:       ws,
		deadCh:   make(chan struct{}),
		upPack:   upPack,
		chunkMs:  chunkMs,
		maxQueue: maxQueue,
	}
}

func (c *WSNetConn) signalDead(err error) {
	if err == nil {
		return
	}
	c.deadMu.Lock()
	if c.deadErr == nil {
		c.deadErr = err
	} else {
		log.Printf("[WSNetConn] additional dead error: %v", err)
	}
	c.deadMu.Unlock()
	c.deadOnce.Do(func() {
		close(c.deadCh)
	})
}

func (c *WSNetConn) Dead() <-chan struct{} { return c.deadCh }

func (c *WSNetConn) DeadErr() error {
	c.deadMu.Lock()
	defer c.deadMu.Unlock()
	return c.deadErr
}

func (c *WSNetConn) isClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

func (c *WSNetConn) Read(p []byte) (int, error) {
	if c.isClosed() {
		return 0, net.ErrClosed
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		if c.reader == nil {
			mt, r, err := c.ws.NextReader()
			if err != nil {
				c.signalDead(err)
				return 0, err
			}
			if mt != websocket.BinaryMessage {
				continue
			}
			c.reader = r
		}
		n, err := c.reader.Read(p)
		if errors.Is(err, io.EOF) {
			c.reader = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		if err != nil {
			c.signalDead(err)
		}
		return n, err
	}
}

func (c *WSNetConn) Write(p []byte) (int, error) {
	if c.isClosed() {
		return 0, net.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}

	c.upMu.Lock()
	defer c.upMu.Unlock()

	if c.upSize+len(p) > c.maxQueue {
		if err := c.flushLocked(); err != nil {
			return 0, err
		}
		if len(p) >= c.upPack {
			return c.writeDirect(p)
		}
		if len(p) > c.maxQueue {
			return c.writeDirect(p)
		}
	}

	data := make([]byte, len(p))
	copy(data, p)
	c.upQueue = append(c.upQueue, data)
	c.upSize += len(data)

	if c.upSize >= c.upPack {
		if err := c.flushLocked(); err != nil {
			return 0, err
		}
	} else if c.chunkMs > 0 {
		if c.timer != nil {
			c.timer.Stop()
		}
		c.timer = time.AfterFunc(time.Duration(c.chunkMs)*time.Millisecond, func() {
			c.upMu.Lock()
			defer c.upMu.Unlock()
			_ = c.flushLocked()
		})
	}
	return len(p), nil
}

func (c *WSNetConn) writeDirect(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	w, err := c.ws.NextWriter(websocket.BinaryMessage)
	if err != nil {
		c.signalDead(err)
		return 0, err
	}
	n, err := w.Write(p)
	if err != nil {
		c.signalDead(err)
		_ = w.Close()
		return n, err
	}
	if err := w.Close(); err != nil {
		c.signalDead(err)
		return n, err
	}
	return n, nil
}

func (c *WSNetConn) flushLocked() error {
	if c.upSize == 0 {
		return nil
	}
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	total := c.upSize
	buf := make([]byte, total)
	off := 0
	for _, chunk := range c.upQueue {
		copy(buf[off:], chunk)
		off += len(chunk)
	}
	c.upQueue = nil
	c.upSize = 0

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.isClosed() {
		return net.ErrClosed
	}
	w, err := c.ws.NextWriter(websocket.BinaryMessage)
	if err != nil {
		c.signalDead(err)
		return err
	}
	_, err = w.Write(buf)
	if err != nil {
		c.signalDead(err)
		_ = w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		c.signalDead(err)
		return err
	}
	return nil
}

func (c *WSNetConn) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return net.ErrClosed
	}
	c.upMu.Lock()
	if err := c.flushLocked(); err != nil {
		c.upMu.Unlock()
		log.Printf("[WSNetConn] flush on close error: %v", err)
		c.signalDead(err)
	} else {
		c.upMu.Unlock()
	}

	err := c.ws.Close()
	if err != nil {
		c.signalDead(err)
	} else {
		c.signalDead(io.EOF)
	}
	return err
}

func (c *WSNetConn) LocalAddr() net.Addr {
	if nc := c.ws.UnderlyingConn(); nc != nil {
		return nc.LocalAddr()
	}
	return &net.TCPAddr{IP: net.IPv4zero, Port: 0}
}

func (c *WSNetConn) RemoteAddr() net.Addr {
	if nc := c.ws.UnderlyingConn(); nc != nil {
		return nc.RemoteAddr()
	}
	return &net.TCPAddr{IP: net.IPv4zero, Port: 0}
}

func (c *WSNetConn) SetDeadline(t time.Time) error {
	if c.isClosed() {
		return net.ErrClosed
	}
	if err := c.ws.SetReadDeadline(t); err != nil {
		return err
	}
	if err := c.ws.SetWriteDeadline(t); err != nil {
		return err
	}
	return nil
}

func (c *WSNetConn) SetReadDeadline(t time.Time) error {
	if c.isClosed() {
		return net.ErrClosed
	}
	return c.ws.SetReadDeadline(t)
}

func (c *WSNetConn) SetWriteDeadline(t time.Time) error {
	if c.isClosed() {
		return net.ErrClosed
	}
	return c.ws.SetWriteDeadline(t)
}
