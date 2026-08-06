package session

import (
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xtaci/smux"
)

type ClientSession struct {
	clientID  string
	wsConn    *websocket.Conn
	smuxSess  *smux.Session
	channels  map[uint32]*WSChannel
	mu        sync.Mutex
	closeOnce sync.Once
	done      chan struct{}
}

type WSChannel struct {
	id         uint32
	session    *ClientSession
	stream     *smux.Stream
	localAddr  net.Addr
	remoteAddr net.Addr
	createdAt  time.Time
}

func NewClientSession(ws *websocket.Conn, sess *smux.Session, clientID string) *ClientSession {
	return &ClientSession{
		clientID: clientID,
		wsConn:   ws,
		smuxSess: sess,
		channels: make(map[uint32]*WSChannel),
		done:     make(chan struct{}),
	}
}

func (s *ClientSession) GetID() string {
	return s.clientID
}

func (s *ClientSession) OpenStream() (*smux.Stream, error) {
	return s.smuxSess.OpenStream()
}

func (s *ClientSession) AcceptStream() (*smux.Stream, error) {
	return s.smuxSess.AcceptStream()
}

func (s *ClientSession) IsClosed() bool {
	return s.smuxSess.IsClosed()
}

func (s *ClientSession) AddChannel(ch *WSChannel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels[ch.id] = ch
}

func (s *ClientSession) RemoveChannel(id uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.channels, id)
}

func (s *ClientSession) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.smuxSess.Close()
		s.wsConn.Close()
	})
}

func (s *ClientSession) Done() <-chan struct{} {
	return s.done
}

func NewWSChannel(sess *ClientSession, stream *smux.Stream, id uint32) *WSChannel {
	return &WSChannel{
		id:        id,
		session:   sess,
		stream:    stream,
		createdAt: time.Now(),
	}
}

func (ch *WSChannel) GetStream() *smux.Stream {
	return ch.stream
}

func (ch *WSChannel) Close() {
	ch.stream.Close()
	ch.session.RemoveChannel(ch.id)
}
