package transport

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtaci/smux"
)

type DnAggregator struct {
	stream   *smux.Stream
	mu       sync.Mutex
	cond     *sync.Cond
	queue    [][]byte
	total    int
	dnPack   int
	dnTail   int
	dnQr     int
	closed   int32
	stopCh   chan struct{}
	wg       sync.WaitGroup
	maxQueue int
	chunkMs  int
}

func NewDnAggregator(stream *smux.Stream, dnPack, dnTail, dnQr, maxQueue int) *DnAggregator {
	if dnPack <= 0 {
		dnPack = 32768
	}
	if dnTail <= 0 {
		dnTail = 512
	}
	if dnQr <= 0 {
		dnQr = 4
	}
	if maxQueue <= 0 {
		maxQueue = dnPack * 4
	}
	a := &DnAggregator{
		stream:   stream,
		dnPack:   dnPack,
		dnTail:   dnTail,
		dnQr:     dnQr,
		stopCh:   make(chan struct{}),
		maxQueue: maxQueue,
		chunkMs:  20,
	}
	a.cond = sync.NewCond(&a.mu)
	a.wg.Add(1)
	go a.flusher()
	return a
}

func (a *DnAggregator) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	a.mu.Lock()
	if a.closed != 0 {
		a.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if a.total+len(data) > a.maxQueue {
		if err := a.flushLocked(); err != nil {
			a.mu.Unlock()
			return 0, err
		}
		if len(data) >= a.dnPack {
			a.mu.Unlock()
			return a.writeDirect(data)
		}
		if len(data) > a.maxQueue {
			a.mu.Unlock()
			return a.writeDirect(data)
		}
	}
	d := make([]byte, len(data))
	copy(d, data)
	a.queue = append(a.queue, d)
	a.total += len(d)
	shouldSignal := a.total >= a.dnPack || len(a.queue) >= a.dnQr
	a.mu.Unlock()
	if shouldSignal {
		a.cond.Signal()
	}
	return len(data), nil
}

func (a *DnAggregator) Close() error {
	if !atomic.CompareAndSwapInt32(&a.closed, 0, 1) {
		return nil
	}
	close(a.stopCh)
	a.cond.Signal()
	a.wg.Wait()
	return nil
}

func (a *DnAggregator) flusher() {
	defer a.wg.Done()
	var timer *time.Timer
	var timerCh <-chan time.Time
	qr := 0
	low := a.dnTail * 12
	if low < 4096 {
		low = 4096
	}
	tail := a.dnTail
	chunkMs := a.chunkMs
	for {
		a.mu.Lock()
		for a.closed == 0 && len(a.queue) == 0 {
			a.cond.Wait()
		}
		if a.closed != 0 && len(a.queue) == 0 {
			a.mu.Unlock()
			return
		}
		if len(a.queue) == 0 {
			a.mu.Unlock()
			continue
		}
		if a.total >= a.dnPack || len(a.queue) >= a.dnQr {
			queueCopy := a.copyQueueLocked()
			a.mu.Unlock()
			a.writeQueue(queueCopy)
			qr = 0
			continue
		}
		if a.dnPack-a.total < tail {
			queueCopy := a.copyQueueLocked()
			a.mu.Unlock()
			a.writeQueue(queueCopy)
			qr = 0
			continue
		}
		if timer == nil {
			timer = time.NewTimer(time.Duration(chunkMs) * time.Millisecond)
			timerCh = timer.C
			qr = 0
		}
		a.mu.Unlock()

		select {
		case <-timerCh:
			timer.Stop()
			timer = nil
			timerCh = nil
			a.mu.Lock()
			if a.closed != 0 {
				if len(a.queue) > 0 {
					queueCopy := a.copyQueueLocked()
					a.mu.Unlock()
					a.writeQueue(queueCopy)
				} else {
					a.mu.Unlock()
				}
				continue
			}
			if len(a.queue) == 0 {
				a.mu.Unlock()
				continue
			}
			if a.total >= a.dnPack || len(a.queue) >= a.dnQr {
				queueCopy := a.copyQueueLocked()
				a.mu.Unlock()
				a.writeQueue(queueCopy)
				qr = 0
				continue
			}
			if a.total >= low || qr >= a.dnQr {
				queueCopy := a.copyQueueLocked()
				a.mu.Unlock()
				a.writeQueue(queueCopy)
				qr = 0
				continue
			}
			qr++
			if qr < a.dnQr {
				a.mu.Unlock()
				continue
			}
			queueCopy := a.copyQueueLocked()
			a.mu.Unlock()
			a.writeQueue(queueCopy)
			qr = 0
		case <-a.stopCh:
			if timer != nil {
				timer.Stop()
				timer = nil
				timerCh = nil
			}
			a.mu.Lock()
			if len(a.queue) > 0 {
				queueCopy := a.copyQueueLocked()
				a.mu.Unlock()
				a.writeQueue(queueCopy)
			} else {
				a.mu.Unlock()
			}
			return
		}
	}
}

func (a *DnAggregator) copyQueueLocked() [][]byte {
	if a.total == 0 {
		return nil
	}
	copyQueue := make([][]byte, len(a.queue))
	copy(copyQueue, a.queue)
	return copyQueue
}

func (a *DnAggregator) writeQueue(queue [][]byte) {
	if len(queue) == 0 {
		return
	}
	totalSize := 0
	for _, chunk := range queue {
		totalSize += len(chunk)
	}
	buf := make([]byte, totalSize)
	off := 0
	for _, chunk := range queue {
		copy(buf[off:], chunk)
		off += len(chunk)
	}
	_, err := a.stream.Write(buf)
	a.mu.Lock()
	if err != nil {
		atomic.StoreInt32(&a.closed, 1)
	}
	if a.closed == 0 {
		a.queue = nil
		a.total = 0
	} else {
		a.queue = nil
		a.total = 0
	}
	a.mu.Unlock()
}

func (a *DnAggregator) writeDirect(p []byte) (int, error) {
	_, err := a.stream.Write(p)
	if err != nil {
		atomic.StoreInt32(&a.closed, 1)
		return 0, err
	}
	return len(p), nil
}

func ProxyConnStream(c net.Conn, stream *smux.Stream, dnPack, dnTail, dnQr, maxQueue int) {
	agg := NewDnAggregator(stream, dnPack, dnTail, dnQr, maxQueue)
	defer agg.Close()

	go func() {
		io.Copy(c, stream)
		c.Close()
	}()

	buf := make([]byte, 64*1024)
	for {
		n, err := c.Read(buf)
		if err != nil {
			break
		}
		if n > 0 {
			_, err := agg.Write(buf[:n])
			if err != nil {
				break
			}
		}
	}
	agg.Close()
	stream.Close()
}
