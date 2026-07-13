package diode

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/rs/diode"
)

// Writer is a io.Writer wrapper that uses a diode to buffer writes.
type Writer struct {
	w      io.Writer
	d      diode.Diode
	c      context.CancelFunc
	wg     *sync.WaitGroup
	mux    *sync.RWMutex
	closed *uint32
}

// NewWriter creates a new Writer.
func NewWriter(w io.Writer, size int, pollInterval time.Duration, cb func(missed int)) Writer {
	ctx, cancel := context.WithCancel(context.Background())
	dw := Writer{
		w:      w,
		c:      cancel,
		wg:     &sync.WaitGroup{},
		mux:    &sync.RWMutex{},
		closed: new(uint32),
	}
	dw.d = diode.New(size, diode.Alerter(cb))
	dw.wg.Add(1)
	go dw.poll(ctx, pollInterval)
	return dw
}

// Write implements io.Writer.
func (dw Writer) Write(p []byte) (n int, err error) {
	dw.mux.RLock()
	defer dw.mux.RUnlock()
	if atomic.LoadUint32(dw.closed) == 1 {
		return 0, io.ErrClosedPipe
	}
	// Copy p because diode is async and p can be reused by the caller.
	c := make([]byte, len(p))
	copy(c, p)
	dw.d.Set(unsafe.Pointer(&c))
	return len(p), nil
}

// Close closes the writer.
func (dw Writer) Close() error {
	dw.mux.Lock()
	atomic.StoreUint32(dw.closed, 1)
	dw.mux.Unlock()

	dw.c()
	dw.wg.Wait()
	if c, ok := dw.w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (dw Writer) poll(ctx context.Context, interval time.Duration) {
	defer dw.wg.Done()
	var ticker *time.Ticker
	if interval > 0 {
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}
	for {
		if ticker != nil {
			select {
			case <-ctx.Done():
				dw.drain()
				return
			case <-ticker.C:
			}
		} else {
			select {
			case <-ctx.Done():
				dw.drain()
				return
			default:
			}
		}
		if p := dw.d.Next(); p != nil {
			dw.w.Write(*(*[]byte)(p))
		} else if ticker == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (dw Writer) drain() {
	for {
		p := dw.d.Next()
		if p == nil {
			break
		}
		dw.w.Write(*(*[]byte)(p))
	}
}
