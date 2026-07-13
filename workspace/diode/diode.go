package diode

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"code.cloudfoundry.org/go-diodes"
)

// Writer is a io.Writer wrapper that uses a diode to buffer writes.
type Writer struct {
	w          io.Writer
	d          *diodes.ManyToOne
	c          context.CancelFunc
	wg         *sync.WaitGroup
	mux        *sync.RWMutex
	closed     *uint32
	closeState *writerCloseState
}

type writerCloseState struct {
	once sync.Once
	err  error
}

// NewWriter creates a new Writer.
func NewWriter(w io.Writer, size int, pollInterval time.Duration, cb func(missed int)) Writer {
	ctx, cancel := context.WithCancel(context.Background())
	dw := Writer{
		w:          w,
		c:          cancel,
		wg:         &sync.WaitGroup{},
		mux:        &sync.RWMutex{},
		closed:     new(uint32),
		closeState: &writerCloseState{},
	}
	var alerter diodes.Alerter
	if cb != nil {
		alerter = diodes.AlertFunc(cb)
	}
	dw.d = diodes.NewManyToOne(size, alerter)
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
	dw.d.Set(diodes.GenericDataType(unsafe.Pointer(&c)))
	return len(p), nil
}

// Close closes the writer. It blocks until every write accepted before Close
// started has been flushed to the underlying writer. Writes attempted after
// shutdown starts return io.ErrClosedPipe.
func (dw Writer) Close() error {
	dw.closeState.once.Do(func() {
		// Taking the exclusive lock waits for in-flight writes and prevents new
		// writes from being accepted while the consumer drains the queue.
		dw.mux.Lock()
		atomic.StoreUint32(dw.closed, 1)
		dw.mux.Unlock()

		dw.c()
		// poll drains the queue after cancellation and calls Done only after the
		// final underlying write has completed.
		dw.wg.Wait()
		if c, ok := dw.w.(io.Closer); ok {
			dw.closeState.err = c.Close()
		}
	})
	return dw.closeState.err
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
				// Process messages accepted before Close before exiting.
				dw.drain()
				return
			case <-ticker.C:
			}
		} else {
			select {
			case <-ctx.Done():
				// Process messages accepted before Close before exiting.
				dw.drain()
				return
			default:
			}
		}
		if p, ok := dw.d.TryNext(); ok {
			dw.w.Write(*(*[]byte)(unsafe.Pointer(p)))
		} else if ticker == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// drain synchronously writes every item remaining in the diode queue.
func (dw Writer) drain() {
	for {
		p, ok := dw.d.TryNext()
		if !ok {
			break
		}
		dw.w.Write(*(*[]byte)(unsafe.Pointer(p)))
	}
}
