package diode

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewWriter(buf, 1000, 10*time.Millisecond, func(missed int) {
		fmt.Printf("Missed %d messages\n", missed)
	})
	w.Write([]byte("test\n"))
	w.Close()
	if buf.String() != "test\n" {
		t.Errorf("expected test, got %q", buf.String())
	}
}

func TestWriterCloseDrainsBuffer(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewWriter(buf, 2000, 10*time.Millisecond, func(missed int) {
		t.Errorf("Missed %d messages", missed)
	})

	numMessages := 1000
	for i := 0; i < numMessages; i++ {
		_, err := w.Write([]byte(fmt.Sprintf("message %d\n", i)))
		if err != nil {
			t.Fatalf("unexpected error on write: %v", err)
		}
	}

	err := w.Close()
	if err != nil {
		t.Fatalf("unexpected error on close: %v", err)
	}

	// Verify that all messages were written
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != numMessages {
		t.Errorf("expected %d messages, got %d", numMessages, len(lines))
	}

	// Verify that writing after close returns the documented error.
	n, err := w.Write([]byte("after close"))
	if err != io.ErrClosedPipe {
		t.Fatalf("expected io.ErrClosedPipe, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected a rejected write to report 0 bytes, got %d", n)
	}
}

func TestWriterCloseWaitsForDrain(t *testing.T) {
	dst := newBlockingWriter()
	w := NewWriter(dst, 16, 0, nil)

	if _, err := w.Write([]byte("first\n")); err != nil {
		t.Fatalf("write first message: %v", err)
	}

	select {
	case <-dst.started:
	case <-time.After(time.Second):
		t.Fatal("background writer did not start")
	}

	// Queue another message while the consumer is blocked writing the first.
	if _, err := w.Write([]byte("second\n")); err != nil {
		t.Fatalf("write second message: %v", err)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- w.Close()
	}()

	deadline := time.Now().Add(time.Second)
	for atomic.LoadUint32(w.closed) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Close did not mark the writer closed")
		}
		runtime.Gosched()
	}

	if n, err := w.Write([]byte("after close started\n")); err != io.ErrClosedPipe || n != 0 {
		t.Fatalf("write during Close = (%d, %v), want (0, io.ErrClosedPipe)", n, err)
	}

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the underlying write completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(dst.release)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close returned an error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the underlying writer was released")
	}

	if got, want := dst.String(), "first\nsecond\n"; got != want {
		t.Fatalf("drained output = %q, want %q", got, want)
	}
}

func TestWriterConcurrentWriteAndClose(t *testing.T) {
	const writers = 64

	var dst bytes.Buffer
	w := NewWriter(&dst, writers*2+1, 0, nil)
	startRace := make(chan struct{})
	results := make(chan writeResult, writers*2)

	var ready sync.WaitGroup
	var writes sync.WaitGroup
	ready.Add(writers)
	writes.Add(writers)
	for i := 0; i < writers; i++ {
		go func(id int) {
			defer writes.Done()

			first := fmt.Sprintf("before-%d", id)
			n, err := w.Write([]byte(first + "\n"))
			results <- writeResult{message: first, n: n, err: err}
			ready.Done()

			<-startRace
			second := fmt.Sprintf("during-%d", id)
			n, err = w.Write([]byte(second + "\n"))
			results <- writeResult{message: second, n: n, err: err}
		}(i)
	}

	ready.Wait()
	closeDone := make(chan error, 1)
	go func() {
		<-startRace
		closeDone <- w.Close()
	}()
	close(startRace)

	writes.Wait()
	if err := <-closeDone; err != nil {
		t.Fatalf("Close returned an error: %v", err)
	}
	close(results)

	accepted := make(map[string]struct{})
	for result := range results {
		switch result.err {
		case nil:
			if result.n != len(result.message)+1 {
				t.Errorf("accepted write %q reported %d bytes", result.message, result.n)
			}
			accepted[result.message] = struct{}{}
		case io.ErrClosedPipe:
			if result.n != 0 {
				t.Errorf("rejected write %q reported %d bytes", result.message, result.n)
			}
		default:
			t.Errorf("write %q returned unexpected error: %v", result.message, result.err)
		}
	}

	written := make(map[string]struct{})
	for _, message := range strings.Fields(dst.String()) {
		written[message] = struct{}{}
	}
	if len(written) != len(accepted) {
		t.Fatalf("wrote %d unique messages; accepted %d", len(written), len(accepted))
	}
	for message := range accepted {
		if _, ok := written[message]; !ok {
			t.Errorf("accepted message %q was not drained", message)
		}
	}
}

func TestWriterConcurrentCloseIsIdempotent(t *testing.T) {
	closeErr := errors.New("close failed")
	dst := &countingWriteCloser{closeErr: closeErr}
	w := NewWriter(dst, 16, 0, nil)

	if _, err := w.Write([]byte("message\n")); err != nil {
		t.Fatalf("Write returned an error: %v", err)
	}

	const closers = 16
	start := make(chan struct{})
	results := make(chan error, closers)
	for i := 0; i < closers; i++ {
		go func() {
			<-start
			results <- w.Close()
		}()
	}
	close(start)

	for i := 0; i < closers; i++ {
		if err := <-results; !errors.Is(err, closeErr) {
			t.Errorf("Close returned %v, want %v", err, closeErr)
		}
	}

	if got := dst.CloseCalls(); got != 1 {
		t.Fatalf("underlying Close called %d times, want 1", got)
	}
	if got, want := dst.String(), "message\n"; got != want {
		t.Fatalf("drained output = %q, want %q", got, want)
	}
}

type blockingWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	buf     bytes.Buffer
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *blockingWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

type writeResult struct {
	message string
	n       int
	err     error
}

type countingWriteCloser struct {
	mu         sync.Mutex
	buf        bytes.Buffer
	closeCalls int
	closeErr   error
}

func (w *countingWriteCloser) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *countingWriteCloser) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeCalls++
	return w.closeErr
}

func (w *countingWriteCloser) CloseCalls() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closeCalls
}

func (w *countingWriteCloser) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}
