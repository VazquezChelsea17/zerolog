package diode

import (
	"bytes"
	"fmt"
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

	// Verify that writing after close returns an error
	_, err = w.Write([]byte("after close"))
	if err == nil {
		t.Error("expected error writing after close, got nil")
	}
}
