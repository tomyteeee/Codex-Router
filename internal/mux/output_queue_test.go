package mux

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

type blockingOutputWriter struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}

	mu     sync.Mutex
	buffer bytes.Buffer
}

func newBlockingOutputWriter() *blockingOutputWriter {
	return &blockingOutputWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (w *blockingOutputWriter) Write(
	payload []byte,
) (int, error) {
	w.once.Do(func() {
		close(w.started)
	})

	<-w.release

	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buffer.Write(payload)
}

func (w *blockingOutputWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buffer.String()
}

func TestOutputQueueDecouplesRendererWriteAndPreservesOrder(
	t *testing.T,
) {
	writer := newBlockingOutputWriter()

	m := &Multiplexer{
		output:      writer,
		outputQueue: make(chan []byte, 8),
	}

	m.outputStarted.Store(true)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	go m.outputLoop(ctx)

	m.writeRaw(
		[]byte(`first`),
	)

	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal(
			"output writer never received first message",
		)
	}

	// The dedicated writer is now intentionally blocked. A second writeRaw
	// must still complete because it only appends to the ordered queue.
	secondDone := make(
		chan struct{},
	)

	go func() {
		m.writeRaw(
			[]byte(`second`),
		)
		close(secondDone)
	}()

	select {
	case <-secondDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal(
			"writeRaw blocked behind the renderer writer",
		)
	}

	close(writer.release)

	want := "first\nsecond\n"

	deadline := time.Now().Add(
		time.Second,
	)

	for time.Now().Before(deadline) {
		if writer.String() == want {
			return
		}

		time.Sleep(
			5 * time.Millisecond,
		)
	}

	t.Fatalf(
		"unexpected renderer output order: got %q want %q",
		writer.String(),
		want,
	)
}

func TestWriteRawBeforeStartRemainsSynchronous(
	t *testing.T,
) {
	var output bytes.Buffer

	m := &Multiplexer{
		output: &output,
	}

	m.writeRaw(
		[]byte(`hello`),
	)

	if got, want := output.String(), "hello\n"; got != want {
		t.Fatalf(
			"unexpected pre-start output: got %q want %q",
			got,
			want,
		)
	}
}
