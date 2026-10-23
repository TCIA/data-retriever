package app

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

// ctxAwareReader simulates how the real network stack behaves once a
// request's context is cancelled: a pending Read unblocks and returns the
// context's error. idleTimeoutReader never cancels the underlying read
// itself - it only calls the context's cancel func and relies on whatever
// it's wrapping (here, this fake; in production, net/http) to notice.
type ctxAwareReader struct {
	ctx context.Context
}

func (r *ctxAwareReader) Read(p []byte) (int, error) {
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func TestIdleTimeoutReader_PassesDataThroughWithoutStalling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := strings.NewReader("hello world")
	it := newIdleTimeoutReader(src, time.Hour, cancel)
	defer it.Stop()

	data, err := io.ReadAll(it)
	if err != nil {
		t.Fatalf("unexpected error reading active data: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("got %q, want %q", data, "hello world")
	}
	if ctx.Err() != nil {
		t.Fatalf("context should not have been cancelled, got: %v", ctx.Err())
	}
}

func TestIdleTimeoutReader_CancelsAfterNoBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const idleTimeout = 20 * time.Millisecond
	stalled := &ctxAwareReader{ctx: ctx}
	it := newIdleTimeoutReader(stalled, idleTimeout, cancel)
	defer it.Stop()

	start := time.Now()
	_, err := it.Read(make([]byte, 1024))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error after the idle timeout fired, got nil")
	}
	if !strings.Contains(err.Error(), "connection stalled") {
		t.Fatalf("expected a stalled-connection error, got: %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("expected the context to be cancelled by the idle timer")
	}
	if elapsed < idleTimeout {
		t.Fatalf("returned before the idle timeout elapsed: %v < %v", elapsed, idleTimeout)
	}
}

func TestIdleTimeoutReader_ResetsOnEveryByte(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const idleTimeout = 40 * time.Millisecond
	pr, pw := io.Pipe()
	it := newIdleTimeoutReader(pr, idleTimeout, cancel)
	defer it.Stop()

	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(it)
		done <- err
	}()

	// Trickle bytes in slower than it would take to time out overall, but
	// faster than the idle window itself, so the reader should never see a
	// gap longer than idleTimeout.
	for i := 0; i < 5; i++ {
		time.Sleep(idleTimeout / 2)
		if _, err := pw.Write([]byte("x")); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected the trickling read to succeed, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read did not complete; idle timer likely fired despite active trickle")
	}
	if ctx.Err() != nil {
		t.Fatalf("context should not have been cancelled, got: %v", ctx.Err())
	}
}

func TestIdleTimeoutReader_StopPreventsLateCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const idleTimeout = 15 * time.Millisecond
	it := newIdleTimeoutReader(strings.NewReader("done"), idleTimeout, cancel)

	if _, err := io.ReadAll(it); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	it.Stop()

	time.Sleep(idleTimeout * 3)
	if ctx.Err() != nil {
		t.Fatalf("context should remain uncancelled after Stop, got: %v", ctx.Err())
	}
}
