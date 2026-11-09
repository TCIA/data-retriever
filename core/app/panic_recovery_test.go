package app

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestRecoverPanicStopsPanicAndRunsOnRecover(t *testing.T) {
	var recovered any
	func() {
		defer recoverPanic("test goroutine", func(r any) { recovered = r })
		panic("boom")
	}()
	if recovered != "boom" {
		t.Fatalf("onRecover was not called with the panic value: got %v", recovered)
	}
}

func TestRecoverPanicWithoutOnRecoverStillStopsPanic(t *testing.T) {
	func() {
		defer recoverPanic("test goroutine", nil)
		panic("boom")
	}()
	// Reaching this line at all proves the panic did not escape.
}

// TestHandleFileRecoveredSurvivesPanicInHandleFile proves a worker goroutine
// does not crash the whole process when handleFile panics on one file, and
// that it reports the series as failed instead of leaving it stuck or
// silently dropped. wc.Context is left nil to force handleFile's own
// `wc.Context.Err()` call to panic (a nil context.Context interface has no
// concrete type to dispatch Err() to) — a real, reachable panic site, not a
// synthetic one — while keeping every field the recover handler itself
// reads (wc.Options, fileInfo) valid, so the test isolates the panic to
// handleFile rather than to the recovery path.
func TestHandleFileRecoveredSurvivesPanicInHandleFile(t *testing.T) {
	outDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(outDir, "metadata"), 0755); err != nil {
		t.Fatalf("mkdir metadata: %v", err)
	}
	fileInfo := &FileInfo{SeriesInstanceUID: "panic-series"}
	if err := InitCompletionStatus(outDir, []*FileInfo{fileInfo}); err != nil {
		t.Fatalf("InitCompletionStatus: %v", err)
	}

	wc := &WorkerContext{
		Context:  nil, // triggers the panic: handleFile calls wc.Context.Err()
		Options:  &Options{Output: outDir},
		Stats:    &DownloadStats{},
		WorkerID: 1,
	}

	wc.handleFileRecovered(fileInfo)

	if got := atomic.LoadInt32(&wc.Stats.Failed); got != 1 {
		t.Fatalf("Stats.Failed = %d, want 1", got)
	}
	if got := readCompletionStatusFor(t, outDir, fileInfo.SeriesInstanceUID); got == "" || got == "incomplete" {
		t.Fatalf("completion status for panicked series was not recorded: got %q", got)
	}
}
