package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testConsole(t *testing.T, mode string) (*ConsoleProgress, *bytes.Buffer, *time.Time) {
	t.Helper()
	var output bytes.Buffer
	p, err := NewConsoleProgress(&output, mode)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	p.now = func() time.Time { return now }
	return p, &output, &now
}

func TestConsolePlainShowsFileBytesWithOneGlobalThrottle(t *testing.T) {
	p, out, now := testConsole(t, "plain")
	p.Update(ProgressUpdate{Kind: "items", Total: 10})
	for i := 0; i < 100; i++ {
		p.Update(ProgressUpdate{Kind: "series", Series: SeriesEvent{SeriesInstanceUID: "file-one", Status: seriesStatusDownloading, BytesDownloaded: int64(i+1) * 1024 * 1024, BytesTotal: 200 * 1024 * 1024}})
	}
	if strings.Count(out.String(), "\n") != 1 {
		t.Fatalf("per-worker/byte output escaped global throttle: %q", out.String())
	}
	*now = now.Add(5 * time.Second)
	p.Update(ProgressUpdate{Kind: "series", Series: SeriesEvent{SeriesInstanceUID: "file-one", Status: seriesStatusDownloading, BytesDownloaded: 100 * 1024 * 1024, BytesTotal: 200 * 1024 * 1024}})
	text := out.String()
	if !strings.Contains(text, "Items completed: 0/10 (0.0%)") || !strings.Contains(text, "Download: 100.0 MiB/200.0 MiB (50.0%)") {
		t.Fatalf("overall and active-file progress not distinguished: %q", text)
	}
	if strings.ContainsAny(text, "\r\x1b") {
		t.Fatalf("plain output contains terminal controls: %q", text)
	}
	p.Finish(&Summary{Total: 10, Downloaded: 10}, nil)
	if strings.Count(out.String(), "\n") != 3 || !strings.Contains(out.String(), "Run complete | Items completed: 10/10 (100.0%)") {
		t.Fatalf("final snapshot missing: %q", out.String())
	}
}

func TestConsoleTerminalThrottlesAndFinalizesLine(t *testing.T) {
	p, out, now := testConsole(t, "terminal")
	p.Update(ProgressUpdate{Kind: "items", Total: 1})
	p.Update(ProgressUpdate{Kind: "series", Series: SeriesEvent{SeriesInstanceUID: "file", Status: seriesStatusDownloading, BytesDownloaded: 10, BytesTotal: 20}})
	if strings.Count(out.String(), "\r\033[K") != 1 {
		t.Fatal("terminal updates not throttled")
	}
	*now = now.Add(200 * time.Millisecond)
	p.Update(ProgressUpdate{Kind: "series", Series: SeriesEvent{SeriesInstanceUID: "file", Status: seriesStatusDownloading, BytesDownloaded: 20, BytesTotal: 20}})
	if !strings.Contains(out.String(), "20 B/20 B (99.9%)") {
		t.Fatalf("file was declared complete before verification: %q", out.String())
	}
	for _, line := range strings.Split(out.String(), "\r\033[K") {
		if len([]rune(line)) > 78 {
			t.Fatalf("terminal update would wrap an 80-column window: %q", line)
		}
	}
	p.Finish(&Summary{Total: 1, Failed: 1}, errors.New("failed checksum secret-url"))
	final := strings.Split(out.String(), "\r\033[K")
	last := final[len(final)-1]
	if !strings.HasPrefix(last, "Run failed") || strings.Contains(last, "100.0%") || strings.Contains(last, "secret-url") || !strings.HasSuffix(last, "\n") {
		t.Fatalf("misleading failure snapshot: %q", last)
	}
}

func TestConsoleTerminalFitsLargeFileProgress(t *testing.T) {
	p, out, now := testConsole(t, "terminal")
	p.Update(ProgressUpdate{Kind: "items", Total: 10000, Downloaded: 10})
	*now = now.Add(time.Second)
	p.Update(ProgressUpdate{Kind: "series", Series: SeriesEvent{SeriesInstanceUID: strings.Repeat("long-name", 20), Status: seriesStatusDownloading, BytesDownloaded: 1024 * 1024 * 1024, BytesTotal: 2 * 1024 * 1024 * 1024}})
	for _, line := range strings.Split(out.String(), "\r\033[K") {
		if len([]rune(line)) > 78 {
			t.Fatalf("terminal line wraps: %q", line)
		}
	}
	if !strings.Contains(out.String(), "Items 10/10000") || !strings.Contains(out.String(), "1.0 GiB/2.0 GiB (50.0%)") {
		t.Fatalf("important counters/bytes lost: %q", out.String())
	}
}

func TestConsoleCancellationUnknownTotalsAndNoRawMessages(t *testing.T) {
	p, out, now := testConsole(t, "plain")
	p.Update(ProgressUpdate{Kind: "items"})
	*now = now.Add(5 * time.Second)
	p.Update(ProgressUpdate{Kind: "series", Series: SeriesEvent{SeriesInstanceUID: "file", SeriesDescription: "safe\nname", Status: seriesStatusDownloading, BytesDownloaded: 2048, Message: "https://storage.test/?secret=token"}})
	p.Finish(&Summary{Total: 2, Downloaded: 1}, context.Canceled)
	p.Finish(&Summary{Total: 2, Downloaded: 2}, nil)
	text := out.String()
	if !strings.Contains(text, "2.0 KiB (total pending)") || !strings.Contains(text, "Run cancelled | Items completed: 1/2") || strings.Contains(text, "secret") || strings.Contains(text, "100.0%") || strings.Contains(text, "Run complete") {
		t.Fatalf("bad cancellation/unknown-total display: %q", text)
	}
	if !strings.Contains(text, "File: safename") {
		t.Fatalf("label controls not removed: %q", text)
	}
}

func TestConsoleMetadataUsesSharedPlainRenderer(t *testing.T) {
	p, out, now := testConsole(t, "plain")
	m := &MetadataStats{Total: 3, Progress: p.Update}
	m.updateProgress("fetched", "one")
	m.updateProgress("cached", "two")
	if strings.Count(out.String(), "\n") != 1 {
		t.Fatal("metadata not globally throttled")
	}
	*now = now.Add(5 * time.Second)
	m.updateProgress("failed", "three")
	if !strings.Contains(out.String(), "Metadata processed: 3/3 (100.0%) | Fetched: 1 | Cached: 1 | Failed: 1") || strings.ContainsAny(out.String(), "\r\x1b") {
		t.Fatalf("metadata display: %q", out.String())
	}
	if !m.LastUpdate.IsZero() {
		t.Fatal("legacy metadata terminal renderer ran")
	}
}

func TestConsoleRawProgressDoesNotChangeGUIEventCadence(t *testing.T) {
	p, out, now := testConsole(t, "plain")
	guiEvents := 0
	callbacks := p.Wrap(Callbacks{Series: func(SeriesEvent) { guiEvents++ }})
	wc := &WorkerContext{Callbacks: callbacks, EventGate: NewSeriesEventGate(10 * time.Second)}
	base := *now
	wc.emitSeries(SeriesEvent{SeriesInstanceUID: "file", Status: seriesStatusDownloading, BytesDownloaded: 1, BytesTotal: 10, Timestamp: base})
	*now = now.Add(5 * time.Second)
	wc.emitSeries(SeriesEvent{SeriesInstanceUID: "file", Status: seriesStatusDownloading, BytesDownloaded: 5, BytesTotal: 10, Timestamp: base.Add(time.Second)})
	if guiEvents != 1 || !strings.Contains(out.String(), "5 B/10 B (50.0%)") {
		t.Fatalf("GUI cadence or independent bytes progress wrong: gui=%d output=%q", guiEvents, out.String())
	}
	stats := &DownloadStats{Total: 10, Downloaded: 2, LastUpdate: *now}
	updateProgress(stats, "file", false, callbacks)
	if p.items.Downloaded != 2 {
		t.Fatal("console totals were blocked by legacy 200ms gate")
	}
}

func TestConsoleAutoAndModeValidation(t *testing.T) {
	p, _, _ := testConsole(t, "auto")
	if p.terminal {
		t.Fatal("non-TTY was treated as terminal")
	}
	t.Setenv("TERM", "dumb")
	p, _, _ = testConsole(t, "auto")
	if p.terminal {
		t.Fatal("dumb terminal used ANSI")
	}
	if _, err := NewConsoleProgress(&bytes.Buffer{}, "quiet"); err == nil {
		t.Fatal("unsupported mode accepted")
	}
}

func TestConsoleWarningStartsOnSeparateTerminalLine(t *testing.T) {
	p, out, now := testConsole(t, "terminal")
	p.Update(ProgressUpdate{Kind: "items", Total: 2})
	setLoggerOutput(false, false, "", p.Writer(out))
	t.Cleanup(func() { setLogger(false, false, "") })
	Logger.Warn("example warning")
	*now = now.Add(time.Second)
	p.Update(ProgressUpdate{Kind: "items", Total: 2, Downloaded: 1})
	text := out.String()
	if !strings.Contains(text, "Active 0\n") || !strings.Contains(text, "WARN") || !strings.Contains(text, "example warning\n\r\033[KItems 1/2") {
		t.Fatalf("log message appended to progress row: %q", text)
	}
}

func TestStreamingMetadataProgressUsesRendererInsteadOfBatchLines(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "SeriesInstanceUID,FileSize\n1.2.3,2") }))
	defer srv.Close()
	old := MetaUrl
	MetaUrl = srv.URL
	t.Cleanup(func() { MetaUrl = old })
	var messages bytes.Buffer
	var updates []ProgressUpdate
	_, files := streamFilesFromSeriesIDs(context.Background(), nil, []string{"1.2.3"}, srv.Client(), &Options{Output: t.TempDir()}, Callbacks{Stdout: func(s string) { messages.WriteString(s) }, Progress: func(u ProgressUpdate) { updates = append(updates, u) }})
	for range files {
	}
	if len(updates) != 2 || updates[0].Kind != "metadata" || updates[0].Total != 1 || updates[1].Fetched != 1 {
		t.Fatalf("metadata updates missing: %+v", updates)
	}
	if strings.Contains(messages.String(), "Fetched metadata batch") {
		t.Fatalf("batch output bypassed global renderer: %q", messages.String())
	}
}
