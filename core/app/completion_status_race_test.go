package app

import (
	"context"
	"encoding/csv"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// readCompletionStatusFor returns the completion_status value recorded for
// seriesUID, or "" if the row is missing entirely.
func readCompletionStatusFor(t *testing.T, outDir, seriesUID string) string {
	t.Helper()
	f, err := os.Open(filepath.Join(outDir, "metadata", "completion_status.csv"))
	if err != nil {
		t.Fatalf("could not open completion status file: %v", err)
	}
	defer func() { _ = f.Close() }()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("could not read completion status file: %v", err)
	}
	for _, row := range records[1:] {
		if len(row) >= 2 && row[0] == seriesUID {
			return row[1]
		}
	}
	return ""
}

// TestStreamFilesFromSeriesIDsInitializesCompletionStatusBeforeDownloadsCanStart
// guards against a regression where completion_status.csv was (re)initialized
// only after every TCIA metadata batch finished fetching, inside the same
// goroutine that streams files out for download. Because downstream workers
// start downloading the moment a file arrives on the returned channel —
// including prefixFiles, sent before any TCIA batch even starts fetching —
// a worker could finish a download and record its status before that late
// init ran, only to have init's os.Create truncate the file back to
// "incomplete" for everything, discarding the just-recorded status. This is
// exactly what made resumed runs forget which series were already done.
func TestStreamFilesFromSeriesIDsInitializesCompletionStatusBeforeDownloadsCanStart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a slow TCIA metadata batch so the prefix file's "download"
		// below can complete well before this response is even sent.
		time.Sleep(100 * time.Millisecond)
		fmt.Fprintln(w, "SeriesInstanceUID,FileSize\n1.2.3,2")
	}))
	defer srv.Close()
	old := MetaUrl
	MetaUrl = srv.URL
	t.Cleanup(func() { MetaUrl = old })

	outDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(outDir, "metadata"), 0755); err != nil {
		t.Fatalf("mkdir metadata: %v", err)
	}

	prefix := &FileInfo{SeriesInstanceUID: "prefix-series"}
	_, out := streamFilesFromSeriesIDs(context.Background(), []*FileInfo{prefix}, []string{"1.2.3"}, srv.Client(), &Options{Output: outDir}, Callbacks{})

	first := <-out
	if first == nil || first.SeriesInstanceUID != prefix.SeriesInstanceUID {
		t.Fatalf("expected prefix file first on the channel, got %+v", first)
	}

	// Before the slow TCIA batch above has returned, record this file as
	// successfully downloaded — mirroring what a download worker does the
	// instant a file streams through.
	if err := AppendCompletionStatus(outDir, prefix.SeriesInstanceUID, nil, false); err != nil {
		t.Fatalf("AppendCompletionStatus: %v", err)
	}

	for range out {
		// Drain the rest so the producer goroutine can finish and close out.
	}

	if got := readCompletionStatusFor(t, outDir, prefix.SeriesInstanceUID); got != StatusSuccess {
		t.Fatalf("prefix file's completion status was lost: got %q, want %q", got, StatusSuccess)
	}
}
