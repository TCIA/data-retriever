package app

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Both existing-file checks must report a completed skip when no payload
// transfer is needed, including the per-series event and completion record.

func TestApplyExistingSeriesDispositionResumeCountsAsSkipped(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(outputDir, "metadata"), 0o755); err != nil {
		t.Fatalf("failed to create metadata directory: %v", err)
	}

	fileInfo := &FileInfo{SeriesInstanceUID: "series-resume"}
	if err := InitCompletionStatus(outputDir, []*FileInfo{fileInfo}); err != nil {
		t.Fatalf("failed to initialise completion status: %v", err)
	}

	var events []SeriesEvent
	wc := &WorkerContext{
		Options: &Options{Output: outputDir},
		Stats: &DownloadStats{
			Total:     1,
			StartTime: time.Now(),
		},
		Callbacks: Callbacks{
			Series: func(evt SeriesEvent) {
				events = append(events, evt)
			},
			Stderr: func(string) {},
		},
	}

	wc.applyExistingSeriesDisposition(fileInfo, resolveExistingSeriesDisposition(), "resume")

	if wc.Stats.Skipped != 1 {
		t.Fatalf("Skipped = %d, want 1", wc.Stats.Skipped)
	}
	if wc.Stats.Downloaded != 0 {
		t.Fatalf("Downloaded = %d, want 0", wc.Stats.Downloaded)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].Status != seriesStatusSkipped {
		t.Fatalf("event status = %q, want %q", events[0].Status, seriesStatusSkipped)
	}

	status := readCompletionStatusForSeries(t, outputDir, fileInfo.SeriesInstanceUID)
	if status != StatusSkipped {
		t.Fatalf("completion status = %q, want %q", status, StatusSkipped)
	}
}

func TestApplyExistingSeriesDispositionSkipCountsAsSkipped(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(outputDir, "metadata"), 0o755); err != nil {
		t.Fatalf("failed to create metadata directory: %v", err)
	}

	fileInfo := &FileInfo{SeriesInstanceUID: "series-skip"}
	if err := InitCompletionStatus(outputDir, []*FileInfo{fileInfo}); err != nil {
		t.Fatalf("failed to initialise completion status: %v", err)
	}

	var events []SeriesEvent
	wc := &WorkerContext{
		Options: &Options{Output: outputDir},
		Stats: &DownloadStats{
			Total:     1,
			StartTime: time.Now(),
		},
		Callbacks: Callbacks{
			Series: func(evt SeriesEvent) {
				events = append(events, evt)
			},
			Stderr: func(string) {},
		},
	}

	wc.applyExistingSeriesDisposition(fileInfo, resolveExistingSeriesDisposition(), "skip")

	if wc.Stats.Skipped != 1 {
		t.Fatalf("Skipped = %d, want 1", wc.Stats.Skipped)
	}
	if wc.Stats.Downloaded != 0 {
		t.Fatalf("Downloaded = %d, want 0", wc.Stats.Downloaded)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].Status != seriesStatusSkipped {
		t.Fatalf("event status = %q, want %q", events[0].Status, seriesStatusSkipped)
	}

	status := readCompletionStatusForSeries(t, outputDir, fileInfo.SeriesInstanceUID)
	if status != StatusSkipped {
		t.Fatalf("completion status = %q, want %q", status, StatusSkipped)
	}
}

func readCompletionStatusForSeries(t *testing.T, outputDir string, seriesUID string) string {
	t.Helper()

	f, err := os.Open(filepath.Join(outputDir, "metadata", "completion_status.csv"))
	if err != nil {
		t.Fatalf("failed to open completion status file: %v", err)
	}
	defer f.Close()

	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("failed to read completion status file: %v", err)
	}

	for _, row := range records[1:] {
		if len(row) >= 2 && row[0] == seriesUID {
			return row[1]
		}
	}

	t.Fatalf("series %q not found in completion status file", seriesUID)
	return ""
}

// Exercise the worker's actual prechecks, not just its counter helper. The
// transport is in-memory: DRS reuse must not contact an auth or storage service.
func TestWorkerExistingFileAccounting(t *testing.T) {
	t.Setenv("TCIA_COMPLETION_LOG_URL", "")
	cases := []struct {
		name                                      string
		drs, resume, force                        bool
		existing                                  string
		wantRequests, wantDownloaded, wantSkipped int32
	}{
		{name: "direct existing", existing: "DATA", wantSkipped: 1},
		{name: "DRS existing", drs: true, existing: "DATA", wantSkipped: 1},
		{name: "direct resume", resume: true, existing: "DATA", wantSkipped: 1},
		{name: "DRS resume", drs: true, resume: true, existing: "DATA", wantSkipped: 1},
		{name: "direct resume overrides force", resume: true, force: true, existing: "DATA", wantSkipped: 1},
		{name: "DRS resume overrides force", drs: true, resume: true, force: true, existing: "DATA", wantSkipped: 1},
		{name: "direct force downloads", force: true, existing: "DATA", wantRequests: 1, wantDownloaded: 1},
		{name: "missing file downloads", wantRequests: 1, wantDownloaded: 1},
		{name: "incomplete file downloads", existing: "DA", wantRequests: 1, wantDownloaded: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output := t.TempDir()
			if err := os.MkdirAll(filepath.Join(output, "metadata"), 0755); err != nil {
				t.Fatal(err)
			}
			options := &Options{Output: output, SkipExisting: tc.resume, Force: tc.force, CLI: true}
			file := &FileInfo{SeriesInstanceUID: "series-1", FileName: "payload.zip", FileSize: "4", Collection: "synthetic"}
			if tc.drs {
				file.DRSURI = "drs://nci-crdc.datacommons.io/synthetic"
			} else {
				file.DownloadURL = "https://example.invalid/payload.zip"
			}
			path := filepath.Join(file.DcimFiles(output, options), file.FileName)
			if tc.existing != "" {
				if err := os.WriteFile(path, []byte(tc.existing), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := InitCompletionStatus(output, []*FileInfo{file}); err != nil {
				t.Fatal(err)
			}
			var requests int32
			client := &http.Client{Transport: existingCounterTransport(func(req *http.Request) (*http.Response, error) {
				requests++
				return &http.Response{StatusCode: http.StatusOK, ContentLength: 4,
					Header: http.Header{"Content-Length": []string{"4"}},
					Body:   io.NopCloser(bytes.NewReader([]byte("DATA"))), Request: req}, nil
			})}
			var events []SeriesEvent
			worker := WorkerContext{Context: context.Background(), Options: options,
				HTTPClient: client, Stats: &DownloadStats{Total: 1, StartTime: time.Now()},
				Callbacks: Callbacks{Series: func(evt SeriesEvent) { events = append(events, evt) },
					Stdout: func(string) {}, Stderr: func(string) {}}}
			worker.handleFile(file)
			stats := worker.Stats
			if requests != tc.wantRequests || stats.Downloaded != tc.wantDownloaded || stats.Skipped != tc.wantSkipped || stats.Synced != 0 || stats.Failed != 0 {
				t.Fatalf("requests=%d, stats=%+v; want requests=%d downloaded=%d skipped=%d synced=0 failed=0", requests, stats, tc.wantRequests, tc.wantDownloaded, tc.wantSkipped)
			}
			if stats.Downloaded+stats.Synced+stats.Skipped+stats.Failed != stats.Total {
				t.Fatalf("completed counters do not add up to total: %+v", stats)
			}
			wantEvent, wantCompletion := seriesStatusSucceeded, StatusSuccess
			if tc.wantSkipped > 0 {
				wantEvent, wantCompletion = seriesStatusSkipped, StatusSkipped
			}
			if len(events) == 0 || events[len(events)-1].Status != wantEvent || events[len(events)-1].Progress != 100 {
				t.Fatalf("terminal event = %+v; want %s at 100%%", events, wantEvent)
			}
			if status := readCompletionStatusForSeries(t, output, file.SeriesInstanceUID); status != wantCompletion {
				t.Fatalf("completion status=%q, want %q", status, wantCompletion)
			}
			content, err := os.ReadFile(path)
			if err != nil || string(content) != "DATA" {
				t.Fatalf("payload=%q, err=%v", content, err)
			}
		})
	}
}

type existingCounterTransport func(*http.Request) (*http.Response, error)

func (f existingCounterTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }
