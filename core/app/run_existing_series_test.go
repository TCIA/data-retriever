package app

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestApplyExistingSeriesDispositionResumeCountsAsSucceeded(t *testing.T) {
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

	wc.applyExistingSeriesDisposition(fileInfo, resolveExistingSeriesDisposition(true), "resume")

	if wc.Stats.Downloaded != 1 {
		t.Fatalf("Downloaded = %d, want 1", wc.Stats.Downloaded)
	}
	if wc.Stats.Skipped != 0 {
		t.Fatalf("Skipped = %d, want 0", wc.Stats.Skipped)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].Status != seriesStatusSucceeded {
		t.Fatalf("event status = %q, want %q", events[0].Status, seriesStatusSucceeded)
	}

	status := readCompletionStatusForSeries(t, outputDir, fileInfo.SeriesInstanceUID)
	if status != StatusSuccess {
		t.Fatalf("completion status = %q, want %q", status, StatusSuccess)
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

	wc.applyExistingSeriesDisposition(fileInfo, resolveExistingSeriesDisposition(false), "skip")

	if wc.Stats.Downloaded != 0 {
		t.Fatalf("Downloaded = %d, want 0", wc.Stats.Downloaded)
	}
	if wc.Stats.Skipped != 1 {
		t.Fatalf("Skipped = %d, want 1", wc.Stats.Skipped)
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
