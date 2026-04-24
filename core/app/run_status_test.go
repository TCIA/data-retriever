package app

import (
	"testing"
	"time"
)

func TestResolvePhaseWithDetailedStatuses(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		status string
		want   string
	}{
		{name: "queued", status: seriesStatusQueued, want: seriesPhaseQueued},
		{name: "worker initiated", status: seriesStatusWorkerInitiated, want: seriesPhaseQueued},
		{name: "pre check", status: seriesStatusPreCheck, want: seriesPhaseQueued},
		{name: "metadata", status: seriesStatusMetadata, want: seriesPhaseMetadata},
		{name: "download initiated", status: seriesStatusDownloadInitiated, want: seriesPhaseDownload},
		{name: "downloading", status: seriesStatusDownloading, want: seriesPhaseDownload},
		{name: "decompressing", status: seriesStatusDecompressing, want: seriesPhaseDecompress},
		{name: "succeeded", status: seriesStatusSucceeded, want: seriesPhaseComplete},
		{name: "skipped", status: seriesStatusSkipped, want: seriesPhaseComplete},
		{name: "failed", status: seriesStatusFailed, want: seriesPhaseFailure},
		{name: "cancelled", status: seriesStatusCancelled, want: seriesPhaseFailure},
		{name: "unknown", status: "mystery", want: ""},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := resolvePhase(tc.status)
			if got != tc.want {
				t.Fatalf("resolvePhase(%q) = %q, want %q", tc.status, got, tc.want)
			}
		})
	}
}

func TestSeriesEventGateAllowDownloadingThrottles(t *testing.T) {
	t.Parallel()

	gate := NewSeriesEventGate(2 * time.Second)
	base := time.Now()

	first := SeriesEvent{SeriesInstanceUID: "series-1", Status: seriesStatusDownloading, Timestamp: base}
	if !gate.Allow(first) {
		t.Fatalf("first downloading event should be allowed")
	}

	second := SeriesEvent{SeriesInstanceUID: "series-1", Status: seriesStatusDownloading, Timestamp: base.Add(1 * time.Second)}
	if gate.Allow(second) {
		t.Fatalf("second downloading event within interval should be throttled")
	}

	third := SeriesEvent{SeriesInstanceUID: "series-1", Status: seriesStatusDownloading, Timestamp: base.Add(3 * time.Second)}
	if !gate.Allow(third) {
		t.Fatalf("downloading event after interval should be allowed")
	}
}

func TestSeriesEventGateAllowAlwaysEmitStatuses(t *testing.T) {
	t.Parallel()

	gate := NewSeriesEventGate(10 * time.Second)
	base := time.Now()

	alwaysEmitStatuses := []string{
		seriesStatusQueued,
		seriesStatusWorkerInitiated,
		seriesStatusPreCheck,
		seriesStatusMetadata,
		seriesStatusDownloadInitiated,
		seriesStatusSucceeded,
		seriesStatusFailed,
		seriesStatusCancelled,
		seriesStatusSkipped,
	}

	for _, status := range alwaysEmitStatuses {
		evt := SeriesEvent{SeriesInstanceUID: "series-1", Status: status, Timestamp: base}
		if !gate.Allow(evt) {
			t.Fatalf("status %q should always emit", status)
		}
	}
}

func TestAlwaysEmitSeriesEventDownloadHeartbeat(t *testing.T) {
	t.Parallel()

	heartbeat := SeriesEvent{Status: seriesStatusDownloading, Message: "[Worker 1] Download of test-series in progress"}
	if !alwaysEmitSeriesEvent(heartbeat) {
		t.Fatalf("download heartbeat should always emit")
	}

	normal := SeriesEvent{Status: seriesStatusDownloading, Message: ""}
	if alwaysEmitSeriesEvent(normal) {
		t.Fatalf("downloading event without heartbeat message should not always emit")
	}
}
