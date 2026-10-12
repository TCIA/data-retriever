package app

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, time.October, 12, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "seconds", value: "7", want: 7 * time.Second},
		{name: "http date", value: now.Add(12 * time.Second).Format(http.TimeFormat), want: 12 * time.Second},
		{name: "zero", value: "0", want: 0},
		{name: "past date", value: now.Add(-time.Second).Format(http.TimeFormat), want: 0},
		{name: "invalid", value: "later", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseRetryAfter(tt.value, now); got != tt.want {
				t.Fatalf("parseRetryAfter(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestHTTPStatusErrorRetryClassification(t *testing.T) {
	rateLimited := fmt.Errorf("wrapped: %w", &httpStatusError{
		statusCode: http.StatusTooManyRequests,
		retryAfter: 3 * time.Second,
	})
	if !isRetryableError(rateLimited) {
		t.Fatal("HTTP 429 should be retryable")
	}
	if got := retryAfterFromError(rateLimited); got != 3*time.Second {
		t.Fatalf("wrapped Retry-After = %v, want 3s", got)
	}
	if isRetryableError(&httpStatusError{statusCode: http.StatusNotFound}) {
		t.Fatal("HTTP 404 should not be retryable")
	}
}

func TestDownloadRetryHonorsRetryAfter(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte("payload"))
	}))
	defer srv.Close()

	info := &FileInfo{
		Collection:        "TEST",
		PatientID:         "P-1",
		StudyInstanceUID:  "study",
		SeriesInstanceUID: "series",
		DownloadURL:       srv.URL + "/file.bin",
		FileName:          "file.bin",
	}
	options := &Options{
		DirectoryMode: "classic",
		MaxRetries:    1,
		RetryDelay:    time.Millisecond,
	}

	start := time.Now()
	err := info.DownloadWithRetry(context.Background(), t.TempDir(), srv.Client(), options, nil, nil, nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("DownloadWithRetry returned error: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("retry occurred after %v; Retry-After was not honored", elapsed)
	}
}

func TestWaitWithContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := waitWithContext(ctx, time.Minute); err != context.Canceled {
		t.Fatalf("waitWithContext error = %v, want %v", err, context.Canceled)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("cancelled wait took too long: %v", elapsed)
	}
}
