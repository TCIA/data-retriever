package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"io"
)

var ansiEscapePattern = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)

// TextEventLogger writes unified human-readable event-only logs.
type TextEventLogger struct {
	mu           sync.Mutex
	file         *os.File
	writer       *bufio.Writer
	runStart     time.Time
	interval     time.Duration
	manifestPath string
	totalSeries  int
	statuses     map[string]string
	current      string
	stopCh       chan struct{}
	doneCh       chan struct{}
	closed       bool
}

// NewTextEventLogger creates an event-only text logger.
func NewTextEventLogger(filePath string, runStart time.Time, interval time.Duration) (*TextEventLogger, error) {
	if filePath == "" {
		return nil, errors.New("file path is required")
	}
	if runStart.IsZero() {
		runStart = time.Now()
	}
	if interval <= 0 {
		interval = DefaultInterimUpdateInterval
	}

	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	multi := io.MultiWriter(f, os.Stdout)
	
	l := &TextEventLogger{
	    file:     f,
	    writer:   bufio.NewWriter(multi),
	    runStart: runStart,
	    interval: interval,
	    statuses: make(map[string]string),
	    stopCh:   make(chan struct{}),
	    doneCh:   make(chan struct{}),
	}

	l.writeLineLocked(runStart, "INFO", "run", "run", "run started", map[string]string{})
	go l.snapshotLoop()

	return l, nil
}

func (l *TextEventLogger) snapshotLoop() {
	next := l.runStart.Add(l.interval)
	for {
		wait := time.Until(next)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			l.writeSnapshot("snapshot")
			next = next.Add(l.interval)
		case <-l.stopCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			close(l.doneCh)
			return
		}
	}
}

// HandleManifest logs manifest metadata events and updates known total series.
func (l *TextEventLogger) HandleManifest(payload ManifestPayload) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if payload.ManifestPath != "" {
		l.manifestPath = payload.ManifestPath
	}
	if n := len(payload.Series); n > l.totalSeries {
		l.totalSeries = n
	}

	fields := map[string]string{
		"series_count": strconv.Itoa(len(payload.Series)),
	}
	if payload.ManifestPath != "" {
		fields["manifest_path"] = payload.ManifestPath
	}

	l.writeLineLocked(time.Now(), "INFO", "manifest", "run", "manifest metadata received", fields)
}

// HandleSeries logs a series lifecycle event.
func (l *TextEventLogger) HandleSeries(evt SeriesEvent) {
	if l == nil {
		return
	}

	ts := evt.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if evt.SeriesInstanceUID != "" {
		l.statuses[evt.SeriesInstanceUID] = evt.Status
		l.current = evt.SeriesInstanceUID
		if len(l.statuses) > l.totalSeries {
			l.totalSeries = len(l.statuses)
		}
	}

	msg := strings.TrimSpace(evt.Message)
	if msg == "" {
		msg = evt.Status
	}

	level := "INFO"
	if evt.Status == "failed" {
		level = "ERROR"
	} else if evt.Status == "cancelled" || evt.Status == "skipped" {
		level = "WARN"
	}

	fields := map[string]string{
		"status": evt.Status,
	}
	if evt.Phase != "" {
		fields["phase"] = evt.Phase
	}
	if evt.Progress > 0 {
		fields["progress_pct"] = fmt.Sprintf("%.1f", evt.Progress)
	}
	if evt.PhaseProgress > 0 {
		fields["phase_pct"] = fmt.Sprintf("%.1f", evt.PhaseProgress)
	}
	if evt.BytesDownloaded > 0 {
		fields["bytes_downloaded"] = strconv.FormatInt(evt.BytesDownloaded, 10)
	}
	if evt.BytesTotal > 0 {
		fields["bytes_total"] = strconv.FormatInt(evt.BytesTotal, 10)
	}
	if evt.UncompressedBytes > 0 {
		fields["uncompressed_bytes"] = strconv.FormatInt(evt.UncompressedBytes, 10)
	}
	if evt.UncompressedTotal > 0 {
		fields["uncompressed_total"] = strconv.FormatInt(evt.UncompressedTotal, 10)
	}

	scope := "run"
	if evt.SeriesInstanceUID != "" {
		scope = evt.SeriesInstanceUID
	}

	l.writeLineLocked(ts, level, "series", scope, msg, fields)
}

// HandleStdout captures selected lifecycle event lines from stdout.
func (l *TextEventLogger) HandleStdout(msg string) {
	l.handleStreamLine(msg, "stdout")
}

// HandleStderr captures selected lifecycle event lines from stderr.
func (l *TextEventLogger) HandleStderr(msg string) {
	l.handleStreamLine(msg, "stderr")
}

func (l *TextEventLogger) handleStreamLine(raw string, source string) {
	if l == nil {
		return
	}

	normalized := sanitizeEventLine(raw)
	if normalized == "" {
		return
	}

	for _, line := range strings.Split(normalized, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		now := time.Now()
		if strings.HasPrefix(trimmed, "Downloading ") {
			l.mu.Lock()
			l.writeLineLocked(now, "INFO", "run", "run", trimmed, map[string]string{"source": source})
			l.mu.Unlock()
			continue
		}

		if strings.EqualFold(trimmed, "Download cancelled by user") || strings.Contains(trimmed, "Ctrl+C pressed") {
			l.mu.Lock()
			l.writeLineLocked(now, "WARN", "shutdown", "run", trimmed, map[string]string{"source": source})
			l.mu.Unlock()
			continue
		}
	}
}

// LogRunFinished writes summary/completion lines and a forced final snapshot.
func (l *TextEventLogger) LogRunFinished(summary *Summary, err error) {
	if l == nil {
		return
	}

	now := time.Now()

	l.mu.Lock()
	if summary != nil {
		fields := map[string]string{
			"total":      strconv.FormatInt(int64(summary.Total), 10),
			"downloaded": strconv.FormatInt(int64(summary.Downloaded), 10),
			"synced":     strconv.FormatInt(int64(summary.Synced), 10),
			"skipped":    strconv.FormatInt(int64(summary.Skipped), 10),
			"failed":     strconv.FormatInt(int64(summary.Failed), 10),
			"elapsed":    summary.Elapsed.Round(time.Second).String(),
		}
		l.writeLineLocked(now, "INFO", "summary", "run", "download summary", fields)
	}

	if err != nil {
		level := "ERROR"
		msg := err.Error()
		if errors.Is(err, context.Canceled) {
			level = "WARN"
			msg = "run cancelled"
		}
		l.writeLineLocked(now, level, "shutdown", "run", msg, map[string]string{})
	} else {
		l.writeLineLocked(now, "INFO", "shutdown", "run", "run completed", map[string]string{})
	}

	l.mu.Unlock()
	l.writeSnapshot("final")
}

func (l *TextEventLogger) writeSnapshot(reason string) {
	if l == nil {
		return
	}

	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	total := l.totalSeries
	completed := 0
	active := 0
	succeeded := 0
	failed := 0
	skipped := 0
	cancelled := 0

	for _, status := range l.statuses {
		switch status {
		case "succeeded":
			succeeded++
			completed++
		case "failed":
			failed++
			completed++
		case "skipped":
			skipped++
			completed++
		case "cancelled":
			cancelled++
			completed++
		case "metadata", "downloading", "decompressing":
			active++
		}
	}

	progress := 0.0
	if total > 0 {
		progress = float64(completed) * 100.0 / float64(total)
	}

	fields := map[string]string{
		"total":        strconv.Itoa(total),
		"completed":    strconv.Itoa(completed),
		"active":       strconv.Itoa(active),
		"succeeded":    strconv.Itoa(succeeded),
		"failed":       strconv.Itoa(failed),
		"skipped":      strconv.Itoa(skipped),
		"cancelled":    strconv.Itoa(cancelled),
		"progress_pct": fmt.Sprintf("%.1f", progress),
		"reason":       reason,
	}
	if l.current != "" {
		fields["current"] = l.current
	}
	if l.manifestPath != "" {
		fields["manifest_path"] = l.manifestPath
	}

	elapsed := now.Sub(l.runStart)
	if completed > 0 && total > completed && elapsed > 0 {
		rate := float64(completed) / elapsed.Seconds()
		if rate > 0 {
			remaining := float64(total-completed) / rate
			fields["eta"] = (time.Duration(remaining * float64(time.Second))).Round(time.Second).String()
		}
	}

	l.writeLineLocked(now, "INFO", "progress", "run", "snapshot", fields)
}

// Close flushes and closes the logger.
func (l *TextEventLogger) Close() {
	if l == nil {
		return
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	l.mu.Unlock()

	close(l.stopCh)
	<-l.doneCh

	l.mu.Lock()
	defer l.mu.Unlock()
	_ = l.writer.Flush()
	_ = l.file.Close()
}

func (l *TextEventLogger) writeLineLocked(ts time.Time, level string, kind string, scope string, message string, fields map[string]string) {
	if l == nil || l.writer == nil {
		return
	}

	tsValue := ts.Local().Format(time.RFC3339)
	line := fmt.Sprintf("%s | %s | %s | %s | %s", tsValue, strings.ToUpper(level), kind, scope, sanitizeFieldValue(message))

	if len(fields) > 0 {
		keys := make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			v := sanitizeFieldValue(fields[k])
			if v == "" {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s=%s", k, quoteIfNeeded(v)))
		}

		if len(parts) > 0 {
			line += " | " + strings.Join(parts, " ")
		}
	}

	_, _ = l.writer.WriteString(line + "\n")
	_ = l.writer.Flush()
}

func sanitizeEventLine(value string) string {
	clean := ansiEscapePattern.ReplaceAllString(value, "")
	clean = strings.ReplaceAll(clean, "\r", "")
	return clean
}

func sanitizeFieldValue(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.ReplaceAll(trimmed, "\n", " ")
	trimmed = strings.ReplaceAll(trimmed, "\t", " ")
	trimmed = strings.Join(strings.Fields(trimmed), " ")
	return trimmed
}

func quoteIfNeeded(value string) string {
	if strings.ContainsAny(value, " |=") {
		return strconv.Quote(value)
	}
	return value
}
