package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/mattn/go-isatty"
)

// ProgressUpdate carries console progress independently of GUI event cadence.
// Kind is items, metadata or series. Per-series messages are never printed:
// errors and temporary URLs belong to the normal, separately handled logs.
type ProgressUpdate struct {
	Kind       string
	Total      int32
	Downloaded int32
	Synced     int32
	Skipped    int32
	Failed     int32
	Fetched    int32
	Cached     int32
	Series     SeriesEvent
}

// ConsoleProgress renders one shared view across all workers. Plain mode
// emits newline snapshots at most every five seconds; terminal mode rewrites
// one line at most five times per second. Finish always emits a final snapshot.
type ConsoleProgress struct {
	mu         sync.Mutex
	writer     io.Writer
	terminal   bool
	now        func() time.Time
	last       time.Time
	lineActive bool
	finished   bool
	stage      string
	items      ProgressUpdate
	metadata   ProgressUpdate
	active     map[string]SeriesEvent
}

// NewConsoleProgress chooses a terminal or plain renderer. Auto uses a real
// terminal descriptor and honors TERM=dumb; pipes and notebooks use plain.
func NewConsoleProgress(writer io.Writer, mode string) (*ConsoleProgress, error) {
	if writer == nil {
		writer = os.Stderr
	}
	p := &ConsoleProgress{writer: writer, now: time.Now, active: make(map[string]SeriesEvent)}
	switch mode {
	case "", "auto":
		p.terminal = consoleTerminal(writer)
	case "plain":
	case "terminal":
		p.terminal = true
	default:
		return nil, fmt.Errorf("progress mode must be auto, plain or terminal")
	}
	return p, nil
}

func consoleTerminal(writer io.Writer) bool {
	if strings.EqualFold(os.Getenv("TERM"), "dumb") {
		return false
	}
	f, ok := writer.(*os.File)
	return ok && (isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd()))
}

// Wrap preserves existing GUI/log callbacks and installs a separate console
// progress channel. GUI events retain their original gate and cadence.
func (p *ConsoleProgress) Wrap(callbacks Callbacks) Callbacks {
	originalProgress := callbacks.Progress
	callbacks.Progress = func(update ProgressUpdate) {
		p.Update(update)
		if originalProgress != nil {
			originalProgress(update)
		}
	}
	stdout, stderr := callbacks.Stdout, callbacks.Stderr
	callbacks.Stdout = func(s string) {
		p.writeText(s, stdout, os.Stdout)
	}
	callbacks.Stderr = func(s string) {
		p.writeText(s, stderr, os.Stderr)
	}
	return callbacks
}

func (p *ConsoleProgress) writeText(s string, callback func(string), fallback io.Writer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminal && p.lineActive {
		fmt.Fprint(p.writer, "\n")
		p.lineActive = false
	}
	if callback != nil {
		callback(s)
	} else {
		fmt.Fprint(fallback, s)
	}
}

type consoleLogWriter struct {
	progress *ConsoleProgress
	writer   io.Writer
}

// Writer coordinates log output with the updating progress line. Use this as
// Options.ConsoleLogWriter; the underlying writer still receives normal logs.
func (p *ConsoleProgress) Writer(writer io.Writer) io.Writer {
	if writer == nil {
		writer = os.Stdout
	}
	return consoleLogWriter{progress: p, writer: writer}
}

func (w consoleLogWriter) Write(b []byte) (int, error) {
	p := w.progress
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminal && p.lineActive {
		fmt.Fprint(p.writer, "\n")
		p.lineActive = false
	}
	return w.writer.Write(b)
}

// Update records every progress event while applying one global output limit.
func (p *ConsoleProgress) Update(update ProgressUpdate) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	switch update.Kind {
	case "metadata":
		p.metadata = update
		if len(p.active) == 0 && p.items.Total == 0 {
			p.stage = "metadata"
		}
	case "items":
		p.stage = "items"
		p.items.Total = update.Total
		// Workers may arrive at this lock in a different order from their
		// atomic counter reads. Never display counters going backwards.
		p.items.Downloaded = max(p.items.Downloaded, update.Downloaded)
		p.items.Synced = max(p.items.Synced, update.Synced)
		p.items.Skipped = max(p.items.Skipped, update.Skipped)
		p.items.Failed = max(p.items.Failed, update.Failed)
	case "series":
		evt := update.Series
		key := evt.SeriesInstanceUID
		if key == "" {
			return
		}
		if isTerminalSeriesStatus(evt.Status) {
			delete(p.active, key)
		} else if evt.Status == seriesStatusDownloadInitiated || evt.Status == seriesStatusDownloading || evt.Status == seriesStatusDecompressing || evt.Status == seriesStatusMetadata {
			p.stage = "items"
			p.active[key] = evt
		}
	default:
		return
	}
	p.renderLocked(false, "")
}

// Finish writes a final snapshot, including partial counts on failure or
// cancellation. It does not turn a failed/cancelled item's lifecycle progress
// of 100 into a successful file-download percentage.
func (p *ConsoleProgress) Finish(summary *Summary, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	if summary != nil {
		p.items = ProgressUpdate{Total: summary.Total, Downloaded: summary.Downloaded, Synced: summary.Synced, Skipped: summary.Skipped, Failed: summary.Failed}
	}
	status := "Run complete"
	if errors.Is(err, context.Canceled) {
		status = "Run cancelled"
	} else if err != nil || p.items.Failed > 0 || p.items.Total == 0 {
		status = "Run failed"
	} else if p.items.Downloaded+p.items.Synced+p.items.Skipped < p.items.Total {
		status = "Run incomplete"
	}
	p.stage = "items"
	p.renderLocked(true, status)
	p.finished = true
}

func (p *ConsoleProgress) renderLocked(final bool, status string) {
	now := p.now()
	interval := 5 * time.Second
	if p.terminal {
		interval = 200 * time.Millisecond
	}
	if !final && !p.last.IsZero() && now.Sub(p.last) < interval {
		return
	}
	p.last = now
	line := p.snapshotLocked(final, status)
	if p.terminal {
		fmt.Fprintf(p.writer, "\r\033[K%s", line)
		p.lineActive = true
		if final {
			fmt.Fprint(p.writer, "\n")
			p.lineActive = false
		}
	} else {
		fmt.Fprintln(p.writer, line)
	}
}

func (p *ConsoleProgress) snapshotLocked(final bool, status string) string {
	if p.terminal {
		return p.compactSnapshotLocked(final, status)
	}
	if p.stage == "metadata" && !final {
		m := p.metadata
		return fmt.Sprintf("Metadata processed: %s | Fetched: %d | Cached: %d | Failed: %d", completedText(m.Fetched+m.Cached+m.Failed, m.Total, true), m.Fetched, m.Cached, m.Failed)
	}
	m := p.items
	completed := m.Downloaded + m.Synced + m.Skipped + m.Failed
	prefix := ""
	if final {
		prefix = status + " | "
	}
	line := fmt.Sprintf("%sItems completed: %s | Downloaded: %d | Synced: %d | Skipped: %d | Failed: %d", prefix, completedText(completed, m.Total, !final || status == "Run complete"), m.Downloaded, m.Synced, m.Skipped, m.Failed)
	if final {
		return line
	}
	if p.metadata.Total > 0 {
		metaDone := p.metadata.Fetched + p.metadata.Cached + p.metadata.Failed
		if metaDone < p.metadata.Total {
			line += fmt.Sprintf(" | Metadata processed: %d/%d", metaDone, p.metadata.Total)
		}
	}
	keys := make([]string, 0, len(p.active))
	for key := range p.active {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	line += fmt.Sprintf(" | Active: %d", len(keys))
	if len(keys) == 0 {
		return line
	}
	evt := p.active[keys[0]]
	label := evt.SeriesDescription
	if label == "" {
		label = evt.SeriesInstanceUID
	}
	label = safeConsoleLabel(label)
	done, total, phase := evt.BytesDownloaded, evt.BytesTotal, "Download"
	if evt.Status == seriesStatusDecompressing {
		done, total, phase = evt.UncompressedBytes, evt.UncompressedTotal, "Unpacked"
	} else if evt.Status == seriesStatusMetadata {
		return line + " | File: " + label + " | Saving metadata"
	}
	if total > 0 {
		percent := min(99.9, max(0, 100*float64(done)/float64(total)))
		return fmt.Sprintf("%s | File: %s | %s: %s/%s (%.1f%%)", line, label, phase, formatBytes(done), formatBytes(total), percent)
	}
	return fmt.Sprintf("%s | File: %s | %s: %s (total pending)", line, label, phase, formatBytes(done))
}

// Keep terminal updates within a conservative 78-column line so an ordinary
// 80-column terminal does not wrap and leave a trail of previous updates.
// Completed-item counts and active transfer bytes take priority over labels.
func (p *ConsoleProgress) compactSnapshotLocked(final bool, status string) string {
	const width = 78
	m := p.items
	if p.stage == "metadata" && !final {
		m = p.metadata
		return fmt.Sprintf("Metadata %d/%d | Fetched %d Cached %d Failed %d", m.Fetched+m.Cached+m.Failed, m.Total, m.Fetched, m.Cached, m.Failed)
	}
	done := m.Downloaded + m.Synced + m.Skipped + m.Failed
	if final {
		return fmt.Sprintf("%s | Items %d/%d | Downloaded %d Synced %d Skipped %d Failed %d", status, done, m.Total, m.Downloaded, m.Synced, m.Skipped, m.Failed)
	}
	base := "Items " + completedText(done, m.Total, true)
	keys := make([]string, 0, len(p.active))
	for key := range p.active {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	base += fmt.Sprintf(" | Active %d", len(keys))
	if len(keys) == 0 {
		return base
	}
	evt := p.active[keys[0]]
	label := evt.SeriesDescription
	if label == "" {
		label = evt.SeriesInstanceUID
	}
	label = safeConsoleLabel(label)
	transferred, total, phase := evt.BytesDownloaded, evt.BytesTotal, "File"
	if evt.Status == seriesStatusDecompressing {
		transferred, total, phase = evt.UncompressedBytes, evt.UncompressedTotal, "Unpack"
	}
	activity := phase + " " + formatBytes(transferred) + " (total pending)"
	if total > 0 {
		activity = fmt.Sprintf("%s %s/%s (%.1f%%)", phase, formatBytes(transferred), formatBytes(total), min(99.9, max(0, 100*float64(transferred)/float64(total))))
	}
	if evt.Status == seriesStatusMetadata {
		activity = "Saving metadata"
	}
	line := base + " | " + activity
	if remaining := width - len([]rune(line)) - 3; remaining > 4 {
		name := []rune(label)
		if len(name) > remaining {
			name = append(name[:remaining-3], '.', '.', '.')
		}
		line += " | " + string(name)
	}
	// Extremely large numeric values can exceed the conservative width;
	// remove optional spacing first, preserving counts and transfer values.
	if len([]rune(line)) > width {
		line = strings.ReplaceAll(line, " | ", "|")
	}
	return line
}

func completedText(done, total int32, percent bool) string {
	if total <= 0 {
		return fmt.Sprintf("%d (total pending)", done)
	}
	text := fmt.Sprintf("%d/%d", done, total)
	if percent {
		text += fmt.Sprintf(" (%.1f%%)", min(100, max(0, 100*float64(done)/float64(total))))
	}
	return text
}

func safeConsoleLabel(s string) string {
	runes := []rune(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s))
	if len(runes) > 60 {
		return string(runes[:57]) + "..."
	}
	return string(runes)
}

func formatBytes(bytes int64) string {
	value := float64(max(int64(0), bytes))
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	u := 0
	for value >= 1024 && u < len(units)-1 {
		value /= 1024
		u++
	}
	if u == 0 {
		return fmt.Sprintf("%.0f %s", value, units[u])
	}
	return fmt.Sprintf("%.1f %s", value, units[u])
}
