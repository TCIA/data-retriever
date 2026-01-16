package app

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Summary captures the outcome of a download run.
type Summary struct {
	Total      int32
	Downloaded int32
	Skipped    int32
	Failed     int32
	Elapsed    time.Duration
}

// Callbacks allows callers to intercept CLI-style output.
type Callbacks struct {
	Stdout func(string)
	Stderr func(string)
	Series func(SeriesEvent)
}

func (cb Callbacks) emitStdout(msg string) {
	if cb.Stdout != nil {
		cb.Stdout(msg)
		return
	}
	fmt.Fprint(os.Stdout, msg)
}

func (cb Callbacks) emitStderr(msg string) {
	if cb.Stderr != nil {
		cb.Stderr(msg)
		return
	}
	fmt.Fprint(os.Stderr, msg)
}

func (cb Callbacks) emitSeries(evt SeriesEvent) {
	if cb.Series != nil {
		cb.Series(evt)
	}
}

// SeriesEvent represents a lifecycle update for a series download.
type SeriesEvent struct {
	SeriesInstanceUID         string    `json:"seriesUID"`
	StudyInstanceUID          string    `json:"studyUID,omitempty"`
	PatientID         string    `json:"subjectID,omitempty"`
	SeriesDescription string    `json:"seriesDescription,omitempty"`
	Modality          string    `json:"modality,omitempty"`
	Status            string    `json:"status"`
	Progress          float64   `json:"progress,omitempty"`
	BytesDownloaded   int64     `json:"bytesDownloaded,omitempty"`
	BytesTotal        int64     `json:"bytesTotal,omitempty"`
	Message           string    `json:"message,omitempty"`
	Timestamp         time.Time `json:"timestamp"`
}

func newSeriesEvent(file *FileInfo, status, message string, progress float64) SeriesEvent {
	progress = clampProgress(progress)
	if file == nil {
		return SeriesEvent{
			Status:    status,
			Message:   message,
			Progress:  progress,
			Timestamp: time.Now(),
		}
	}

	return SeriesEvent{
		SeriesInstanceUID:         file.SeriesInstanceUID,
		StudyInstanceUID:          file.StudyInstanceUID,
		PatientID:         file.PatientID,
		SeriesDescription: file.SeriesDescription,
		Modality:          file.Modality,
		Status:            status,
		Progress:          progress,
		Message:           message,
		Timestamp:         time.Now(),
	}
}

func clampProgress(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

// DownloadStats tracks download statistics across workers.
type DownloadStats struct {
	Total          int32
	Downloaded     int32
	Skipped        int32
	Failed         int32
	StartTime      time.Time
	LastUpdate     time.Time
	LastPercentage int
	mu             sync.Mutex
}

// WorkerContext bundles worker dependencies.
type WorkerContext struct {
	Context    context.Context
	HTTPClient *http.Client
	AuthToken  *Token
	Options    *Options
	Stats      *DownloadStats
	WorkerID   int
	Callbacks  Callbacks
}

// Run executes the shared download workflow.
func Run(ctx context.Context, options *Options, callbacks Callbacks) (*Summary, error) {
	if options == nil {
		return nil, errors.New("options cannot be nil")
	}

	if Logger == nil {
		logPath := ""
		if options.SaveLog {
			logPath = filepath.Join(options.Output, "progress.log")
		}
		setLogger(options.Debug, logPath)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	client := newClient(options.Proxy, options.MaxConnsPerHost)

	if err := os.MkdirAll(options.Output, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	tokenPath := filepath.Join(options.Output, fmt.Sprintf("%s.json", options.Username))
	token, err := NewToken(options.Username, options.Password, tokenPath, client)
	if err != nil {
		return nil, err
	}

	if err := createMetadataDir(options.Output); err != nil {
		return nil, fmt.Errorf("failed to create metadata directory: %w", err)
	}

	files, err := decodeInputFile(ctx, options.Input, client, token, options, callbacks)
	if err != nil {
		return nil, fmt.Errorf("failed to decode input file: %w", err)
	}

	seenQueued := make(map[string]struct{})
	for _, f := range files {
		if f == nil || f.SeriesInstanceUID == "" {
			continue
		}
		if _, ok := seenQueued[f.SeriesInstanceUID]; ok {
			continue
		}
		seenQueued[f.SeriesInstanceUID] = struct{}{}
		callbacks.emitSeries(newSeriesEvent(f, "queued", "Queued for download", 0))
	}

	ext := strings.ToLower(filepath.Ext(options.Input))
	if ext == ".csv" || ext == ".tsv" || ext == ".xlsx" {
		metaDir := filepath.Join(options.Output, "metadata")
		if err := os.MkdirAll(metaDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create metadata directory: %w", err)
		}
		destPath := filepath.Join(metaDir, filepath.Base(options.Input))
		if err := copyFile(options.Input, destPath); err != nil {
			Logger.Warnf("Failed to copy spreadsheet to metadata folder: %v", err)
		}
	}

	stats := &DownloadStats{Total: int32(len(files)), StartTime: time.Now()}

	if options.Debug {
		Logger.Infof("Starting download of %d series with %d workers", len(files), options.Concurrent)
	} else {
		callbacks.emitStderr(fmt.Sprintf("\nDownloading %d series with %d workers...\n\n", len(files), options.Concurrent))
	}

	workerCtx := WorkerContext{
		Context:    ctx,
		HTTPClient: client,
		AuthToken:  token,
		Options:    options,
		Stats:      stats,
		Callbacks:  callbacks,
	}

	summary := &Summary{Total: int32(len(files))}

	var wg sync.WaitGroup
	inputChan := make(chan *FileInfo, len(files))

	for i := 0; i < options.Concurrent; i++ {
		wc := workerCtx
		wc.WorkerID = i + 1
		wg.Add(1)
		go func(ctx WorkerContext) {
			defer wg.Done()
			ctx.processFiles(inputChan)
		}(wc)
	}

	for _, f := range files {
		select {
		case <-ctx.Done():
			callbacks.emitStderr("\nDownload cancelled by user\n")
			close(inputChan)
			wg.Wait()
			stats.Failed += int32(len(files)) - stats.Downloaded - stats.Skipped - stats.Failed
			summary.Downloaded = stats.Downloaded
			summary.Skipped = stats.Skipped
			summary.Failed = stats.Failed
			summary.Elapsed = time.Since(stats.StartTime)
			return summary, ctx.Err()
		case inputChan <- f:
		}
	}
	close(inputChan)
	wg.Wait()

	callbacks.emitProgress(stats, "Complete", options.Debug)
	if !options.Debug {
		callbacks.emitStderr("\n")
	}

	summary.Downloaded = stats.Downloaded
	summary.Skipped = stats.Skipped
	summary.Failed = stats.Failed
	summary.Elapsed = time.Since(stats.StartTime)

	callbacks.emitStdout("\n=== Download Summary ===\n")
	callbacks.emitStdout(fmt.Sprintf("Total files: %d\n", summary.Total))
	callbacks.emitStdout(fmt.Sprintf("Downloaded: %d\n", summary.Downloaded))
	callbacks.emitStdout(fmt.Sprintf("Skipped: %d\n", summary.Skipped))
	callbacks.emitStdout(fmt.Sprintf("Failed: %d\n", summary.Failed))
	callbacks.emitStdout(fmt.Sprintf("Total time: %s\n", summary.Elapsed.Round(time.Second)))

	if summary.Total > 0 && summary.Elapsed > 0 {
		rate := float64(summary.Downloaded+summary.Skipped) / summary.Elapsed.Seconds()
		callbacks.emitStdout(fmt.Sprintf("Average rate: %.1f files/second\n", rate))
	}

	if summary.Failed > 0 {
		Logger.Warnf("Some downloads failed. Check the logs above for details.")
	}

	return summary, nil
}

func (wc *WorkerContext) processFiles(input <-chan *FileInfo) {
	for {
		select {
		case <-wc.Context.Done():
			return
		case fileInfo, ok := <-input:
			if !ok {
				return
			}
			wc.handleFile(fileInfo)
		}
	}
}

func (wc *WorkerContext) emitSeriesEvent(fileInfo *FileInfo, status, message string, progress float64) {
	wc.Callbacks.emitSeries(newSeriesEvent(fileInfo, status, message, progress))
}

func (wc *WorkerContext) handleFile(fileInfo *FileInfo) {
	updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)

	isSpreadsheetInput := fileInfo.DownloadURL != ""

	if wc.Options.Meta {
		wc.emitSeriesEvent(fileInfo, "metadata", fmt.Sprintf("[Worker %d] Fetching metadata", wc.WorkerID), 25)
		wc.handleMetadataOnly(fileInfo, isSpreadsheetInput)
		return
	}

	wc.emitSeriesEvent(fileInfo, "downloading", fmt.Sprintf("[Worker %d] Preparing download", wc.WorkerID), 25)

	if wc.Options.SkipExisting && !fileInfo.NeedsDownload(wc.Options.Output, false, wc.Options.NoDecompress) {
		Logger.Debugf("[Worker %d] Skip existing %s", wc.WorkerID, fileInfo.SeriesInstanceUID)
		atomic.AddInt32(&wc.Stats.Skipped, 1)
		updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)
		wc.emitSeriesEvent(fileInfo, "skipped", "Series already present (skip existing)", 100)
		return
	}

	if !fileInfo.NeedsDownload(wc.Options.Output, wc.Options.Force, wc.Options.NoDecompress) {
		Logger.Debugf("[Worker %d] Skip %s (already exists with correct size/checksum)", wc.WorkerID, fileInfo.SeriesInstanceUID)
		atomic.AddInt32(&wc.Stats.Skipped, 1)
		updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)
		wc.emitSeriesEvent(fileInfo, "skipped", "Series already present with expected size", 100)
		return
	}

	if wc.Context.Err() != nil {
		wc.emitSeriesEvent(fileInfo, "cancelled", "Download cancelled", 100)
		return
	}

	wc.emitSeriesEvent(fileInfo, "downloading", fmt.Sprintf("[Worker %d] Download started", wc.WorkerID), 0)

	// Create progress callback that emits series events with bytes info
	onProgress := func(percent float64, bytesDownloaded int64, bytesTotal int64) {
		// Build event with bytes for frontend display; omit message to avoid log spam
		evt := SeriesEvent{
			SeriesInstanceUID:  fileInfo.SeriesInstanceUID,
			StudyInstanceUID:   fileInfo.StudyInstanceUID,
			PatientID:          fileInfo.PatientID,
			SeriesDescription:  fileInfo.SeriesDescription,
			Modality:           fileInfo.Modality,
			Status:             "downloading",
			Progress:           clampProgress(percent),
			BytesDownloaded:    bytesDownloaded,
			BytesTotal:         bytesTotal,
			Timestamp:          time.Now(),
		}
		wc.Callbacks.emitSeries(evt)
	}

	err := fileInfo.Download(wc.Context, wc.Options.Output, wc.HTTPClient, wc.AuthToken, wc.Options, onProgress)
	if err != nil {
		Logger.Warnf("[Worker %d] Download %s failed - %s", wc.WorkerID, fileInfo.SeriesInstanceUID, err)
		atomic.AddInt32(&wc.Stats.Failed, 1)
		updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)
		wc.emitSeriesEvent(fileInfo, "failed", err.Error(), 100)
		return
	}

	if !isSpreadsheetInput {
		if err := fileInfo.GetMeta(wc.Context, wc.Options.Output); err != nil {
			Logger.Warnf("[Worker %d] Save meta info %s failed - %s", wc.WorkerID, fileInfo.SeriesInstanceUID, err)
		}
	}

	atomic.AddInt32(&wc.Stats.Downloaded, 1)
	updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)
	wc.emitSeriesEvent(fileInfo, "succeeded", "Download completed", 100)
}

func (wc *WorkerContext) handleMetadataOnly(fileInfo *FileInfo, isSpreadsheetInput bool) {
	if isSpreadsheetInput {
		Logger.Debugf("[Worker %d] Skipping metadata for spreadsheet entry %s", wc.WorkerID, fileInfo.SeriesInstanceUID)
		atomic.AddInt32(&wc.Stats.Skipped, 1)
		updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)
		wc.emitSeriesEvent(fileInfo, "skipped", "Spreadsheet inputs do not expose metadata", 100)
		return
	}

	wc.emitSeriesEvent(fileInfo, "metadata", fmt.Sprintf("[Worker %d] Saving metadata", wc.WorkerID), 60)
	if err := fileInfo.GetMeta(wc.Context, wc.Options.Output); err != nil {
		Logger.Warnf("[Worker %d] Save meta info %s failed - %s", wc.WorkerID, fileInfo.SeriesInstanceUID, err)
		atomic.AddInt32(&wc.Stats.Failed, 1)
		wc.emitSeriesEvent(fileInfo, "failed", err.Error(), 100)
	} else {
		atomic.AddInt32(&wc.Stats.Downloaded, 1)
		wc.emitSeriesEvent(fileInfo, "succeeded", "Metadata saved", 100)
	}
	updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)
}

func updateProgress(stats *DownloadStats, currentSeriesID string, debugMode bool, callbacks Callbacks) {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	now := time.Now()
	if now.Sub(stats.LastUpdate) < 200*time.Millisecond {
		return
	}
	stats.LastUpdate = now

	processed := atomic.LoadInt32(&stats.Downloaded) + atomic.LoadInt32(&stats.Skipped) + atomic.LoadInt32(&stats.Failed)
	percentage := float64(processed)
	if stats.Total > 0 {
		percentage = percentage / float64(stats.Total) * 100
	}

	elapsed := time.Since(stats.StartTime)
	var eta string
	if stats.Downloaded > 0 && elapsed > 0 {
		rate := float64(stats.Downloaded) / elapsed.Seconds()
		remainingFiles := float64(stats.Total - stats.Downloaded - stats.Skipped - stats.Failed)
		if remainingFiles > 0 && rate > 0 {
			remainingTime := remainingFiles / rate
			etaDuration := time.Duration(remainingTime * float64(time.Second))
			eta = fmt.Sprintf(" | ETA: %s", etaDuration.Round(time.Second))
		}
	}

	displayID := currentSeriesID
	if len(displayID) > 30 {
		displayID = displayID[:30] + "..."
	}

	callbacks.emitStderr(fmt.Sprintf("\r\033[K[%d/%d] %.1f%% | Downloaded: %d | Skipped: %d | Failed: %d%s | Current: %s",
		processed, stats.Total, percentage,
		stats.Downloaded, stats.Skipped, stats.Failed,
		eta, displayID))
}

func (callbacks Callbacks) emitProgress(stats *DownloadStats, currentSeriesID string, debugMode bool) {
	updateProgress(stats, currentSeriesID, debugMode, callbacks)
}

func decodeInputFile(ctx context.Context, filePath string, client *http.Client, token *Token, options *Options, callbacks Callbacks) ([]*FileInfo, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".tcia":
		files := decodeTCIA(ctx, filePath, client, token, options, callbacks)
		return files, nil
	case ".csv", ".tsv", ".xlsx":
		return decodeSpreadsheet(filePath)
	default:
		return nil, fmt.Errorf("unsupported input file format: %s", ext)
	}
}
