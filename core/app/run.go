package app

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Summary captures the outcome of a download run.
type Summary struct {
	Total      int32
	Downloaded int32
	Synced     int32
	Skipped    int32
	Failed     int32
	Elapsed    time.Duration
}

// Callbacks allows callers to intercept CLI-style output.
type Callbacks struct {
	Stdout    func(string)
	Stderr    func(string)
	Series    func(SeriesEvent)
	Manifest  func(ManifestPayload)
	EmitEvent func(name string, data ...interface{})
}

func (cb Callbacks) emitEvent(name string, data ...interface{}) {
	if cb.EmitEvent != nil {
		cb.EmitEvent(name, data...)
	}
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

func (cb Callbacks) emitManifest(payload ManifestPayload) {
	if cb.Manifest != nil {
		cb.Manifest(payload)
	}
}

type ManifestPayload struct {
	ManifestPath string                 `json:"manifestPath,omitempty"`
	Timestamp    string                 `json:"timestamp,omitempty"`
	Series       []ManifestSeriesRecord `json:"series"`
}

type ManifestSeriesRecord struct {
	SeriesUID         string `json:"seriesUID"`
	BytesTotal        int64  `json:"bytesTotal,omitempty"`
	SeriesDescription string `json:"seriesDescription,omitempty"`
	StudyUID          string `json:"studyUID,omitempty"`
	SubjectID         string `json:"subjectID,omitempty"`
	Modality          string `json:"modality,omitempty"`
}

// SeriesEvent represents a lifecycle update for a series download.
type SeriesEvent struct {
	SeriesInstanceUID string    `json:"seriesUID"`
	StudyInstanceUID  string    `json:"studyUID,omitempty"`
	PatientID         string    `json:"subjectID,omitempty"`
	SeriesDescription string    `json:"seriesDescription,omitempty"`
	Modality          string    `json:"modality,omitempty"`
	Status            string    `json:"status"`
	Progress          float64   `json:"progress,omitempty"`
	Phase             string    `json:"phase,omitempty"`
	PhaseProgress     float64   `json:"phaseProgress,omitempty"`
	BytesDownloaded   int64     `json:"bytesDownloaded,omitempty"`
	BytesTotal        int64     `json:"bytesTotal,omitempty"`
	UncompressedBytes int64     `json:"uncompressedBytes,omitempty"`
	UncompressedTotal int64     `json:"uncompressedTotal,omitempty"`
	Message           string    `json:"message,omitempty"`
	Timestamp         time.Time `json:"timestamp"`
}

const (
	seriesPhaseQueued     = "queued"
	seriesPhaseMetadata   = "metadata"
	seriesPhaseDownload   = "download"
	seriesPhaseDecompress = "decompress"
	seriesPhaseComplete   = "complete"
	seriesPhaseFailure    = "failed"

	seriesStatusQueued            = "queued"
	seriesStatusWorkerInitiated   = "worker-initiated"
	seriesStatusPreCheck          = "pre-check"
	seriesStatusMetadata          = "metadata"
	seriesStatusDownloadInitiated = "download-initiated"
	seriesStatusDownloading       = "downloading"
	seriesStatusDecompressing     = "decompressing"
	seriesStatusSucceeded         = "succeeded"
	seriesStatusFailed            = "failed"
	seriesStatusCancelled         = "cancelled"
	seriesStatusSkipped           = "skipped"

	downloadHeartbeatInterval = 5 * time.Second
)

func seriesDisplayLabel(file *FileInfo) string {
	if file == nil {
		return "series"
	}

	if description := strings.TrimSpace(file.SeriesDescription); description != "" {
		return description
	}

	if seriesUID := strings.TrimSpace(file.SeriesInstanceUID); seriesUID != "" {
		return seriesUID
	}

	return "series"
}

func emitManifestMetadata(callbacks Callbacks, manifestPath string, files []*FileInfo) {
	if callbacks.Manifest == nil || len(files) == 0 {
		return
	}

	payload := ManifestPayload{
		ManifestPath: manifestPath,
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		Series:       make([]ManifestSeriesRecord, 0, len(files)),
	}

	seen := make(map[string]struct{})
	for _, file := range files {
		if file == nil {
			continue
		}

		seriesUID := strings.TrimSpace(file.SeriesInstanceUID)
		if seriesUID == "" {
			continue
		}
		if _, duplicate := seen[seriesUID]; duplicate {
			continue
		}
		seen[seriesUID] = struct{}{}

		record := ManifestSeriesRecord{
			SeriesUID:         seriesUID,
			SeriesDescription: strings.TrimSpace(file.SeriesDescription),
			StudyUID:          strings.TrimSpace(file.StudyInstanceUID),
			SubjectID:         strings.TrimSpace(file.PatientID),
			Modality:          strings.TrimSpace(file.Modality),
		}

		if bytes := parseManifestBytes(file.FileSize); bytes > 0 {
			record.BytesTotal = bytes
		}

		payload.Series = append(payload.Series, record)
	}

	if len(payload.Series) == 0 {
		return
	}

	callbacks.emitManifest(payload)
}

func parseManifestBytes(raw string) int64 {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0
	}

	clean := strings.ReplaceAll(trimmed, ",", "")
	if value, err := strconv.ParseInt(clean, 10, 64); err == nil {
		if value > 0 {
			return value
		}
		return 0
	}

	if value, err := strconv.ParseFloat(clean, 64); err == nil {
		if value > 0 {
			return int64(value)
		}
	}

	return 0
}

func newSeriesEvent(file *FileInfo, status, message string, progress float64) SeriesEvent {
	progress = clampProgress(progress)
	if file == nil {
		return SeriesEvent{
			Status:   status,
			Message:  message,
			Progress: progress,
			Phase:    resolvePhase(status),
			PhaseProgress: func() float64 {
				if isTerminalSeriesStatus(status) {
					return 100
				}
				return progress
			}(),
			Timestamp: time.Now(),
		}
	}

	return SeriesEvent{
		SeriesInstanceUID: file.SeriesInstanceUID,
		StudyInstanceUID:  file.StudyInstanceUID,
		PatientID:         file.PatientID,
		SeriesDescription: file.SeriesDescription,
		Modality:          file.Modality,
		Status:            status,
		Progress:          progress,
		Phase:             resolvePhase(status),
		PhaseProgress: func() float64 {
			if isTerminalSeriesStatus(status) {
				return 100
			}
			return progress
		}(),
		Message:   message,
		Timestamp: time.Now(),
	}
}

func resolvePhase(status string) string {
	switch status {
	case seriesStatusQueued, seriesStatusWorkerInitiated, seriesStatusPreCheck:
		return seriesPhaseQueued
	case seriesStatusMetadata:
		return seriesPhaseMetadata
	case seriesStatusDownloadInitiated, seriesStatusDownloading:
		return seriesPhaseDownload
	case seriesStatusDecompressing:
		return seriesPhaseDecompress
	case seriesStatusSucceeded, seriesStatusSkipped:
		return seriesPhaseComplete
	case seriesStatusFailed, seriesStatusCancelled:
		return seriesPhaseFailure
	default:
		return ""
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

type existingSeriesDisposition struct {
	status            string
	completionSkipped bool
	countAsDownloaded bool
}

func resolveExistingSeriesDisposition(resumeMode bool) existingSeriesDisposition {
	if resumeMode {
		return existingSeriesDisposition{
			status:            seriesStatusSucceeded,
			completionSkipped: false,
			countAsDownloaded: true,
		}
	}

	return existingSeriesDisposition{
		status:            seriesStatusSkipped,
		completionSkipped: true,
		countAsDownloaded: false,
	}
}

// DownloadStats tracks download statistics across workers.
type DownloadStats struct {
	Total          int32
	Downloaded     int32
	Synced         int32
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
	Gen3Auth   *Gen3AuthManager
	Options    *Options
	Stats      *DownloadStats
	WorkerID   int
	Callbacks  Callbacks
	EventGate  *SeriesEventGate
	AuthGate   *AuthGate
	EmitEvent  func(string, ...interface{}) // wrap runtime.EventsEmit
	// Semaphore caps how many files are processed at once across every
	// WorkerContext sharing it (see Options.Semaphore). nil means
	// unlimited beyond the local worker pool size.
	Semaphore *WorkerSemaphore
}

// SeriesEventGate throttles interim per-series events while always allowing
// lifecycle-critical events.
type SeriesEventGate struct {
	interval time.Duration
	mu       sync.Mutex
	lastSent map[string]time.Time
}

func NewSeriesEventGate(interval time.Duration) *SeriesEventGate {
	if interval <= 0 {
		interval = DefaultInterimUpdateInterval
	}

	return &SeriesEventGate{
		interval: interval,
		lastSent: make(map[string]time.Time),
	}
}

func (g *SeriesEventGate) Allow(evt SeriesEvent) bool {
	if g == nil {
		return true
	}

	if alwaysEmitSeriesEvent(evt) || !isInterimSeriesStatus(evt.Status) {
		return true
	}

	ts := evt.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}

	key := evt.SeriesInstanceUID
	if key == "" {
		key = "run"
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if last, ok := g.lastSent[key]; ok {
		if ts.Sub(last) < g.interval {
			return false
		}
	}

	g.lastSent[key] = ts
	return true
}

func isInterimSeriesStatus(status string) bool {
	switch status {
	case seriesStatusMetadata, seriesStatusDownloading, seriesStatusDecompressing:
		return true
	default:
		return false
	}
}

func isTerminalSeriesStatus(status string) bool {
	switch status {
	case seriesStatusSucceeded, seriesStatusFailed, seriesStatusCancelled, seriesStatusSkipped:
		return true
	default:
		return false
	}
}

func alwaysEmitSeriesEvent(evt SeriesEvent) bool {
	if isTerminalSeriesStatus(evt.Status) {
		return true
	}

	if evt.Status == seriesStatusDownloading && strings.Contains(strings.ToLower(evt.Message), "in progress") {
		return true
	}

	switch evt.Status {
	case seriesStatusQueued,
		seriesStatusWorkerInitiated,
		seriesStatusPreCheck,
		seriesStatusMetadata,
		seriesStatusDownloadInitiated:
		return true
	default:
		return false
	}
}

// Run executes the shared download workflow.
func Run(ctx context.Context, options *Options, callbacks Callbacks) (*Summary, error) {
	if options == nil {
		return nil, errors.New("options cannot be nil")
	}
	if options.InterimUpdateInterval <= 0 {
		options.InterimUpdateInterval = DefaultInterimUpdateInterval
	}

	// Always (re)initialise the logger so per-run verbose/debug flags and an
	// optional GUI LogSink take effect even after earlier runs configured it.
	setLogger(options.Debug, options.Verbose, "", options.LogSink)

	if ctx == nil {
		ctx = context.Background()
	}

	client := options.SharedHTTPClient
	if client == nil {
		client = newClient(options.Proxy, options.MaxConnsPerHost)
	}

	if err := os.MkdirAll(options.Output, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	if err := createMetadataDir(options.Output); err != nil {
		return nil, fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Load the s5cmd series map
	s5cmdMap, err := loadS5cmdSeriesMapFromCSVs(options.Output)
	if err != nil {
		return nil, fmt.Errorf("Failed to load s5cmd series map from CSVs: %v", err)
	}

	ext := strings.ToLower(filepath.Ext(options.Input))

	// For .tcia manifests, run a single pass that classifies each line: IDC
	// s5cmd lines and bare UIDs resolvable via the parquet index produce
	// pre-built FileInfos; bare UIDs not in the parquet are routed through the
	// TCIA streaming/batch path. When both paths produce work, queue the
	// s5cmd jobs first and stream the TCIA batches behind them so workers can
	// start immediately on the s5cmd side while TCIA metadata is fetched.
	if ext == ".tcia" {
		s5Files, _, tciaUIDs := decodeS5cmd(options.Input, options.Output, s5cmdMap, callbacks, options)
		if len(s5Files) == 0 && len(tciaUIDs) == 0 {
			// Nothing parsed as either IDC or bare-UID — try the lenient
			// TCIA reader (handles edge-case manifests decodeS5cmd skipped).
			seriesCount, fileChan, serr := decodeTCIAStreaming(ctx, options.Input, client, options, callbacks)
			if serr != nil {
				return nil, fmt.Errorf("failed to decode tcia file: %w", serr)
			}
			return runTCIAStreamingDownload(ctx, client, options, callbacks, seriesCount, fileChan)
		}
		if len(tciaUIDs) > 0 {
			// Partial pre-check: TCIA-side sizes aren't known until metadata
			// streams in, so this only catches shortfalls already evident
			// from the s5cmd portion.
			if spaceErr := checkDiskSpace(options.Output, s5Files); spaceErr != nil {
				return nil, spaceErr
			}
			seriesCount, fileChan := streamFilesFromSeriesIDs(ctx, s5Files, tciaUIDs, client, options, callbacks)
			return runTCIAStreamingDownload(ctx, client, options, callbacks, seriesCount, fileChan)
		}
		// Pure IDC .tcia — fall through to the standard path; decodeInputFile
		// will re-run decodeS5cmd against the already-built in-memory map.
	}

	// Spreadsheets with a SeriesInstanceUID column: classify each UID against
	// the parquet index. IDC-known UIDs become s5cmd jobs; the rest stream
	// through the TCIA path. When both paths produce work, queue s5cmd jobs
	// first so workers can start while TCIA metadata is still being fetched.
	if ext == ".csv" || ext == ".tsv" || ext == ".xlsx" {
		s5Files, _, tciaUIDs, splitErr := decodeSpreadsheetSplit(options.Input, options.Output, s5cmdMap, callbacks, options)
		if splitErr == nil {
			metaDir := filepath.Join(options.Output, "metadata")
			if mkErr := os.MkdirAll(metaDir, 0755); mkErr != nil {
				return nil, fmt.Errorf("failed to create metadata directory: %w", mkErr)
			}
			destPath := filepath.Join(metaDir, filepath.Base(options.Input))
			if copyErr := copyFile(options.Input, destPath); copyErr != nil {
				Logger.Warnf("Failed to copy spreadsheet to metadata folder: %v", copyErr)
			}

			// Partial pre-check: TCIA-side sizes aren't known until metadata
			// streams in, so this only catches shortfalls already evident
			// from the s5cmd portion (sizes from the parquet index).
			if spaceErr := checkDiskSpace(options.Output, s5Files); spaceErr != nil {
				return nil, spaceErr
			}

			seriesCount, fileChan := streamFilesFromSeriesIDs(ctx, s5Files, tciaUIDs, client, options, callbacks)
			return runTCIAStreamingDownload(ctx, client, options, callbacks, seriesCount, fileChan)
		} else if splitErr != ErrSeriesInstanceUIDColumnNotFound {
			return nil, fmt.Errorf("could not get series UIDs from spreadsheet: %w", splitErr)
		}
		// No SeriesInstanceUID column — fall through to the standard path
		// which handles raw spreadsheet rows (drs_uri, imageUrl, etc.).
	}

	files, _, err := decodeInputFile(ctx, options.Input, client, options, callbacks, s5cmdMap)
	if err != nil {
		return nil, fmt.Errorf("failed to decode input file: %w", err)
	}

	if spaceErr := checkDiskSpace(options.Output, files); spaceErr != nil {
		return nil, spaceErr
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
		callbacks.emitSeries(newSeriesEvent(f, seriesStatusQueued, fmt.Sprintf("Queued for download: %s", seriesDisplayLabel(f)), 0))
	}

	if ext == ".csv" || ext == ".tsv" || ext == ".xlsx" {
		metaDir := filepath.Join(options.Output, "metadata")
		if err := os.MkdirAll(metaDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create metadata directory: %w", err)
		}
		destPath := filepath.Join(metaDir, filepath.Base(options.Input))
		if err := copyFile(options.Input, destPath); err != nil {
			Logger.Warnf("Failed to copy spreadsheet to metadata folder: %v", err)
		}
		InitCompletionStatus(options.Output, files)
	}

	stats := &DownloadStats{Total: int32(len(files)), StartTime: time.Now()}
	callbacks.emitEvent("series-total-known", len(files))

	itemType := "items"
	if len(files) > 0 {
		if files[0].S5cmdManifestPath != "" {
			itemType = "series"
		} else if files[0].DRSURI != "" || files[0].DownloadURL != "" {
			itemType = "files"
		}
	}

	Logger.Infof("Starting download of %d %s with %d workers", len(files), itemType, options.Concurrent)
	if !options.Debug && !options.Verbose {
		callbacks.emitStderr(fmt.Sprintf("\nDownloading %d %s with %d workers...\n\n", len(files), itemType, options.Concurrent))
	}

	// Create Gen3 Auth Manager
	gen3Auth, err := NewGen3AuthManager(client, options.Auth)
	if err != nil {
		Logger.Warnf("Failed to initialize Gen3 auth manager: %v", err)
		gen3Auth = &Gen3AuthManager{}
	}

	workerCtx := WorkerContext{
		Context:    ctx,
		HTTPClient: client,
		Gen3Auth:   gen3Auth,
		Options:    options,
		Stats:      stats,
		Callbacks:  callbacks,
		EventGate:  NewSeriesEventGate(options.InterimUpdateInterval),
		AuthGate:   &AuthGate{},
		Semaphore:  options.Semaphore,
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

	MergeCompletionStatus(options.Output)

	callbacks.emitProgress(stats, "Complete", options.Debug)
	if !options.Debug {
		callbacks.emitStderr("\n")
	}

	summary.Downloaded = stats.Downloaded
	summary.Synced = stats.Synced
	summary.Skipped = stats.Skipped
	summary.Failed = stats.Failed
	summary.Elapsed = time.Since(stats.StartTime)

	callbacks.emitStdout("\n=== Download Summary ===\n")
	callbacks.emitStdout(fmt.Sprintf("Total items: %d\n", summary.Total))
	callbacks.emitStdout(fmt.Sprintf("Downloaded: %d\n", summary.Downloaded))
	callbacks.emitStdout(fmt.Sprintf("Synced: %d\n", summary.Synced))
	callbacks.emitStdout(fmt.Sprintf("Skipped: %d\n", summary.Skipped))
	callbacks.emitStdout(fmt.Sprintf("Failed: %d\n", summary.Failed))
	callbacks.emitStdout(fmt.Sprintf("Total time: %s\n", summary.Elapsed.Round(time.Second)))

	if summary.Total > 0 && summary.Elapsed > 0 {
		rate := float64(summary.Downloaded+summary.Synced+summary.Skipped) / summary.Elapsed.Seconds()
		callbacks.emitStdout(fmt.Sprintf("Average rate: %.1f items/second\n", rate))
	}

	if summary.Failed > 0 {
		Logger.Warnf("Some downloads failed. Check the logs above for details.")
	}

	return summary, nil
}

// runTCIAStreamingDownload runs the worker-pool download pipeline against a
// channel of FileInfo values produced by streaming metadata fetches. Workers
// begin downloading as soon as the first batch of metadata arrives.
func runTCIAStreamingDownload(ctx context.Context, client *http.Client, options *Options, callbacks Callbacks, seriesCount int, fileChan <-chan *FileInfo) (*Summary, error) {
	Logger.Infof("Starting streaming download of ~%d series with %d workers", seriesCount, options.Concurrent)
	if !options.Debug && !options.Verbose {
		callbacks.emitStderr(fmt.Sprintf("\nDownloading ~%d series with %d workers...\n\n", seriesCount, options.Concurrent))
	}

	gen3Auth, gaErr := NewGen3AuthManager(client, options.Auth)
	if gaErr != nil {
		Logger.Warnf("Failed to initialize Gen3 auth manager: %v", gaErr)
		gen3Auth = &Gen3AuthManager{}
	}

	stats := &DownloadStats{Total: int32(seriesCount), StartTime: time.Now()}
	summary := &Summary{Total: int32(seriesCount)}
	// Sent before any per-series events so the UI can pin its progress
	// denominator to the true total instead of however many series have
	// streamed in so far — otherwise a batch of instant "already exists"
	// skips can make progress read 100% while later batches are still
	// being fetched from TCIA, then drop back down once they arrive.
	callbacks.emitEvent("series-total-known", seriesCount)

	workerCtx := WorkerContext{
		Context:    ctx,
		HTTPClient: client,
		Gen3Auth:   gen3Auth,
		Options:    options,
		Stats:      stats,
		Callbacks:  callbacks,
		EventGate:  NewSeriesEventGate(options.InterimUpdateInterval),
		AuthGate:   &AuthGate{},
		Semaphore:  options.Semaphore,
	}

	var wg sync.WaitGroup
	bufSize := seriesCount
	if bufSize <= 0 {
		bufSize = 1
	}
	inputChan := make(chan *FileInfo, bufSize)

	for i := 0; i < options.Concurrent; i++ {
		wc := workerCtx
		wc.WorkerID = i + 1
		wg.Add(1)
		go func(wc WorkerContext) {
			defer wg.Done()
			wc.processFiles(inputChan)
		}(wc)
	}

	seen := make(map[string]struct{})
	allFiles := make([]*FileInfo, 0, seriesCount)
	var queued int32
feedLoop:
	for {
		select {
		case <-ctx.Done():
			callbacks.emitStderr("\nDownload cancelled by user\n")
			go func() {
				for range fileChan {
				}
			}()
			close(inputChan)
			wg.Wait()
			stats.Failed += queued - stats.Downloaded - stats.Skipped - stats.Failed
			summary.Downloaded = stats.Downloaded
			summary.Skipped = stats.Skipped
			summary.Failed = stats.Failed
			summary.Elapsed = time.Since(stats.StartTime)
			return summary, ctx.Err()
		case f, ok := <-fileChan:
			if !ok {
				break feedLoop
			}
			allFiles = append(allFiles, f)
			if f != nil && f.SeriesInstanceUID != "" {
				if _, already := seen[f.SeriesInstanceUID]; !already {
					seen[f.SeriesInstanceUID] = struct{}{}
					callbacks.emitSeries(newSeriesEvent(f, seriesStatusQueued, "Queued for download", 0))
				}
			}
			queued++
			inputChan <- f
		}
	}
	close(inputChan)
	wg.Wait()

	emitManifestMetadata(callbacks, options.Input, allFiles)
	MergeCompletionStatus(options.Output)
	callbacks.emitProgress(stats, "Complete", options.Debug)
	if !options.Debug {
		callbacks.emitStderr("\n")
	}
	summary.Downloaded = stats.Downloaded
	summary.Synced = stats.Synced
	summary.Skipped = stats.Skipped
	summary.Failed = stats.Failed
	summary.Elapsed = time.Since(stats.StartTime)

	callbacks.emitStdout("\n=== Download Summary ===\n")
	callbacks.emitStdout(fmt.Sprintf("Total items: %d\n", summary.Total))
	callbacks.emitStdout(fmt.Sprintf("Downloaded: %d\n", summary.Downloaded))
	callbacks.emitStdout(fmt.Sprintf("Synced: %d\n", summary.Synced))
	callbacks.emitStdout(fmt.Sprintf("Skipped: %d\n", summary.Skipped))
	callbacks.emitStdout(fmt.Sprintf("Failed: %d\n", summary.Failed))
	callbacks.emitStdout(fmt.Sprintf("Total time: %s\n", summary.Elapsed.Round(time.Second)))
	if summary.Total > 0 && summary.Elapsed > 0 {
		rate := float64(summary.Downloaded+summary.Synced+summary.Skipped) / summary.Elapsed.Seconds()
		callbacks.emitStdout(fmt.Sprintf("Average rate: %.1f items/second\n", rate))
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
			// Blocks until a global slot is free when Semaphore is shared
			// across multiple concurrently running manifests; a nil
			// Semaphore (CLI) makes this a no-op.
			if err := wc.Semaphore.Acquire(wc.Context); err != nil {
				return
			}
			wc.handleFile(fileInfo)
			wc.Semaphore.Release()
		}
	}
}

func (wc *WorkerContext) emitSeriesEvent(fileInfo *FileInfo, status, message string, progress float64) {
	wc.emitSeries(newSeriesEvent(fileInfo, status, message, progress))
}

func (wc *WorkerContext) emitSeries(evt SeriesEvent) {
	if wc.EventGate != nil && !wc.EventGate.Allow(evt) {
		return
	}
	wc.Callbacks.emitSeries(evt)
}

func (wc *WorkerContext) applyExistingSeriesDisposition(fileInfo *FileInfo, disposition existingSeriesDisposition, message string) {
	if disposition.countAsDownloaded {
		atomic.AddInt32(&wc.Stats.Downloaded, 1)
	} else {
		atomic.AddInt32(&wc.Stats.Skipped, 1)
	}

	updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)
	wc.emitSeriesEvent(fileInfo, disposition.status, message, 100)
	AppendCompletionStatus(wc.Options.Output, fileInfo.SeriesInstanceUID, nil, disposition.completionSkipped)
}

func (wc *WorkerContext) handleFile(fileInfo *FileInfo) {
	updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)
	displayName := seriesDisplayLabel(fileInfo)

	wc.emitSeriesEvent(fileInfo, seriesStatusWorkerInitiated, fmt.Sprintf("[Worker %d] Worker initiated for %s", wc.WorkerID, displayName), 5)

	isSpreadsheetInput := fileInfo.DownloadURL != "" || fileInfo.DRSURI != "" || fileInfo.S5cmdManifestPath != ""

	if wc.Options.Meta {
		wc.emitSeriesEvent(fileInfo, seriesStatusMetadata, fmt.Sprintf("[Worker %d] Metadata fetch initiated for %s", wc.WorkerID, displayName), 25)
		wc.handleMetadataOnly(fileInfo, isSpreadsheetInput)
		return
	}

	wc.emitSeriesEvent(fileInfo, seriesStatusPreCheck, fmt.Sprintf("[Worker %d] Running pre-checks for %s", wc.WorkerID, displayName), 15)

	//handle needs download so that all s5cmd downloads need download.  that way they can sync properly instead of being skipped

	if !fileInfo.IsSyncJob {
		if wc.Options.SkipExisting && !fileInfo.NeedsDownload(wc.Options.Output, false, wc.Options.NoDecompress, wc.Options) {
			Logger.Debugf("[Worker %d] Resume hit existing %s", wc.WorkerID, fileInfo.SeriesInstanceUID)
			wc.applyExistingSeriesDisposition(
				fileInfo,
				resolveExistingSeriesDisposition(true),
				fmt.Sprintf("Series already present (resume complete): %s", displayName),
			)
			return
		}

		if !fileInfo.NeedsDownload(wc.Options.Output, wc.Options.Force, wc.Options.NoDecompress, wc.Options) {
			Logger.Debugf("[Worker %d] Skip %s (already exists with correct size/checksum)", wc.WorkerID, fileInfo.SeriesInstanceUID)
			wc.applyExistingSeriesDisposition(
				fileInfo,
				resolveExistingSeriesDisposition(false),
				fmt.Sprintf("Series already present with expected size: %s", displayName),
			)
			return
		}
	} else {
		Logger.Debugf("this is a sync job")
	}

	if wc.Context.Err() != nil {
		wc.emitSeriesEvent(fileInfo, seriesStatusCancelled, fmt.Sprintf("Download cancelled: %s", displayName), 100)
		return
	}

	wc.emitSeriesEvent(fileInfo, seriesStatusDownloadInitiated, fmt.Sprintf("[Worker %d] Download initiated for %s", wc.WorkerID, displayName), 30)

	var lastCompressedTotal int64
	var lastDownloadHeartbeat time.Time
	// Create progress callback that emits series events with bytes info during download phase
	onProgress := func(percent float64, bytesDownloaded int64, bytesTotal int64) {
		if bytesTotal > 0 {
			if bytesTotal > lastCompressedTotal {
				lastCompressedTotal = bytesTotal
			}
		} else if bytesDownloaded > lastCompressedTotal {
			lastCompressedTotal = bytesDownloaded
		}

		now := time.Now()
		message := ""
		if lastDownloadHeartbeat.IsZero() || now.Sub(lastDownloadHeartbeat) >= downloadHeartbeatInterval {
			message = fmt.Sprintf("[Worker %d] Download of %s in progress", wc.WorkerID, displayName)
			lastDownloadHeartbeat = now
		}

		evt := SeriesEvent{
			SeriesInstanceUID: fileInfo.SeriesInstanceUID,
			StudyInstanceUID:  fileInfo.StudyInstanceUID,
			PatientID:         fileInfo.PatientID,
			SeriesDescription: fileInfo.SeriesDescription,
			Modality:          fileInfo.Modality,
			Status:            seriesStatusDownloading,
			Progress:          clampProgress(percent),
			Phase:             seriesPhaseDownload,
			PhaseProgress:     clampProgress(percent),
			BytesDownloaded:   bytesDownloaded,
			BytesTotal:        bytesTotal,
			Message:           message,
			Timestamp:         now,
		}
		wc.emitSeries(evt)
	}

	var expectedUncompressed int64
	if fileInfo.FileSize != "" {
		if parsed, err := strconv.ParseInt(strings.ReplaceAll(fileInfo.FileSize, ",", ""), 10, 64); err == nil && parsed > 0 {
			expectedUncompressed = parsed
		}
	}

	onDecompress := func(percent float64, bytesUnpacked int64, bytesTotal int64) {
		if bytesTotal <= 0 {
			bytesTotal = expectedUncompressed
		}
		if bytesUnpacked <= 0 && bytesTotal > 0 {
			bytesUnpacked = int64(math.Round((percent / 100.0) * float64(bytesTotal)))
		}
		evt := SeriesEvent{
			SeriesInstanceUID: fileInfo.SeriesInstanceUID,
			StudyInstanceUID:  fileInfo.StudyInstanceUID,
			PatientID:         fileInfo.PatientID,
			SeriesDescription: fileInfo.SeriesDescription,
			Modality:          fileInfo.Modality,
			Status:            seriesStatusDecompressing,
			Progress:          clampProgress(percent),
			Phase:             seriesPhaseDecompress,
			PhaseProgress:     clampProgress(percent),
			BytesDownloaded:   lastCompressedTotal,
			BytesTotal:        lastCompressedTotal,
			UncompressedBytes: bytesUnpacked,
			UncompressedTotal: bytesTotal,
			Timestamp:         time.Now(),
		}
		wc.emitSeries(evt)
	}

	localAuth := *wc.Gen3Auth

	err := fileInfo.Download(wc.Context, wc.Options.Output, wc.HTTPClient, wc.Options, onProgress, onDecompress, &localAuth)

	if err != nil && isAuthError(err) && wc.Options.AuthGate != nil {
		// Distinguish format failure (already caught in Run) vs server rejection
		Logger.Debugf("auth check: err=%v, Auth=%q, isAuthErr=%v", err, wc.Options.Auth, isAuthError(err))

		if wc.Options.Auth != "" {
			if _, fmtErr := NewGen3AuthManager(wc.HTTPClient, wc.Options.Auth); fmtErr != nil {
				wc.Callbacks.emitEvent("auth-error", fmt.Sprintf("Auth file has invalid format: %s", fmtErr.Error()))
			} else {
				wc.Callbacks.emitEvent("auth-error", "Credentials file was rejected by the server (expired or insufficient permissions).")
			}
		}
	}

	for err != nil && isAuthError(err) && wc.Options.AuthGate != nil {
		// Try the saved auth file silently before opening the prompt.
		if savedPath := LoadSavedAuthFilePath(); savedPath != "" && savedPath != wc.Options.Auth {
			if silentAuth, silentErr := NewGen3AuthManager(wc.HTTPClient, savedPath); silentErr == nil {
				wc.Options.Auth = savedPath
				localAuth = *silentAuth
				err = fileInfo.Download(wc.Context, wc.Options.Output, wc.HTTPClient, wc.Options, onProgress, onDecompress, &localAuth)
				if err == nil || !isAuthError(err) {
					continue
				}
			}
		}

		resolvedPath := wc.Options.AuthGate.WaitForAuth(func() {
			wc.Callbacks.emitEvent("open:auth-modal")
		})

		if resolvedPath == "" {
			break
		}

		newAuth, authErr := NewGen3AuthManager(wc.HTTPClient, resolvedPath)
		if authErr != nil {
			wc.Callbacks.emitEvent("auth-error", fmt.Sprintf("Invalid auth file: %s", authErr.Error()))
			wc.Options.AuthGate.PrepareRetry()
			continue
		}

		wc.Options.Auth = resolvedPath
		localAuth = *newAuth
		err = fileInfo.Download(wc.Context, wc.Options.Output, wc.HTTPClient, wc.Options, onProgress, onDecompress, &localAuth)
		if err != nil && isAuthError(err) {
			wc.Callbacks.emitEvent("auth-error", "Credentials file was rejected by the server (expired or insufficient permissions).")
			wc.Options.AuthGate.PrepareRetry()
		}
	}

	if err != nil {
		Logger.Warnf("[Worker %d] Download %s failed - %s", wc.WorkerID, fileInfo.SeriesInstanceUID, err)
		atomic.AddInt32(&wc.Stats.Failed, 1)
		updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)
		wc.emitSeriesEvent(fileInfo, seriesStatusFailed, err.Error(), 100)
		return
	}

	if fileInfo.IsSyncJob {
		atomic.AddInt32(&wc.Stats.Synced, 1)
	} else {
		atomic.AddInt32(&wc.Stats.Downloaded, 1)
	}

	updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)
	wc.emitSeriesEvent(fileInfo, seriesStatusSucceeded, fmt.Sprintf("Download completed: %s", displayName), 100)
	postSeriesCompletionLog(fileInfo)
}

func (wc *WorkerContext) handleMetadataOnly(fileInfo *FileInfo, isSpreadsheetInput bool) {
	displayName := seriesDisplayLabel(fileInfo)

	if isSpreadsheetInput {
		Logger.Debugf("[Worker %d] Skipping metadata for item %s", wc.WorkerID, fileInfo.SeriesInstanceUID)
		atomic.AddInt32(&wc.Stats.Skipped, 1)
		updateProgress(wc.Stats, fileInfo.SeriesInstanceUID, wc.Options.Debug, wc.Callbacks)
		wc.emitSeriesEvent(fileInfo, seriesStatusSkipped, fmt.Sprintf("Spreadsheet inputs do not expose metadata: %s", displayName), 100)
		return
	}

	wc.emitSeriesEvent(fileInfo, seriesStatusMetadata, fmt.Sprintf("[Worker %d] Saving metadata for %s", wc.WorkerID, displayName), 60)
	if err := fileInfo.GetMeta(wc.Context, wc.Options.Output); err != nil {
		Logger.Warnf("[Worker %d] Save meta info %s failed - %s", wc.WorkerID, fileInfo.SeriesInstanceUID, err)
		atomic.AddInt32(&wc.Stats.Failed, 1)
		wc.emitSeriesEvent(fileInfo, seriesStatusFailed, err.Error(), 100)
	} else {
		if fileInfo.IsSyncJob {
			atomic.AddInt32(&wc.Stats.Synced, 1)
		} else {
			atomic.AddInt32(&wc.Stats.Downloaded, 1)
		}
		wc.emitSeriesEvent(fileInfo, seriesStatusSucceeded, fmt.Sprintf("Metadata saved: %s", displayName), 100)
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

	processed := atomic.LoadInt32(&stats.Downloaded) + atomic.LoadInt32(&stats.Synced) + atomic.LoadInt32(&stats.Skipped) + atomic.LoadInt32(&stats.Failed)
	percentage := float64(processed)
	if stats.Total > 0 {
		percentage = percentage / float64(stats.Total) * 100
	}

	elapsed := time.Since(stats.StartTime)
	var eta string
	if downloadedAndSynced := atomic.LoadInt32(&stats.Downloaded) + atomic.LoadInt32(&stats.Synced); downloadedAndSynced > 0 && elapsed > 0 {
		rate := float64(downloadedAndSynced) / elapsed.Seconds()
		remainingFiles := float64(stats.Total - processed)
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

	callbacks.emitStderr(fmt.Sprintf("\r\033[K[%d/%d] %.1f%% | Downloaded: %d | Synced: %d | Skipped: %d | Failed: %d%s | Current: %s",
		processed, stats.Total, percentage,
		stats.Downloaded, stats.Synced, stats.Skipped, stats.Failed,
		eta, displayID))
}

func (callbacks Callbacks) emitProgress(stats *DownloadStats, currentSeriesID string, debugMode bool) {
	updateProgress(stats, currentSeriesID, debugMode, callbacks)
}

func saveSeriesUIDsToFile(originalPath string, seriesUIDs []string) (string, error) {
	dir := filepath.Dir(originalPath)
	base := filepath.Base(originalPath)
	outPath := filepath.Join(dir, base+".series_uids.txt")

	f, err := os.Create(outPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	writer := bufio.NewWriter(f)
	defer writer.Flush()

	for _, uid := range seriesUIDs {
		if uid == "" {
			continue
		}
		if _, err := writer.WriteString(uid + "\n"); err != nil {
			return "", err
		}
	}

	return outPath, nil
}

func decodeInputFile(ctx context.Context, filePath string, client *http.Client, options *Options, callbacks Callbacks, s5cmdMap map[string]string) ([]*FileInfo, int, error) {
	return decodeInputFileInternal(ctx, filePath, client, options, callbacks, s5cmdMap, true)
}

func decodeInputFileInternal(ctx context.Context, filePath string, client *http.Client, options *Options, callbacks Callbacks, s5cmdMap map[string]string, emitManifest bool) ([]*FileInfo, int, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".tcia":
		s5Files, _, tciaUIDs := decodeS5cmd(filePath, options.Output, s5cmdMap, callbacks, options)
		if len(s5Files) == 0 && len(tciaUIDs) == 0 {
			files := decodeTCIA(ctx, filePath, client, options, callbacks)
			if emitManifest {
				emitManifestMetadata(callbacks, filePath, files)
			}
			return files, 0, nil
		}
		files := combineWithTCIABatch(ctx, s5Files, tciaUIDs, client, options, callbacks)
		if emitManifest {
			emitManifestMetadata(callbacks, filePath, files)
		}
		return files, 0, nil
	case ".s5cmd":
		s5Files, newJobs, tciaUIDs := decodeS5cmd(filePath, options.Output, s5cmdMap, callbacks, options)
		files := combineWithTCIABatch(ctx, s5Files, tciaUIDs, client, options, callbacks)
		if emitManifest {
			emitManifestMetadata(callbacks, filePath, files)
		}
		return files, newJobs, nil
	case ".csv", ".tsv", ".xlsx":
		s5Files, _, tciaUIDs, splitErr := decodeSpreadsheetSplit(filePath, options.Output, s5cmdMap, callbacks, options)
		if splitErr == nil {
			files := combineWithTCIABatch(ctx, s5Files, tciaUIDs, client, options, callbacks)
			if emitManifest {
				emitManifestMetadata(callbacks, filePath, files)
			}
			return files, 0, nil
		} else if splitErr != ErrSeriesInstanceUIDColumnNotFound {
			return nil, 0, fmt.Errorf("could not get series UIDs from spreadsheet: %w", splitErr)
		}

		// Fallback to regular spreadsheet handling
		files, err := decodeSpreadsheet(filePath)
		if err != nil {
			return nil, 0, err
		}
		if emitManifest {
			emitManifestMetadata(callbacks, filePath, files)
		}
		return files, 0, nil
	case ".json", ".jsonld":
		files, err := decodeCroissant(ctx, filePath, client, options, callbacks, s5cmdMap)
		if err != nil {
			return nil, 0, err
		}
		if emitManifest {
			emitManifestMetadata(callbacks, filePath, files)
		}
		return files, 0, nil
	default:
		return nil, 0, fmt.Errorf("unsupported input file format: %s", ext)
	}
}

// combineWithTCIABatch fetches TCIA metadata for tciaUIDs (if any), appends
// the resulting FileInfos to s5Files, and writes the combined metadata.csv +
// completion status. Used by decodeInputFile for non-streaming hybrid runs.
func combineWithTCIABatch(ctx context.Context, s5Files []*FileInfo, tciaUIDs []string, client *http.Client, options *Options, callbacks Callbacks) []*FileInfo {
	files := s5Files
	if len(tciaUIDs) > 0 {
		callbacks.emitStdout(fmt.Sprintf("Fetching TCIA metadata for %d series\n", len(tciaUIDs)))
		tciaFiles := fetchTCIABatch(ctx, tciaUIDs, client)
		files = append(files, tciaFiles...)
	}
	if len(tciaUIDs) > 0 || len(files) > 0 {
		csvPath := filepath.Join(options.Output, "metadata", "metadata.csv")
		if err := WriteAllMetadataToCSV(files, csvPath); err != nil {
			Logger.Errorf("Failed to save combined CSV: %v", err)
		} else {
			callbacks.emitStdout(fmt.Sprintf("Saved metadata for %d files to %s\n", len(files), csvPath))
		}
		InitCompletionStatus(options.Output, files)
	}
	return files
}

func isAuthError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "401") ||
		strings.Contains(s, "403") ||
		strings.Contains(s, "Unauthorized") ||
		strings.Contains(s, "Forbidden")
}

// MergeCompletionStatus merges completion_status.csv into metadata.csv and removes the sidecar.
func MergeCompletionStatus(outDir string) error {
	metadataPath := filepath.Join(outDir, "metadata", "metadata.csv")
	statusPath := filepath.Join(outDir, "metadata", "completion_status.csv")

	// Read completion statuses into a map
	sf, err := os.Open(statusPath)
	if err != nil {
		return fmt.Errorf("could not open completion status CSV: %w", err)
	}
	statusRecords, err := csv.NewReader(sf).ReadAll()
	sf.Close()
	if err != nil {
		return fmt.Errorf("could not read completion status CSV: %w", err)
	}

	statusMap := make(map[string]string)    // seriesUID -> status
	for _, row := range statusRecords[1:] { // skip header
		if len(row) >= 2 {
			statusMap[row[0]] = row[1]
		}
	}

	// Read metadata CSV
	mf, err := os.Open(metadataPath)
	if err != nil {
		return fmt.Errorf("could not open metadata CSV: %w", err)
	}
	records, err := csv.NewReader(mf).ReadAll()
	mf.Close()
	if err != nil {
		return fmt.Errorf("could not read metadata CSV: %w", err)
	}
	if len(records) == 0 {
		return fmt.Errorf("metadata CSV is empty")
	}

	// Find SeriesInstanceUID column
	header := records[0]
	uidCol := -1
	for i, h := range header {
		if h == "SeriesInstanceUID" {
			uidCol = i
			break
		}
	}
	if uidCol == -1 {
		return fmt.Errorf("SeriesInstanceUID column not found in metadata CSV")
	}

	// Add completion_status column to header and rows
	records[0] = append(header, "completion_status")
	for i, row := range records[1:] {
		uid := ""
		if len(row) > uidCol {
			uid = row[uidCol]
		}
		records[i+1] = append(row, statusMap[uid]) // empty string if not found
	}

	// Write back atomically
	tmpPath := metadataPath + ".tmp"
	out, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("could not create temp file: %w", err)
	}
	w := csv.NewWriter(out)
	if err := w.WriteAll(records); err != nil {
		out.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("could not write merged metadata CSV: %w", err)
	}
	w.Flush()
	out.Close()

	if err := os.Rename(tmpPath, metadataPath); err != nil {
		return fmt.Errorf("could not replace metadata CSV: %w", err)
	}

	// Remove sidecar now that it's merged
	if err := os.Remove(statusPath); err != nil {
		logger.Warnf("Could not remove completion status sidecar: %v", err)
	}

	return nil
}
