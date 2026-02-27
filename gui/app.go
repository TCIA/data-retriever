package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	stdRuntime "runtime"
	"strings"
	"sync"
	"time"

	"github.com/GrigoryEvko/NBIA_data_retriever_CLI/core/app"
	wailsRuntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func linuxDownloadsDir(home string) string {
	config := filepath.Join(home, ".config", "user-dirs.dirs")
	file, err := os.Open(config)
	if err != nil {
		return ""
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "XDG_DOWNLOAD_DIR=") {
			value := strings.TrimPrefix(line, "XDG_DOWNLOAD_DIR=")
			value = strings.Trim(value, `"`)
			value = strings.Replace(value, "$HOME", home, 1)

			if dirExists(value) {
				return value
			}
		}
	}
	return ""
}

func (b *App) GetDefaultOutputDirectory() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	switch stdRuntime.GOOS {
	case "windows":
		// Windows: %USERPROFILE%\Downloads (standard since Win 7)
		downloads := filepath.Join(home, "Downloads")
		if dirExists(downloads) {
			return downloads
		}
		return home

	case "darwin":
		// macOS: ~/Downloads
		u, err := user.Lookup(os.Getenv("LOGNAME"))
		if err == nil {
			return filepath.Join(u.HomeDir, "Downloads")
		}
		downloads := filepath.Join(home, "Downloads")
		if dirExists(downloads) {
			return downloads
		}
		return home

	case "linux":
		// Linux: try XDG user-dirs first
		if xdg := linuxDownloadsDir(home); xdg != "" {
			return xdg
		}

		// Fallback: ~/Downloads
		downloads := filepath.Join(home, "Downloads")
		if dirExists(downloads) {
			return downloads
		}
		return home

	default:
		return home
	}
}

// OpenAuthFileDialog opens a system file dialog and returns the selected file path
func (b *App) OpenAuthFileDialog() (string, error) {
	result, err := wailsRuntime.OpenFileDialog(b.ctx, wailsRuntime.OpenDialogOptions{
		Title: "Select .json Auth Token",
	})
	if err != nil {
		return "", err
	}
	if result == "" {
		return "", nil // User cancelled
	}
	return result, nil
}

// OpenInputFileDialog opens a system file dialog and returns the selected file path
func (b *App) OpenInputFileDialog() (string, error) {
	result, err := wailsRuntime.OpenFileDialog(b.ctx, wailsRuntime.OpenDialogOptions{
		Title: "Select TCIA Manifest File",
	})
	if err != nil {
		return "", err
	}
	if result == "" {
		return "", nil // User cancelled
	}
	return result, nil
}

// OpenOutputDirectoryDialog opens a system directory dialog and returns the selected directory path
func (b *App) OpenOutputDirectoryDialog() (string, error) {
	result, err := wailsRuntime.OpenDirectoryDialog(b.ctx, wailsRuntime.OpenDialogOptions{
		Title: "Download Directory",
	})
	if err != nil {
		return "", err
	}
	if result == "" {
		return "", nil // User cancelled
	}
	return result, nil
}

// RunCLIFetch runs the CLI tool asynchronously with the given manifest and output directory and advanced options.
func (b *App) RunCLIFetch(
	manifestPath string,
	outputDir string,
	maxConnections int,
	maxRetries int,
	simultaneousDownloads int,
	skipExisting bool,
	downloadInParallel bool,
	authPath string,
	directoryMode string,
) (string, error) {

	if b.ctx == nil {
		return "", fmt.Errorf("application context not initialised")
	}

	// Create a new batch
	b.mu.Lock()
	b.runID++
	id := b.runID

	ctx, cancel := context.WithCancel(b.ctx)

	batch := &DownloadBatch{
		ID:            id,
		Ctx:           ctx,
		Cancel:        cancel,
		Manifest:      manifestPath,
		OutputDir:     outputDir,
		MaxConn:       maxConnections,
		MaxRetries:    maxRetries,
		Parallel:      simultaneousDownloads,
		SkipExist:     skipExisting,
		AuthPath:      authPath,
		DirectoryMode: directoryMode,
	}

	if b.batches == nil {
		b.batches = make(map[uint64]*DownloadBatch)
	}
	b.batches[id] = batch
	b.mu.Unlock()

	// Run the batch in its own goroutine
	go b.runBatch(batch)

	// Return immediately so frontend is free to repaint
	return fmt.Sprintf("started batch %d", id), nil
}

func (b *App) runBatch(batch *DownloadBatch) {
	defer func() {
		// Remove batch from map when done
		b.mu.Lock()
		delete(b.batches, batch.ID)
		b.mu.Unlock()
	}()

	user := os.Getenv("NBIA_USER")
	if user == "" {
		user = "nbia_guest"
	}
	pass := os.Getenv("NBIA_PASS")

	options := &app.Options{
		Input:                 batch.Manifest,
		Output:                batch.OutputDir,
		Proxy:                 "",
		Concurrent:            batch.Parallel,
		Meta:                  false,
		Username:              user,
		Password:              pass,
		Version:               false,
		Debug:                 false,
		Help:                  false,
		MetaUrl:               app.MetaUrl,
		TokenUrl:              app.TokenUrl,
		ImageUrl:              app.ImageUrl,
		SaveLog:               false,
		Prompt:                false,
		Force:                 false,
		SkipExisting:          batch.SkipExist,
		MaxRetries:            batch.MaxRetries,
		RetryDelay:            10 * time.Second,
		InterimUpdateInterval: app.DefaultInterimUpdateInterval,
		MaxConnsPerHost:       batch.MaxConn,
		ServerFriendly:        false,
		RequestDelay:          500 * time.Millisecond,
		NoMD5:                 false,
		NoDecompress:          false,
		RefreshMetadata:       false,
		MetadataWorkers:       20,
		Auth:                  batch.AuthPath,
		DirectoryMode:         batch.DirectoryMode,
	}

	logTimestamp := time.Now().Format("20060102-150405")
	logPath := app.DefaultLogFilePath(fmt.Sprintf("nbia-output-%s.log", logTimestamp))
	runStart := time.Now()

	var eventLog *app.TextEventLogger
	if l, logErr := app.NewTextEventLogger(logPath, runStart, options.InterimUpdateInterval); logErr == nil {
		eventLog = l
	}

	defer func() {
		if eventLog != nil {
			eventLog.Close()
		}
	}()

	callbacks := app.Callbacks{
		Stdout: func(line string) {
			if eventLog != nil {
				eventLog.HandleStdout(line)
			}
		},
		Stderr: func(line string) {
			if eventLog != nil {
				eventLog.HandleStderr(line)
			}
		},
		Series: func(evt app.SeriesEvent) {
			if eventLog != nil {
				eventLog.HandleSeries(evt)
			}
			wailsRuntime.EventsEmit(b.ctx, "download-series-event", evt)
		},
		Manifest: func(p app.ManifestPayload) {
			if eventLog != nil {
				eventLog.HandleManifest(p)
			}
			wailsRuntime.EventsEmit(b.ctx, "manifest-series-metadata", p)
		},
	}

	// Run the CLI download (blocking inside goroutine)
	summary, err := app.Run(batch.Ctx, options, callbacks)
	if eventLog != nil {
		eventLog.LogRunFinished(summary, err)
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			wailsRuntime.EventsEmit(b.ctx, "cli-finished", "")
			return
		}
		wailsRuntime.EventsEmit(b.ctx, "cli-error", fmt.Sprintf("download failed: %v", err))
		return
	}

	summaryText := ""
	if summary != nil {
		summaryText = fmt.Sprintf(
			"Download Summary: total %d, downloaded %d, synced %d, skipped %d, failed %d, elapsed %s",
			summary.Total,
			summary.Downloaded,
			summary.Synced,
			summary.Skipped,
			summary.Failed,
			summary.Elapsed.String(),
		)
	}
	wailsRuntime.EventsEmit(b.ctx, "cli-finished", summaryText)
}

func (b *App) CancelDownload() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for id, batch := range b.batches {
		batch.Cancel()        // cancel the batch
		delete(b.batches, id) // remove it from the map
	}
}

type App struct {
	ctx           context.Context
	mu            sync.Mutex
	runID         uint64
	batches       map[uint64]*DownloadBatch
	pausedBatches map[string]*DownloadBatch // keyed by manifest path for resume
}

func NewApp(ctx context.Context) *App {
	return &App{
		ctx:           ctx,
		batches:       make(map[uint64]*DownloadBatch),
		pausedBatches: make(map[string]*DownloadBatch),
	}
}

// PauseManifest pauses a download by canceling its context and storing the batch for resume
func (b *App) PauseManifest(manifestPath string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for id, batch := range b.batches {
		if batch.Manifest == manifestPath {
			batch.Cancel() // Cancel the context to stop downloads
			b.pausedBatches[manifestPath] = batch
			delete(b.batches, id)
			wailsRuntime.EventsEmit(b.ctx, "manifest-paused", manifestPath)
			return nil
		}
	}
	return fmt.Errorf("no active download found for manifest: %s", manifestPath)
}

// ResumeManifest resumes a paused download by restarting with skip-existing enabled
func (b *App) ResumeManifest(manifestPath string) error {
	b.mu.Lock()
	pausedBatch, exists := b.pausedBatches[manifestPath]
	if exists {
		delete(b.pausedBatches, manifestPath)
	}
	b.mu.Unlock()

	if !exists {
		return fmt.Errorf("no paused download found for manifest: %s", manifestPath)
	}

	// Resume by re-running with skip-existing enabled
	_, err := b.RunCLIFetch(
		pausedBatch.Manifest,
		pausedBatch.OutputDir,
		pausedBatch.MaxConn,
		pausedBatch.MaxRetries,
		pausedBatch.Parallel,
		true, // skipExisting = true to resume from where we left off
		true, // downloadInParallel
		pausedBatch.AuthPath,
		pausedBatch.DirectoryMode,
	)
	if err != nil {
		return err
	}

	wailsRuntime.EventsEmit(b.ctx, "manifest-resumed", manifestPath)
	return nil
}

type DownloadBatch struct {
	ID     uint64
	Ctx    context.Context
	Cancel context.CancelFunc

	Manifest  string
	OutputDir string

	MaxConn       int
	MaxRetries    int
	Parallel      int
	SkipExist     bool
	AuthPath      string
	DirectoryMode string
}

func (a *App) FetchFiles() string {
	return "Done!"
}

func (b *App) startup(ctx context.Context) {
	b.ctx = ctx
}

func (b *App) shutdown(ctx context.Context) {
	// Perform teardown here
}

func (b *App) Greet(name string) string {
	return fmt.Sprintf("Hello %s, It's show time!", name)
}

func (b *App) ShowDialog() {
	_, err := wailsRuntime.MessageDialog(b.ctx, wailsRuntime.MessageDialogOptions{
		Type:    wailsRuntime.InfoDialog,
		Title:   "Native Dialog from Go",
		Message: "This is a Native Dialog send from Go.",
	})

	if err != nil {
		panic(err)
	}
}
