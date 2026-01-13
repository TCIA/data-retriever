package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
	"path/filepath"
	"bufio"
	stdRuntime "runtime"

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
        ID:         id,
        Ctx:        ctx,
        Cancel:     cancel,
        Manifest:   manifestPath,
        OutputDir:  outputDir,
        MaxConn:    maxConnections,
        MaxRetries: maxRetries,
        Parallel:   simultaneousDownloads,
        SkipExist:  skipExisting,
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
        Input:           batch.Manifest,
        Output:          batch.OutputDir,
        Proxy:           "",
        Concurrent:      batch.Parallel,
        Meta:            false,
        Username:        user,
        Password:        pass,
        Version:         false,
        Debug:           false,
        Help:            false,
        MetaUrl:         app.MetaUrl,
        TokenUrl:        app.TokenUrl,
        ImageUrl:        app.ImageUrl,
        SaveLog:         false,
        Prompt:          false,
        Force:           false,
        SkipExisting:    batch.SkipExist,
        MaxRetries:      batch.MaxRetries,
        RetryDelay:      10 * time.Second,
        MaxConnsPerHost: batch.MaxConn,
        ServerFriendly:  false,
        RequestDelay:    500 * time.Millisecond,
        NoMD5:           false,
        NoDecompress:    false,
        RefreshMetadata: false,
        MetadataWorkers: 20,
    }

    var (
        lines   []string
        linesMu sync.Mutex
    )

    emit := func(line string) {
        linesMu.Lock()
        lines = append(lines, line)
        linesMu.Unlock()
        wailsRuntime.EventsEmit(b.ctx, "cli-output-line", line)
    }

    callbacks := app.Callbacks{
        Stdout: emit,
        Stderr: emit,
        Series: func(evt app.SeriesEvent) {
            wailsRuntime.EventsEmit(b.ctx, "download-series-event", evt)
        },
    }

    // Run the CLI download (blocking inside goroutine)
    summary, err := app.Run(batch.Ctx, options, callbacks)

    linesMu.Lock()
    combined := strings.Join(lines, "\n")
    linesMu.Unlock()

    if err != nil {
        if errors.Is(err, context.Canceled) {
            wailsRuntime.EventsEmit(b.ctx, "cli-finished", combined)
            return
        }
        wailsRuntime.EventsEmit(b.ctx, "cli-error", fmt.Sprintf("download failed: %v", err))
        return
    }

    _ = summary
    wailsRuntime.EventsEmit(b.ctx, "cli-finished", combined)
}




func (b *App) CancelDownload() {
    b.mu.Lock()
    defer b.mu.Unlock()

    for id, batch := range b.batches {
        batch.Cancel()           // cancel the batch
        delete(b.batches, id)   // remove it from the map
    }
}


type App struct {
    ctx     context.Context
    mu      sync.Mutex
    runID   uint64
    batches map[uint64]*DownloadBatch  // <-- add this
}

func NewApp(ctx context.Context) *App {
    return &App{
        ctx:     ctx,
        batches: make(map[uint64]*DownloadBatch),
    }
}

type DownloadBatch struct {
    ID         uint64 
    Ctx        context.Context
    Cancel     context.CancelFunc

    Manifest   string
    OutputDir  string

    MaxConn    int
    MaxRetries int
    Parallel   int
    SkipExist  bool
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
