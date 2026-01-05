package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/GrigoryEvko/NBIA_data_retriever_CLI/core/app"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// OpenInputFileDialog opens a system file dialog and returns the selected file path
func (b *App) OpenInputFileDialog() (string, error) {
	result, err := runtime.OpenFileDialog(b.ctx, runtime.OpenDialogOptions{
		Title: "Select TCIA Manifest File",
		Filters: []runtime.FileFilter{
			{DisplayName: "TCIA Manifest Files", Pattern: "*.tcia"},
			{DisplayName: "All Files", Pattern: "*"},
		},
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
	result, err := runtime.OpenDirectoryDialog(b.ctx, runtime.OpenDialogOptions{
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

    _ = downloadInParallel // currently unused

    // Cancel any existing run before starting a new one
    b.mu.Lock()
    if b.cancel != nil {
        b.cancel()
    }
    b.runID++
    currentID := b.runID
    ctx, cancel := context.WithCancel(b.ctx)
    b.cancel = cancel
    b.mu.Unlock()

    // Clear cancel reference when done
    go func() {
        defer func() {
            b.mu.Lock()
            if b.runID == currentID {
                b.cancel = nil
            }
            b.mu.Unlock()
        }()

        user := os.Getenv("NBIA_USER")
        if user == "" {
            user = "nbia_guest"
        }
        pass := os.Getenv("NBIA_PASS")

        options := &app.Options{
            Input:           manifestPath,
            Output:          outputDir,
            Proxy:           "",
            Concurrent:      simultaneousDownloads,
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
            SkipExisting:    skipExisting,
            MaxRetries:      maxRetries,
            RetryDelay:      10 * time.Second,
            MaxConnsPerHost: maxConnections,
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
            runtime.EventsEmit(b.ctx, "cli-output-line", line)
        }

        callbacks := app.Callbacks{
            Stdout: emit,
            Stderr: emit,
            Series: func(evt app.SeriesEvent) {
                runtime.EventsEmit(b.ctx, "download-series-event", evt)
            },
        }

        // Run the CLI download (blocking inside goroutine)
        summary, err := app.Run(ctx, options, callbacks)
        linesMu.Lock()
        combined := strings.Join(lines, "\n")
        linesMu.Unlock()

        if err != nil {
            if errors.Is(err, context.Canceled) {
                runtime.EventsEmit(b.ctx, "cli-finished", combined)
                return
            }
            runtime.EventsEmit(b.ctx, "cli-error", fmt.Sprintf("download failed: %v", err))
            return
        }

        _ = summary
        runtime.EventsEmit(b.ctx, "cli-finished", combined)
    }()

    // Return immediately so frontend is free to repaint
    return "started", nil
}



func (b *App) CancelDownload() {
	b.mu.Lock()
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}
	b.mu.Unlock()
}

type App struct {
	ctx    context.Context
	cancel context.CancelFunc
	runID  uint64
	mu     sync.Mutex
}

func NewApp() *App {
	return &App{}
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
	_, err := runtime.MessageDialog(b.ctx, runtime.MessageDialogOptions{
		Type:    runtime.InfoDialog,
		Title:   "Native Dialog from Go",
		Message: "This is a Native Dialog send from Go.",
	})

	if err != nil {
		panic(err)
	}
}
