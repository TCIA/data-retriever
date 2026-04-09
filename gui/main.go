//go:build !cli

package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GrigoryEvko/NBIA_data_retriever_CLI/core/app"
	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/logger"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/mac"
	"github.com/wailsapp/wails/v2/pkg/options/windows"
)

//go:embed frontend/dist
var assets embed.FS

var (
	buildStamp string
	gitHash    string
	goVersion  string
	version    string
)

func main() {

	cliMode := false
	for _, arg := range os.Args[1:] {
		if arg == "--cli" || arg == "-cli" {
			cliMode = true
			break
		}
	}

	if cliMode {
		runCLI()
		os.Exit(0)
	}

	// Create an instance of the app structure
	app := NewApp()

	// Windows and Linux pass the opened file as the first argument.
	// Store it before Wails starts so HandleFileOpen-equivalent logic works.
	if len(os.Args) > 1 && os.Args[1] != "--cli" {
		candidate := os.Args[1]
		app.pendingFileOpen = candidate
	}

	// Create application with options
	err := wails.Run(&options.App{
		Title:             "TCIA Data Retriever",
		Width:             650,
		Height:            500,
		MinWidth:          650,
		MaxWidth:          650,
		MinHeight:         500,
		DisableResize:     false,
		Fullscreen:        false,
		Frameless:         false,
		StartHidden:       false,
		HideWindowOnClose: false,
		Assets:            assets,
		LogLevel:          logger.DEBUG,
		OnStartup:         app.startup,
		OnShutdown:        app.shutdown,
		Bind: []interface{}{
			app,
		},
		Windows: &windows.Options{
			WebviewIsTransparent: false,
			WindowIsTranslucent:  false,
			DisableWindowIcon:    false,
		},
		Mac: &mac.Options{
			OnFileOpen: func(filePath string) {
				app.HandleFileOpen(filePath)
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}

func runCLI() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setupCloseHandler(cancel)

	options := app.InitOptions()
	appLogger := app.Logger

	if options.Version {
		appLogger.Infof("Current version: %s", version)
		appLogger.Infof("Git Commit Hash: %s", gitHash)
		appLogger.Infof("UTC Build Time : %s", buildStamp)
		appLogger.Infof("Golang Version : %s", goVersion)
		return
	}

	var eventLog *app.TextEventLogger
	runStart := time.Now()

	if options.SaveLog {
		logPath := app.DefaultLogFilePath("progress.log")
		l, err := app.NewTextEventLogger(logPath, runStart, options.InterimUpdateInterval)
		if err != nil {
			appLogger.Warnf("Failed to initialise event log file: %v", err)
		} else {
			eventLog = l
			defer eventLog.Close()
		}
	}

	callbacks := app.Callbacks{
		Stdout: func(msg string) {
			fmt.Fprint(os.Stdout, msg)
			if eventLog != nil {
				eventLog.HandleStdout(msg)
			}
		},
		Stderr: func(msg string) {
			fmt.Fprint(os.Stderr, msg)
			if eventLog != nil {
				eventLog.HandleStderr(msg)
			}
		},
		Series: func(evt app.SeriesEvent) {
			if eventLog != nil {
				eventLog.HandleSeries(evt)
			}
		},
		Manifest: func(payload app.ManifestPayload) {
			if eventLog != nil {
				eventLog.HandleManifest(payload)
			}
		},
	}

	summary, err := app.Run(ctx, options, callbacks)
	if eventLog != nil {
		eventLog.LogRunFinished(summary, err)
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			appLogger.Warn("Download cancelled by user")
		} else {
			appLogger.Fatalf("Download failed: %v", err)
		}
	}
	_ = summary
}

func setupCloseHandler(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal")
		cancel()
	}()
}
