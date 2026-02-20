package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GrigoryEvko/NBIA_data_retriever_CLI/core/app"
)

var (
	buildStamp string
	gitHash    string
	goVersion  string
	version    string
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setupCloseHandler(cancel)

	options := app.InitOptions()
	logger := app.Logger

	if options.Version {
		logger.Infof("Current version: %s", version)
		logger.Infof("Git Commit Hash: %s", gitHash)
		logger.Infof("UTC Build Time : %s", buildStamp)
		logger.Infof("Golang Version : %s", goVersion)
		return
	}

	var eventLog *app.TextEventLogger
	runStart := time.Now()
	if options.SaveLog {
		logPath := app.DefaultLogFilePath("progress.log")
		l, err := app.NewTextEventLogger(logPath, runStart, 10*time.Second)
		if err != nil {
			logger.Warnf("Failed to initialise event log file: %v", err)
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
			logger.Warn("Download cancelled by user")
		} else {
			logger.Fatalf("Download failed: %v", err)
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
