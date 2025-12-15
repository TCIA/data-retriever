package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	callbacks := app.Callbacks{
		Stdout: func(msg string) { fmt.Fprint(os.Stdout, msg) },
		Stderr: func(msg string) { fmt.Fprint(os.Stderr, msg) },
	}

	summary, err := app.Run(ctx, options, callbacks)
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
