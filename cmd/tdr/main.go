// Command tdr runs the shared transfer engine without a desktop runtime.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"TCIA_Data_Retriever/internal/cli"
)

var version = "dev"

// gitHash is set alongside version by release builds for binary provenance.
var gitHash string

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Main(ctx, os.Args[1:], os.Stdout, os.Stderr, version)
	stop()
	os.Exit(code)
}
