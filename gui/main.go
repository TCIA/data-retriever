//go:build !cli

package main

import (
	"bufio"
	"context"
	"embed"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
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

	if !options.AcceptDataPolicy {
		if !promptDataPolicy() {
			fmt.Fprintln(os.Stderr, "Data usage policy not accepted. Exiting.")
			os.Exit(1)
		}
	}

	if options.Version {
		appLogger.Infof("Current version: %s", version)
		appLogger.Infof("Git Commit Hash: %s", gitHash)
		appLogger.Infof("UTC Build Time : %s", buildStamp)
		appLogger.Infof("Golang Version : %s", goVersion)
		return
	}

	if info, err := (&App{}).CheckForUpdate(); err == nil && info.Available {
		appLogger.Warnf("A new version is available: %s (you have %s). Download it at %s", info.LatestVersion, version, info.URL)
	}

	if paths, err := app.EnsureParquetsUpToDate(); err != nil {
		appLogger.Warnf("parquet init failed: %v", err)
	} else {
		options.IDCParquetPath = paths.IDCIndex
		options.PriorParquetPath = paths.PriorVersions
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

	manifestReceived := false
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
			manifestReceived = true
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
	} else if !manifestReceived {
		appLogger.Fatalf("No metadata can be found for this manifest: %s", options.Input)
	}
	_ = summary
}

const duaText = `
=== Data Usage Agreement ===

Any user accessing TCIA data must agree to:

- Not use the requested datasets, either alone or in concert with any other
  information, to identify or contact individual participants from whom data
  and/or samples were collected and follow all other conditions specified in
  the TCIA Site Disclaimer. Approved Users also agree not to generate and use
  information (e.g., facial images or comparable representations) in a manner
  that could allow the identities of research participants to be readily
  ascertained. These provisions do not apply to research investigators
  operating with specific IRB approval, pursuant to 45 CFR 46, to contact
  individuals within datasets or to obtain and use identifying information
  under an IRB-approved research protocol. All investigators including any
  Approved User conducting "human subjects research" within the scope of
  45 CFR 46 must comply with the requirements contained therein.

- Acknowledge in all oral or written presentations, disclosures, or
  publications the specific dataset(s) or applicable accession number(s) and
  the NIH-designated data repositories through which the investigator accessed
  any data. Citation guidelines for doing this are outlined below.

- If you are considering mirroring a copy of our publicly available datasets
  or providing direct access to any of the TCIA data via another tool or
  website using the REST API (https://wiki.cancerimagingarchive.net/x/NIIiAQ)
  please review our Data Analysis Centers (DACs) page
  (https://wiki.cancerimagingarchive.net/x/x49XAQ) for more information. DACs
  must provide attribution and links back to this TCIA data use policy and
  must require downstream users to do the same.

The summary page for every TCIA dataset includes a Citations & Data Usage
Policy tab. Please consult the Citation & Data Usage Policy for each
Collection before using them.

- Most data are freely available to browse, download, and use for commercial,
  scientific and educational purposes as outlined in the Creative Commons
  Attribution 3.0 Unported License or the Creative Commons Attribution 4.0
  International License. In rare circumstances commercial use may be
  prohibited using Attribution-NonCommercial 3.0 Unported (CC BY-NC 3.0) or
  Creative Commons Attribution-NonCommercial 4.0 International (CC BY-NC 4.0).

- Most data are immediately accessible and do not require account
  registration. A small subset of collections do require registration and
  special permission to gain access. Refer to the "Access" column on
  https://www.cancerimagingarchive.net/collections/ for more details.

=================================
`

func promptDataPolicy() bool {
	fmt.Fprint(os.Stdout, duaText)
	fmt.Fprint(os.Stdout, "Do you agree to the TCIA Data Usage Agreement? [y/N]: ")
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
		return answer == "y" || answer == "yes"
	}
	return false
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

func (a *App) AgreeToLicense() {
	// You can persist acceptance here (e.g. write a flag file) if you want
	// to skip the dialog on subsequent launches. For now it's session-only.
}

// DeclineLicense is called by the frontend when the user clicks "Decline".
// It exits the process cleanly.
func (a *App) DeclineLicense() {
	os.Exit(0)
}
