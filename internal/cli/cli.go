// Package cli implements the noninteractive command used by Python and servers.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"TCIA_Data_Retriever/core/app"
)

// Report is the versioned completion contract. Counts use the same work-item
// units as app.Summary (files or series depending on the manifest route).
// It deliberately excludes identifiers, credentials, URLs and raw errors.
type Report struct {
	SchemaVersion   int     `json:"schema_version"`
	Status          string  `json:"status"`
	ErrorCode       string  `json:"error_code,omitempty"`
	Version         string  `json:"version"`
	OutputDirectory string  `json:"output_directory"`
	Total           int32   `json:"total"`
	Downloaded      int32   `json:"downloaded"`
	Synced          int32   `json:"synced"`
	Skipped         int32   `json:"skipped"`
	Failed          int32   `json:"failed"`
	ElapsedSeconds  float64 `json:"elapsed_seconds"`
}

type runFunc func(context.Context, *app.Options, app.Callbacks) (*app.Summary, error)

// Main returns 0 on success, 1 on transfer/report failure, 2 on invalid usage,
// or 130 on cancellation. The caller owns signal handling and process exit.
func Main(ctx context.Context, args []string, stdout, stderr io.Writer, version string) int {
	return execute(ctx, args, stdout, stderr, version, app.Run)
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer, version string, run runFunc) int {
	if ctx == nil {
		ctx = context.Background()
	}
	opts, parseErr := app.ParseHeadlessOptions(args)
	if errors.Is(parseErr, app.ErrShowHelp) {
		fmt.Fprint(stdout, opts.HelpText())
		return 0
	}
	report := Report{SchemaVersion: 1, Version: version, Status: "failed"}
	if opts != nil {
		report.OutputDirectory, _ = filepath.Abs(opts.Output)
	}
	finish := func(code int, status, errorCode, message string) int {
		report.Status, report.ErrorCode = status, errorCode
		if message != "" {
			fmt.Fprintln(stderr, message)
		}
		if opts != nil && opts.ReportJSON != "" {
			if err := writeReport(opts.ReportJSON, report); err != nil {
				fmt.Fprintln(stderr, "Could not write completion report; check its directory and permissions.")
				return 1
			}
		}
		return code
	}
	// The report must never replace an input or credential file. Check this
	// even on parser errors, which may leave partially populated options.
	if opts != nil && opts.ReportJSON != "" && (sameFile(opts.ReportJSON, opts.Input) || sameFile(opts.ReportJSON, opts.Auth)) {
		fmt.Fprintln(stderr, "The completion report must use a different path from the manifest and credential file.")
		return 2
	}
	if parseErr != nil {
		// Parser errors can contain supplied values, including passwords and
		// signed URLs. Do not write to a partially parsed report path: the
		// parser may not have reached a conflicting manifest/auth argument.
		fmt.Fprintln(stderr, "Invalid arguments. Run tdr --help for supported options; --prompt is unavailable in headless mode.")
		return 2
	}
	if opts.Version {
		fmt.Fprintln(stdout, version)
		return 0
	}
	if opts.Input == "" {
		return finish(2, "failed", "missing_input", "Supply --input with a local manifest file.")
	}
	input, err := os.Open(opts.Input)
	if err != nil {
		return finish(2, "failed", "invalid_input", "Could not open the manifest file.")
	}
	stat, err := input.Stat()
	_ = input.Close()
	if err != nil || !stat.Mode().IsRegular() {
		return finish(2, "failed", "invalid_input", "The input must be a readable regular manifest file.")
	}
	if opts.CroissantList {
		plan, err := app.InspectCroissantManifest(opts.Input)
		if err != nil {
			return finish(2, "failed", "invalid_manifest", "Could not inspect the Croissant manifest.")
		}
		for _, item := range plan.Items {
			fmt.Fprintf(stdout, "%s\t%s\t%s\t%t\n", item.ID, item.Group, item.FileName, item.Supported)
		}
		return finish(0, "success", "", "")
	}
	if !opts.AcceptDataPolicy {
		return finish(2, "failed", "policy_not_accepted", "Review the TCIA data usage policy and pass --accept-data-policy to download: https://www.cancerimagingarchive.net/data-usage-policies-and-restrictions/")
	}
	if opts.Concurrent < 1 || opts.MetadataWorkers < 1 || opts.MaxConnsPerHost < 1 || opts.MaxRetries < 0 {
		return finish(2, "failed", "invalid_arguments", "Worker and connection counts must be positive; retries must be nonnegative.")
	}
	if opts.Proxy != "" {
		proxy, err := url.Parse(opts.Proxy)
		if err != nil || proxy.Host == "" || (proxy.Scheme != "http" && proxy.Scheme != "https" && proxy.Scheme != "socks5" && proxy.Scheme != "socks5h") {
			return finish(2, "failed", "invalid_arguments", "Supply a valid HTTP, HTTPS or SOCKS5 proxy URL.")
		}
	}
	switch strings.ToLower(filepath.Ext(opts.Input)) {
	case ".tcia", ".s5cmd", ".csv", ".tsv", ".xlsx", ".json", ".jsonld":
	default:
		return finish(2, "failed", "invalid_manifest", "Unsupported manifest format. Run tdr --help for supported formats.")
	}
	// Fail before any transfer if the report cannot be created. Publishing
	// the initial failure prevents a previous success from being mistaken
	// for this run if the process terminates unexpectedly.
	if opts.ReportJSON != "" {
		report.ErrorCode = "incomplete_run"
		if err := writeReport(opts.ReportJSON, report); err != nil {
			fmt.Fprintln(stderr, "Could not create completion report; check its directory and permissions.")
			return 1
		}
	}
	opts.CLI = true
	if ctx.Err() != nil {
		return finish(130, "cancelled", "cancelled", "Download cancelled.")
	}
	start := time.Now()
	manifestReceived := false
	summary, runErr := run(ctx, opts, app.Callbacks{
		Stdout:   func(s string) { fmt.Fprint(stdout, s) },
		Stderr:   func(s string) { fmt.Fprint(stderr, s) },
		Manifest: func(app.ManifestPayload) { manifestReceived = true },
	})
	report.ElapsedSeconds = time.Since(start).Seconds()
	if summary != nil {
		report.Total, report.Downloaded = summary.Total, summary.Downloaded
		report.Synced, report.Skipped, report.Failed = summary.Synced, summary.Skipped, summary.Failed
	}
	if errors.Is(runErr, context.Canceled) || ctx.Err() != nil {
		return finish(130, "cancelled", "cancelled", "Download cancelled.")
	}
	if runErr != nil {
		return finish(1, "failed", "transfer_failed", "Download failed; review the transfer log for details.")
	}
	if !manifestReceived || summary == nil || summary.Total == 0 {
		return finish(1, "failed", "empty_manifest", "No downloadable metadata was found for this manifest.")
	}
	if summary.Failed > 0 {
		return finish(1, "failed", "items_failed", "Download completed with failed items.")
	}
	return finish(0, "success", "", "")
}

func sameFile(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	aa, _ := filepath.Abs(a)
	bb, _ := filepath.Abs(b)
	if aa == bb {
		return true
	}
	ai, ae := os.Stat(a)
	bi, be := os.Stat(b)
	return ae == nil && be == nil && os.SameFile(ai, bi)
}

func writeReport(path string, report Report) error {
	// Reports contain local paths; keep their permissions private.
	f, err := os.CreateTemp(filepath.Dir(path), ".tdr-report-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
