package app

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func withEndpointReset(t *testing.T) {
	t.Helper()
	originalTokenURL := TokenUrl
	originalMetaURL := MetaUrl
	originalImageURL := ImageUrl
	t.Cleanup(func() {
		TokenUrl = originalTokenURL
		MetaUrl = originalMetaURL
		ImageUrl = originalImageURL
	})
}

func TestParseOptions_AllFlags(t *testing.T) {
	withEndpointReset(t)

	args := []string{
		"--input", "input.tcia",
		"--output", "/tmp/out",
		"--proxy", "http://proxy.local:8080",
		"--processes", "4",
		"--meta",
		"--user", "alice",
		"--passwd", "secret",
		"--token-url", "https://token.example.test",
		"--meta-url", "https://meta.example.test",
		"--image-url", "https://image.example.test",
		"--force",
		"--skip-existing",
		"--max-retries", "9",
		"--max-connections", "11",
		"--no-md5",
		"--no-decompress",
		"--refresh-metadata",
		"--metadata-workers", "7",
		"--auth", "/tmp/key.json",
		"--directory-mode", "classic",
		"--save-log",
		"--debug",
		"--version",
	}

	opt, err := ParseOptions(args, strings.NewReader(""))
	if err != nil {
		t.Fatalf("ParseOptions returned error: %v", err)
	}

	if opt.Input != "input.tcia" || opt.Output != "/tmp/out" || opt.Proxy != "http://proxy.local:8080" {
		t.Fatalf("unexpected path/proxy values: %+v", opt)
	}
	if opt.Concurrent != 4 || !opt.Meta || opt.Username != "alice" || opt.Password != "secret" {
		t.Fatalf("unexpected core option values: %+v", opt)
	}
	if !opt.Force || !opt.SkipExisting || opt.MaxRetries != 9 || opt.MaxConnsPerHost != 11 {
		t.Fatalf("unexpected download flags: %+v", opt)
	}
	if !opt.NoMD5 || !opt.NoDecompress || !opt.RefreshMetadata || opt.MetadataWorkers != 7 {
		t.Fatalf("unexpected integrity/metadata flags: %+v", opt)
	}
	if opt.Auth != "/tmp/key.json" || opt.DirectoryMode != "classic" {
		t.Fatalf("unexpected auth/directory-mode flags: %+v", opt)
	}
	if !opt.SaveLog || !opt.Debug || !opt.Version {
		t.Fatalf("unexpected debug/version flags: %+v", opt)
	}

	if TokenUrl != "https://token.example.test" {
		t.Fatalf("TokenUrl not updated, got %q", TokenUrl)
	}
	if MetaUrl != "https://meta.example.test" {
		t.Fatalf("MetaUrl not updated, got %q", MetaUrl)
	}
	if ImageUrl != "https://image.example.test" {
		t.Fatalf("ImageUrl not updated, got %q", ImageUrl)
	}
}

func TestParseOptions_AliasesAndPrompt(t *testing.T) {
	withEndpointReset(t)

	opt, err := ParseOptions([]string{"-i", "in.tcia", "-o", "out", "-p", "3", "-m", "-u", "bob", "-w", "-f", "-v"}, strings.NewReader("pw123\n"))
	if err != nil {
		t.Fatalf("ParseOptions returned error: %v", err)
	}

	if opt.Input != "in.tcia" || opt.Output != "out" || opt.Concurrent != 3 {
		t.Fatalf("alias parse failed: %+v", opt)
	}
	if !opt.Meta || !opt.Prompt || !opt.Force || !opt.Version || opt.Username != "bob" {
		t.Fatalf("alias parse failed: %+v", opt)
	}
	if opt.Password != "pw123" {
		t.Fatalf("expected prompted password, got %q", opt.Password)
	}
}

func TestParseOptions_Defaults(t *testing.T) {
	withEndpointReset(t)

	opt, err := ParseOptions([]string{"--input", "default.tcia"}, strings.NewReader(""))
	if err != nil {
		t.Fatalf("ParseOptions returned error: %v", err)
	}

	if opt.Output != "./" || opt.Concurrent != 2 || opt.MaxRetries != 3 || opt.MaxConnsPerHost != 8 {
		t.Fatalf("unexpected defaults: %+v", opt)
	}
	if opt.RetryDelay != 10*time.Second || opt.RequestDelay != 500*time.Millisecond || opt.MetadataWorkers != 20 {
		t.Fatalf("unexpected duration/worker defaults: %+v", opt)
	}
	if opt.Username != "nbia_guest" || opt.DirectoryMode != "descriptive" || opt.Auth != "" {
		t.Fatalf("unexpected user/default string values: %+v", opt)
	}
	if ImageUrl != "https://services.cancerimagingarchive.net/nbia-api/services/v4/getImage" {
		t.Fatalf("expected v4 image endpoint by default, got %q", ImageUrl)
	}
}

func TestParseOptions_ServerFriendlyOverrides(t *testing.T) {
	withEndpointReset(t)

	opt, err := ParseOptions([]string{"--input", "in.tcia", "--server-friendly", "--max-connections", "25", "--processes", "9", "--metadata-workers", "80"}, strings.NewReader(""))
	if err != nil {
		t.Fatalf("ParseOptions returned error: %v", err)
	}

	if !opt.ServerFriendly {
		t.Fatalf("expected server-friendly mode enabled")
	}
	if opt.Concurrent != 1 || opt.MaxConnsPerHost != 2 || opt.MetadataWorkers != 5 {
		t.Fatalf("server-friendly overrides not applied: %+v", opt)
	}
	if opt.RetryDelay != 30*time.Second || opt.RequestDelay != 2*time.Second {
		t.Fatalf("server-friendly delays not applied: %+v", opt)
	}
}

func TestParseOptions_HelpAndNoArgs(t *testing.T) {
	withEndpointReset(t)

	opt, err := ParseOptions([]string{"--help"}, strings.NewReader(""))
	if !errors.Is(err, ErrShowHelp) {
		t.Fatalf("expected ErrShowHelp for --help, got %v", err)
	}
	if !opt.Help {
		t.Fatalf("expected Help flag true")
	}

	_, err = ParseOptions([]string{}, strings.NewReader(""))
	if !errors.Is(err, ErrShowHelp) {
		t.Fatalf("expected ErrShowHelp for no args, got %v", err)
	}
}

func TestParseOptions_IncompatibleNoDecompress(t *testing.T) {
	withEndpointReset(t)

	_, err := ParseOptions([]string{"--input", "in.tcia", "--no-decompress"}, strings.NewReader(""))
	if err == nil {
		t.Fatalf("expected incompatibility error, got nil")
	}
	if !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("expected incompatible message, got %v", err)
	}
}

func TestParseOptions_InvalidFlag(t *testing.T) {
	withEndpointReset(t)

	_, err := ParseOptions([]string{"--input", "in.tcia", "--not-a-flag"}, strings.NewReader(""))
	if err == nil {
		t.Fatalf("expected parse error for unknown flag")
	}
}
