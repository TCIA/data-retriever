package main

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewestLogFileInDirSelectsMostRecentLog(t *testing.T) {
	dir := t.TempDir()

	olderLog := filepath.Join(dir, "older.log")
	newerLog := filepath.Join(dir, "newer.log")
	nonLogFile := filepath.Join(dir, "notes.txt")

	if err := os.WriteFile(olderLog, []byte("older"), 0o644); err != nil {
		t.Fatalf("failed to create older log: %v", err)
	}
	if err := os.WriteFile(newerLog, []byte("newer"), 0o644); err != nil {
		t.Fatalf("failed to create newer log: %v", err)
	}
	if err := os.WriteFile(nonLogFile, []byte("ignore"), 0o644); err != nil {
		t.Fatalf("failed to create non-log file: %v", err)
	}

	olderTime := time.Now().Add(-2 * time.Hour)
	newerTime := time.Now().Add(-1 * time.Hour)
	if err := os.Chtimes(olderLog, olderTime, olderTime); err != nil {
		t.Fatalf("failed to set older log time: %v", err)
	}
	if err := os.Chtimes(newerLog, newerTime, newerTime); err != nil {
		t.Fatalf("failed to set newer log time: %v", err)
	}

	path, err := newestLogFileInDir(dir)
	if err != nil {
		t.Fatalf("expected newest log, got error: %v", err)
	}

	expected := filepath.Clean(newerLog)
	if path != expected {
		t.Fatalf("expected %s, got %s", expected, path)
	}
}

func TestNewestLogFileInDirErrorsWhenNoLogFilesExist(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "not-a-log.txt"), []byte("data"), 0o644); err != nil {
		t.Fatalf("failed to create fixture file: %v", err)
	}

	_, err := newestLogFileInDir(dir)
	if err == nil {
		t.Fatalf("expected error when no .log files exist")
	}
}

func TestNewestLogFileInDirIgnoresDirectories(t *testing.T) {
	dir := t.TempDir()

	logDirNamedLikeFile := filepath.Join(dir, "archive.log")
	if err := os.Mkdir(logDirNamedLikeFile, 0o755); err != nil {
		t.Fatalf("failed to create directory fixture: %v", err)
	}

	realLog := filepath.Join(dir, "current.log")
	if err := os.WriteFile(realLog, []byte("current"), 0o644); err != nil {
		t.Fatalf("failed to create real log: %v", err)
	}

	path, err := newestLogFileInDir(dir)
	if err != nil {
		t.Fatalf("expected real log file, got error: %v", err)
	}

	expected := filepath.Clean(realLog)
	if path != expected {
		t.Fatalf("expected %s, got %s", expected, path)
	}
}

func TestBuildMailtoURLIncludesRecipientAndFields(t *testing.T) {
	recipient := "help@cancerimagingarchive.net"
	subject := "Support Request"
	body := "Please review attached log"
	attachmentPath := "/tmp/current.log"

	mailto := buildMailtoURL(recipient, subject, body, attachmentPath)
	prefix := "mailto:" + recipient
	if !strings.HasPrefix(mailto, prefix) {
		t.Fatalf("expected mailto prefix %q, got %q", prefix, mailto)
	}

	queryIndex := strings.Index(mailto, "?")
	if queryIndex == -1 {
		t.Fatalf("expected query parameters in %q", mailto)
	}

	values, err := url.ParseQuery(mailto[queryIndex+1:])
	if err != nil {
		t.Fatalf("failed to parse query params: %v", err)
	}

	if got := values.Get("subject"); got != subject {
		t.Fatalf("expected subject %q, got %q", subject, got)
	}
	if got := values.Get("body"); got != body {
		t.Fatalf("expected body %q, got %q", body, got)
	}

	expectedAttachment := "file:///tmp/current.log"
	if got := values.Get("attach"); got != expectedAttachment {
		t.Fatalf("expected attach %q, got %q", expectedAttachment, got)
	}
	if got := values.Get("attachment"); got != expectedAttachment {
		t.Fatalf("expected attachment %q, got %q", expectedAttachment, got)
	}
}

func TestBuildMailtoURLEmptyRecipientNoFields(t *testing.T) {
	mailto := buildMailtoURL("", "", "", "")
	if mailto != "mailto:" {
		t.Fatalf("expected bare mailto URL, got %q", mailto)
	}
}
