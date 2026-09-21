package app

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewClientNegotiatesHTTP2(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	client := newClient("", 2)
	t.Cleanup(client.CloseIdleConnections)

	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("HTTP/2 request failed: %v", err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("reading HTTP/2 response: %v", err)
	}

	if resp.ProtoMajor != 2 {
		t.Fatalf("expected HTTP/2, got %s", resp.Proto)
	}
}

func TestNewClientFallsBackToHTTP1(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	client := newClient("", 2)
	t.Cleanup(client.CloseIdleConnections)

	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("HTTP/1.1 request failed: %v", err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("reading HTTP/1.1 response: %v", err)
	}

	if resp.ProtoMajor != 1 {
		t.Fatalf("expected HTTP/1.x fallback, got %s", resp.Proto)
	}
}

func TestDirectDownloadOutlivesSharedClientTimeout(t *testing.T) {
	const content = "large-file-content"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "18")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(75 * time.Millisecond)
		_, _ = io.WriteString(w, content)
	}))
	t.Cleanup(server.Close)

	client := &http.Client{Timeout: 20 * time.Millisecond}
	info := &FileInfo{
		Collection:        "collection",
		PatientID:         "patient",
		StudyInstanceUID:  "study",
		SeriesInstanceUID: "series",
		DownloadURL:       server.URL,
		FileName:          "large.svs",
	}
	output := t.TempDir()
	options := &Options{}

	if err := info.downloadDirect(context.Background(), output, client, options, nil, nil); err != nil {
		t.Fatalf("direct download was canceled by the shared client timeout: %v", err)
	}

	path := filepath.Join(info.DcimFiles(output, options), info.FileName)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading downloaded file: %v", err)
	}
	if string(got) != content {
		t.Fatalf("downloaded content = %q, want %q", got, content)
	}
}
