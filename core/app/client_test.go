package app

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
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
