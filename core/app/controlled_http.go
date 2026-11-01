package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const controlledDRSHost = "nci-crdc.datacommons.io"

// parseControlledDRS deliberately supports the CRDC/Gen3 route, not general DRS.
// No caller-controlled authority may receive the user's credential.
func parseControlledDRS(raw string) (string, string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "drs" || u.Host != controlledDRSHost || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(raw, "#") || u.Opaque != "" {
		return "", "", fmt.Errorf("unrecognized controlled-download host or invalid DRS URI; no credential was sent")
	}
	id := strings.TrimPrefix(u.Path, "/")
	if id == "" {
		return "", "", fmt.Errorf("DRS URI requires an object identifier")
	}
	for _, segment := range strings.Split(id, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", "", fmt.Errorf("invalid controlled-file identifier")
		}
		for _, c := range segment {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
				return "", "", fmt.Errorf("invalid controlled-file identifier")
			}
		}
	}
	return u.Host, id, nil
}

// controlledServiceClient refuses redirects, including redirects to subdomains
// (where Go can otherwise forward Authorization). It never changes the shared client.
func controlledServiceClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copy
}

// redactedRequestError keeps retry/cancellation classification without exposing
// net/url errors containing a signed URL, credential endpoint, or response body.
type redactedRequestError struct {
	operation string
	cause     error
}

func (e *redactedRequestError) Error() string { return e.operation + " failed" }
func (e *redactedRequestError) Unwrap() error { return e.cause }

func validControlledStorageURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Fragment == "" && u.Opaque == ""
}

func (info *FileInfo) downloadFromGen3(ctx context.Context, output string, client *http.Client, auth *Gen3AuthManager, options *Options, onProgress ProgressFunc) error {
	host, id, err := parseControlledDRS(info.DRSURI)
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	location, err := getGen3DownloadURLContext(ctx, client, host, url.PathEscape(id), auth)
	if err != nil {
		return fmt.Errorf("failed to authorize controlled download: %w", err)
	}
	// Keep the temporary signed URL out of cached metadata and completion logs.
	payload := *info
	payload.DownloadURL = location
	storageClient := controlledServiceClient(client)
	storageClient.Jar = nil
	storageClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("too many storage redirects")
		}
		if !validControlledStorageURL(req.URL.String()) {
			return fmt.Errorf("controlled storage redirect requires HTTPS without user information")
		}
		req.Header.Del("Authorization")
		req.Header.Del("Cookie")
		req.Header.Del("Referer")
		return nil
	}
	return payload.downloadDirect(ctx, output, storageClient, options, onProgress, nil)
}

type Gen3AuthManager struct {
	client *http.Client
	apiKey string
	tokens map[string]string
	mu     sync.Mutex
}

func NewGen3AuthManager(client *http.Client, authFile string) (*Gen3AuthManager, error) {
	if authFile == "" {
		return &Gen3AuthManager{}, nil
	}
	data, err := os.ReadFile(authFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read API key file: %w", err)
	}
	var key struct {
		APIKey string `json:"api_key"`
	}
	if err := json.Unmarshal(data, &key); err != nil {
		return nil, fmt.Errorf("failed to parse API key JSON")
	}
	key.APIKey = strings.TrimSpace(key.APIKey)
	if key.APIKey == "" {
		return nil, fmt.Errorf("'api_key' not found in JSON key file")
	}
	if err := SaveAuthFile(authFile); err != nil {
		logger.Warn("Failed to save auth file to safe location")
	}
	return &Gen3AuthManager{client: client, apiKey: key.APIKey, tokens: make(map[string]string)}, nil
}

// Token expiry is only a cache hint, not a JWT signature/authorization check.
// Opaque tokens remain usable and are invalidated on a service 401.
func tokenNearExpiry(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	return json.Unmarshal(data, &claims) == nil && claims.Exp > 0 && claims.Exp <= time.Now().Add(30*time.Second).Unix()
}

func (m *Gen3AuthManager) GetAccessToken(host string) (string, error) {
	return m.getAccessToken(context.Background(), host)
}

func (m *Gen3AuthManager) getAccessToken(ctx context.Context, host string) (string, error) {
	if host != controlledDRSHost {
		return "", fmt.Errorf("unrecognized controlled-download host; no credential was sent")
	}
	if m == nil || m.apiKey == "" {
		return "", fmt.Errorf("Gen3 authentication requires an API key, but none was provided")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if token := m.tokens[host]; token != "" && !tokenNearExpiry(token) {
		return token, nil
	}
	token, err := getGen3AccessTokenContext(ctx, m.client, host, m.apiKey)
	if err != nil {
		return "", err
	}
	if m.tokens == nil {
		m.tokens = make(map[string]string)
	}
	m.tokens[host] = token
	return token, nil
}

func (m *Gen3AuthManager) invalidateToken(host, rejected string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tokens[host] == rejected {
		delete(m.tokens, host)
	}
}

func getGen3AccessTokenContext(ctx context.Context, client *http.Client, host, key string) (string, error) {
	if host != controlledDRSHost {
		return "", fmt.Errorf("unrecognized controlled-download host; no credential was sent")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	data, _ := json.Marshal(map[string]string{"api_key": key})
	req, err := http.NewRequestWithContext(ctx, "POST", "https://"+host+"/user/credentials/api/access_token", bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("failed to construct authentication request")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := controlledServiceClient(client).Do(req)
	if err != nil {
		return "", &redactedRequestError{"controlled authentication request", err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", newHTTPStatusError(resp, time.Now())
	}
	var result struct {
		Token string `json:"access_token"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&result) != nil || strings.TrimSpace(result.Token) == "" {
		return "", fmt.Errorf("controlled service returned no valid access token")
	}
	return result.Token, nil
}

func getGen3DownloadURLContext(ctx context.Context, client *http.Client, host, objectID string, auth *Gen3AuthManager) (string, error) {
	if host != controlledDRSHost {
		return "", fmt.Errorf("unrecognized controlled-download host; no credential was sent")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", "https://"+host+"/user/data/download/"+objectID, nil)
		if err != nil {
			return "", fmt.Errorf("failed to construct controlled download request")
		}
		var token string
		if auth != nil && auth.apiKey != "" {
			token, err = auth.getAccessToken(ctx, host)
			if err != nil {
				return "", err
			}
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := controlledServiceClient(client).Do(req)
		if err != nil {
			return "", &redactedRequestError{"controlled authorization request", err}
		}
		if resp.StatusCode == http.StatusUnauthorized && token != "" && attempt == 0 {
			resp.Body.Close()
			auth.invalidateToken(host, token)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return "", newHTTPStatusError(resp, time.Now())
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
		resp.Body.Close()
		if readErr != nil {
			return "", fmt.Errorf("failed to read controlled authorization response")
		}
		var result struct {
			URL string `json:"url"`
		}
		if json.Unmarshal(data, &result) != nil {
			// Preserve the legacy plain-text URL response without logging it.
			result.URL = strings.TrimSpace(string(data))
		}
		if !validControlledStorageURL(result.URL) {
			return "", fmt.Errorf("controlled service returned no valid HTTPS file location")
		}
		return result.URL, nil
	}
	return "", fmt.Errorf("controlled authorization failed")
}
