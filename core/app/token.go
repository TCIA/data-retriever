package app

import (
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

// Token handles NBIA token lifecycle management.
type Token struct {
	AccessToken      string    `json:"access_token"`
	SessionState     string    `json:"session_state"`
	ExpiresIn        int       `json:"expires_in"`
	NotBeforePolicy  int       `json:"not-before-policy"`
	RefreshExpiresIn int       `json:"refresh_expires_in"`
	Scope            string    `json:"scope"`
	IdToken          string    `json:"id_token"`
	RefreshToken     string    `json:"refresh_token"`
	TokenType        string    `json:"token_type"`
	ExpiredTime      time.Time `json:"expires_time"`

	mu         sync.RWMutex
	username   string
	password   string
	path       string
	httpClient *http.Client
}

// NewToken creates a token from the NBIA API, optionally restoring from disk.
func NewToken(username, passwd, path string, client *http.Client) (*Token, error) {
	logger.Debugf("creating token")
	token := &Token{
		username:   username,
		password:   passwd,
		path:       path,
		httpClient: client,
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		logger.Infof("restore token from %v", path)
		if err := token.Load(path); err != nil {
			logger.Error(err)
			logger.Infof("create new token")
		} else if token.ExpiredTime.Compare(time.Now()) > 0 {
			token.username = username
			token.password = passwd
			token.path = path
			token.httpClient = client
			return token, nil
		} else {
			logger.Warn("token expired, create new token")
		}
	}

	newToken, err := createNewToken(username, passwd, path, client)
	if err != nil {
		return nil, err
	}

	newToken.username = username
	newToken.password = passwd
	newToken.path = path
	newToken.httpClient = client

	return newToken, nil
}

// GetAccessToken returns the access token, refreshing if necessary.
func (token *Token) GetAccessToken() (string, error) {
	token.mu.RLock()
	if time.Now().Before(token.ExpiredTime) {
		accessToken := token.AccessToken
		token.mu.RUnlock()
		return accessToken, nil
	}
	token.mu.RUnlock()

	token.mu.Lock()
	defer token.mu.Unlock()

	if time.Now().Before(token.ExpiredTime) {
		return token.AccessToken, nil
	}

	logger.Infof("Token expired, refreshing...")
	newToken, err := createNewToken(token.username, token.password, token.path, token.httpClient)
	if err != nil {
		return "", fmt.Errorf("failed to refresh token: %v", err)
	}

	token.AccessToken = newToken.AccessToken
	token.SessionState = newToken.SessionState
	token.ExpiresIn = newToken.ExpiresIn
	token.NotBeforePolicy = newToken.NotBeforePolicy
	token.RefreshExpiresIn = newToken.RefreshExpiresIn
	token.Scope = newToken.Scope
	token.IdToken = newToken.IdToken
	token.RefreshToken = newToken.RefreshToken
	token.TokenType = newToken.TokenType
	token.ExpiredTime = newToken.ExpiredTime

	if err := token.dumpInternal(); err != nil {
		logger.Warnf("Failed to save refreshed token: %v", err)
	}

	return token.AccessToken, nil
}

func makeURL(urlStr string, values map[string]interface{}) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return urlStr, fmt.Errorf("failed to parse url: %v", err)
	}

	queries := u.Query()
	for k, v := range values {
		queries.Set(k, fmt.Sprintf("%v", v))
	}

	u.RawQuery = queries.Encode()
	return u.String(), nil
}

func createNewToken(username, passwd, path string, client *http.Client) (*Token, error) {
	formData := url.Values{}
	formData.Set("username", username)
	formData.Set("password", passwd)
	formData.Set("client_id", "NBIA")
	formData.Set("grant_type", "password")

	req, err := http.NewRequest("POST", TokenUrl, strings.NewReader(formData.Encode()))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := doRequest(client, req)
	if err != nil {
		return nil, fmt.Errorf("failed to do request: %v", err)
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token request failed with status %d: %s", resp.StatusCode, string(content))
	}

	token := new(Token)
	if err := json.Unmarshal(content, token); err != nil {
		return nil, fmt.Errorf("failed to unmarshal token: %v", err)
	}

	token.ExpiredTime = time.Now().Local().Add(time.Second * time.Duration(token.ExpiresIn))
	token.username = username
	token.password = passwd
	token.path = path
	token.httpClient = client

	if path != "" {
		if err := token.Dump(path); err != nil {
			logger.Warnf("Failed to save token: %v", err)
		}
	}

	return token, nil
}

// Dump persists the token to disk.
func (token *Token) Dump(path string) error {
	token.mu.RLock()
	defer token.mu.RUnlock()
	return token.dumpInternal()
}

func (token *Token) dumpInternal() error {
	if token.path == "" {
		return nil
	}

	logger.Debugf("saving token to %s", token.path)

	tempPath := token.path + ".tmp"
	f, err := os.OpenFile(tempPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to open token json: %v", err)
	}

	tokenCopy := struct {
		AccessToken      string    `json:"access_token"`
		SessionState     string    `json:"session_state"`
		ExpiresIn        int       `json:"expires_in"`
		NotBeforePolicy  int       `json:"not-before-policy"`
		RefreshExpiresIn int       `json:"refresh_expires_in"`
		Scope            string    `json:"scope"`
		IdToken          string    `json:"id_token"`
		RefreshToken     string    `json:"refresh_token"`
		TokenType        string    `json:"token_type"`
		ExpiredTime      time.Time `json:"expires_time"`
	}{
		AccessToken:      token.AccessToken,
		SessionState:     token.SessionState,
		ExpiresIn:        token.ExpiresIn,
		NotBeforePolicy:  token.NotBeforePolicy,
		RefreshExpiresIn: token.RefreshExpiresIn,
		Scope:            token.Scope,
		IdToken:          token.IdToken,
		RefreshToken:     token.RefreshToken,
		TokenType:        token.TokenType,
		ExpiredTime:      token.ExpiredTime,
	}

	content, err := json.MarshalIndent(tokenCopy, "", "    ")
	if err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to marshal token: %v", err)
	}

	if _, err := f.Write(content); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to dump token: %v", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to close token file: %v", err)
	}

	if err := os.Rename(tempPath, token.path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename token file: %v", err)
	}

	return nil
}

// Load restores token state from disk.
func (token *Token) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open token json: %v", err)
	}
	defer f.Close()

	content, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("failed to read token: %v", err)
	}
	if err := json.Unmarshal(content, token); err != nil {
		return fmt.Errorf("failed to unmarshal token: %v", err)
	}

	return nil
}
