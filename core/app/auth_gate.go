package app

import "sync"

type authResult struct {
    path      string
    cancelled bool
}

type AuthGate struct {
    mu           sync.Mutex
    pending      bool
    cancelled    bool
    waiters      []chan authResult
    resolvedPath string
}



func (g *AuthGate) PrepareRetry() {
    g.mu.Lock()
    defer g.mu.Unlock()
    g.pending = false
    g.resolvedPath = ""
    for _, ch := range g.waiters {
        ch <- authResult{} // wake waiters to re-register; path="" + cancelled=false = retry
    }
    g.waiters = nil
}

func (g *AuthGate) Resolve(authFilePath string) {
    g.mu.Lock()
    defer g.mu.Unlock()
    if authFilePath != "" {
        g.resolvedPath = authFilePath
    } else {
        g.cancelled = true
    }
    result := authResult{path: authFilePath, cancelled: authFilePath == ""}
    for _, ch := range g.waiters {
        ch <- result
    }
    g.waiters = nil
    g.pending = false
}

func (g *AuthGate) WaitForAuth(triggerPrompt func()) string {
    for {
        g.mu.Lock()
        if g.resolvedPath != "" {
            g.mu.Unlock()
            return g.resolvedPath
        }
        if g.cancelled {
            g.mu.Unlock()
            return ""
        }
        ch := make(chan authResult, 1)
        g.waiters = append(g.waiters, ch)
        firstCaller := !g.pending
        if firstCaller {
            g.pending = true
        }
        g.mu.Unlock()
        if firstCaller {
            triggerPrompt()
        }

        result := <-ch
        if result.cancelled {
            return ""
        }
        if result.path != "" {
            return result.path
        }
        // path=="" and not cancelled = PrepareRetry woke us, loop and re-register
    }
}




func (g *AuthGate) Reset() {
    g.mu.Lock()
    defer g.mu.Unlock()
    // Only reset the prompt state, never touch resolvedPath or cancelled —
    // those are permanent once set
    g.waiters = nil
    g.pending = false
}
