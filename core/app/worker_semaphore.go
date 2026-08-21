package app

import (
	"context"
	"sync"
)

// WorkerSemaphore is a counting semaphore whose capacity can be resized
// while callers are blocked waiting on it. It exists so a single limit can
// be shared across multiple independent Run() calls (e.g. several manifests
// downloading concurrently in the GUI): the total number of holders across
// all of them never exceeds the configured limit, and changing the limit
// takes effect immediately for everyone sharing the instance.
//
// A nil *WorkerSemaphore is valid and imposes no limit — Acquire and
// Release are no-ops — so callers that don't need cross-run coordination
// (the CLI) can simply leave it unset.
type WorkerSemaphore struct {
	mu     sync.Mutex
	limit  int
	inUse  int
	waitCh chan struct{}
}

// NewWorkerSemaphore creates a semaphore that allows at most limit
// concurrent holders. limit is clamped to at least 1.
func NewWorkerSemaphore(limit int) *WorkerSemaphore {
	if limit < 1 {
		limit = 1
	}
	return &WorkerSemaphore{
		limit:  limit,
		waitCh: make(chan struct{}),
	}
}

// notifyLocked wakes every goroutine currently blocked in Acquire so it can
// re-check whether a slot is now available. Must be called with mu held.
func (s *WorkerSemaphore) notifyLocked() {
	close(s.waitCh)
	s.waitCh = make(chan struct{})
}

// SetLimit changes the maximum number of concurrent holders, effective
// immediately for anyone currently blocked in Acquire. limit is clamped to
// at least 1.
func (s *WorkerSemaphore) SetLimit(limit int) {
	if s == nil {
		return
	}
	if limit < 1 {
		limit = 1
	}
	s.mu.Lock()
	s.limit = limit
	s.notifyLocked()
	s.mu.Unlock()
}

// Limit returns the current maximum number of concurrent holders.
func (s *WorkerSemaphore) Limit() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limit
}

// Acquire blocks until a slot is available or ctx is done, whichever comes
// first. On a nil receiver it returns immediately with no error.
func (s *WorkerSemaphore) Acquire(ctx context.Context) error {
	if s == nil {
		return nil
	}
	for {
		s.mu.Lock()
		if s.inUse < s.limit {
			s.inUse++
			s.mu.Unlock()
			return nil
		}
		wait := s.waitCh
		s.mu.Unlock()

		select {
		case <-wait:
			// Limit grew or a slot was released — recheck.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Release frees a slot acquired via Acquire. On a nil receiver it is a
// no-op.
func (s *WorkerSemaphore) Release() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.inUse > 0 {
		s.inUse--
	}
	s.notifyLocked()
	s.mu.Unlock()
}
