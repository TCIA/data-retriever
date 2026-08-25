package app

import (
	"container/heap"
	"context"
	"sync"
)

// WorkerSemaphore is a counting semaphore whose capacity can be resized
// while callers are blocked waiting on it, and which grants a freed slot to
// the lowest-priority (oldest) waiter first rather than to whichever
// blocked caller happens to win a scheduling race. It exists so a single
// limit can be shared across multiple independent Run() calls (e.g.
// several manifests downloading concurrently in the GUI): the total number
// of holders across all of them never exceeds the configured limit,
// changing the limit takes effect immediately for everyone sharing the
// instance, and — via priority — an earlier-started manifest's pending
// work is favored over a later one's, so the goal of "the first manifest
// finishes before the next makes much progress" holds even though several
// manifests may technically be running at once.
//
// A nil *WorkerSemaphore is valid and imposes no limit — Acquire and
// Release are no-ops — so callers that don't need cross-run coordination
// (the CLI) can simply leave it unset.
type WorkerSemaphore struct {
	mu      sync.Mutex
	limit   int
	inUse   int
	waiters semWaiterHeap
	nextSeq int64
}

// semWaiter is one blocked Acquire call. ready is closed once a slot has
// been granted. index is maintained by container/heap and is -1 once the
// waiter has left the heap (granted or cancelled), used to detect and
// safely handle the race between a grant and a context cancellation.
type semWaiter struct {
	priority int64
	seq      int64
	ready    chan struct{}
	index    int
}

// semWaiterHeap orders waiters by priority (ascending — lower value first)
// and breaks ties by arrival order, so waiters of equal priority are
// granted FIFO.
type semWaiterHeap []*semWaiter

func (h semWaiterHeap) Len() int { return len(h) }
func (h semWaiterHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority < h[j].priority
	}
	return h[i].seq < h[j].seq
}
func (h semWaiterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *semWaiterHeap) Push(x interface{}) {
	w := x.(*semWaiter)
	w.index = len(*h)
	*h = append(*h, w)
}
func (h *semWaiterHeap) Pop() interface{} {
	old := *h
	n := len(old)
	w := old[n-1]
	old[n-1] = nil
	w.index = -1
	*h = old[:n-1]
	return w
}

// NewWorkerSemaphore creates a semaphore that allows at most limit
// concurrent holders. limit is clamped to at least 1.
func NewWorkerSemaphore(limit int) *WorkerSemaphore {
	if limit < 1 {
		limit = 1
	}
	return &WorkerSemaphore{limit: limit}
}

// grantLocked hands out freed slots to the lowest-priority (oldest)
// waiters until the semaphore is full or no waiters remain. Must be called
// with mu held.
func (s *WorkerSemaphore) grantLocked() {
	for s.inUse < s.limit && len(s.waiters) > 0 {
		w := heap.Pop(&s.waiters).(*semWaiter)
		s.inUse++
		close(w.ready)
	}
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
	s.grantLocked()
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
//
// priority determines who wins when multiple callers are waiting and a
// slot frees up: lower values are granted first. Callers should pass a
// value that reflects how long they've effectively been waiting — e.g. a
// sequence number assigned once when a manifest starts, shared by every
// file that manifest tries to acquire a slot for — so an older manifest's
// pending work is favored over a newer manifest's. Ties are broken by
// arrival order. A caller with no meaningful priority (e.g. the CLI, which
// never shares its semaphore) can pass 0.
func (s *WorkerSemaphore) Acquire(ctx context.Context, priority int64) error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	if s.inUse < s.limit && len(s.waiters) == 0 {
		s.inUse++
		s.mu.Unlock()
		return nil
	}

	w := &semWaiter{priority: priority, seq: s.nextSeq, ready: make(chan struct{})}
	s.nextSeq++
	heap.Push(&s.waiters, w)
	s.mu.Unlock()

	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		if w.index >= 0 {
			// Still queued — pull it out before it can be granted.
			heap.Remove(&s.waiters, w.index)
			s.mu.Unlock()
			return ctx.Err()
		}
		// Granted concurrently with cancellation, racing the select above:
		// we already hold a slot. Give it back rather than leaking it.
		s.mu.Unlock()
		s.Release()
		return ctx.Err()
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
	s.grantLocked()
	s.mu.Unlock()
}
