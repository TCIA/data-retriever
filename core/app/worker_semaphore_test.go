package app

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerSemaphoreCapsGlobalConcurrency(t *testing.T) {
	s := NewWorkerSemaphore(3)
	ctx := context.Background()

	var current, max int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Acquire(ctx); err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			defer s.Release()

			n := atomic.AddInt32(&current, 1)
			for {
				old := atomic.LoadInt32(&max)
				if n <= old || atomic.CompareAndSwapInt32(&max, old, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			atomic.AddInt32(&current, -1)
		}()
	}
	wg.Wait()

	if max > 3 {
		t.Fatalf("observed %d concurrent holders, want <= 3", max)
	}
}

func TestWorkerSemaphoreSharedAcrossTwoRuns(t *testing.T) {
	// Simulates two manifests sharing one semaphore: their combined
	// concurrency must respect the shared limit, not 2x it.
	s := NewWorkerSemaphore(2)
	ctx := context.Background()

	var current, max int32
	run := func(n int) {
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := s.Acquire(ctx); err != nil {
					t.Errorf("Acquire: %v", err)
					return
				}
				defer s.Release()
				c := atomic.AddInt32(&current, 1)
				for {
					old := atomic.LoadInt32(&max)
					if c <= old || atomic.CompareAndSwapInt32(&max, old, c) {
						break
					}
				}
				time.Sleep(5 * time.Millisecond)
				atomic.AddInt32(&current, -1)
			}()
		}
		wg.Wait()
	}

	var outer sync.WaitGroup
	outer.Add(2)
	go func() { defer outer.Done(); run(5) }()
	go func() { defer outer.Done(); run(5) }()
	outer.Wait()

	if max > 2 {
		t.Fatalf("observed %d concurrent holders across two runs, want <= 2", max)
	}
}

func TestWorkerSemaphoreSetLimitUnblocksWaiters(t *testing.T) {
	s := NewWorkerSemaphore(1)
	ctx := context.Background()

	if err := s.Acquire(ctx); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		if err := s.Acquire(ctx); err != nil {
			t.Errorf("second Acquire: %v", err)
			return
		}
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("second Acquire returned before limit was raised")
	case <-time.After(50 * time.Millisecond):
	}

	s.SetLimit(2)

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second Acquire did not unblock after SetLimit raised the cap")
	}
}

func TestWorkerSemaphoreAcquireRespectsContextCancellation(t *testing.T) {
	s := NewWorkerSemaphore(1)
	ctx := context.Background()
	if err := s.Acquire(ctx); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	cctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := s.Acquire(cctx)
	if err == nil {
		t.Fatal("expected Acquire to fail once context deadline passed")
	}
}

func TestNilWorkerSemaphoreIsUnbounded(t *testing.T) {
	var s *WorkerSemaphore
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("nil semaphore Acquire should no-op, got: %v", err)
	}
	s.Release() // must not panic
	if got := s.Limit(); got != 0 {
		t.Fatalf("nil semaphore Limit() = %d, want 0", got)
	}
}
