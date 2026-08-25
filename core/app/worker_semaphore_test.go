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
			if err := s.Acquire(ctx, 0); err != nil {
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
	run := func(priority int64, n int) {
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := s.Acquire(ctx, priority); err != nil {
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
	go func() { defer outer.Done(); run(0, 5) }()
	go func() { defer outer.Done(); run(1, 5) }()
	outer.Wait()

	if max > 2 {
		t.Fatalf("observed %d concurrent holders across two runs, want <= 2", max)
	}
}

func TestWorkerSemaphoreSetLimitUnblocksWaiters(t *testing.T) {
	s := NewWorkerSemaphore(1)
	ctx := context.Background()

	if err := s.Acquire(ctx, 0); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		if err := s.Acquire(ctx, 0); err != nil {
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
	if err := s.Acquire(ctx, 0); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	cctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := s.Acquire(cctx, 0)
	if err == nil {
		t.Fatal("expected Acquire to fail once context deadline passed")
	}
}

func TestNilWorkerSemaphoreIsUnbounded(t *testing.T) {
	var s *WorkerSemaphore
	if err := s.Acquire(context.Background(), 0); err != nil {
		t.Fatalf("nil semaphore Acquire should no-op, got: %v", err)
	}
	s.Release() // must not panic
	if got := s.Limit(); got != 0 {
		t.Fatalf("nil semaphore Limit() = %d, want 0", got)
	}
}

// TestWorkerSemaphoreFavorsLowerPriorityWaiter is the core of "workers
// favor the oldest manifest in the queue": when several callers are
// blocked waiting for the same semaphore and a slot frees up, the waiter
// with the lowest priority value (representing the oldest manifest) must
// be granted the slot first, regardless of the order Acquire was called in.
func TestWorkerSemaphoreFavorsLowerPriorityWaiter(t *testing.T) {
	s := NewWorkerSemaphore(1)
	ctx := context.Background()

	// Hold the only slot so subsequent Acquire calls queue up.
	if err := s.Acquire(ctx, 0); err != nil {
		t.Fatalf("initial Acquire: %v", err)
	}

	type result struct {
		priority int64
		order    int
	}
	var mu sync.Mutex
	var granted []result
	var seq int32

	waitFor := func(priority int64) chan struct{} {
		started := make(chan struct{})
		done := make(chan struct{})
		go func() {
			close(started)
			if err := s.Acquire(ctx, priority); err != nil {
				t.Errorf("Acquire(priority=%d): %v", priority, err)
				return
			}
			mu.Lock()
			granted = append(granted, result{priority: priority, order: int(atomic.AddInt32(&seq, 1))})
			mu.Unlock()
			close(done)
		}()
		<-started
		return done
	}

	// Queue newer (higher-priority-number) manifests' waiters first, then
	// the oldest one last, to prove ordering comes from priority and not
	// from call order.
	doneHigh := waitFor(5)
	doneMed := waitFor(2)
	time.Sleep(20 * time.Millisecond) // let both actually queue up
	doneOld := waitFor(0)
	time.Sleep(20 * time.Millisecond)

	s.Release() // frees the initial slot; only one waiter can take it

	select {
	case <-doneOld:
	case <-time.After(time.Second):
		t.Fatal("lowest-priority (oldest) waiter was not granted first")
	}

	s.Release()
	select {
	case <-doneMed:
	case <-time.After(time.Second):
		t.Fatal("second-lowest-priority waiter was not granted second")
	}

	s.Release()
	select {
	case <-doneHigh:
	case <-time.After(time.Second):
		t.Fatal("highest-priority-number waiter was not granted last")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(granted) != 3 {
		t.Fatalf("granted = %d waiters, want 3", len(granted))
	}
	if granted[0].priority != 0 || granted[1].priority != 2 || granted[2].priority != 5 {
		t.Fatalf("grant order by priority = %v, want [0 2 5]", granted)
	}
}

// TestPriorityWorkersHoldingSlotsFavorOlderManifest simulates the
// processFiles pattern (a worker acquires once and holds its slot across
// every file it processes, releasing only once it has none left) for two
// competing manifests. It's the regression test for "still allocating one
// slot to the second manifest instead of all of them to the first": with
// release-then-reacquire-per-file, a newer manifest's already-queued
// waiter could win a just-released slot in the gap before the same
// (older, favored) worker asked for it again. Holding the slot across
// files closes that gap, so the newer manifest must get zero slots for as
// long as the older manifest's workers still have files queued.
func TestPriorityWorkersHoldingSlotsFavorOlderManifest(t *testing.T) {
	const slots = 4
	const workersPerManifest = 4
	const filesPerManifest = 12
	const workDuration = 2 * time.Millisecond

	s := NewWorkerSemaphore(slots)
	ctx := context.Background()

	runManifest := func(priority int64) (count *int32, done <-chan struct{}) {
		input := make(chan int, filesPerManifest)
		for i := 0; i < filesPerManifest; i++ {
			input <- i
		}
		close(input)

		var processed int32
		var wg sync.WaitGroup
		wg.Add(workersPerManifest)
		for i := 0; i < workersPerManifest; i++ {
			go func() {
				defer wg.Done()
				acquired := false
				defer func() {
					if acquired {
						s.Release()
					}
				}()
				for range input {
					if !acquired {
						if err := s.Acquire(ctx, priority); err != nil {
							return
						}
						acquired = true
					}
					atomic.AddInt32(&processed, 1)
					time.Sleep(workDuration)
				}
			}()
		}

		doneCh := make(chan struct{})
		go func() {
			wg.Wait()
			close(doneCh)
		}()
		return &processed, doneCh
	}

	countA, doneA := runManifest(0)
	time.Sleep(5 * time.Millisecond) // let manifest A actually claim all 4 slots
	countB, doneB := runManifest(1)

	select {
	case <-doneA:
		t.Fatal("manifest A finished before manifest B's progress could be sampled; increase workDuration/filesPerManifest")
	default:
	}
	bWhileAActive := atomic.LoadInt32(countB)

	<-doneA
	<-doneB

	if bWhileAActive != 0 {
		t.Fatalf("manifest B (newer) processed %d files while manifest A (older, favored) still had work queued; want 0", bWhileAActive)
	}
	if got := atomic.LoadInt32(countA); got != filesPerManifest {
		t.Fatalf("manifest A processed %d files, want %d", got, filesPerManifest)
	}
	if got := atomic.LoadInt32(countB); got != filesPerManifest {
		t.Fatalf("manifest B processed %d files, want %d", got, filesPerManifest)
	}
}

// TestWorkerSemaphoreCancelledWaiterDoesNotBlockOthers ensures a waiter
// whose context is cancelled while queued is removed cleanly, without
// leaking a slot or blocking whoever is granted next.
func TestWorkerSemaphoreCancelledWaiterDoesNotBlockOthers(t *testing.T) {
	s := NewWorkerSemaphore(1)
	ctx := context.Background()

	if err := s.Acquire(ctx, 0); err != nil {
		t.Fatalf("initial Acquire: %v", err)
	}

	cctx, cancel := context.WithCancel(context.Background())
	cancelledDone := make(chan error, 1)
	go func() {
		cancelledDone <- s.Acquire(cctx, 0)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-cancelledDone:
		if err == nil {
			t.Fatal("expected cancelled Acquire to return an error")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled Acquire did not return")
	}

	survivorDone := make(chan struct{})
	go func() {
		if err := s.Acquire(context.Background(), 1); err != nil {
			t.Errorf("survivor Acquire: %v", err)
			return
		}
		close(survivorDone)
	}()

	s.Release() // frees the initial slot
	select {
	case <-survivorDone:
	case <-time.After(time.Second):
		t.Fatal("surviving waiter was never granted a slot after the cancelled one left the queue")
	}
}
