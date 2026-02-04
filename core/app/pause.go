package app

import (
	"context"
	"sync"
)

// PauseController coordinates per-series pause/resume requests.
type PauseController struct {
	mu      sync.Mutex
	paused  map[string]bool
	waiters map[string]chan struct{}
	cancels map[string]context.CancelFunc
}

func NewPauseController() *PauseController {
	return &PauseController{
		paused:  make(map[string]bool),
		waiters: make(map[string]chan struct{}),
		cancels: make(map[string]context.CancelFunc),
	}
}

func (p *PauseController) Pause(seriesUID string) {
	if seriesUID == "" {
		return
	}
	p.mu.Lock()
	p.paused[seriesUID] = true
	if _, ok := p.waiters[seriesUID]; !ok {
		p.waiters[seriesUID] = make(chan struct{})
	}
	cancel := p.cancels[seriesUID]
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (p *PauseController) Resume(seriesUID string) {
	if seriesUID == "" {
		return
	}
	p.mu.Lock()
	paused := p.paused[seriesUID]
	ch := p.waiters[seriesUID]
	delete(p.paused, seriesUID)
	delete(p.waiters, seriesUID)
	p.mu.Unlock()
	if paused && ch != nil {
		close(ch)
	}
}

func (p *PauseController) IsPaused(seriesUID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused[seriesUID]
}

func (p *PauseController) WaitIfPaused(ctx context.Context, seriesUID string) bool {
	if seriesUID == "" {
		return false
	}
	p.mu.Lock()
	if !p.paused[seriesUID] {
		p.mu.Unlock()
		return false
	}
	ch := p.waiters[seriesUID]
	if ch == nil {
		ch = make(chan struct{})
		p.waiters[seriesUID] = ch
	}
	p.mu.Unlock()

	select {
	case <-ctx.Done():
		return true
	case <-ch:
		return false
	}
}

func (p *PauseController) RegisterCancel(seriesUID string, cancel context.CancelFunc) {
	if seriesUID == "" || cancel == nil {
		return
	}
	p.mu.Lock()
	p.cancels[seriesUID] = cancel
	paused := p.paused[seriesUID]
	p.mu.Unlock()
	if paused {
		cancel()
	}
}

func (p *PauseController) PauseAll() {
	p.mu.Lock()
	seriesIDs := make([]string, 0, len(p.cancels))
	for seriesUID := range p.cancels {
		seriesIDs = append(seriesIDs, seriesUID)
		p.paused[seriesUID] = true
		if _, ok := p.waiters[seriesUID]; !ok {
			p.waiters[seriesUID] = make(chan struct{})
		}
	}
	cancels := make([]context.CancelFunc, 0, len(seriesIDs))
	for _, seriesUID := range seriesIDs {
		if cancel := p.cancels[seriesUID]; cancel != nil {
			cancels = append(cancels, cancel)
		}
	}
	p.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func (p *PauseController) ResumeAll() {
	p.mu.Lock()
	waiters := make([]chan struct{}, 0, len(p.waiters))
	for seriesUID, ch := range p.waiters {
		if ch != nil {
			waiters = append(waiters, ch)
		}
		delete(p.paused, seriesUID)
		delete(p.waiters, seriesUID)
	}
	p.mu.Unlock()

	for _, ch := range waiters {
		close(ch)
	}
}

func (p *PauseController) UnregisterCancel(seriesUID string, cancel context.CancelFunc) {
	if seriesUID == "" || cancel == nil {
		return
	}
	p.mu.Lock()
	if _, ok := p.cancels[seriesUID]; ok {
		delete(p.cancels, seriesUID)
	}
	p.mu.Unlock()
}
