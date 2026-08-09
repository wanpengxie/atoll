package runtime

import (
	"context"
	"sync"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type workerSlot struct {
	mu         sync.Mutex
	closed     bool
	generation uint64
	worker     driverproto.Worker
	cancel     context.CancelFunc
	retiring   bool
}

func (s *workerSlot) install(g uint64, w driverproto.Worker, cancel context.CancelFunc) bool {
	s.mu.Lock()
	if s.closed || s.worker != nil {
		s.mu.Unlock()
		retirePhysical(w, cancel)
		return false
	}
	s.generation, s.worker, s.cancel, s.retiring = g, w, cancel, false
	s.mu.Unlock()
	return true
}

func (s *workerSlot) get(g uint64) driverproto.Worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generation != g || s.retiring {
		return nil
	}
	return s.worker
}

// retire is the sole physical Worker.Retire write mouth.
func (s *workerSlot) retire(g uint64) {
	s.mu.Lock()
	if s.generation != g || s.worker == nil || s.retiring {
		s.mu.Unlock()
		return
	}
	s.retiring = true
	w, cancel := s.worker, s.cancel
	s.mu.Unlock()
	retirePhysical(w, cancel)
}

func retirePhysical(w driverproto.Worker, cancel context.CancelFunc) {
	if cancel != nil {
		cancel()
	}
	w.Retire()
}

func (s *workerSlot) clear(g uint64, w driverproto.Worker) bool {
	s.mu.Lock()
	if s.generation != g || s.worker != w {
		s.mu.Unlock()
		return false
	}
	cancel := s.cancel
	s.generation, s.worker, s.cancel, s.retiring = 0, nil, nil, false
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return true
}

func (s *workerSlot) close() {
	s.mu.Lock()
	s.closed = true
	g := s.generation
	s.mu.Unlock()
	if g != 0 {
		s.retire(g)
	}
}
