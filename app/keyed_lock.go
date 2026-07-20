package app

import (
	"sync"
)

type keyedLockSet struct {
	mu sync.Mutex
	m  map[string]*keyedLockEntry
}

type keyedLockEntry struct {
	mu   sync.Mutex
	refs int
}

func newKeyedLockSet() *keyedLockSet { return &keyedLockSet{m: map[string]*keyedLockEntry{}} }

func (s *keyedLockSet) lock(key string) func() {
	s.mu.Lock()
	e := s.m[key]
	if e == nil {
		e = &keyedLockEntry{}
		s.m[key] = e
	}
	e.refs++
	s.mu.Unlock()
	e.mu.Lock()
	var once sync.Once
	return func() {
		once.Do(func() {
			e.mu.Unlock()
			s.mu.Lock()
			e.refs--
			if e.refs == 0 && s.m[key] == e {
				delete(s.m, key)
			}
			s.mu.Unlock()
		})
	}
}
