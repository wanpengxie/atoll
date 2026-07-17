package home

import "github.com/wanpengxie/atoll/protocol/actor"

// stateForTest is test-only introspection. Production decisions use the narrow,
// lock-scoped liveness operations instead of copying the full state row.
func (l *livenessLedger) stateForTest(id actor.ActorID) (lstate, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	return s, ok
}
