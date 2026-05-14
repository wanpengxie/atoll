package supervisor

import (
	"context"
	"sync"

	"github.com/coagent-ai/coagent/kernel/actor"
	"github.com/coagent-ai/coagent/kernel/channel"
)

// ActorState models the lifecycle of one channel-actor (L2 §1.4.6).
type ActorState string

const (
	ActorPending  ActorState = "pending"
	ActorRunning  ActorState = "running"
	ActorStopped  ActorState = "stopped"
	ActorFaulted  ActorState = "faulted"
)

// ActorEntry tracks one actor's lifecycle.
type ActorEntry struct {
	ChannelID channel.ID
	ActorID   actor.ActorID
	State     ActorState
	LastError string
}

// Lifecycle is a tiny in-memory state machine the daemon uses to track
// which actors are alive in which channels. The actual start/stop is
// delegated to handler-specific code (workerhost.Spawn for agents,
// adapter.Manager.Install for in_process adapters, etc.).
type Lifecycle struct {
	mu      sync.Mutex
	entries map[string]ActorEntry // key = channelID + ":" + actorID
}

// NewLifecycle returns an empty Lifecycle.
func NewLifecycle() *Lifecycle {
	return &Lifecycle{entries: make(map[string]ActorEntry)}
}

func key(c channel.ID, a actor.ActorID) string { return string(c) + ":" + string(a) }

// MarkRunning marks an actor as running.
func (l *Lifecycle) MarkRunning(c channel.ID, a actor.ActorID) {
	l.mu.Lock()
	l.entries[key(c, a)] = ActorEntry{ChannelID: c, ActorID: a, State: ActorRunning}
	l.mu.Unlock()
}

// MarkStopped flips state to stopped.
func (l *Lifecycle) MarkStopped(c channel.ID, a actor.ActorID) {
	l.mu.Lock()
	entry := l.entries[key(c, a)]
	entry.ChannelID = c
	entry.ActorID = a
	entry.State = ActorStopped
	l.entries[key(c, a)] = entry
	l.mu.Unlock()
}

// MarkFaulted records an error path.
func (l *Lifecycle) MarkFaulted(c channel.ID, a actor.ActorID, err error) {
	l.mu.Lock()
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	l.entries[key(c, a)] = ActorEntry{
		ChannelID: c, ActorID: a, State: ActorFaulted, LastError: msg,
	}
	l.mu.Unlock()
}

// Get returns the entry, ok=false when none.
func (l *Lifecycle) Get(c channel.ID, a actor.ActorID) (ActorEntry, bool) {
	l.mu.Lock()
	e, ok := l.entries[key(c, a)]
	l.mu.Unlock()
	return e, ok
}

// Snapshot returns a copy of all entries.
func (l *Lifecycle) Snapshot() []ActorEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]ActorEntry, 0, len(l.entries))
	for _, e := range l.entries {
		out = append(out, e)
	}
	return out
}

// HasActive reports whether any actor in the channel is in Running state.
func (l *Lifecycle) HasActive(c channel.ID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	prefix := string(c) + ":"
	for k, e := range l.entries {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix && e.State == ActorRunning {
			return true
		}
	}
	return false
}

// Avoid unused-import warning if context is needed by future helpers.
var _ = context.TODO
