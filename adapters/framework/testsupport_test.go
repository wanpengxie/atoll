package framework

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// memoryActorRegistry is the test-only actor.Registry implementation.
type memoryActorRegistry struct {
	mu   sync.Mutex
	rows map[actor.ActorID]actor.Record
}

func newMemoryActorRegistry() *memoryActorRegistry {
	return &memoryActorRegistry{rows: map[actor.ActorID]actor.Record{}}
}

func (r *memoryActorRegistry) Insert(_ context.Context, rec actor.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.rows[rec.ID]; dup {
		return errors.New("duplicate actor")
	}
	r.rows[rec.ID] = rec
	return nil
}

func (r *memoryActorRegistry) Lookup(_ context.Context, id actor.ActorID) (actor.Record, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.rows[id]
	return rec, ok, nil
}

func (r *memoryActorRegistry) Exists(_ context.Context, id actor.ActorID) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.rows[id]
	return ok, nil
}

func (r *memoryActorRegistry) ListActive(_ context.Context) ([]actor.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]actor.Record, 0, len(r.rows))
	for _, rec := range r.rows {
		if rec.IsActive() {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (r *memoryActorRegistry) Deregister(_ context.Context, id actor.ActorID, at int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.rows[id]
	if !ok {
		return errors.New("not found")
	}
	rec.DeregisteredAt = at
	r.rows[id] = rec
	return nil
}

// fakeChain implements harness.Chain for tests. It records every Write
// and returns a configurable WriteResult / error.
type fakeChain struct {
	mu      sync.Mutex
	written []*message.Envelope
	// result returned for each successive Write. nil = always success.
	results []harness.WriteResult
	errs    []error
}

func newFakeChain() *fakeChain { return &fakeChain{} }

func (c *fakeChain) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *env // copy so post-modify by caller doesn't affect the recorded entry
	c.written = append(c.written, &cp)
	var res harness.WriteResult
	if len(c.results) > 0 {
		res = c.results[0]
		c.results = c.results[1:]
	} else {
		res = harness.WriteResult{MessageID: env.ID, Seq: int64(len(c.written))}
	}
	var err error
	if len(c.errs) > 0 {
		err = c.errs[0]
		c.errs = c.errs[1:]
	}
	return res, err
}

func (c *fakeChain) Written() []*message.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*message.Envelope, len(c.written))
	copy(out, c.written)
	return out
}

// fixedClock returns a clock that advances deterministically.
type fixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFixedClock(t time.Time) *fixedClock { return &fixedClock{now: t} }

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// recordingLogger captures every log line for assertion.
type recordingLogger struct {
	mu    sync.Mutex
	lines []logLine
}

type logLine struct {
	level string
	msg   string
	args  []any
}

func (l *recordingLogger) record(level, msg string, args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, logLine{level: level, msg: msg, args: append([]any(nil), args...)})
}

func (l *recordingLogger) Debug(msg string, args ...any) { l.record("debug", msg, args) }
func (l *recordingLogger) Info(msg string, args ...any)  { l.record("info", msg, args) }
func (l *recordingLogger) Warn(msg string, args ...any)  { l.record("warn", msg, args) }
func (l *recordingLogger) Error(msg string, args ...any) { l.record("error", msg, args) }

func (l *recordingLogger) snapshot() []logLine {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]logLine, len(l.lines))
	copy(out, l.lines)
	return out
}

// containsArg returns true if any line contains the given value among
// its args (loose string match for value).
func (l *recordingLogger) containsValue(needle string) bool {
	for _, line := range l.snapshot() {
		for _, a := range line.args {
			if s, ok := a.(string); ok && s == needle {
				return true
			}
		}
		if line.msg == needle {
			return true
		}
	}
	return false
}
