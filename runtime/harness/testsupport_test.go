package harness

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khlog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// memActorRegistry is an actorreg.Registry implementation for harness tests.
type memActorRegistry struct {
	mu   sync.Mutex
	rows map[actor.ActorID]actorreg.Record
}

func newMemActorRegistry() *memActorRegistry {
	return &memActorRegistry{rows: map[actor.ActorID]actorreg.Record{}}
}

func (r *memActorRegistry) Lookup(_ context.Context, id actor.ActorID) (actorreg.Record, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.rows[id]
	return rec, ok, nil
}

func (r *memActorRegistry) Exists(_ context.Context, id actor.ActorID) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.rows[id]
	return ok, nil
}

func (r *memActorRegistry) ListActive(_ context.Context) ([]actorreg.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]actorreg.Record, 0, len(r.rows))
	for _, rec := range r.rows {
		if rec.IsActive() {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (r *memActorRegistry) Insert(_ context.Context, rec actorreg.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.rows[rec.ID]; dup {
		return errors.New("duplicate")
	}
	r.rows[rec.ID] = rec
	return nil
}

func (r *memActorRegistry) Deregister(_ context.Context, id actor.ActorID, at int64) error {
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

// memLog is a MessageLog implementation backed by an in-memory slice.
// It models the harness/store contract closely enough that the chain
// can run end-to-end (dedupe / parent lookup / terminal duplicate).
type memLog struct {
	mu       sync.Mutex
	rows     map[string]message.Envelope
	seq      int64
	terminal map[string]string // parent_id → response_id (only when is_terminal=true + kind=response)
	failOn   string            // injected failure marker
}

func newMemLog() *memLog {
	return &memLog{rows: map[string]message.Envelope{}, terminal: map[string]string{}}
}

func (l *memLog) Append(_ context.Context, env *message.Envelope) (khlog.AppendResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failOn != "" && env.ID == l.failOn {
		return khlog.AppendResult{}, errors.New("memLog: injected failure")
	}
	if existing, ok := l.rows[env.ID]; ok {
		env.Seq = existing.Seq
		env.IsTerminal = existing.IsTerminal
		return khlog.AppendResult{Seq: khlog.Seq(existing.Seq), IsTerminal: existing.IsTerminal, Deduped: true}, nil
	}
	// terminal uniqueness on (parent_id) for kind=response when is_terminal.
	if env.Kind == message.KindResponse && env.IsTerminal {
		if existingID, dup := l.terminal[env.ParentID]; dup {
			return khlog.AppendResult{}, &khlog.AppendError{
				Reason:           message.HarnessTerminalDuplicate,
				Detail:           "terminal already exists for parent=" + env.ParentID,
				PartialMessageID: existingID,
			}
		}
		l.terminal[env.ParentID] = env.ID
	}
	l.seq++
	env.Seq = l.seq
	l.rows[env.ID] = *env
	return khlog.AppendResult{Seq: khlog.Seq(l.seq), IsTerminal: env.IsTerminal, Deduped: false}, nil
}

func (l *memLog) FindByID(_ context.Context, _ channel.ID, id string) (message.Envelope, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	row, ok := l.rows[id]
	return row, ok, nil
}

// newTestChain wires a Chain over an in-memory log + actor registry.
// The actor list is pre-populated with a handful of senders that
// cover human / agent / system / tool kinds.
func newTestChain(t interface {
	Fatalf(string, ...any)
	Helper()
}, opts ...func(*Deps)) (*Chain, *memActorRegistry, *memLog, *InMemoryTypeRegistry) {
	t.Helper()
	areg := newMemActorRegistry()
	_ = areg.Insert(context.Background(), actorreg.Record{ID: "agent:alpha", Kind: actor.KindAgent, CreatedAt: 1})
	_ = areg.Insert(context.Background(), actorreg.Record{ID: "user:demo", Kind: actor.KindHuman, CreatedAt: 1})
	_ = areg.Insert(context.Background(), actorreg.Record{ID: actor.SystemActorID, Kind: actor.KindSystem, CreatedAt: 1})
	_ = areg.Insert(context.Background(), actorreg.Record{ID: "tool:feishu", Kind: actor.KindTool, CreatedAt: 1})

	log := newMemLog()
	treg := NewInMemoryTypeRegistry()
	deps := Deps{
		ChannelID:     channel.ID("ch-1"),
		ActorRegistry: areg,
		TypeRegistry:  treg,
		Log:           log,
		NowMs:         func() int64 { return 1700000000000 },
	}
	for _, opt := range opts {
		opt(&deps)
	}
	c, err := New(deps)
	if err != nil {
		t.Fatalf("New chain: %v", err)
	}
	return c, areg, log, treg
}

func newEvent(senderID actor.ActorID, t string, payload json.RawMessage) *message.Envelope {
	return &message.Envelope{
		ID:        "evt-" + string(senderID),
		ChannelID: "ch-1",
		Sender:    message.Sender{ID: senderID},
		Type:      t,
		Kind:      message.KindEvent,
		Payload:   payload,
		Audience:  []string{"*"},
	}
}

func newRequest(id string, senderID actor.ActorID, t string, audience string, payload json.RawMessage) *message.Envelope {
	return &message.Envelope{
		ID:        id,
		ChannelID: "ch-1",
		Sender:    message.Sender{ID: senderID},
		Type:      t,
		Kind:      message.KindRequest,
		Payload:   payload,
		Audience:  []string{audience},
	}
}

func newResponse(id string, senderID actor.ActorID, parentID, t string, payload json.RawMessage) *message.Envelope {
	return &message.Envelope{
		ID:        id,
		ChannelID: "ch-1",
		Sender:    message.Sender{ID: senderID},
		Type:      t,
		Kind:      message.KindResponse,
		Payload:   payload,
		Audience:  []string{"*"},
		ParentID:  parentID,
	}
}
