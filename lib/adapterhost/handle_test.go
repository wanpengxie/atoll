package adapterhost

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khrn "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
)

type recChain struct{ written []*message.Envelope }

func (c *recChain) Write(_ context.Context, env *message.Envelope) (khrn.WriteResult, error) {
	c.written = append(c.written, env)
	return khrn.WriteResult{MessageID: env.ID}, nil
}

type errModule struct{ err error }

func (errModule) Declares() behavior.Declaration {
	return behavior.Declaration{Name: "t", ActorID: "tool1", Types: []string{"x.do"}}
}
func (errModule) Init(context.Context, *behavior.ModuleContext) error { return nil }
func (errModule) Shutdown(context.Context) error                      { return nil }
func (m errModule) Handle(context.Context, *message.Envelope) error   { return m.err }
func (errModule) OnExternalCallback(context.Context, []byte) error    { return nil }

// TestHandleError_CollapsesReceiverInternalError proves a hard Handle error is
// WIRED to a terminal: the cell writes receiver_internal_error (author #1) and
// clears correlation — the caller does NOT hang.
func TestHandleError_CollapsesReceiverInternalError(t *testing.T) {
	fc := &recChain{}
	a := &adapterActor{
		self:        "tool1",
		module:      errModule{err: errors.New("boom")},
		declaration: behavior.Declaration{Name: "t", ActorID: "tool1", Types: []string{"x.do"}},
		chain:       fc,
		clock:       time.Now,
		correlation: map[behavior.CorrelationKey]behavior.CorrelationEntry{},
		inflight:    map[behavior.CorrelationKey]*message.Envelope{},
	}
	req := &message.Envelope{
		ID: "r1", ChannelID: "ch", Kind: message.KindRequest, Type: "x.do",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"tool1"},
	}
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive returned err: %v", err)
	}
	if len(fc.written) != 1 {
		t.Fatalf("expected 1 terminal written, got %d (Handle error not collapsed)", len(fc.written))
	}
	term := fc.written[0]
	if term.Kind != message.KindResponse || term.ParentID != "r1" || term.Sender.ID != "tool1" {
		t.Fatalf("bad terminal: kind=%s parent=%s sender=%s", term.Kind, term.ParentID, term.Sender.ID)
	}
	if !contains(string(term.Payload), "receiver_internal_error") || !contains(string(term.Payload), "failed") {
		t.Fatalf("payload=%s, want failed+receiver_internal_error", term.Payload)
	}
	// correlation must be cleared (not pending)
	if e, ok := a.correlation["r1"]; ok && e.State == behavior.CorrelationPending {
		t.Fatal("correlation still pending after Handle error (leak)")
	}
}

// TestHandleDeferred_KeepsPending proves ErrHandleDeferred does NOT collapse —
// the terminal arrives later (no premature failure).
func TestHandleDeferred_KeepsPending(t *testing.T) {
	fc := &recChain{}
	a := &adapterActor{
		self: "tool1", module: errModule{err: behavior.ErrHandleDeferred},
		declaration: behavior.Declaration{Name: "t", ActorID: "tool1", Types: []string{"x.do"}},
		chain:       fc, clock: time.Now,
		correlation: map[behavior.CorrelationKey]behavior.CorrelationEntry{},
		inflight:    map[behavior.CorrelationKey]*message.Envelope{},
	}
	req := &message.Envelope{ID: "r2", ChannelID: "ch", Kind: message.KindRequest, Type: "x.do",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"tool1"}}
	_ = a.Receive(context.Background(), req)
	if len(fc.written) != 0 {
		t.Fatalf("deferred Handle wrote %d terminals, want 0 (premature closure)", len(fc.written))
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
