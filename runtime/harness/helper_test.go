package harness

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/internal/store"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// testChannelID is the channel every harness test binds its Deps to.
const testChannelID channel.ID = "ch-test"

// fixedNowMs is the deterministic clock the harness uses in tests so
// engine-filled timestamps (ts_received, default expires_at) are predictable.
const fixedNowMs int64 = 1_700_000_000_000

// newTestStore opens a real per-channel sqlite store in a temp dir. We use the
// real store (not a hand-rolled fake) because the harness leans on its actual
// MessageLog (FindByID / HasFinalResponse / Append) and Registry (Lookup)
// semantics — a drifting fake would test fiction, not substrate truth.
func newTestStore(t *testing.T) *store.ChannelStores {
	t.Helper()
	ctx := context.Background()
	cs, err := store.OpenChannel(ctx, "C-test", filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// testDeps wires a Deps against the real store with a fixed clock.
func testDeps(t *testing.T, cs *store.ChannelStores) Deps {
	t.Helper()
	return Deps{
		ChannelID:     testChannelID,
		ActorRegistry: cs.Registry,
		Log:           cs.Log,
		NowMs:         func() int64 { return fixedNowMs },
	}
}

// registerActor seeds an active actor into the registry so sender/audience
// checks resolve it.
func registerActor(t *testing.T, cs *store.ChannelStores, id actor.ActorID, kind actor.Kind) {
	t.Helper()
	if err := cs.Membership.Insert(context.Background(), storespec.Record{
		ID:        id,
		Kind:      kind,
		CreatedAt: fixedNowMs,
	}); err != nil {
		t.Fatalf("register actor %q: %v", id, err)
	}
}

// deregisterActor soft-deletes an actor (deregistered_at != 0).
func deregisterActor(t *testing.T, cs *store.ChannelStores, id actor.ActorID) {
	t.Helper()
	if err := cs.Membership.Deregister(context.Background(), id, fixedNowMs+1); err != nil {
		t.Fatalf("deregister actor %q: %v", id, err)
	}
}

// ctxCaller returns a context carrying a CallerContext bound to the test channel.
func ctxCaller(id actor.ActorID) context.Context {
	return CtxWithCaller(context.Background(), CallerContext{
		ActorID:   id,
		ChannelID: testChannelID,
	})
}

// validEvent builds a minimally-valid kind=event envelope authored by sender.
func validEvent(id message.ID, sender actor.ActorID) *message.Envelope {
	return &message.Envelope{
		ID:        id,
		TS:        fixedNowMs - 1000,
		ChannelID: testChannelID,
		Sender:    message.Sender{ID: sender},
		Kind:      message.KindEvent,
		Type:      "agent.text",
		Audience:  message.Audience{actor.ActorID("someone")},
	}
}

// runStep constructs a single step via its constructor and runs it once.
// It bypasses Chain so each step's contract is exercised in isolation.
func runStep(t *testing.T, mk func(Deps) step, deps Deps, ctx context.Context, env *message.Envelope) (outcome, error) {
	t.Helper()
	if deps.NowMs == nil {
		deps.NowMs = func() int64 { return fixedNowMs }
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	if deps.Metrics == nil {
		deps.Metrics = NoopMetrics{}
	}
	return mk(deps).Run(ctx, env)
}
