package harness

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/internal/store"
	"github.com/wanpengxie/atoll/runtime/storespec"
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
	cs, err := store.OpenChannel(ctx, "C-test", filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{}, nil)
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
		ChannelID: testChannelID,
		Log:       cs.Log,
		Presence:  testAuthority{durableRows: cs.Actors},
		NowMs:     func() int64 { return fixedNowMs },
	}
}

type testAuthority struct {
	durableRows storespec.ActorRegistryStore
}

func (a testAuthority) IsActive(ctx context.Context, id actor.ActorID) (bool, error) {
	if a.durableRows == nil {
		return true, nil
	}
	_, ok, err := a.durableRows.LookupActive(ctx, id)
	return ok, err
}

// registerActor seeds an active actor into the registry so sender/audience
// checks resolve it.
func registerActor(t *testing.T, cs *store.ChannelStores, id actor.ActorID, kind actor.Kind) actor.ActorID {
	t.Helper()
	draft := storespec.ActorDraft{
		Kind:       kind,
		Definition: storespec.ActorDefinition{Class: string(kind)},
		Placement:  storespec.NewServerPlacement(), CreatedAt: fixedNowMs,
	}
	identity := strings.ReplaceAll(string(id), ":", "-")
	if kind == actor.KindHuman {
		draft.Principal = identity
	} else if kind == actor.KindAgent || kind == actor.KindTool {
		draft.SourceDeclID = identity
	}
	record, err := cs.Actors.Insert(context.Background(), draft)
	if err != nil {
		t.Fatalf("register actor %q: %v", id, err)
	}
	return record.ID
}

// ctxCaller returns a context carrying a caller bound to the test channel.
// Tests drive the internal chain directly (step-isolation), so they set the
// caller via the package-internal ctxWithCaller rather than minting a pen.
func ctxCaller(id actor.ActorID) context.Context {
	return ctxWithCaller(context.Background(), caller{actorID: id})
}

// ctxCallerKind returns a context carrying a caller bound to the test channel
// with an explicit WELDED kind — the pen-weld counterpart of registerActor's
// registry row, for tests that need to inject a specific kind without a
// registry lookup (stepSenderConsistent reads kind from the weld, not the
// registry).
func ctxCallerKind(id actor.ActorID, kind actor.Kind) context.Context {
	return ctxWithCaller(context.Background(), caller{actorID: id, kind: kind})
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
	return mk(deps).Run(ctx, env)
}

// stubLog is a MessageLog whose method behaviour is supplied per-test — the
// injectable seam for error / defensive branches the real store won't produce
// on demand (append faults, lookup faults, panics).
type stubLog struct {
	appendFn   func(ctx context.Context, env *message.Envelope, isTerminal bool) (storespec.AppendResult, error)
	findByID   func(ctx context.Context, id message.ID) (*storespec.StoredRow, bool, error)
	hasFinalFn func(ctx context.Context, parentID message.ID) (bool, error)
}

func (s stubLog) Append(ctx context.Context, env *message.Envelope, isTerminal bool) (storespec.AppendResult, error) {
	return s.appendFn(ctx, env, isTerminal)
}
func (s stubLog) FindByID(ctx context.Context, id message.ID) (*storespec.StoredRow, bool, error) {
	return s.findByID(ctx, id)
}
func (s stubLog) HasFinalResponse(ctx context.Context, parentID message.ID) (bool, error) {
	return s.hasFinalFn(ctx, parentID)
}
