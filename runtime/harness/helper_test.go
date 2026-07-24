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
		Authority: testAuthority{durableRows: cs.Declared},
		NowMs:     func() int64 { return fixedNowMs },
	}
}

type testAuthority struct {
	durableRows storespec.DeclaredControlReader
}

func (a testAuthority) LookupActive(ctx context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	if a.durableRows == nil {
		return storespec.ActorControlRow{ID: id, Kind: actor.KindAgent, CurrentDeclVersion: 1, Placement: storespec.NewServerPlacement()}, true, nil
	}
	rec, ok, err := a.durableRows.LookupDeclaredActive(ctx, id)
	if err != nil || !ok {
		return storespec.ActorControlRow{}, false, err
	}
	return storespec.ActorControlRow{ID: rec.ID, Kind: rec.Kind, CurrentDeclVersion: 1, Placement: storespec.NewServerPlacement()}, true, nil
}
func (a testAuthority) ListActive(ctx context.Context) ([]storespec.ActorControlRow, error) {
	if a.durableRows == nil {
		return nil, nil
	}
	rows, err := a.durableRows.ListDeclaredActive(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]storespec.ActorControlRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, storespec.ActorControlRow{ID: row.ID, Kind: row.Kind, CurrentDeclVersion: 1, Placement: storespec.NewServerPlacement()})
	}
	return out, nil
}
func (a testAuthority) CheckAuthor(ctx context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	_, ok, err := a.LookupActive(ctx, stamp.ID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return storespec.AuthorNotMember, nil
	}
	return storespec.AuthorOK, nil
}

// registerActor seeds an active actor into the registry so sender/audience
// checks resolve it.
func registerActor(t *testing.T, cs *store.ChannelStores, id actor.ActorID, kind actor.Kind) actor.ActorID {
	t.Helper()
	bundle := storespec.AdmitBundle{
		Kind: kind, Class: string(kind),
		Placement: storespec.NewServerPlacement(), CreatedAt: fixedNowMs,
	}
	identity := strings.ReplaceAll(string(id), ":", "-")
	if kind == actor.KindHuman {
		bundle.Principal = identity
	} else if kind == actor.KindAgent || kind == actor.KindTool {
		bundle.SourceDeclID = identity
	}
	result, err := cs.DeclAdmission.AdmitDeclared(context.Background(), bundle)
	if err != nil {
		t.Fatalf("register actor %q: %v", id, err)
	}
	return result.ID
}

// ctxCaller returns a context carrying a caller bound to the test channel.
// Tests drive the internal chain directly (step-isolation), so they set the
// caller via the package-internal ctxWithCaller rather than minting a pen.
func ctxCaller(id actor.ActorID) context.Context {
	return ctxWithCaller(context.Background(), caller{
		actorID: id,
		chID:    testChannelID,
	})
}

// ctxCallerKind returns a context carrying a caller bound to the test channel
// with an explicit WELDED kind — the pen-weld counterpart of registerActor's
// registry row, for tests that need to inject a specific kind without a
// registry lookup (stepSenderConsistent reads kind from the weld, not the
// registry).
func ctxCallerKind(id actor.ActorID, kind actor.Kind) context.Context {
	return ctxWithCaller(context.Background(), caller{
		actorID: id,
		kind:    kind,
		chID:    testChannelID,
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
	return mk(deps).Run(ctx, env)
}
