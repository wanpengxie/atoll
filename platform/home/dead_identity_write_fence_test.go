package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	writeFenceSource = "decl:write-fence"
	writeFenceType   = "test.write_fence.tick"
)

// T19. The timer sink's live gate had a test; the ordinary message write path
// did not. This drives the SAME fence on the path every actor word travels: a
// pen minted from live coordinates (exactly as the managed capability bundle
// and the remote ingress mint one), used once while its author is a member, and
// used again one End later. A pen is a capability, never a snapshot — the
// verdict is re-run inside every Write — so the second call must be refused
// before a single byte reaches the log, and no fresh pen may be minted at all.
func TestEndedIdentityPenIsRefusedOnTheMessageWritePath(t *testing.T) {
	h, err := Open(Config{
		ChannelID:            "write-fence",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  routingResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: writeFenceSource, Class: "routing-live",
			Kind: actor.KindAgent, Placement: storespec.NewServerPlacement(),
			CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	instances, err := h.controller.DeclaredInstances(writeFenceSource)
	if err != nil || len(instances) != 1 {
		t.Fatalf("bootstrap author missing: %v err=%v", instances, err)
	}
	author := instances[0]
	term, _ := serverTerm(t, h, author)

	basis, err := h.controller.PenBasis(author, term)
	if err != nil {
		t.Fatalf("pen basis for a live author: %v", err)
	}
	if basis.Kind != actor.KindAgent {
		t.Fatalf("pen basis kind=%q, want agent", basis.Kind)
	}
	pen := h.minter.MintAuthority(basis.Run, basis.Kind)

	fenceEnv := func(id string) *message.Envelope {
		return &message.Envelope{
			ID: message.ID(id), TS: time.Now().UnixMilli(),
			Kind: message.KindEvent, Type: writeFenceType,
			Payload:  json.RawMessage(`{}`),
			Audience: message.Audience{author},
		}
	}

	result, err := pen.Write(ctx, fenceEnv("write-fence:live"))
	if err != nil || !result.Accepted() {
		t.Fatalf("a live author's write: %+v err=%v", result, err)
	}
	if rows := lifecycleCountRowsOfType(t, h.query, writeFenceType); rows != 1 {
		t.Fatalf("the live write landed %d rows, want 1", rows)
	}

	endIdentityForFixture(t, h, author)

	// The very same pen, one call later.
	dead, err := pen.Write(ctx, fenceEnv("write-fence:dead"))
	if !errors.Is(err, actorctl.ErrInactive) {
		t.Fatalf("a dead author's write: result=%+v err=%v, want %v", dead, err, actorctl.ErrInactive)
	}
	// The fence is a hard error, not a chain verdict: the pen never reached the
	// chain, so it hands back the zero result and no receipt at all. (Reading
	// Accepted() on it would say "accepted" — which is exactly why the error is
	// the contract here and the result is not.)
	if dead.MessageID != "" || dead.Seq != 0 || dead.RejectReason != "" {
		t.Fatalf("a fenced write still produced a receipt: %+v", dead)
	}
	if rows := lifecycleCountRowsOfType(t, h.query, writeFenceType); rows != 1 {
		t.Fatalf("a dead author's write reached the log: %d rows of %q, want 1", rows, writeFenceType)
	}

	// And there is no way to mint a replacement pen for the dead identity.
	if _, err := h.controller.PenBasis(author, term); !errors.Is(err, actorctl.ErrInactive) {
		t.Fatalf("a dead identity handed out a fresh pen basis: %v", err)
	}
}

const (
	deathCutSource   = "decl:death-cut"
	deathCutChannel  = channel.ID("death-cut")
	deathCutResource = resource.ResourceID("resource:creator-work")
)

// T62. The other half of the death cut: what End does to the RESOURCE ROWS a
// dying actor created.
//
// The rule (control-model-harden §3) is "只删不补" — the death path retires the
// identity and touches nothing else. Under the membrane-uniform model
// (PM-D1/PM-D3) that means: the resource row survives byte-for-byte — it is
// neither deleted nor "handed off" (created_by, the PM-D3 delete predicate,
// keeps naming the dead creator; delete authority falls to the channel owner
// root alone, never silently to another member).
//
// Why this test has to live over a real Home rather than beside the door's own
// succession tests: a test that kills the actor by calling the store's
// Deregister cannot observe this rule at all. Deregister is one
// `UPDATE actor_registry SET deregistered_at=?` and structurally cannot reach
// the resources table, so "the row was untouched" is true there by
// construction. The rule lives one layer up, in the End COMMAND —
// Controller.End → Terminal → Deregister, plus home's own ActorsEnded effects
// — which is exactly the layer a compensating rewrite would naturally be
// written into, and exactly the layer this test routes through.
func TestEndingAnActorLeavesItsResourceRowsUntouched(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	h, err := Open(Config{
		ChannelID:            deathCutChannel,
		DBPath:               dbPath,
		CompositionResolver:  routingResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: deathCutSource, Class: "routing-live",
			Kind: actor.KindAgent, Placement: storespec.NewServerPlacement(),
			CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	instances, err := h.controller.DeclaredInstances(deathCutSource)
	if err != nil || len(instances) != 1 {
		t.Fatalf("bootstrap creator missing: %v err=%v", instances, err)
	}
	creator := instances[0]

	// Home keeps no resource-registry face (open.go hands it to the door and
	// lets it go out of scope), so the ledger is read through a second store
	// handle on the same file — the same back-door read restart_crash_window
	// uses to inspect durable truth behind a running Home.
	stores, err := runtime.OpenChannel(ctx, deathCutChannel, dbPath,
		runtime.OpenChannelOptions{MustExist: true})
	if err != nil {
		t.Fatalf("open a store handle on the live channel: %v", err)
	}
	t.Cleanup(func() { _ = stores.Close() })
	registry := stores.Assembly.Resources

	if err := registry.Create(ctx, deathCutResource, resourcespec.KindKV,
		creator, "", "", []byte(`"work product"`), resourcespec.ResourceBirthPlan{}); err != nil {
		t.Fatalf("create the resource: %v", err)
	}
	before, exists, err := registry.Resolve(ctx, deathCutResource)
	if err != nil || !exists {
		t.Fatalf("resolve before the end: exists=%v err=%v", exists, err)
	}
	if before.CreatedBy != creator {
		t.Fatalf("created_by=%q before the end, want the creator", before.CreatedBy)
	}

	// The real command — the one an operator and a declaration removal both
	// land on. The helper also proves the death landed, so what follows is
	// being asked of a channel whose creator is gone, not of one where the
	// command quietly did nothing.
	endIdentityForFixture(t, h, creator)
	if active, err := h.controller.IsActive(ctx, creator); err != nil || active {
		t.Fatalf("the creator survived its own End: active=%v err=%v", active, err)
	}

	// 只删不补: the row survives byte-for-byte. created_by still names the dead
	// creator (no hand-off — the PM-D3 predicate is a birth fact, not a lease),
	// and existence is channel-scoped (the death path did not delete it).
	after, exists, err := registry.Resolve(ctx, deathCutResource)
	if err != nil || !exists {
		t.Fatalf("the resource died with its creator: exists=%v err=%v", exists, err)
	}
	if after != before {
		t.Fatalf("the end rewrote the resource row: before=%+v after=%+v", before, after)
	}
}
