package home

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/access"
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
	grantLedgerSource  = "decl:grant-ledger"
	grantLedgerChannel = channel.ID("grant-ledger")
	grantLedgerPrivate = resource.ResourceID("resource:private-credential")
)

// grantLedgerObjectOps mirrors accessdoor's own closed object-verb set. Asking
// all four is what separates "no members entry" from "no members entry for the
// one op I happened to check".
var grantLedgerObjectOps = []access.Operation{
	access.OpRead, access.OpWrite, access.OpSet, access.OpDelete,
}

// grantLedgerGrants renders one resource's COMPLETE persisted grant projection
// as a sorted, comparable set — every (grantee_kind, grantee) entry with its
// ops. Comparing this across an event is stronger than asking about any single
// predicate: it catches a mint of ANY shape, not only the members-kind one the
// withdrawn rule happened to use.
func grantLedgerGrants(
	t *testing.T,
	registry resourcespec.Registry,
	id resource.ResourceID,
) []string {
	t.Helper()
	rows, _, err := registry.List(context.Background(), string(id), 100, "")
	if err != nil {
		t.Fatalf("list grants of %q: %v", id, err)
	}
	var out []string
	for _, row := range rows {
		if row.ID != id {
			continue
		}
		for _, grant := range row.Grants {
			ops := make([]string, 0, len(grant.Ops))
			for _, op := range grant.Ops {
				ops = append(ops, string(op))
			}
			sort.Strings(ops)
			out = append(out, fmt.Sprintf("%s/%s=[%s]",
				grant.GranteeKind, grant.Grantee, strings.Join(ops, ",")))
		}
	}
	sort.Strings(out)
	return out
}

// T62. The other half of the death cut: what End does to the GRANTS ledger of
// the resources a dying actor created.
//
// The rule (control-model-harden §3) is "只删不补" — the death path retires the
// identity and touches nothing else. The v1.x design it replaced auto-granted
// members{read,write} on a dead creator's resources so the channel would not be
// left holding an orphan; it was withdrawn because it publishes a deliberately
// PRIVATE resource — a credential — to every member the instant its creator
// dies. The collective's reach is decided at BIRTH or not at all.
//
// Why this test has to live over a real Home rather than beside the door's own
// succession tests: a test that kills the actor by calling the store's
// Deregister cannot observe this rule at all. Deregister is one
// `UPDATE actor_registry SET deregistered_at=?` and has no reach into
// resource_grants, so "no compensating grant appeared" is true there by
// construction of the function being called, in every possible world. The rule
// lives one layer up, in the End COMMAND — Controller.End → Terminal →
// Deregister, plus home's own ActorsEnded effects — which is exactly the layer
// a reintroduced auto-grant would naturally be written into, and exactly the
// layer this test routes through. (The door package cannot reach it: actorctl
// depends on accessdoor through lib/actorcaps, so an accessdoor-internal test
// importing actorctl would close an import cycle.)
func TestEndingAnActorMintsNoGrantOnTheResourcesItCreated(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	h, err := Open(Config{
		ChannelID:            grantLedgerChannel,
		DBPath:               dbPath,
		CompositionResolver:  routingResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: grantLedgerSource, Class: "routing-live",
			Kind: actor.KindAgent, Placement: storespec.NewServerPlacement(),
			CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	instances, err := h.controller.DeclaredInstances(grantLedgerSource)
	if err != nil || len(instances) != 1 {
		t.Fatalf("bootstrap creator missing: %v err=%v", instances, err)
	}
	creator := instances[0]

	// Home keeps no resource-registry face (open.go hands it to the door and
	// lets it go out of scope), so the ledger is read through a second store
	// handle on the same file — the same back-door read restart_crash_window
	// uses to inspect durable truth behind a running Home.
	stores, err := runtime.OpenChannel(ctx, grantLedgerChannel, dbPath,
		runtime.OpenChannelOptions{MustExist: true})
	if err != nil {
		t.Fatalf("open a store handle on the live channel: %v", err)
	}
	t.Cleanup(func() { _ = stores.Close() })
	registry := stores.Assembly.Resources

	// A private resource under the CreatorIdentity birth form: birth installs
	// the creator's own entry and nothing else. No members row is written, and
	// that absence is the thing the death path must preserve.
	if err := registry.Create(ctx, grantLedgerPrivate, resourcespec.KindKV,
		creator, "", "", []byte(`"secret"`), resourcespec.ResourceBirthPlan{}); err != nil {
		t.Fatalf("create the private resource: %v", err)
	}

	before := grantLedgerGrants(t, registry, grantLedgerPrivate)
	if len(before) == 0 {
		t.Fatal("birth installed no grant at all; the fixture is not the CreatorIdentity form")
	}
	for _, op := range grantLedgerObjectOps {
		allowed, err := registry.MembersAllow(ctx, grantLedgerPrivate, op)
		if err != nil {
			t.Fatalf("MembersAllow(%q) before the end: %v", op, err)
		}
		if allowed {
			t.Fatalf("the resource was already collective for %q before anyone died", op)
		}
	}

	// The real command — the one an operator and a declaration removal both
	// land on. The helper also proves the death landed, so what follows is
	// being asked of a channel whose creator is gone, not of one where the
	// command quietly did nothing.
	endIdentityForFixture(t, h, creator)
	if active, err := h.controller.IsActive(ctx, creator); err != nil || active {
		t.Fatalf("the creator survived its own End: active=%v err=%v", active, err)
	}

	// 只删不补, asked twice. First the specific rule: no members entry, for any
	// object verb.
	for _, op := range grantLedgerObjectOps {
		allowed, err := registry.MembersAllow(ctx, grantLedgerPrivate, op)
		if err != nil {
			t.Fatalf("MembersAllow(%q) after the end: %v", op, err)
		}
		if allowed {
			t.Fatalf("ending the creator minted a members grant for %q — "+
				"a private resource just became the whole channel's", op)
		}
	}
	// Then the general one: the ledger is byte-for-byte what it was. A
	// compensating grant of any shape — members, or a hand-off to some
	// surviving actor — fails here.
	if after := grantLedgerGrants(t, registry, grantLedgerPrivate); !slices.Equal(before, after) {
		t.Fatalf("the end rewrote the grants ledger: before=%v after=%v", before, after)
	}

	// And the resource itself outlived its creator: existence is channel-scoped,
	// so the death path did not delete it either.
	if _, exists, err := registry.Resolve(ctx, grantLedgerPrivate); err != nil || !exists {
		t.Fatalf("the resource died with its creator: exists=%v err=%v", exists, err)
	}
}
