package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	restartIdentityParkClass = "restart-identity-park"
	restartGateProbeKey      = resource.ResourceID("gate-probe")
)

type restartIdentityFixture struct{}

func newRestartIdentityFixture(bool) *restartIdentityFixture {
	return &restartIdentityFixture{}
}

func (f *restartIdentityFixture) BuildClass(
	_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage,
) (platform.ActorFactory, bool) {
	if class != restartIdentityParkClass {
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return restartParkProc, nil
	}}}, true
}

func restartParkProc(sys actorbase.Sys) error {
	<-sys.Life().Done()
	return nil
}

// restartActiveRecords reads the durable registry's whole active image through
// its own short-lived handle. A "the record came back WHOLE" claim has to
// compare the rows themselves, not a projection of them.
func restartActiveRecords(
	t *testing.T,
	channelID channel.ID,
	dbPath string,
) map[actor.ActorID]storespec.ActorRecord {
	t.Helper()
	cs, err := runtime.OpenChannel(
		context.Background(), channelID, dbPath,
		runtime.OpenChannelOptions{MustExist: true},
	)
	if err != nil {
		t.Fatalf("open registry reader: %v", err)
	}
	defer func() { _ = cs.Close() }()
	records, err := cs.Actors.ListActive(context.Background())
	if err != nil {
		t.Fatalf("list active records: %v", err)
	}
	out := make(map[actor.ActorID]storespec.ActorRecord, len(records))
	for _, record := range records {
		out[record.ID] = record
	}
	return out
}

// restartServerTerms indexes the server domain's desired level by actor, so a
// test can read both the term and the shape (body vs carrier) one boot handed
// each restored identity.
func restartServerTerms(t *testing.T, h *Home) map[actor.ActorID]actorhost.Desired {
	t.Helper()
	desired, err := h.controller.DesiredFor("server", "server")
	if err != nil {
		t.Fatalf("DesiredFor: %v", err)
	}
	out := make(map[actor.ActorID]actorhost.Desired, len(desired))
	for _, row := range desired {
		switch typed := row.(type) {
		case actorhost.BodyDesired:
			out[typed.ActorID] = typed
		case actorhost.CarrierDesired:
			out[typed.ActorID] = typed
		default:
			t.Fatalf("unknown desired row %T", row)
		}
	}
	return out
}

// T6 + T7. Boot is the moment the whole managed truth is republished from the
// durable image. This checks that every record kind that CAN be durable comes
// back with every field intact — not just the one field a roster happens to
// show — and that the restored identities are immediately good enough to pass
// the real permission gates, with no warm-up operation in between.
//
// The kernel is deliberately absent from that set: it is a constant, not a
// member, so boot must restore no row for it while still answering "is it
// addressable" affirmatively.
func TestBootRestoresEveryDurableIdentityKindWholeAndImmediatelyGated(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	const channelID = channel.ID("restart-identity")
	const ownerPrincipal = "owner@example.com"
	ctx := context.Background()
	agentConfig := json.RawMessage(`{"model":"v9"}`)
	createdAt := time.Now().UnixMilli()

	h1, err := Open(Config{
		ChannelID:            channelID,
		DBPath:               dbPath,
		CompositionResolver:  newRestartIdentityFixture(false),
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		Genesis: &storespec.ChannelGenesis{
			ChannelID: string(channelID), Type: "channel",
			OwnerPrincipal: ownerPrincipal, CreatedAt: createdAt,
		},
		BootstrapHumanPrincipals: []string{ownerPrincipal},
		BootstrapDeclarations: []DeclareRequest{
			{
				SourceDeclID: "decl-restore-agent", Seed: "decl-restore-agent", Kind: actor.KindAgent,
				Class: restartIdentityParkClass, Config: &agentConfig,
				Placement: storespec.NewServerPlacement(), CreatedAt: createdAt,
			},
			{
				SourceDeclID: "decl-restore-tool", Seed: "decl-restore-tool", Kind: actor.KindTool,
				Class:     restartIdentityParkClass,
				Placement: mustDaemonPlacement(t, "daemon-a"), CreatedAt: createdAt,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeTerms := restartServerTerms(t, h1)
	if err := h1.closeInternal("test-restart"); err != nil {
		t.Fatalf("close first Home: %v", err)
	}
	before := restartActiveRecords(t, channelID, dbPath)
	if len(before) != 3 {
		t.Fatalf("bootstrap produced %d durable records, want human+agent+tool: %+v", len(before), before)
	}

	h2, err := Open(Config{
		ChannelID:            channelID,
		DBPath:               dbPath,
		CompositionResolver:  newRestartIdentityFixture(false),
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		MustExistDB:          true,
	})
	if err != nil {
		t.Fatalf("restart Open: %v", err)
	}
	t.Cleanup(func() { _ = h2.closeInternal("test") })

	// --- T7: the gates, run FIRST, before anything else touches the ledger ---
	for id, record := range before {
		authority := h2.controller.IdentityAuthorityFor(id)
		if err := authority.Admit(); err != nil {
			t.Fatalf("restored %s (%s) failed AuthorActive right after boot: %v",
				id, record.Kind, err)
		}
		if active, err := h2.controller.IsActive(ctx, id); err != nil || !active {
			t.Fatalf("restored %s IsActive=%v err=%v right after boot", id, active, err)
		}
		admission, ok, err := h2.controller.AdmitIdentity(ctx, id)
		if err != nil || !ok || !admission.Valid() || admission.Kind != record.Kind {
			t.Fatalf("restored %s admission=%+v ok=%v err=%v", id, admission, ok, err)
		}
		handle, err := h2.stateHandles.ResolveAuthority(ctx, authority)
		if err != nil {
			t.Fatalf("restored %s could not resolve a state handle at boot: %v", id, err)
		}
		out, err := handle.Invoke(ctx, access.OpRead, restartGateProbeKey, nil)
		if err != nil || out.RejectReason == access.OwnerInactive {
			t.Fatalf("restored %s was refused by the state door at boot: %+v err=%v", id, out, err)
		}
	}
	stranger := actor.ActorID("actor:never-existed")
	if active, err := h2.controller.IsActive(ctx, stranger); err != nil || active {
		t.Fatalf("a stranger read active=%v err=%v", active, err)
	}
	if err := h2.controller.AuthorActive(stranger); !errors.Is(err, actorctl.ErrInactive) {
		t.Fatalf("a stranger passed AuthorActive: %v", err)
	}
	if _, err := h2.stateHandles.ResolveAuthority(
		ctx, h2.controller.IdentityAuthorityFor(stranger),
	); !errors.Is(err, accessdoor.ErrStateHandleUnavailable) {
		t.Fatalf("a stranger resolved a state handle: %v", err)
	}

	// --- T6: whole records, whole projections -------------------------------
	after := restartActiveRecords(t, channelID, dbPath)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("durable image changed across restart:\nbefore=%+v\nafter=%+v", before, after)
	}

	identities, err := h2.controller.ActiveIdentities()
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != len(before) {
		t.Fatalf("boot restored %d identities, want %d", len(identities), len(before))
	}
	seenKinds := map[actor.Kind]int{}
	for _, identity := range identities {
		record, restored := before[identity.ID]
		if !restored {
			t.Fatalf("boot invented identity %+v", identity)
		}
		if identity.Kind != record.Kind {
			t.Fatalf("%s restored as kind %q, row says %q", identity.ID, identity.Kind, record.Kind)
		}
		seenKinds[identity.Kind]++
	}
	for _, kind := range []actor.Kind{actor.KindHuman, actor.KindAgent, actor.KindTool} {
		if seenKinds[kind] != 1 {
			t.Fatalf("boot restored %d %s identities, want 1: %+v", seenKinds[kind], kind, identities)
		}
	}

	afterTerms := restartServerTerms(t, h2)
	for id, record := range before {
		facts, found, err := h2.controller.ActorFacts(ctx, id)
		if err != nil || !found ||
			facts.Kind != record.Kind || facts.Principal != record.Principal {
			t.Fatalf("%s facts=%+v found=%v err=%v, row=%+v", id, facts, found, err, record)
		}
		basis, err := h2.controller.ResourceActorBasis(ctx, id)
		if err != nil || !basis.Active ||
			basis.Kind != record.Kind || basis.Principal != record.Principal {
			t.Fatalf("%s basis=%+v err=%v, row=%+v", id, basis, err, record)
		}
		wantHost := ""
		if record.Placement.Kind == storespec.PlacementDaemon {
			wantHost = record.Placement.Host
		}
		if basis.PreferredStorageHost != wantHost {
			t.Fatalf("%s restored storage host %q, want %q", id, basis.PreferredStorageHost, wantHost)
		}

		desired, planned := afterTerms[id]
		if !planned {
			t.Fatalf("%s got no desired row after boot", id)
		}
		switch record.Placement.Kind {
		case storespec.PlacementServer:
			body, ok := desired.(actorhost.BodyDesired)
			if !ok {
				t.Fatalf("%s is server-placed but boot planned %T", id, desired)
			}
			if body.ExecutionSpec.Kind != record.Kind ||
				body.ExecutionSpec.Class != record.Definition.Class ||
				string(body.ExecutionSpec.Config) != string(record.Definition.Config) {
				t.Fatalf("%s execution spec=%+v, row definition=%+v",
					id, body.ExecutionSpec, record.Definition)
			}
			if before, had := beforeTerms[id].(actorhost.BodyDesired); had &&
				before.AttemptKey == body.AttemptKey {
				t.Fatalf("%s came back on the previous life's term %q", id, body.AttemptKey)
			}
		case storespec.PlacementDaemon:
			carrier, ok := desired.(actorhost.CarrierDesired)
			if !ok {
				t.Fatalf("%s is daemon-placed but boot planned %T", id, desired)
			}
			if carrier.PeerDomain != actorhost.ExecutionDomain(record.Placement.Host) {
				t.Fatalf("%s carrier peer=%q, row host=%q",
					id, carrier.PeerDomain, record.Placement.Host)
			}
			if before, had := beforeTerms[id].(actorhost.CarrierDesired); had &&
				before.AttemptKey == carrier.AttemptKey {
				t.Fatalf("%s came back on the previous life's term %q", id, carrier.AttemptKey)
			}
		}

		if record.SourceDeclID == "" {
			continue
		}
		instances, err := activeMembersForSource(h2.controller, record.SourceDeclID)
		if err != nil || len(instances) != 1 || instances[0] != id {
			t.Fatalf("declaration %q restored instances=%v err=%v",
				record.SourceDeclID, instances, err)
		}
	}

	reconcile, err := h2.controller.DeclaredReconcileList()
	if err != nil {
		t.Fatal(err)
	}
	if len(reconcile) != 2 {
		t.Fatalf("boot restored %d declared instances, want 2: %+v", len(reconcile), reconcile)
	}
	for _, instance := range reconcile {
		record := before[instance.ID]
		if instance.SourceDeclID != record.SourceDeclID || instance.Kind != record.Kind ||
			!instance.Definition.Equal(record.Definition) {
			t.Fatalf("declared instance %+v does not match its row %+v", instance, record)
		}
	}

	// The kernel: no row, no membership — and still addressable.
	if _, found, err := h2.controller.ActorFacts(ctx, actor.SystemActorID); err != nil || found {
		t.Fatalf("boot restored the kernel as a member: found=%v err=%v", found, err)
	}
	if active, err := h2.actors.IsActive(ctx, actor.SystemActorID); err != nil || !active {
		t.Fatalf("the kernel is not addressable after boot: active=%v err=%v", active, err)
	}
}

func mustDaemonPlacement(t *testing.T, host string) storespec.Placement {
	t.Helper()
	placement, err := storespec.NewDaemonPlacement(host)
	if err != nil {
		t.Fatal(err)
	}
	return placement
}
