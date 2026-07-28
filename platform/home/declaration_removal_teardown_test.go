package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// T65. The generic "desired row disappears → body retires → sparse row is
// deleted" step has host-level coverage. What had none is HOME's own assembly
// of that chain: the declared instance leaving truth is one command, and three
// separate organs have to carry it — Controller drops the record, the desired
// reader republishes a level WITHOUT it, and the server Host acts on that
// level. A break anywhere in that wiring leaves a running body attached to an
// identity that is no longer a member, which no host-level test can see.

const (
	declRemovalClass = "decl-removal-body"
	declRemovalDecl  = "decl:decl-removal"
)

// declRemovalFixture reports both edges of one body's life, so the test can
// tell "the ledger forgot it" apart from "the body actually stopped".
type declRemovalFixture struct {
	started chan actor.ActorID

	mu     sync.Mutex
	exited map[actor.ActorID]struct{}
}

func newDeclRemovalFixture() *declRemovalFixture {
	return &declRemovalFixture{
		started: make(chan actor.ActorID, 4),
		exited:  map[actor.ActorID]struct{}{},
	}
}

func (f *declRemovalFixture) BuildClass(
	_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage,
) (platform.ActorFactory, bool) {
	if class != declRemovalClass {
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error {
			f.started <- sys.Self()
			<-sys.Life().Done()
			f.mu.Lock()
			f.exited[sys.Self()] = struct{}{}
			f.mu.Unlock()
			return nil
		}, nil
	}}}, true
}

func (f *declRemovalFixture) hasExited(id actor.ActorID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.exited[id]
	return ok
}

func TestRemovingADeclaredInstanceUnpublishesItsDesiredRowAndTearsTheBodyDown(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	const channelID = channel.ID("decl-removal")
	ctx := context.Background()

	fixture := newDeclRemovalFixture()
	h, err := Open(Config{
		ChannelID:            channelID,
		DBPath:               dbPath,
		CompositionResolver:  fixture,
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: declRemovalDecl, Kind: actor.KindAgent,
			Class: declRemovalClass, Placement: storespec.NewServerPlacement(),
			CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })

	instance := restartRecv(t, "the declared body to start", fixture.started)
	if declared := routingAgent(t, h, declRemovalDecl); declared != instance {
		t.Fatalf("the running body %s is not the declaration's instance %s", instance, declared)
	}
	term, spec := serverTerm(t, h, instance)
	if term == "" || spec.Class != declRemovalClass {
		t.Fatalf("desired row before removal = (%q, %+v)", term, spec)
	}
	restartEventually(t, "the host to own a live body for the instance", func() bool {
		snapshot, ok := h.serverHost.Inspect(instance)
		return ok && snapshot.Actual == actorhost.ActualBody && snapshot.Unit.IsAlive()
	})

	if err := removeThroughSysOp(h, ctx, instance); err != nil {
		t.Fatalf("remove the declared instance: %v", err)
	}

	// Link one: the declaration's projection is empty and the identity is gone
	// from the ledger.
	instances, err := h.controller.DeclaredInstances(declRemovalDecl)
	if err != nil || len(instances) != 0 {
		t.Fatalf("declaration instances after removal = %v err=%v", instances, err)
	}
	if active, err := h.controller.IsActive(ctx, instance); err != nil || active {
		t.Fatalf("removed instance active=%v err=%v", active, err)
	}

	// Link two: the desired level republished WITHOUT the row.
	terms := restartServerTerms(t, h)
	if row, present := terms[instance]; present {
		t.Fatalf("the removed instance still has a desired row: %+v", row)
	}

	// Link three: the Host really acted on that level — the body stopped and
	// its sparse row is gone. Nothing here polls the ledger for the verdict;
	// the verdict is the body's own exit.
	restartEventually(t, "the removed instance's body to be retired", func() bool {
		_, present := h.serverHost.Inspect(instance)
		return !present && fixture.hasExited(instance)
	})

	// And the durable side agrees: no active row is left to boot from.
	if record, alive := restartActiveRecords(t, channelID, dbPath)[instance]; alive {
		t.Fatalf("the removed instance kept a durable row: %+v", record)
	}
}
