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
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const crashWindowClass = "crash-window-park"

// crashWindowFixture records which identities actually got a running body, so
// the test can distinguish "the ledger says so" from "the host really acted on
// it".
type crashWindowFixture struct {
	mu      sync.Mutex
	started map[actor.ActorID]struct{}
}

func newCrashWindowFixture() *crashWindowFixture {
	return &crashWindowFixture{started: map[actor.ActorID]struct{}{}}
}

func (f *crashWindowFixture) BuildClass(
	_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage,
) (platform.ActorFactory, bool) {
	if class != crashWindowClass {
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error {
			f.mu.Lock()
			f.started[sys.Self()] = struct{}{}
			f.mu.Unlock()
			<-sys.Life().Done()
			return nil
		}, nil
	}}}, true
}

func (f *crashWindowFixture) ran(id actor.ActorID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.started[id]
	return ok
}

// T10. The crash window is the gap between "the durable transaction committed"
// and "the ledger publication ran". Publication is process memory, so a crash
// inside that window leaves the row on disk with nobody having ever announced
// it — or, on the terminal side, a row already gone with the live ledger still
// holding it. Both halves are opened here against a running Home by committing
// through a second registry handle, and boot must converge on the durable image
// alone: the unannounced birth becomes a full member with a running body, the
// unannounced terminal is gone for good.
func TestBootConvergesOnDurableTruthAfterACommitPublishCrashWindow(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	const channelID = channel.ID("restart-crash-window")
	ctx := context.Background()
	createdAt := time.Now().UnixMilli()

	firstBodies := newCrashWindowFixture()
	h1, err := Open(Config{
		ChannelID:            channelID,
		DBPath:               dbPath,
		CompositionResolver:  firstBodies,
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: "decl:pre-crash", Kind: actor.KindAgent,
			Class: crashWindowClass, Placement: storespec.NewServerPlacement(),
			CreatedAt: createdAt,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	instances, err := h1.controller.DeclaredInstances("decl:pre-crash")
	if err != nil || len(instances) != 1 {
		t.Fatalf("bootstrap declaration instances=%v err=%v", instances, err)
	}
	preCrash := instances[0]
	restartEventually(t, "the pre-crash body to run", func() bool { return firstBodies.ran(preCrash) })

	// The crash window itself: commit both durable halves behind the running
	// Controller's back, exactly as a process that died before publishing would
	// have left the db.
	registry, err := runtime.OpenChannel(ctx, channelID, dbPath, runtime.OpenChannelOptions{MustExist: true})
	if err != nil {
		t.Fatalf("open registry handle: %v", err)
	}
	ghostRecord, err := registry.Actors.Insert(ctx, storespec.ActorDraft{
		Kind: actor.KindAgent, SourceDeclID: "decl:in-window",
		Definition: storespec.ActorDefinition{
			Class: crashWindowClass, Config: json.RawMessage(`{"born":"in-window"}`),
		},
		Placement: storespec.NewServerPlacement(), CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatalf("commit the in-window birth: %v", err)
	}
	if err := registry.Actors.Deregister(ctx, []actor.ActorID{preCrash}, time.Now().UnixMilli()); err != nil {
		t.Fatalf("commit the in-window terminal: %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("close registry handle: %v", err)
	}

	// Neither publication ran: the live ledger still shows the pre-crash image.
	if active, err := h1.controller.IsActive(ctx, ghostRecord.ID); err != nil || active {
		t.Fatalf("the in-window birth was published after all: active=%v err=%v", active, err)
	}
	if found, err := h1.controller.DeclaredInstances("decl:in-window"); err != nil || len(found) != 0 {
		t.Fatalf("the in-window birth reached the projection: %v err=%v", found, err)
	}
	if active, err := h1.controller.IsActive(ctx, preCrash); err != nil || !active {
		t.Fatalf("the in-window terminal was published after all: active=%v err=%v", active, err)
	}
	if err := h1.closeInternal("test-crash"); err != nil {
		t.Fatalf("close first Home: %v", err)
	}

	secondBodies := newCrashWindowFixture()
	h2, err := Open(Config{
		ChannelID:            channelID,
		DBPath:               dbPath,
		CompositionResolver:  secondBodies,
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		MustExistDB:          true,
	})
	if err != nil {
		t.Fatalf("restart Open: %v", err)
	}
	t.Cleanup(func() { _ = h2.closeInternal("test") })

	// Converged on the durable image: the unannounced birth is a whole member.
	if active, err := h2.controller.IsActive(ctx, ghostRecord.ID); err != nil || !active {
		t.Fatalf("boot did not adopt the in-window birth: active=%v err=%v", active, err)
	}
	facts, found, err := h2.controller.ActorFacts(ctx, ghostRecord.ID)
	if err != nil || !found || facts.Kind != actor.KindAgent {
		t.Fatalf("in-window birth facts=%+v found=%v err=%v", facts, found, err)
	}
	if adopted, err := h2.controller.DeclaredInstances("decl:in-window"); err != nil ||
		len(adopted) != 1 || adopted[0] != ghostRecord.ID {
		t.Fatalf("in-window declaration instances=%v err=%v", adopted, err)
	}
	terms := restartServerTerms(t, h2)
	body, planned := terms[ghostRecord.ID].(actorhost.BodyDesired)
	if !planned {
		t.Fatalf("boot planned %T for the in-window birth", terms[ghostRecord.ID])
	}
	if body.AttemptKey == "" ||
		body.ExecutionSpec.Class != crashWindowClass ||
		string(body.ExecutionSpec.Config) != `{"born":"in-window"}` {
		t.Fatalf("in-window birth desired row = %+v", body)
	}
	restartEventually(t, "the adopted in-window body to run", func() bool {
		return secondBodies.ran(ghostRecord.ID)
	})

	// And the unannounced terminal stayed terminal.
	if active, err := h2.controller.IsActive(ctx, preCrash); err != nil || active {
		t.Fatalf("boot revived the in-window terminal: active=%v err=%v", active, err)
	}
	if _, found, err := h2.controller.ActorFacts(ctx, preCrash); err != nil || found {
		t.Fatalf("the in-window terminal still has facts: found=%v err=%v", found, err)
	}
	if stale, err := h2.controller.DeclaredInstances("decl:pre-crash"); err != nil || len(stale) != 0 {
		t.Fatalf("the in-window terminal came back as an instance: %v err=%v", stale, err)
	}
	if _, planned := terms[preCrash]; planned {
		t.Fatalf("boot planned a desired row for the in-window terminal")
	}
	if secondBodies.ran(preCrash) {
		t.Fatal("boot started a body for the in-window terminal")
	}
	records := restartActiveRecords(t, channelID, dbPath)
	if _, alive := records[preCrash]; alive {
		t.Fatalf("the in-window terminal still has a durable row: %+v", records[preCrash])
	}
	if _, alive := records[ghostRecord.ID]; !alive {
		t.Fatalf("the in-window birth lost its durable row: %+v", records)
	}
}
