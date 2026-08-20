package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// T22. §13.2's no-lifecycle-replay group had only a static symbol scan behind
// it. The BEHAVIOURAL claim is about what a crash cut does to committed work:
// the substrate replays no lifecycle at all, so an actor whose business effect
// already committed converges by reading its OWN durable state — it does not
// re-run it, and nothing underneath re-runs it for the actor either.
//
// The two tests below are each other's control. The first cuts AFTER the
// actor's own fence is committed and demands zero re-execution. The second cuts
// BETWEEN the effect and the fence and demands re-execution — which is what
// proves the first result comes from the durable fence and not from some hidden
// substrate dedup that would make the whole exercise vacuous.

const (
	crashCutClass     = "crash-cut-worker"
	crashCutDecl      = "decl-crash-cut"
	crashCutFenceKey  = resource.ResourceID("job-committed")
	crashCutEventType = "test.crash_cut.business_effect"
)

// crashCutRun is one incarnation's account of the one business unit.
type crashCutRun struct {
	actorID actor.ActorID
	fenced  bool // the durable fence was already there: business skipped
	ran     bool // this life executed the business effect
	err     string
}

// crashCutFixture builds a worker whose single business unit is guarded by its
// own durable state. markFence=false stops the life one step short of the
// fence, which is exactly the crash window at-least-once talks about.
type crashCutFixture struct {
	markFence bool
	runs      chan crashCutRun
}

func newCrashCutFixture(markFence bool) *crashCutFixture {
	return &crashCutFixture{markFence: markFence, runs: make(chan crashCutRun, 4)}
}

func (f *crashCutFixture) BuildClass(
	_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage,
) (platform.ActorFactory, bool) {
	if class != crashCutClass {
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return f.proc(), nil
	}}}, true
}

func (f *crashCutFixture) proc() actorbase.Proc {
	return func(sys actorbase.Sys) error {
		run := crashCutRun{actorID: sys.Self()}
		out, err := sys.State().Get(crashCutFenceKey)
		switch {
		case err != nil:
			run.err = "read fence: " + err.Error()
		case out.Accepted():
			run.fenced = out.Found
		case out.RejectReason != access.ResourceNotFound:
			run.err = "read fence rejected: " + string(out.RejectReason)
		}
		if run.err == "" && !run.fenced {
			// The business effect: one committed row in the channel log.
			// Root: this effect runs on the actor's own boot, not to serve any
			// message on the ledger.
			spec, err := behavior.EventSpecJSON(message.Root(), crashCutEventType,
				map[string]string{"unit": "the-one-job"}, sys.Self())
			if err == nil {
				_, err = sys.Emit(spec)
			}
			if err != nil {
				run.err = "business effect: " + err.Error()
			} else {
				run.ran = true
			}
			if run.err == "" && f.markFence {
				raw, _ := json.Marshal("committed")
				if put, err := sys.State().Put(crashCutFenceKey, raw); err != nil {
					run.err = "write fence: " + err.Error()
				} else if !put.Accepted() {
					run.err = "write fence rejected: " + string(put.RejectReason)
				}
			}
		}
		f.runs <- run
		<-sys.Life().Done()
		return nil
	}
}

func crashCutDeclaration() DeclareRequest {
	return DeclareRequest{
		SourceDeclID: crashCutDecl, Seed: crashCutDecl, Kind: actor.KindAgent,
		Class: crashCutClass, Placement: storespec.NewServerPlacement(),
		CreatedAt: time.Now().UnixMilli(),
	}
}

// openCrashCutHome boots the worker's channel, seeding the declaration on the
// first open and restarting over the same file afterwards.
func openCrashCutHome(
	t *testing.T,
	channelID channel.ID,
	dbPath string,
	fixture *crashCutFixture,
	seed bool,
) *Home {
	t.Helper()
	h, err := Open(Config{
		ChannelID:            channelID,
		DBPath:               dbPath,
		CompositionResolver:  fixture,
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            seed,
		MustExistDB:          !seed,
		BootstrapDeclarations: func() []DeclareRequest {
			if seed {
				return []DeclareRequest{crashCutDeclaration()}
			}
			return nil
		}(),
	})
	if err != nil {
		t.Fatalf("open home (seed=%v): %v", seed, err)
	}
	return h
}

func crashCutBusinessRows(t *testing.T, h *Home) int {
	t.Helper()
	return lifecycleCountRowsOfType(t, h.query, crashCutEventType)
}

// The committed-fence case: cut after the actor's own durable fence lands, and
// the next incarnation must do nothing but observe it.
func TestCrashCutDoesNotRerunBusinessAlreadyFencedByDurableState(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	const channelID = channel.ID("crash-cut-fenced")

	before := newCrashCutFixture(true)
	h1 := openCrashCutHome(t, channelID, dbPath, before, true)
	first := restartRecv(t, "the first life to run its business unit", before.runs)
	if first.err != "" {
		t.Fatalf("first life: %s", first.err)
	}
	if first.fenced || !first.ran {
		t.Fatalf("first life fenced=%v ran=%v, want a fresh execution", first.fenced, first.ran)
	}
	if rows := crashCutBusinessRows(t, h1); rows != 1 {
		t.Fatalf("business rows after the first life = %d, want 1", rows)
	}
	if err := h1.closeInternal("test-crash-cut"); err != nil {
		t.Fatalf("cut the first life: %v", err)
	}

	after := newCrashCutFixture(true)
	h2 := openCrashCutHome(t, channelID, dbPath, after, false)
	t.Cleanup(func() { _ = h2.closeInternal("test") })
	second := restartRecv(t, "the second life to converge on the fence", after.runs)
	if second.err != "" {
		t.Fatalf("second life: %s", second.err)
	}
	if second.actorID != first.actorID {
		t.Fatalf("the cut produced a different identity: %s → %s", first.actorID, second.actorID)
	}
	if !second.fenced || second.ran {
		t.Fatalf("second life fenced=%v ran=%v, want convergence with no re-execution",
			second.fenced, second.ran)
	}
	if rows := crashCutBusinessRows(t, h2); rows != 1 {
		t.Fatalf("business rows after the cut = %d, want the one already committed", rows)
	}

	// And the substrate itself replayed nothing on the way: no second life was
	// started for the same identity, and a full reconcile sweep adds none.
	h2.reconcileSweep(context.Background())
	select {
	case extra := <-after.runs:
		t.Fatalf("boot started a second life for the same identity: %+v", extra)
	default:
	}
	if rows := crashCutBusinessRows(t, h2); rows != 1 {
		t.Fatalf("a reconcile sweep replayed business: rows = %d", rows)
	}
}

// The control: cut in the window BETWEEN the committed effect and the fence.
// The effect runs again — at-least-once is the declared contract (S3), the
// fence is the actor's own property, and nothing in the substrate is silently
// deduplicating on the actor's behalf.
func TestCrashCutInsideTheUnfencedWindowRepeatsTheEffect(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	const channelID = channel.ID("crash-cut-unfenced")

	before := newCrashCutFixture(false)
	h1 := openCrashCutHome(t, channelID, dbPath, before, true)
	first := restartRecv(t, "the first life to run its business unit", before.runs)
	if first.err != "" || !first.ran {
		t.Fatalf("first life ran=%v err=%s", first.ran, first.err)
	}
	if rows := crashCutBusinessRows(t, h1); rows != 1 {
		t.Fatalf("business rows after the first life = %d, want 1", rows)
	}
	if err := h1.closeInternal("test-crash-cut"); err != nil {
		t.Fatalf("cut the first life: %v", err)
	}

	after := newCrashCutFixture(true)
	h2 := openCrashCutHome(t, channelID, dbPath, after, false)
	t.Cleanup(func() { _ = h2.closeInternal("test") })
	second := restartRecv(t, "the second life to re-run the unfenced unit", after.runs)
	if second.err != "" {
		t.Fatalf("second life: %s", second.err)
	}
	if second.fenced || !second.ran {
		t.Fatalf("second life fenced=%v ran=%v, want the unfenced re-execution",
			second.fenced, second.ran)
	}
	if rows := crashCutBusinessRows(t, h2); rows != 2 {
		t.Fatalf("business rows after the unfenced cut = %d, want 2", rows)
	}
}
