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
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// T23. Spec §8.3 replaced the deleted boot wake-debt case with one named
// acceptance: "boot 保留 open truth 但新 incarnation handler 零执行". Both halves
// matter and they pull against each other — an implementation that closes the
// stranded request at boot satisfies "no replay" while destroying truth, and
// one that rescans the log to wake its receiver preserves truth while replaying
// the lifecycle. §13.2's no-lifecycle-replay group says the same thing about
// the in-session half: R stays open across G1→G2 and G2 processes it zero
// times.
//
// The zero is only worth anything next to a live control, so the successor
// incarnations here are genuinely consuming bodies: each one is proven to
// receive a request written AFTER it came up, in the same test, before the
// never-saw-R claim is made.

const (
	bootOpenCallerClass   = "boot-open-caller"
	bootOpenReceiverClass = "boot-open-receiver"
	bootOpenCallerDecl    = "decl-boot-open-caller"
	bootOpenReceiverDecl  = "decl-boot-open-receiver"
)

// bootOpenFixture builds the two declared bodies. The caller always parks. The
// receiver parks in the life that must leave R open, and consumes in the lives
// whose delivery count is the verdict.
type bootOpenFixture struct {
	consume bool

	mu       sync.Mutex
	received []message.ID
	starts   int
}

func newBootOpenFixture(consume bool) *bootOpenFixture {
	return &bootOpenFixture{consume: consume}
}

func (f *bootOpenFixture) BuildClass(
	_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage,
) (platform.ActorFactory, bool) {
	switch class {
	case bootOpenCallerClass:
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error { <-sys.Life().Done(); return nil }, nil
		}}}, true
	case bootOpenReceiverClass:
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			return f.receiverProc(), nil
		}}}, true
	default:
		return platform.ActorFactory{}, false
	}
}

func (f *bootOpenFixture) receiverProc() actorbase.Proc {
	return func(sys actorbase.Sys) error {
		f.mu.Lock()
		f.starts++
		f.mu.Unlock()
		if !f.consume {
			<-sys.Life().Done()
			return nil
		}
		for {
			msg, err := sys.Recv()
			if err != nil {
				return nil
			}
			f.mu.Lock()
			f.received = append(f.received, msg.ID)
			f.mu.Unlock()
		}
	}
}

func (f *bootOpenFixture) sawDelivery(id message.ID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, seen := range f.received {
		if seen == id {
			return true
		}
	}
	return false
}

func (f *bootOpenFixture) incarnations() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts
}

func bootOpenDeclarations() []DeclareRequest {
	at := time.Now().UnixMilli()
	return []DeclareRequest{
		{
			SourceDeclID: bootOpenCallerDecl, Kind: actor.KindAgent,
			Class: bootOpenCallerClass, Placement: storespec.NewServerPlacement(),
			CreatedAt: at,
		},
		{
			SourceDeclID: bootOpenReceiverDecl, Kind: actor.KindAgent,
			Class: bootOpenReceiverClass, Placement: storespec.NewServerPlacement(),
			CreatedAt: at,
		},
	}
}

func openBootOpenHome(
	t *testing.T,
	channelID channel.ID,
	dbPath string,
	fixture *bootOpenFixture,
	seed bool,
) *Home {
	t.Helper()
	cfg := Config{
		ChannelID:            channelID,
		DBPath:               dbPath,
		CompositionResolver:  fixture,
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            seed,
		MustExistDB:          !seed,
	}
	if seed {
		cfg.BootstrapDeclarations = bootOpenDeclarations()
	}
	h, err := Open(cfg)
	if err != nil {
		t.Fatalf("open home (seed=%v): %v", seed, err)
	}
	return h
}

// bootOpenProveLive writes one fresh request through the ordinary caller pen and
// waits for the consuming body to report it. It is the control for every "zero
// executions" claim in this file: the body that never saw R demonstrably sees
// what arrives after it came up.
func bootOpenProveLive(
	t *testing.T,
	h *Home,
	fixture *bootOpenFixture,
	caller, receiver actor.ActorID,
	what string,
) {
	t.Helper()
	probe := closureCall(t, h, caller, receiver)
	restartEventually(t, what, func() bool { return fixture.sawDelivery(probe) })
}

func TestBootKeepsTheOpenRequestAndTheNewIncarnationNeverProcessesIt(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	const channelID = channel.ID("boot-open-truth")
	ctx := context.Background()

	before := newBootOpenFixture(false)
	h1 := openBootOpenHome(t, channelID, dbPath, before, true)
	caller := routingAgent(t, h1, bootOpenCallerDecl)
	receiver := routingAgent(t, h1, bootOpenReceiverDecl)

	stranded := closureCall(t, h1, caller, receiver)
	if !closureHasOpenRequest(t, h1, receiver) {
		t.Fatal("the request did not become open truth against its receiver")
	}
	if err := h1.closeInternal("test-boot-cut"); err != nil {
		t.Fatalf("cut the first session: %v", err)
	}

	after := newBootOpenFixture(true)
	h2 := openBootOpenHome(t, channelID, dbPath, after, false)
	t.Cleanup(func() { _ = h2.closeInternal("test") })

	// Half one — boot PRESERVED the open truth. Boot authored no terminal for
	// it and it is still a closure candidate, so the deadline reaper and the
	// closure reconciler both still own it.
	if terminals := closureTerminalsFor(t, h2, stranded); len(terminals) != 0 {
		t.Fatalf("boot closed the stranded request: %+v", terminals)
	}
	if !closureHasOpenRequest(t, h2, receiver) {
		t.Fatal("boot dropped the stranded request out of open truth")
	}

	// Half two — the new incarnation executes it zero times. The proof that the
	// zero is real: this very body receives a request written after boot.
	bootOpenProveLive(t, h2, after, caller, receiver, "the booted incarnation to receive a fresh request")
	if after.sawDelivery(stranded) {
		t.Fatal("boot redelivered the stranded request to the new incarnation")
	}

	// Neither does a reconcile sweep — the level loop owns closure and presence,
	// never redelivery.
	h2.reconcileSweep(ctx)
	bootOpenProveLive(t, h2, after, caller, receiver, "the incarnation to receive a request after a sweep")
	if after.sawDelivery(stranded) {
		t.Fatal("a reconcile sweep redelivered the stranded request")
	}

	// Nor does the next incarnation of the same identity: Restart publishes a
	// fresh term, and G2 processes the still-open R zero times as well.
	firstTerm, _ := serverTerm(t, h2, receiver)
	generations := after.incarnations()
	if err := h2.actors.Restart(ctx, actorctl.RestartRequest{ActorID: receiver}); err != nil {
		t.Fatalf("restart the receiver: %v", err)
	}
	restartEventually(t, "the successor incarnation to be published and running", func() bool {
		term, _ := serverTerm(t, h2, receiver)
		return term != firstTerm && after.incarnations() > generations
	})
	bootOpenProveLive(t, h2, after, caller, receiver, "the successor incarnation to receive a fresh request")
	if after.sawDelivery(stranded) {
		t.Fatal("the successor incarnation was handed the still-open request")
	}

	// And truth is where it started: still open, still unclosed, still owned by
	// the deadline.
	if terminals := closureTerminalsFor(t, h2, stranded); len(terminals) != 0 {
		t.Fatalf("the stranded request was closed along the way: %+v", terminals)
	}
	if !closureHasOpenRequest(t, h2, receiver) {
		t.Fatal("the stranded request left open truth without a terminal")
	}
}
