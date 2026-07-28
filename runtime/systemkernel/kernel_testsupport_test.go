package systemkernel

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// kernelTestBudget is deliberately generous: these tests run under -race on a
// machine that may be executing other packages' suites concurrently. Every wait
// is a poll or a channel receive — the budget only bounds failure, it is never
// the synchronisation itself.
const kernelTestBudget = 10 * time.Second

// kernelQuietWindow is the observation window used by NEGATIVE assertions
// ("no fatal was reported"). It is not a synchronisation device: each use is
// anchored after a happens-before edge (Close returned / Unit Done) and only
// needs to outlast the one remaining watcher goroutine's wake-up.
const kernelQuietWindow = 300 * time.Millisecond

// kernelTestBody is one adoptable SystemActor body. It implements every
// optional actorrt occupant seam the Kernel's surface touches, so a single type
// covers work delivery (Receive), the cancel organ (CancelRequest), the
// self-reported exit edge (Dying) and cleanup (Stop).
type kernelTestBody struct {
	// startErr, when non-nil, makes the Unit's run loop fail immediately at
	// Starter.Start — the only in-process way to produce a Unit that is adopted
	// and then dies without anyone stopping it.
	startErr error
	// stopGate, when non-nil, blocks Stopper.Stop until the test closes it. A
	// blocked Stop keeps Unit.Done() open, which is what makes Close's context
	// deadline observable.
	stopGate <-chan struct{}

	received  chan *message.Envelope
	cancelled chan message.ID
	dying     chan error
	stopped   chan struct{}
}

func newKernelTestBody() *kernelTestBody {
	return &kernelTestBody{
		received:  make(chan *message.Envelope, 8),
		cancelled: make(chan message.ID, 8),
		dying:     make(chan error, 1),
		stopped:   make(chan struct{}, 1),
	}
}

func (b *kernelTestBody) Receive(_ context.Context, env *message.Envelope) error {
	select {
	case b.received <- env:
	default:
	}
	return nil
}

func (b *kernelTestBody) Start(context.Context, actorrt.ActorContext) error { return b.startErr }

func (b *kernelTestBody) Stop(context.Context) error {
	select {
	case b.stopped <- struct{}{}:
	default:
	}
	if b.stopGate != nil {
		<-b.stopGate
	}
	return nil
}

func (b *kernelTestBody) Dying() <-chan error { return b.dying }

func (b *kernelTestBody) CancelRequest(id message.ID) {
	select {
	case b.cancelled <- id:
	default:
	}
}

var (
	_ actorrt.Actor            = (*kernelTestBody)(nil)
	_ actorrt.Starter          = (*kernelTestBody)(nil)
	_ actorrt.Stopper          = (*kernelTestBody)(nil)
	_ actorrt.DownReporter     = (*kernelTestBody)(nil)
	_ actorrt.RequestCanceller = (*kernelTestBody)(nil)
)

// kernelSinkStub is a foreign event sink, used only to occupy a Unit's single
// sink slot before the Kernel gets a chance to adopt it.
type kernelSinkStub struct{}

func (kernelSinkStub) OnExited(actorrt.ExitedEvent) {}
func (kernelSinkStub) OnObs(actorrt.UnitObsEvent)   {}

// prepareKernelUnit builds a Unit that satisfies every Kernel.Start precondition:
// SystemActorID + KindSystem + UnitPrepared + no sink installed.
func prepareKernelUnit(t *testing.T, body actorrt.Actor) *actorrt.Unit {
	t.Helper()
	return prepareKernelUnitAs(t, actor.SystemActorID, actor.KindSystem, body, nil)
}

// prepareKernelUnitAs builds a Unit with an explicit identity/kind/sink so the
// validation table can perturb exactly one precondition at a time.
func prepareKernelUnitAs(t *testing.T, id actor.ActorID, kind actor.Kind, body actorrt.Actor, sink actorrt.UnitEventSink) *actorrt.Unit {
	t.Helper()
	unit, err := actorrt.Prepare(
		actorrt.UnitConfig{ActorID: id, Kind: kind},
		func(actorrt.Incarnation) actorrt.Actor { return body },
		sink,
	)
	if err != nil {
		t.Fatalf("actorrt.Prepare(%s/%s): %v", id, kind, err)
	}
	t.Cleanup(func() {
		unit.Stop()
		select {
		case <-unit.Done():
		case <-time.After(kernelTestBudget):
			t.Errorf("unit %s did not reach Done during cleanup", id)
		}
	})
	return unit
}

// kernelEventually polls until cond holds. Polling — never a bare sleep — is the
// synchronisation primitive for facts produced by another goroutine.
func kernelEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(kernelTestBudget)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// timeAfterBudget is the failure bound for a channel receive that is itself the
// synchronisation edge.
func timeAfterBudget() <-chan time.Time { return time.After(kernelTestBudget) }

func awaitUnitDone(t *testing.T, unit *actorrt.Unit, what string) {
	t.Helper()
	select {
	case <-unit.Done():
	case <-time.After(kernelTestBudget):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// awaitKernelFatal receives the one fatal cause the Kernel is allowed to report.
func awaitKernelFatal(t *testing.T, k *Kernel) error {
	t.Helper()
	select {
	case cause, ok := <-k.Failed():
		if !ok {
			t.Fatal("Failed() was closed without ever carrying a cause")
		}
		return cause
	case <-time.After(kernelTestBudget):
		t.Fatal("kernel reported no fatal")
	}
	return nil
}

// requireNoKernelFatal asserts the fatal channel stayed silent. Callers must
// anchor it after a happens-before edge; the window only covers scheduler lag.
func requireNoKernelFatal(t *testing.T, k *Kernel) {
	t.Helper()
	select {
	case cause, ok := <-k.Failed():
		t.Fatalf("unexpected fatal report: cause=%v open=%v", cause, ok)
	case <-time.After(kernelQuietWindow):
	}
}

// requireKernelNotServing is the §7 fault-table shape "不得 Running": every
// addressable read must refuse and the endpoint must not accept work.
func requireKernelNotServing(t *testing.T, k *Kernel, when string) {
	t.Helper()
	if k.IsRunning() {
		t.Errorf("%s: IsRunning() = true, want false", when)
	}
	if _, ok := k.Stat(); ok {
		t.Errorf("%s: Stat() reported a live unit", when)
	}
	if _, ok := k.Incarnation(); ok {
		t.Errorf("%s: Incarnation() reported a live unit", when)
	}
}

func kernelTestEnvelope(id message.ID) *message.Envelope {
	return &message.Envelope{ID: id, Kind: message.KindEvent, Type: "systemkernel.test"}
}
