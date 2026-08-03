package systemkernel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// The SystemKernel lifecycle state machine: invalid identity/unit rejection,
// double Start, Start×Close mutual serialization (-race clean), unexpected
// Done is fatal, normal close reports last — plus the fault rows for
// adopt/Start failure and unexpected Done.

func TestKernelStartRejectsInvalidUnit(t *testing.T) {
	t.Parallel()

	// A Unit already taken past Prepared, in both directions.
	runningUnit := prepareKernelUnit(t, newKernelTestBody())
	if err := runningUnit.Start(); err != nil {
		t.Fatalf("seed running unit: %v", err)
	}
	stoppedUnit := prepareKernelUnit(t, newKernelTestBody())
	stoppedUnit.Stop()
	awaitUnitDone(t, stoppedUnit, "seed stopped unit")

	cases := []struct {
		name string
		unit *actorrt.Unit
		want error
	}{
		{"nil unit", nil, ErrInvalidUnit},
		{
			"foreign actor id",
			prepareKernelUnitAs(t, actor.ActorID("not-the-system"), actor.KindSystem, newKernelTestBody(), nil),
			ErrInvalidUnit,
		},
		{
			"non-system kind",
			prepareKernelUnitAs(t, actor.SystemActorID, actor.KindAgent, newKernelTestBody(), nil),
			ErrInvalidUnit,
		},
		{"already running unit", runningUnit, ErrInvalidUnit},
		{"already stopped unit", stoppedUnit, ErrInvalidUnit},
		{
			// The Unit's single event-consumer slot is already taken, so the
			// Kernel cannot become the owner of its exit edge. Adoption must
			// fail rather than run a Unit whose death it will never hear about.
			"event sink already installed",
			prepareKernelUnitAs(t, actor.SystemActorID, actor.KindSystem, newKernelTestBody(), kernelSinkStub{}),
			actorrt.ErrUnitSinkInstalled,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := New()
			if err := k.Start(tc.unit); !errors.Is(err, tc.want) {
				t.Fatalf("Start() = %v, want %v", err, tc.want)
			}
			requireKernelNotServing(t, k, "after rejected Start")

			// A rejected argument must not burn the one-shot: nothing was
			// transferred, so the kernel is still able to adopt a real unit.
			good := prepareKernelUnit(t, newKernelTestBody())
			if err := k.Start(good); err != nil {
				t.Fatalf("Start(valid unit) after rejection = %v, want nil", err)
			}
			if !k.IsRunning() {
				t.Fatal("kernel is not running after a successful Start")
			}
			if err := k.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

func TestKernelStartRejectsNilKernel(t *testing.T) {
	t.Parallel()
	var k *Kernel
	unit := prepareKernelUnit(t, newKernelTestBody())
	if err := k.Start(unit); !errors.Is(err, ErrInvalidUnit) {
		t.Fatalf("(*Kernel)(nil).Start() = %v, want %v", err, ErrInvalidUnit)
	}
	if unit.State() != actorrt.UnitPrepared {
		t.Fatalf("unit state = %v, want UnitPrepared (a nil kernel must not touch it)", unit.State())
	}
	if k.Failed() != nil {
		t.Fatal("(*Kernel)(nil).Failed() must be a nil channel")
	}
	if err := k.Close(context.Background()); err != nil {
		t.Fatalf("(*Kernel)(nil).Close() = %v, want nil", err)
	}
}

// TestKernelStartIsOneShot pins the "double Start" case: the adoption slot is
// consumed by the first success, and the losing candidate is left untouched —
// its ownership stays with the caller, so it must not have been started or
// stopped by the kernel.
func TestKernelStartIsOneShot(t *testing.T) {
	t.Parallel()
	k := New()
	incumbent := prepareKernelUnit(t, newKernelTestBody())
	if err := k.Start(incumbent); err != nil {
		t.Fatalf("first Start: %v", err)
	}

	candidate := prepareKernelUnit(t, newKernelTestBody())
	if err := k.Start(candidate); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start = %v, want ErrAlreadyStarted", err)
	}
	if got := candidate.State(); got != actorrt.UnitPrepared {
		t.Fatalf("rejected candidate state = %v, want UnitPrepared", got)
	}
	if !candidate.Stat().StartedAt.IsZero() {
		t.Fatal("rejected candidate was started by the kernel")
	}

	// The incumbent is still the served incarnation.
	inc, ok := k.Incarnation()
	if !ok || inc != incumbent.Self() {
		t.Fatalf("Incarnation() = (%v, %v), want the incumbent", inc, ok)
	}
	if err := k.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestKernelStartAfterCloseRejected(t *testing.T) {
	t.Parallel()

	t.Run("close before any adoption", func(t *testing.T) {
		k := New()
		if err := k.Close(context.Background()); err != nil {
			t.Fatalf("Close on an un-started kernel = %v, want nil", err)
		}
		unit := prepareKernelUnit(t, newKernelTestBody())
		if err := k.Start(unit); !errors.Is(err, ErrClosed) {
			t.Fatalf("Start after Close = %v, want ErrClosed", err)
		}
		if unit.State() != actorrt.UnitPrepared {
			t.Fatalf("unit state = %v, want UnitPrepared", unit.State())
		}
	})

	t.Run("close after adoption", func(t *testing.T) {
		k := New()
		unit := prepareKernelUnit(t, newKernelTestBody())
		if err := k.Start(unit); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := k.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
		replacement := prepareKernelUnit(t, newKernelTestBody())
		if err := k.Start(replacement); !errors.Is(err, ErrClosed) {
			t.Fatalf("Start after Close = %v, want ErrClosed", err)
		}
	})
}

// TestKernelFatalOnSelfReportedExit covers the §7 row "Running kernel
// unexpected Done | fatal once；whole server fail-stop": the body dies on its
// own, the kernel turns that into exactly one fatal report carrying ErrExited
// joined with the body's cause, and stops answering as a live endpoint.
func TestKernelFatalOnSelfReportedExit(t *testing.T) {
	t.Parallel()
	body := newKernelTestBody()
	k := New()
	unit := prepareKernelUnit(t, body)
	if err := k.Start(unit); err != nil {
		t.Fatalf("Start: %v", err)
	}

	bodyCause := errors.New("kernel test: body decided to die")
	body.dying <- bodyCause

	cause := awaitKernelFatal(t, k)
	if !errors.Is(cause, ErrExited) {
		t.Fatalf("fatal cause = %v, want it to wrap ErrExited", cause)
	}
	if !errors.Is(cause, bodyCause) {
		t.Fatalf("fatal cause = %v, want it to carry the body's own cause", cause)
	}

	// "fatal once": the channel is closed behind the single cause, so no second
	// report can ever be produced.
	select {
	case _, open := <-k.Failed():
		if open {
			t.Fatal("Failed() produced a second cause")
		}
	case <-time.After(kernelTestBudget):
		t.Fatal("Failed() was not closed after the single fatal")
	}

	awaitUnitDone(t, unit, "unit Done after self-reported exit")
	kernelEventually(t, "kernel to stop reporting a live unit", func() bool { return !k.IsRunning() })
	requireKernelNotServing(t, k, "after unexpected exit")
}

// TestKernelFatalOnUnitStoppedBehindItsBack covers the other producer of the
// same edge: something outside the kernel stops the adopted Unit. A Done that
// the kernel did not order is still unexpected, so it is still fatal.
func TestKernelFatalOnUnitStoppedBehindItsBack(t *testing.T) {
	t.Parallel()
	k := New()
	unit := prepareKernelUnit(t, newKernelTestBody())
	if err := k.Start(unit); err != nil {
		t.Fatalf("Start: %v", err)
	}

	unit.Stop()

	cause := awaitKernelFatal(t, k)
	if !errors.Is(cause, ErrExited) {
		t.Fatalf("fatal cause = %v, want it to wrap ErrExited", cause)
	}
	awaitUnitDone(t, unit, "unit Done")
	kernelEventually(t, "kernel to stop reporting a live unit", func() bool { return !k.IsRunning() })
}

// TestKernelCloseDrainsUnitAndReportsNoFatal is the "normal close last" case:
// Close does not return until the adopted Unit has actually reached Done (its
// Stop hook ran), and an ordered shutdown must never be mistaken for the fatal
// unexpected-exit edge.
func TestKernelCloseDrainsUnitAndReportsNoFatal(t *testing.T) {
	t.Parallel()
	body := newKernelTestBody()
	k := New()
	unit := prepareKernelUnit(t, body)
	if err := k.Start(unit); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := k.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Close returning is the happens-before edge for all three facts below.
	select {
	case <-unit.Done():
	default:
		t.Fatal("Close returned before the adopted unit reached Done")
	}
	select {
	case <-body.stopped:
	default:
		t.Fatal("Close returned before the body's Stop hook ran")
	}
	requireKernelNotServing(t, k, "after Close")
	requireNoKernelFatal(t, k)
}

// TestKernelCloseHonoursContextDeadline pins the drain contract's other half:
// Close waits on the Unit, but the caller's context bounds that wait. A body
// that ignores its Stop budget must surface as the caller's deadline error, not
// as an unbounded shutdown hang.
func TestKernelCloseHonoursContextDeadline(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{})
	body := newKernelTestBody()
	body.stopGate = gate

	k := New()
	unit := prepareKernelUnit(t, body)
	// Cleanups run LIFO, so releasing the gate here runs BEFORE the unit-drain
	// cleanup registered by prepareKernelUnit.
	t.Cleanup(func() { close(gate) })

	if err := k.Start(unit); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	closeErr := make(chan error, 1)
	go func() { closeErr <- k.Close(ctx) }()

	// The body has entered its blocking Stop, so Done cannot close.
	select {
	case <-body.stopped:
	case <-time.After(kernelTestBudget):
		t.Fatal("body Stop hook was never invoked")
	}
	select {
	case err := <-closeErr:
		t.Fatalf("Close returned %v while the unit was still draining", err)
	default:
	}

	cancel()
	select {
	case err := <-closeErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close = %v, want context.Canceled", err)
		}
	case <-time.After(kernelTestBudget):
		t.Fatal("Close ignored its context deadline")
	}

	// The kernel is sealed even though the drain did not finish.
	requireKernelNotServing(t, k, "after a deadline-bounded Close")
}

func TestKernelCloseIsIdempotentUnderConcurrency(t *testing.T) {
	t.Parallel()
	k := New()
	unit := prepareKernelUnit(t, newKernelTestBody())
	if err := k.Start(unit); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const callers = 8
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- k.Close(context.Background())
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(kernelTestBudget):
		t.Fatal("concurrent Close callers blocked")
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Close = %v, want nil", err)
		}
	}
	awaitUnitDone(t, unit, "unit Done after concurrent Close")
	requireNoKernelFatal(t, k)
}

// TestKernelStartCloseSerialisation is the §13.2 "Start×Close 共同串行且 -race
// 无裂锁" case. The invariant under test is not who wins, but that no
// interleaving can leave a started Unit running behind a closed kernel: either
// Close rejects the adoption outright, or it owns the drain.
func TestKernelStartCloseSerialisation(t *testing.T) {
	t.Parallel()
	for range 64 {
		k := New()
		unit := prepareKernelUnit(t, newKernelTestBody())

		var startErr, closeErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); startErr = k.Start(unit) }()
		go func() { defer wg.Done(); closeErr = k.Close(context.Background()) }()
		wg.Wait()

		if closeErr != nil {
			t.Fatalf("Close = %v, want nil", closeErr)
		}
		if startErr != nil &&
			!errors.Is(startErr, ErrClosed) &&
			!errors.Is(startErr, ErrInvalidUnit) &&
			!errors.Is(startErr, actorrt.ErrUnitNotPrepared) {
			t.Fatalf("Start = %v, want nil, ErrClosed, ErrInvalidUnit or ErrUnitNotPrepared", startErr)
		}
		if k.IsRunning() {
			t.Fatal("kernel still reports a running unit after Close")
		}
		if !unit.Stat().StartedAt.IsZero() {
			// It was started, so somebody owed it a drain.
			kernelEventually(t, "started unit to reach Done", func() bool {
				return unit.State() == actorrt.UnitDone
			})
		} else if unit.State() != actorrt.UnitPrepared && unit.State() != actorrt.UnitDone {
			t.Fatalf("never-started unit in state %v", unit.State())
		}
	}
}

// TestKernelConcurrentStartElectsExactlyOneUnit pins the one-shot adoption slot
// under contention: exactly one candidate is adopted, the other is refused with
// ErrAlreadyStarted and left unstarted.
func TestKernelConcurrentStartElectsExactlyOneUnit(t *testing.T) {
	t.Parallel()
	for range 32 {
		k := New()
		units := [2]*actorrt.Unit{
			prepareKernelUnit(t, newKernelTestBody()),
			prepareKernelUnit(t, newKernelTestBody()),
		}
		var errs [2]error
		var wg sync.WaitGroup
		for i := range units {
			wg.Add(1)
			go func() { defer wg.Done(); errs[i] = k.Start(units[i]) }()
		}
		wg.Wait()

		winners := 0
		for i, err := range errs {
			switch {
			case err == nil:
				winners++
			case errors.Is(err, ErrAlreadyStarted):
				if !units[i].Stat().StartedAt.IsZero() {
					t.Fatalf("refused candidate %d was started anyway", i)
				}
			default:
				t.Fatalf("Start(%d) = %v, want nil or ErrAlreadyStarted", i, err)
			}
		}
		if winners != 1 {
			t.Fatalf("adopted %d units, want exactly 1", winners)
		}
		inc, ok := k.Incarnation()
		if !ok {
			t.Fatal("kernel serves no incarnation after a successful Start")
		}
		if inc != units[0].Self() && inc != units[1].Self() {
			t.Fatal("kernel serves an incarnation that is neither candidate")
		}
		if err := k.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

// TestKernelAdoptedUnitDyingAtStartLeavesKernelNotRunning covers the §7 row
// "SystemKernel adopt/Start失败 | exact cleanup owned Unit；不得Running".
//
// The body fails its own Starter.Start, so the adopted Unit dies immediately.
// Whether Kernel.Start observes that before returning is a genuine race with
// the Unit's run goroutine, so BOTH outcomes are accepted; what the spec
// requires — and what is asserted here — is that neither outcome leaves the
// kernel serving, and that the owned Unit is exactly cleaned up (Done).
//
// NOT asserted: whether a failed Start may be retried. See the package notes on
// Kernel.release / Kernel.fail — the spec text does not settle it and the
// production caller (platform/home/actor_system.go start) never retries.
func TestKernelAdoptedUnitDyingAtStartLeavesKernelNotRunning(t *testing.T) {
	t.Parallel()
	body := newKernelTestBody()
	body.startErr = errors.New("kernel test: body refuses to start")

	k := New()
	unit := prepareKernelUnit(t, body)
	err := k.Start(unit)
	if err != nil && !errors.Is(err, ErrInvalidUnit) {
		t.Fatalf("Start = %v, want nil or ErrInvalidUnit", err)
	}
	if err == nil {
		// Start won the race with the run goroutine, so the kernel was briefly
		// Running and the death is an unexpected Done: it must fail-stop the
		// server through the one fatal report.
		if cause := awaitKernelFatal(t, k); !errors.Is(cause, ErrExited) {
			t.Fatalf("fatal cause = %v, want it to wrap ErrExited", cause)
		}
	}

	awaitUnitDone(t, unit, "owned unit to be cleaned up")
	kernelEventually(t, "kernel to stop reporting a running unit", func() bool { return !k.IsRunning() })
	requireKernelNotServing(t, k, "after an adopted unit died at Start")
}
