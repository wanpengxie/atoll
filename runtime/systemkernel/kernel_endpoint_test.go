package systemkernel

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

// This file covers the Kernel's fixed-endpoint surface — the addressability
// answer the routing organ asks for SystemActorID (build spec §4.1: fixed
// endpoint, no managed lifecycle lookup). The lifecycle state machine itself is
// covered in kernel_lifecycle_test.go.

func TestKernelEndpointsRefuseBeforeAdoption(t *testing.T) {
	t.Parallel()
	k := New()

	if err := k.Deliver(kernelTestEnvelope("before-start")); !errors.Is(err, actorhost.ErrNotHosted) {
		t.Fatalf("Deliver before Start = %v, want ErrNotHosted", err)
	}
	// A cancel with no adopted unit is a silent no-op, not a panic.
	k.CancelRequest("before-start")
	requireKernelNotServing(t, k, "before adoption")
}

func TestKernelEndpointsServeAdoptedUnit(t *testing.T) {
	t.Parallel()
	body := newKernelTestBody()
	k := New()
	unit := prepareKernelUnit(t, body)
	if err := k.Start(unit); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !k.IsRunning() {
		t.Fatal("IsRunning() = false for an adopted, live unit")
	}
	stat, ok := k.Stat()
	if !ok {
		t.Fatal("Stat() reported no live unit")
	}
	if stat.Kind != actor.KindSystem {
		t.Fatalf("Stat().Kind = %q, want %q", stat.Kind, actor.KindSystem)
	}
	if stat.StartedAt.IsZero() {
		t.Fatal("Stat().StartedAt is zero for a started unit")
	}
	inc, ok := k.Incarnation()
	if !ok {
		t.Fatal("Incarnation() reported no live unit")
	}
	if inc.ID() != actor.SystemActorID {
		t.Fatalf("Incarnation().ID() = %q, want %q", inc.ID(), actor.SystemActorID)
	}
	if inc != unit.Self() {
		t.Fatal("Incarnation() is not the adopted unit's own identity")
	}

	env := kernelTestEnvelope("delivered")
	if err := k.Deliver(env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	select {
	case got := <-body.received:
		if got.ID != env.ID {
			t.Fatalf("body received %q, want %q", got.ID, env.ID)
		}
	case <-timeAfterBudget():
		t.Fatal("adopted body never received the delivered envelope")
	}

	k.CancelRequest("delivered")
	select {
	case got := <-body.cancelled:
		if got != "delivered" {
			t.Fatalf("body cancelled %q, want %q", got, "delivered")
		}
	case <-timeAfterBudget():
		t.Fatal("adopted body never saw the cancel signal")
	}

	if err := k.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestKernelEndpointsSealAfterClose pins the close-last shape from the endpoint
// side: once the kernel is closing, the fixed endpoint refuses work with the
// "wrong address" answer rather than pushing it at a dying unit.
func TestKernelEndpointsSealAfterClose(t *testing.T) {
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

	if err := k.Deliver(kernelTestEnvelope("after-close")); !errors.Is(err, actorhost.ErrNotHosted) {
		t.Fatalf("Deliver after Close = %v, want ErrNotHosted", err)
	}
	k.CancelRequest("after-close")
	select {
	case got := <-body.cancelled:
		t.Fatalf("cancel %q was forwarded to a closed kernel's body", got)
	default:
	}
	requireKernelNotServing(t, k, "after Close")
}

// TestKernelEndpointsSealAfterUnexpectedExit: a dead body must stop being an
// addressable endpoint. The exact refusal error on Deliver belongs to actorrt
// (the unit answers for itself), so only "refused" is pinned here.
func TestKernelEndpointsSealAfterUnexpectedExit(t *testing.T) {
	t.Parallel()
	body := newKernelTestBody()
	k := New()
	unit := prepareKernelUnit(t, body)
	if err := k.Start(unit); err != nil {
		t.Fatalf("Start: %v", err)
	}

	body.dying <- errors.New("kernel test: endpoint body died")
	if cause := awaitKernelFatal(t, k); !errors.Is(cause, ErrExited) {
		t.Fatalf("fatal cause = %v, want it to wrap ErrExited", cause)
	}
	awaitUnitDone(t, unit, "unit Done")

	kernelEventually(t, "endpoint to refuse work after the body died", func() bool {
		return k.Deliver(kernelTestEnvelope("after-death")) != nil
	})
	requireKernelNotServing(t, k, "after unexpected exit")
}
