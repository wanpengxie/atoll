package link_test

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// TestRebindableArms_FlapContinuity is S4's flap-continuity proof (§10.13
// 推导3): a hosted cell's write capability survives a wire flap without ever
// rebuilding the cell. The SAME RebindableArms backs the SAME daemon-side
// embodiment (installed exactly once) across a full link death + reconnect —
// only the underlying stream/proxy is swapped.
func TestRebindableArms_FlapContinuity(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolID = actor.ActorID("tool:flap")
	d1, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial 1: %v", err)
	}

	h := newDaemonHost()
	defer h.Stop()

	arms1, err := d1.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, nil)
	if err != nil {
		t.Fatalf("OpenStream 1: %v", err)
	}
	rb := link.NewRebindableArms(arms1)
	// The cell is installed ONCE against the membrane's facade — never a raw
	// per-stream proxy directly — and never re-Spawned below (this is the
	// reopen-not-respawn discipline the reconcile ring's F6 path rests on).
	h.Install(toolID, &echoCell{w: rb.Pen()}, nil)
	d1.Start()

	write := func(id message.ID) error {
		_, err := rb.Pen().Write(context.Background(), &message.Envelope{
			ID: id, Kind: message.KindResponse, Type: "flap.probe",
			Audience: message.Audience{"user:a"},
		})
		return err
	}

	if err := write("resp-1"); err != nil {
		t.Fatalf("pre-flap write: %v", err)
	}

	// Break the link (a kill -9 analogue): the raw arm dies, but the daemon-side
	// embodiment (h.rt's cell for toolID) is untouched — link death degrades the
	// wire, it never touches hosted work (§10.13 推导3).
	if err := d1.Close(); err != nil {
		t.Fatalf("d1.Close: %v", err)
	}

	// Disconnect window: the membrane keeps pointing at the now-closed arm, so a
	// write returns a transport error. No artificial "dead arm" was built for
	// this — the existing closed-proxy behavior (RemoteWriter.Close) already IS
	// the disconnect window's contract (红线12 fail-closed).
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := write("resp-during-flap"); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("write during the disconnect window unexpectedly succeeded")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Reconnect: a NEW Dialer, a NEW stream opened for the SAME actor id.
	// Rebind swaps the membrane onto it — h.Install is NOT called again, so the
	// actorrt embodiment is the exact same one that served the pre-flap write.
	d2, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial 2: %v", err)
	}
	defer func() { _ = d2.Close() }()

	arms2, err := d2.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, nil)
	if err != nil {
		t.Fatalf("OpenStream 2: %v", err)
	}
	rb.Rebind(arms2)
	d2.Start()

	if err := write("resp-2"); err != nil {
		t.Fatalf("post-reconnect write: %v", err)
	}

	got := r.minter.all()
	if len(got) != 2 || got[0].ID != "resp-1" || got[1].ID != "resp-2" {
		var ids []message.ID
		for _, e := range got {
			ids = append(ids, e.ID)
		}
		t.Fatalf("home writes across the flap = %v, want [resp-1 resp-2] (resp-during-flap must never land)", ids)
	}
}

func TestRuntimeSealRejectsActorArmWithoutKillingWholeLink(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)
	r.rt.Seal()
	const id = actor.ActorID("tool:sealed-arm")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: id, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	open := func() link.CellArms {
		arms, err := d.OpenStream(id, func(*message.Envelope) error { return nil }, nil)
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		d.StartStream(id)
		return arms
	}
	arms := open()
	deadline := time.Now().Add(time.Second)
	for {
		_, err = arms.Pen.Write(context.Background(), &message.Envelope{
			ID: "probe", Kind: message.KindEvent, Type: "sealed.probe",
			Payload: []byte(`{}`), Visibility: message.VisibilityPublic,
			Audience: message.Audience{"nobody"},
		})
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sealed actor stream did not close")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-d.Done():
		t.Fatal("one sealed actor arm killed the whole link")
	default:
	}
	// The same live Dialer can perform the stream-level 补臂 attempt again;
	// compute.Run's reconcile loop therefore reopens this arm, not the whole link.
	_ = open()
}
