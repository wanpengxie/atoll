package home

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// dialAttach is a minimal net.Pipe-backed remote for driving
// actorrt.Runtime.PrepareHandshake from within platform/home tests (the
// real attach-race window spans BOTH the ledger's atomic ticket word,
// platform/home/liveness.go, and the physical live-flip, runtime/actorrt's
// PreparedAttach.Commit — spec §2.6 "attach fence"). It presents leaseID over
// the wire and returns the *actorrt.PreparedAttach BEFORE Commit is called,
// so the test can insert a concurrent ledger event in the window between
// "handshake validated" and "Runtime live-flip".
func dialAttach(t *testing.T, rt *actorrt.Runtime, leaseID string, id actor.ActorID) *actorrt.PreparedAttach {
	t.Helper()
	hostConn, remoteConn := net.Pipe()
	t.Cleanup(func() { _ = remoteConn.Close() })
	go func() {
		codec := ipc.NewCodec(remoteConn, remoteConn)
		payload, _ := json.Marshal(ipc.HandshakePayload{LeaseID: leaseID})
		_ = codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload})
		_, _ = codec.Read() // consume the ack (or find nothing on rejection)
	}()
	resolve := func(ipc.HandshakePayload) (actor.ActorID, error) { return id, nil }
	emit := func(context.Context, actorrt.Incarnation, *message.Envelope) (ipc.EmitResult, error) {
		return ipc.EmitResult{}, nil
	}
	prepared, err := rt.PrepareHandshake(context.Background(), hostConn, actorrt.Sinks{Emit: emit}, resolve, nil, nil, func(actorrt.Incarnation) {})
	if err != nil {
		t.Fatalf("PrepareHandshake(%s): %v", leaseID, err)
	}
	return prepared
}

// TestAttachRaceWindowRejectsStaleCandidateAndPreservesIncumbent is the S9/§2.6
// "attach 并发插窗" acceptance test: it drives the REAL livenessLedger fence
// (platform/home/liveness.go prepareAttachmentFence/attachmentFence.Valid)
// together with the REAL runtime/actorrt physical Commit — proving that an
// apply (decl version advance) or a restart (ticket exchange) landing AFTER
// the handshake validated the ticket but BEFORE Runtime's live-flip fences the
// stale candidate out, while any pre-existing incumbent embodiment is left
// untouched (Commit only ever closes the CANDIDATE, never the incumbent —
// §2.6 "拒绝只关 candidate、绝不碰 incumbent").
func TestAttachRaceWindowRejectsStaleCandidateAndPreservesIncumbent(t *testing.T) {
	const id = actor.ActorID("race-target")

	t.Run("apply_advances_decl_version_mid_window", func(t *testing.T) {
		rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
		defer rt.StopAll()

		// Establish an incumbent embodiment first (a prior, already-committed
		// attach) so we can prove it survives the raced candidate's rejection.
		incumbentPrep := dialAttach(t, rt, "incumbent", id)
		if _, err := incumbentPrep.Commit(func() bool { return true }); err != nil {
			t.Fatalf("incumbent commit: %v", err)
		}
		old, ok := rt.CurrentIncarnation(id)
		if !ok {
			t.Fatal("incumbent missing after commit")
		}

		// The ledger side: a fresh ensure attempt mints a ticket, and the
		// handshake validates it — prepareAttachmentFence is exactly the point
		// spec §2.6 calls "验票不能等到账本 Attach": the candidate's fence is
		// captured HERE, before the physical live-flip.
		l := newLivenessLedger()
		l.Bootstrap([]actor.ActorID{id})
		ticket, verdict := l.BeginEnsure(id, 1)
		if verdict != transitionApplied {
			t.Fatalf("BeginEnsure=%v", verdict)
		}
		fence, verdict := l.prepareAttachmentFence(id, ticket, 1)
		if verdict != transitionApplied || !fence.Valid() {
			t.Fatalf("prepare fence=(%v,%v)", verdict, fence.Valid())
		}

		// The candidate's physical handshake also validates concurrently
		// (mirrors accept.go: PrepareAttachmentFence happens pre-handshake,
		// PrepareHandshake/Commit happens around the wire ACK).
		candidatePrep := dialAttach(t, rt, "stale-shell", id)

		// INSERT the concurrent apply here — inside the window between
		// "handshake validated" (fence captured above) and "Runtime
		// live-flip" (candidatePrep.Commit below): a decl edit+apply landed,
		// advancing the actor's current version to 2, which retires the
		// in-flight attempt via RetireIfVersionSkew (the real apply path,
		// composition.go, drives this same ledger method).
		if _, retired := l.RetireIfVersionSkew(id, 2); !retired {
			t.Fatal("concurrent apply (version skew) did not retire the in-flight attempt")
		}
		if fence.Valid() {
			t.Fatal("fence still valid after concurrent apply advanced the decl version — window not fenced")
		}

		// The stale candidate's Commit must now be rejected by the fence —
		// the OLD SHELL (this handshake) never gets to live-flip.
		if _, err := candidatePrep.Commit(func() bool { return fence.Valid() }); err == nil {
			t.Fatal("stale candidate attach committed despite concurrent apply invalidating its fence")
		}

		// The incumbent — a wholly separate embodiment — must be untouched:
		// Commit only ever closes the rejected candidate.
		current, ok := rt.CurrentIncarnation(id)
		if !ok || current != old || !rt.IsLive(old) {
			t.Fatalf("fenced candidate displaced incumbent: current=%v old=%v ok=%v live=%v", current, old, ok, rt.IsLive(old))
		}
	})

	t.Run("restart_ticket_exchange_mid_window", func(t *testing.T) {
		rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
		defer rt.StopAll()

		incumbentPrep := dialAttach(t, rt, "incumbent", id)
		if _, err := incumbentPrep.Commit(func() bool { return true }); err != nil {
			t.Fatalf("incumbent commit: %v", err)
		}
		old, ok := rt.CurrentIncarnation(id)
		if !ok {
			t.Fatal("incumbent missing after commit")
		}

		l := newLivenessLedger()
		l.Bootstrap([]actor.ActorID{id})
		ticket, verdict := l.BeginEnsure(id, 1)
		if verdict != transitionApplied {
			t.Fatalf("BeginEnsure=%v", verdict)
		}
		fence, verdict := l.prepareAttachmentFence(id, ticket, 1)
		if verdict != transitionApplied || !fence.Valid() {
			t.Fatalf("prepare fence=(%v,%v)", verdict, fence.Valid())
		}

		candidatePrep := dialAttach(t, rt, "stale-shell-restart", id)

		// INSERT the concurrent restart here — a concurrent restart claims THIS
		// exact in-flight ticket and retires it mid-window (the real restart
		// path — the restart word's post-commit effect in opentry applyEffects —
		// drives this same ledger method with restartIntent=true).
		if _, retired := l.RetireIfTicketMatches(id, ticket, true); !retired {
			t.Fatal("concurrent restart (ticket exchange) did not retire the matching in-flight attempt")
		}
		if fence.Valid() {
			t.Fatal("fence still valid after concurrent restart consumed its ticket — window not fenced")
		}

		if _, err := candidatePrep.Commit(func() bool { return fence.Valid() }); err == nil {
			t.Fatal("stale candidate attach committed despite concurrent restart invalidating its fence")
		}

		current, ok := rt.CurrentIncarnation(id)
		if !ok || current != old || !rt.IsLive(old) {
			t.Fatalf("fenced candidate displaced incumbent: current=%v old=%v ok=%v live=%v", current, old, ok, rt.IsLive(old))
		}
	})
}
