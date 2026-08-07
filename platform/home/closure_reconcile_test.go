package home

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// Closure is the substrate's answer to the black hole: a caller blocked on a
// request whose receiver can never answer. With the death EDGE gone from
// production (grep MaterialiseReceiverUnavailable — the only production caller
// left is the reconciler below it), the level scan in reconcileClosure is the
// WHOLE mechanism. Its two ends have unit tests of their own; what nobody
// exercised is the wiring between them — that Home's own sweep really carries a
// real removal through to a real receiver_unavailable terminal in the log, and
// that a write fault on the way is really recorded instead of swallowed.

const (
	closureCallerDecl   = "decl:closure-caller"
	closureReceiverDecl = "decl:closure-receiver"
	closureRequestType  = "test.closure.work"
)

// openClosureHome boots a channel holding two live declared agents: one that
// calls and one that is called. Both park, so nothing in the process answers a
// request except closure itself.
func openClosureHome(t *testing.T, name string) *Home {
	t.Helper()
	h, err := Open(Config{
		ChannelID:            channel.ID(name),
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  routingResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{
			routingDeclaration(closureCallerDecl, "routing-live"),
			routingDeclaration(closureReceiverDecl, "routing-live"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	return h
}

// closureCall writes ONE real kind=request from caller to receiver through a
// real minted authority pen — the same pen shape a live body writes with. The
// request is then open truth and stays open: nobody in the process reads the
// receiver's mailbox.
func closureCall(t *testing.T, h *Home, caller, receiver actor.ActorID) message.ID {
	t.Helper()
	term, _ := serverTerm(t, h, caller)
	basis, err := h.controller.PenBasis(caller, term)
	if err != nil {
		t.Fatalf("pen basis for the caller: %v", err)
	}
	pen := h.minter.MintAuthority(basis.Run, basis.Kind)
	env, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{
		Type:     closureRequestType,
		Payload:  json.RawMessage(`{"unit":"work"}`),
		Audience: message.Audience{receiver},
	})
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}
	result, err := pen.Write(context.Background(), env)
	if err != nil || !result.Accepted() {
		t.Fatalf("write the request: %+v err=%v", result, err)
	}
	return env.ID
}

// closureTerminalsFor returns every response row the log holds for one request.
// Reading the rows themselves (not a count, not a projection) is the point: the
// claim under test is about WHO authored the terminal and with WHICH word.
func closureTerminalsFor(t *testing.T, h *Home, request message.ID) []message.Envelope {
	t.Helper()
	rows, err := h.query.ReadAfterSeq(context.Background(), 0, 2000)
	if err != nil {
		t.Fatalf("read the channel log: %v", err)
	}
	var out []message.Envelope
	for _, row := range rows {
		if row.Envelope.Kind == message.KindResponse && row.Envelope.ParentID == request {
			out = append(out, row.Envelope)
		}
	}
	return out
}

// closureHasOpenRequest asks the truth-derived receiver set — the same view the
// reconciler scans — whether id is still holding an unanswered request.
func closureHasOpenRequest(t *testing.T, h *Home, id actor.ActorID) bool {
	t.Helper()
	receivers, err := h.query.DistinctOpenRequestReceivers(context.Background())
	if err != nil {
		t.Fatalf("distinct open request receivers: %v", err)
	}
	for _, receiver := range receivers {
		if receiver == id {
			return true
		}
	}
	return false
}

// closureStopReconcileLoop shuts the background sweep down and joins it, so a
// test that reaches into Home's own fields afterwards is the only thing running
// a sweep. Close tolerates the already-cancelled loop.
func closureStopReconcileLoop(t *testing.T, h *Home) {
	t.Helper()
	if h.reconcileStop == nil || h.reconcileDone == nil {
		t.Fatal("home has no reconcile loop to stop")
	}
	h.reconcileStop()
	select {
	case <-h.reconcileDone:
	case <-time.After(restartWaitBudget):
		t.Fatal("timed out joining the reconcile loop")
	}
}

// T21. The whole wire, driven by production verbs only: a real caller writes a
// real request to a real member, the member is really Removed, and NOTHING in
// the test asks for closure — the Remove's own poke drives Home's reconcile
// loop, which drives the closure scan, which authors the terminal. This is the
// interconnect the two end-unit tests never covered; the sweep's own verdicts
// (no false close, idempotence) are pinned separately below, where the loop is
// joined first so the test owns every sweep.
func TestReconcileClosureCarriesARealRemovalToAReceiverUnavailableTerminal(t *testing.T) {
	h := openClosureHome(t, "closure-e2e")
	ctx := context.Background()
	caller := routingAgent(t, h, closureCallerDecl)
	receiver := routingAgent(t, h, closureReceiverDecl)

	request := closureCall(t, h, caller, receiver)
	if !closureHasOpenRequest(t, h, receiver) {
		t.Fatal("the request did not become open truth against its receiver")
	}

	if err := removeThroughSysOp(h, ctx, receiver); err != nil {
		t.Fatalf("remove the receiver: %v", err)
	}

	// Nothing below asks for a sweep: Remove poked the loop, and the loop owns
	// the rest.
	var terminal message.Envelope
	restartEventually(t, "the removed receiver's caller to be closed", func() bool {
		terminals := closureTerminalsFor(t, h, request)
		if len(terminals) == 0 {
			return false
		}
		terminal = terminals[0]
		return true
	})

	if terminal.Sender.ID != actor.SystemActorID || terminal.Sender.Kind != actor.KindSystem {
		t.Fatalf("closure terminal authored by %+v, want the substrate", terminal.Sender)
	}
	if len(terminal.Audience) != 1 || terminal.Audience[0] != caller {
		t.Fatalf("closure terminal addressed %v, want the stranded caller %s", terminal.Audience, caller)
	}
	var payload struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(terminal.Payload, &payload); err != nil {
		t.Fatalf("decode terminal payload %s: %v", terminal.Payload, err)
	}
	if payload.Status != string(message.StatusFailed) ||
		payload.Reason != string(message.TerminalReceiverUnavailable) {
		t.Fatalf("closure terminal payload = %s, want failed/receiver_unavailable", terminal.Payload)
	}

	// Closed truth is closed for good: the receiver leaves the open-request set
	// the reconciler scans, so it is never a candidate again.
	if closureHasOpenRequest(t, h, receiver) {
		t.Fatal("the closed request is still counted as open")
	}
}

// T21, verdict half. Closure is a MONOTONE predicate, not liveness, and it is
// idempotent by construction. Both are properties OF THE SWEEP, so the loop is
// joined first and every sweep below is one this test asked for.
func TestReconcileClosureSparesLiveReceiversAndAuthorsAtMostOneTerminal(t *testing.T) {
	h := openClosureHome(t, "closure-verdicts")
	ctx := context.Background()
	caller := routingAgent(t, h, closureCallerDecl)
	receiver := routingAgent(t, h, closureReceiverDecl)
	closureStopReconcileLoop(t, h)

	request := closureCall(t, h, caller, receiver)
	h.reconcileSweep(ctx)
	if terminals := closureTerminalsFor(t, h, request); len(terminals) != 0 {
		t.Fatalf("a live receiver's caller was closed: %+v", terminals)
	}

	if err := removeThroughSysOp(h, ctx, receiver); err != nil {
		t.Fatalf("remove the receiver: %v", err)
	}
	h.reconcileSweep(ctx)
	if terminals := closureTerminalsFor(t, h, request); len(terminals) != 1 {
		t.Fatalf("the first sweep after removal produced %d terminals, want 1", len(terminals))
	}
	h.reconcileSweep(ctx)
	h.reconcileSweep(ctx)
	if terminals := closureTerminalsFor(t, h, request); len(terminals) != 1 {
		t.Fatalf("re-scanning produced %d terminals, want exactly 1", len(terminals))
	}
}

// T21, fault half. A per-request write fault must be RECORDED, not swallowed:
// the request stays open (so the level scan retries it) and the fault reaches
// the log through Home's own onFault. The loop is joined first so the only
// sweeps in this test are the ones it asks for — otherwise the broken pen would
// be swapped in underneath a running sweep.
func TestReconcileClosureRecordsAWriteFaultAndClosesOnTheNextSweep(t *testing.T) {
	probe := newLifecycleLogProbe("", nil)
	h, err := Open(Config{
		ChannelID:            "closure-fault",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  routingResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Logger:               slog.New(probe),
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{
			routingDeclaration(closureCallerDecl, "routing-live"),
			routingDeclaration(closureReceiverDecl, "routing-live"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()
	caller := routingAgent(t, h, closureCallerDecl)
	receiver := routingAgent(t, h, closureReceiverDecl)
	request := closureCall(t, h, caller, receiver)

	closureStopReconcileLoop(t, h)
	if err := removeThroughSysOp(h, ctx, receiver); err != nil {
		t.Fatalf("remove the receiver: %v", err)
	}

	honest := h.systemPen
	broken := errors.New("closure pen is down")
	h.systemPen = penFunc(func(context.Context, *message.Envelope) (harness.WriteResult, error) {
		return harness.WriteResult{}, broken
	})
	h.reconcileSweep(ctx)

	if got := probe.count("platform.closure.reconcile_fault"); got != 1 {
		t.Fatalf("closure faults logged = %d, want exactly 1", got)
	}
	if terminals := closureTerminalsFor(t, h, request); len(terminals) != 0 {
		t.Fatalf("a failed closure write still landed a terminal: %+v", terminals)
	}
	if !closureHasOpenRequest(t, h, receiver) {
		t.Fatal("a failed closure dropped the request out of the open set")
	}

	// The scan is level-triggered, so the retry is the next sweep, unassisted.
	h.systemPen = honest
	h.reconcileSweep(ctx)
	if terminals := closureTerminalsFor(t, h, request); len(terminals) != 1 {
		t.Fatalf("the retry sweep produced %d terminals, want exactly 1", len(terminals))
	}
	if got := probe.count("platform.closure.reconcile_fault"); got != 1 {
		t.Fatalf("the successful retry logged another fault: total = %d", got)
	}
}

func TestShutdownLeavesOpenUntilDeadlineThenReaperCloses(t *testing.T) {
	h := openClosureHome(t, "shutdown-deadline-reaper")
	closureStopReconcileLoop(t, h)
	caller := routingAgent(t, h, closureCallerDecl)
	receiver := routingAgent(t, h, closureReceiverDecl)
	term, _ := serverTerm(t, h, caller)
	basis, err := h.controller.PenBasis(caller, term)
	if err != nil {
		t.Fatal(err)
	}
	pen := h.minter.MintAuthority(basis.Run, basis.Kind)
	deadline := h.nowMs() + 10_000
	env, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{
		Type: closureRequestType, Payload: json.RawMessage(`{"unit":"shutdown"}`),
		Audience: message.Audience{receiver}, ExpiresAt: &deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := pen.Write(context.Background(), env)
	if err != nil || !result.Accepted() {
		t.Fatalf("write request: result=%+v err=%v", result, err)
	}
	if terminals := closureTerminalsFor(t, h, env.ID); len(terminals) != 0 {
		t.Fatalf("shutdown path synthesized an early terminal: %+v", terminals)
	}

	h.nowMs = func() int64 { return deadline - 1 }
	h.sweepExpired(context.Background())
	if terminals := closureTerminalsFor(t, h, env.ID); len(terminals) != 0 {
		t.Fatalf("reaper closed before deadline: %+v", terminals)
	}
	h.nowMs = func() int64 { return deadline }
	h.sweepExpired(context.Background())
	terminals := closureTerminalsFor(t, h, env.ID)
	if len(terminals) != 1 || terminals[0].Sender.ID != actor.SystemActorID {
		t.Fatalf("deadline terminals=%+v", terminals)
	}
	var payload struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(terminals[0].Payload, &payload); err != nil || payload.Status != string(message.StatusFailed) || payload.Reason != string(message.TerminalUnansweredTimeout) {
		t.Fatalf("deadline payload=%s err=%v", terminals[0].Payload, err)
	}
}
