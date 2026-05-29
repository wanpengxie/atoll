package harness

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// newHumanRequest builds a kind=request envelope authored by a human
// sender with the supplied (possibly empty) audience. Mirrors
// newRequest but lets the audience be controlled directly so the
// resolve-half tests can exercise the empty-intent path.
func newHumanRequest(id string, sender actor.ActorID, audience message.Audience) *message.Envelope {
	return &message.Envelope{
		ID:        message.ID(id),
		ChannelID: "ch-1",
		TS:        testTS,
		Sender:    message.Sender{ID: sender},
		Type:      "human.text",
		Kind:      message.KindRequest,
		Payload:   json.RawMessage(`{"text":"hi"}`),
		Audience:  audience,
	}
}

// --- StepAudienceResolve unit tests (resolve-half only) ---------------

// ChannelAgentTestID is the well-known default route used in these tests.
const ChannelAgentTestID actor.ActorID = "agent:channel-agent"

// withDefaultAudience returns a Deps option wiring DefaultAudience.
func withDefaultAudience(def ...actor.ActorID) func(*Deps) {
	return func(d *Deps) {
		d.DefaultAudience = func(channel.ID) []actor.ActorID {
			return append([]actor.ActorID(nil), def...)
		}
	}
}

func runResolve(t *testing.T, deps Deps, env *message.Envelope) khar.Outcome {
	t.Helper()
	out, err := newStepAudienceResolve(deps).Run(context.Background(), env)
	if err != nil {
		t.Fatalf("StepAudienceResolve.Run error: %v", err)
	}
	return out
}

// TestStepAudienceResolve_HumanEmpty_FillsDefault — human + empty
// audience + declared default → audience becomes the default route.
func TestStepAudienceResolve_HumanEmpty_FillsDefault(t *testing.T) {
	deps := Deps{DefaultAudience: func(channel.ID) []actor.ActorID { return []actor.ActorID{ChannelAgentTestID} }}
	env := newHumanRequest("r1", "user:demo", message.Audience{})
	env.Sender.Kind = actor.KindHuman
	out := runResolve(t, deps, env)
	if !out.Continue() {
		t.Fatalf("expected continue, got reject %s", out.RejectReason)
	}
	if len(env.Audience) != 1 || env.Audience[0] != actor.ActorID(ChannelAgentTestID) {
		t.Fatalf("audience not filled with default: %v", env.Audience)
	}
}

// TestStepAudienceResolve_HumanEmpty_NoDefault_NoOp — human + empty +
// no declared default → audience stays empty (downstream rejects).
func TestStepAudienceResolve_HumanEmpty_NoDefault_NoOp(t *testing.T) {
	deps := Deps{DefaultAudience: func(channel.ID) []actor.ActorID { return nil }}
	env := newHumanRequest("r2", "user:demo", message.Audience{})
	env.Sender.Kind = actor.KindHuman
	runResolve(t, deps, env)
	if len(env.Audience) != 0 {
		t.Fatalf("expected audience to stay empty, got %v", env.Audience)
	}
}

// TestStepAudienceResolve_HumanEmpty_NilSeam_NoOp — DefaultAudience seam
// not wired at all → no-op, audience stays empty.
func TestStepAudienceResolve_HumanEmpty_NilSeam_NoOp(t *testing.T) {
	env := newHumanRequest("r3", "user:demo", message.Audience{})
	env.Sender.Kind = actor.KindHuman
	runResolve(t, Deps{}, env)
	if len(env.Audience) != 0 {
		t.Fatalf("expected audience to stay empty, got %v", env.Audience)
	}
}

// TestStepAudienceResolve_AgentEmpty_NoOp — non-human sender with empty
// audience is NOT filled (we surface its bug, not paper over it).
func TestStepAudienceResolve_AgentEmpty_NoOp(t *testing.T) {
	deps := Deps{DefaultAudience: func(channel.ID) []actor.ActorID { return []actor.ActorID{ChannelAgentTestID} }}
	env := newHumanRequest("r4", "agent:alpha", message.Audience{})
	env.Sender.Kind = actor.KindAgent
	runResolve(t, deps, env)
	if len(env.Audience) != 0 {
		t.Fatalf("agent empty audience must not be filled, got %v", env.Audience)
	}
}

// TestStepAudienceResolve_AlreadyNamed_NoOp — a non-empty audience is
// passthrough for any sender kind.
func TestStepAudienceResolve_AlreadyNamed_NoOp(t *testing.T) {
	deps := Deps{DefaultAudience: func(channel.ID) []actor.ActorID { return []actor.ActorID{"agent:override"} }}
	for _, kind := range []actor.Kind{actor.KindHuman, actor.KindAgent} {
		env := newHumanRequest("r5", "user:demo", message.Audience{"agent:explicit"})
		env.Sender.Kind = kind
		runResolve(t, deps, env)
		if len(env.Audience) != 1 || env.Audience[0] != "agent:explicit" {
			t.Fatalf("kind=%s: explicit audience must be preserved, got %v", kind, env.Audience)
		}
	}
}

// --- Chain-level tests (resolve → validate end-to-end) ----------------

// TestChain_HumanEmptyAudience_Resolved — human empty-audience request
// resolves to the channel default and lands in the log.
func TestChain_HumanEmptyAudience_Resolved(t *testing.T) {
	c, areg, log, _ := newTestChain(t, withDefaultAudience(ChannelAgentTestID))
	_ = areg.Insert(context.Background(), actorreg.Record{ID: ChannelAgentTestID, Kind: actor.KindAgent, CreatedAt: 1})

	env := newHumanRequest("req-human-empty", "user:demo", message.Audience{})
	res, err := c.Write(chainCallerCtx("user:demo"), env)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !res.Accepted() {
		t.Fatalf("expected accept, got reject %s detail=%s", res.RejectReason, res.RejectDetail)
	}
	if len(env.Audience) != 1 || env.Audience[0] != actor.ActorID(ChannelAgentTestID) {
		t.Fatalf("audience not resolved to default: %v", env.Audience)
	}
	if _, ok, _ := log.FindByID(context.Background(), "ch-1", env.ID); !ok {
		t.Fatalf("resolved envelope not appended to log")
	}
}

// TestChain_HumanEmptyAudience_NoDefault_RejectsEmpty — human empty
// audience but no declared default → still harness_audience_empty, with
// the reject coming from StepKindAndAudience (post-resolution centre).
func TestChain_HumanEmptyAudience_NoDefault_RejectsEmpty(t *testing.T) {
	c, _, _, _ := newTestChain(t) // no DefaultAudience seam
	env := newHumanRequest("req-human-nodef", "user:demo", message.Audience{})
	res, _ := c.Write(chainCallerCtx("user:demo"), env)
	if res.RejectReason != message.HarnessAudienceEmpty {
		t.Fatalf("expected harness_audience_empty, got %s detail=%s", res.RejectReason, res.RejectDetail)
	}
}

// TestChain_AgentEmptyAudience_RejectsEmpty — agent empty audience is
// not filled even when a default exists → harness_audience_empty.
func TestChain_AgentEmptyAudience_RejectsEmpty(t *testing.T) {
	c, _, _, _ := newTestChain(t, withDefaultAudience(ChannelAgentTestID))
	env := &message.Envelope{
		ID:        "req-agent-empty",
		ChannelID: "ch-1",
		TS:        testTS,
		Sender:    message.Sender{ID: "agent:alpha"},
		Type:      "agent.text",
		Kind:      message.KindEvent,
		Payload:   json.RawMessage(`{"text":"hi"}`),
		Audience:  message.Audience{},
	}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessAudienceEmpty {
		t.Fatalf("expected harness_audience_empty, got %s detail=%s", res.RejectReason, res.RejectDetail)
	}
}

// TestChain_HumanRequestMultiAudience_RejectedByDaemon — human request
// with two explicit audience entries is rejected by the daemon harness
// (StepKindAndAudience), NOT silently filled.
func TestChain_HumanRequestMultiAudience_RejectedByDaemon(t *testing.T) {
	c, areg, _, _ := newTestChain(t, withDefaultAudience(ChannelAgentTestID))
	_ = areg.Insert(context.Background(), actorreg.Record{ID: "agent:beta", Kind: actor.KindAgent, CreatedAt: 1})
	env := newHumanRequest("req-human-multi", "user:demo", message.Audience{"agent:alpha", "agent:beta"})
	res, _ := c.Write(chainCallerCtx("user:demo"), env)
	if res.RejectReason != message.HarnessRequestAudienceInvalid {
		t.Fatalf("expected harness_request_audience_invalid, got %s detail=%s", res.RejectReason, res.RejectDetail)
	}
}
