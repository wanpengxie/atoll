package kimiagent_test

// hosting_test pins the spec's dual-host acceptance: the SAME Bridge package
// passes the SAME behavior assertions when hosted as an in-process cell on a
// channel runtime (server shape, home harness pen) and as a daemon cell whose
// pen emits UP an uplink (daemon shape). Both host through the SAME
// actorrt.Runtime — host-dissolution made the hosting identical, so the only
// axis is the pen. Any per-host special-casing in the Bridge fails here;
// host-agnosticism is a CI assertion, not a slogan.

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/actors/kimiagent"
	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// agentHost abstracts "a place that runs the agent": deliver an envelope in,
// observe emitted envelopes out.
type agentHost interface {
	deliver(t *testing.T, env *message.Envelope)
	emitted() []message.Envelope
	stop()
}

// --- server shape: in-process cell on an actorrt runtime ---

type cellHost struct {
	w   *recordingWriter
	rt  *actorrt.Runtime
	del actorrt.Deliverer
}

func newCellHost(t *testing.T) *cellHost {
	t.Helper()
	w := &recordingWriter{}
	rt, del := actorrt.New(actorrt.Config{})
	b, err := kimiagent.NewBridge(testConfig(), testActorID, testChannelID, w)
	if err != nil {
		t.Fatalf("cell host: NewBridge: %v", err)
	}
	var bb *kimiagent.Bridge = b
	kimiagent.SetAgentFactory(b, func(kimiagent.AgentConfig) (kimiagent.Agent, error) {
		return scriptTextTurn(&bb, "parity reply"), nil
	})
	rt.Spawn(testActorID, b)
	return &cellHost{w: w, rt: rt, del: del}
}

func (h *cellHost) deliver(t *testing.T, env *message.Envelope) {
	t.Helper()
	if _, err := h.del.Deliver([]actor.ActorID{testActorID}, env); err != nil {
		t.Fatalf("cell deliver: %v", err)
	}
}

func (h *cellHost) emitted() []message.Envelope { return h.w.Written() }
func (h *cellHost) stop()                       { h.rt.StopAll() }

// --- daemon shape: actorrt cell whose pen is an uplink-style recorder ---
//
// Post host-dissolution the daemon hosts cells through the SAME actorrt.Runtime
// the server uses (RunCompute inlines it) — there is no separate host package.
// The ONLY host difference is the cell's pen: in production a daemon cell emits
// UP the link's ipc.RemoteWriter, whereas a server cell emits through the home
// harness. The parity rigging substitutes an in-memory recorder for each so the
// SAME Bridge behaviour is asserted off the mailbox/emit path without a real
// link. What is pinned is host-agnosticism (same hosting, different pen), not the
// wire transport.

type daemonHost struct {
	rt  *actorrt.Runtime
	del actorrt.Deliverer
	mu  sync.Mutex
	up  []message.Envelope
}

// daemonPen captures a daemon cell's emits in-memory (stands in for the link's
// ipc.RemoteWriter — the daemon cell emits UP, not through a home harness).
type daemonPen struct{ dh *daemonHost }

func (p *daemonPen) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	p.dh.mu.Lock()
	p.dh.up = append(p.dh.up, *env)
	p.dh.mu.Unlock()
	return harness.WriteResult{MessageID: env.ID}, nil
}

func newDaemonHost(t *testing.T) *daemonHost {
	t.Helper()
	dh := &daemonHost{}
	rt, del := actorrt.New(actorrt.Config{})
	dh.rt, dh.del = rt, del
	pen := &daemonPen{dh: dh}
	var b *kimiagent.Bridge
	bb, err := kimiagent.NewBridge(testConfig(), testActorID, testChannelID, pen)
	if err != nil {
		t.Fatalf("daemon host: NewBridge: %v", err)
	}
	b = bb
	kimiagent.SetAgentFactory(bb, func(kimiagent.AgentConfig) (kimiagent.Agent, error) {
		return scriptTextTurn(&b, "parity reply"), nil
	})
	rt.Spawn(testActorID, bb)
	return dh
}

func (h *daemonHost) deliver(t *testing.T, env *message.Envelope) {
	t.Helper()
	if _, err := h.del.Deliver([]actor.ActorID{testActorID}, env); err != nil {
		t.Fatalf("daemon deliver: %v", err)
	}
}

func (h *daemonHost) emitted() []message.Envelope {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]message.Envelope, len(h.up))
	copy(out, h.up)
	return out
}

func (h *daemonHost) stop() { h.rt.StopAll() }

// waitEmitted polls a host until n envelopes appeared.
func waitEmitted(t *testing.T, h agentHost, n int, timeout time.Duration) []message.Envelope {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := h.emitted()
		if len(got) >= n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// assertAgentBehavior is the ONE behavior suite both hosts must pass.
func assertAgentBehavior(t *testing.T, h agentHost) {
	t.Helper()
	defer h.stop()

	// 1. A request becomes a turn; the reply is a public agent.text event
	//    threaded to the trigger and addressed to the sender.
	trig := triggerEnv("parity-req")
	h.deliver(t, &trig)
	got := waitEmitted(t, h, 1, 2*time.Second)
	if len(got) < 1 {
		t.Fatal("no reply emitted")
	}
	reply := got[0]
	if reply.Type != "agent.text" || reply.Visibility != message.VisibilityPublic {
		t.Fatalf("reply shape: %+v", reply)
	}
	if reply.ParentID != "parity-req" || len(reply.Audience) != 1 || reply.Audience[0] != "user-A" {
		t.Fatalf("reply threading: parent=%s audience=%+v", reply.ParentID, reply.Audience)
	}
	var p map[string]any
	_ = json.Unmarshal(reply.Payload, &p)
	if p["text"] != "parity reply" {
		t.Fatalf("reply payload: %+v", p)
	}

	// 2. actor.describe self-answers mechanically (never an LLM turn).
	desc := message.Envelope{
		ID: "parity-desc", ChannelID: testChannelID, Kind: message.KindRequest,
		Type:   introspect.QueryDescribe,
		Sender: message.Sender{Kind: actor.KindHuman, ID: "user-A"},
	}
	h.deliver(t, &desc)
	got = waitEmitted(t, h, 2, 2*time.Second)
	if len(got) < 2 {
		t.Fatal("describe self-answer missing")
	}
	descReply := got[1]
	if descReply.Kind != message.KindResponse || descReply.ParentID != "parity-desc" {
		t.Fatalf("describe reply shape: %+v", descReply)
	}
	var d map[string]json.RawMessage
	_ = json.Unmarshal(descReply.Payload, &d)
	if _, ok := d["skill_doc"]; !ok {
		t.Fatalf("describe payload missing skill_doc: %s", descReply.Payload)
	}

	// 3. An unawaited FINAL response becomes a continuation turn (the
	//    async-result-feeds-next-reasoning path — pure mailbox routing,
	//    therefore host-coupled and parity-critical).
	finalPayload, _ := json.Marshal(map[string]any{"status": "completed", "url": "https://x"})
	asyncFinal := message.Envelope{
		ID: "parity-async-final", ChannelID: testChannelID, Kind: message.KindResponse,
		Type: "xhs.publish.response", ParentID: "parity-req-long-gone",
		Sender:  message.Sender{Kind: actor.KindTool, ID: "tool:xhs"},
		Payload: finalPayload,
	}
	h.deliver(t, &asyncFinal)
	got = waitEmitted(t, h, 3, 2*time.Second)
	if len(got) < 3 {
		t.Fatal("unawaited final did not produce a continuation turn")
	}
	cont := got[2]
	if cont.Type != "agent.text" || cont.ParentID != "parity-async-final" {
		t.Fatalf("continuation shape: %+v", cont)
	}
}

func TestHostParity_ServerCell(t *testing.T) {
	assertAgentBehavior(t, newCellHost(t))
}

func TestHostParity_DaemonHost(t *testing.T) {
	assertAgentBehavior(t, newDaemonHost(t))
}
