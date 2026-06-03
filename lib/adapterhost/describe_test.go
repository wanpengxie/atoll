package adapterhost

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
)

// describerModule answers actor.describe dynamically (implements behavior.Describer).
type describerModule struct{ apis []behavior.APIDescriptor }

func (describerModule) Declares() behavior.Declaration {
	return behavior.Declaration{Name: "d", ActorID: "tool1", Binding: actor.BindingEmbedded}
}
func (describerModule) Init(context.Context, *behavior.ModuleContext) error { return nil }
func (describerModule) Shutdown(context.Context) error                      { return nil }
func (describerModule) Handle(context.Context, *message.Envelope) error     { return nil }
func (m describerModule) Describe(context.Context) ([]behavior.APIDescriptor, error) {
	return m.apis, nil
}

// newDescribeActor builds an installed-ish cell with mctx wired enough for
// selfRespond, capturing the describe response payload.
func newDescribeActor(t *testing.T, mod behavior.Module) (*adapterActor, *[]*message.Envelope) {
	t.Helper()
	fc := &recChain{}
	a := &adapterActor{
		self: "tool1", module: mod, declaration: mod.Declares(),
		chain: fc, clock: time.Now,
		inflight: map[behavior.CorrelationKey]*message.Envelope{},
	}
	a.mctx = a.buildModuleContext()
	return a, &fc.written
}

// TestDescribe_DynamicSelfAnswer proves actor.describe asks the module LIVE when
// it implements Describer — the capability surface is the actor's own answer,
// not a predefined declaration field.
func TestDescribe_DynamicSelfAnswer(t *testing.T) {
	mod := describerModule{apis: []behavior.APIDescriptor{
		{Name: "xhs.publish", Desc: "publish a note", Schema: json.RawMessage(`{"type":"object"}`)},
	}}
	a, written := newDescribeActor(t, mod)

	req := &message.Envelope{ID: "q1", ChannelID: "ch", Kind: message.KindRequest, Type: actor.ReservedActorDescribe,
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"tool1"}}
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("describe: %v", err)
	}
	if len(*written) != 1 {
		t.Fatalf("expected 1 describe response, got %d", len(*written))
	}
	var body struct {
		Name string                   `json:"name"`
		APIs []behavior.APIDescriptor `json:"apis"`
	}
	if err := json.Unmarshal((*written)[0].Payload, &body); err != nil {
		t.Fatalf("unmarshal describe: %v", err)
	}
	if body.Name != "d" {
		t.Fatalf("name=%s, want d", body.Name)
	}
	if len(body.APIs) != 1 || body.APIs[0].Name != "xhs.publish" {
		t.Fatalf("apis=%+v, want [xhs.publish] from the live Describe answer", body.APIs)
	}
}

// TestDescribe_IdentityFallback proves a module WITHOUT Describer still answers
// describe — identity only, no predefined type list.
func TestDescribe_IdentityFallback(t *testing.T) {
	a, written := newDescribeActor(t, errModule{}) // errModule has no Describe

	req := &message.Envelope{ID: "q2", ChannelID: "ch", Kind: message.KindRequest, Type: actor.ReservedActorDescribe,
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"tool1"}}
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("describe: %v", err)
	}
	if len(*written) != 1 {
		t.Fatalf("expected 1 describe response, got %d", len(*written))
	}
	if contains(string((*written)[0].Payload), "apis") {
		t.Fatalf("identity fallback must not carry apis: %s", (*written)[0].Payload)
	}
}
