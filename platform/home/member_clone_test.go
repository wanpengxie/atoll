package home

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

type forkTestRuntime struct{}

func (*forkTestRuntime) Start(runtimeproto.StartCommand) error     { return nil }
func (*forkTestRuntime) Control(runtimeproto.ControlCommand) error { return nil }
func (*forkTestRuntime) Terminate() error                          { return nil }
func (*forkTestRuntime) EnsureReady(runtimeproto.OpID) error       { return nil }
func (*forkTestRuntime) Close()                                    {}

type forkPathResolver struct{ forkDecl string }

func (r forkPathResolver) BuildClass(_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage) (platform.ActorFactory, bool) {
	if class != "fork-capable" {
		return routingResolver{}.BuildClass("", "", class, nil)
	}
	def, err := base.Def("fork integration actor", base.Config{
		NewRuntime: func(runtimeproto.Deps, []byte, runtimeproto.TurnOptions, runtimeproto.Events) (runtimeproto.Runtime, error) {
			return &forkTestRuntime{}, nil
		},
		Runtime: runtimeproto.Spec{Capabilities: map[string]bool{runtimeproto.CapabilityFork: true}},
	})
	if err != nil {
		panic(err)
	}
	return platform.ActorFactory{Proc: def}, true
}

func (r forkPathResolver) ResolveDeclaration(ctx context.Context, ch channel.ID, source string) (channelspec.DeclarationFacts, error) {
	if source == r.forkDecl {
		return channelspec.DeclarationFacts{Name: source, Class: "fork-capable", Config: json.RawMessage(`{}`), Visibility: "public"}, nil
	}
	return routingResolver{}.ResolveDeclaration(ctx, ch, source)
}
func (forkPathResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return actor.KindAgent, true, nil
}
func (forkPathResolver) ClassPlacement(context.Context, string) (channelspec.PlacementKind, bool, error) {
	return channelspec.PlacementServer, true, nil
}
func (forkPathResolver) AdmitIntroduction(context.Context, channel.ID, channelspec.DeclarationFacts) error {
	return nil
}

func TestMemberCloneCreatesAnotherIdentityAndNarratesForkOrigin(t *testing.T) {
	const decl = "clone-source"
	h := openRoutingHome(t, "clone-channel", routingDeclaration(decl, "routing-live"))
	parent := routingAgent(t, h, decl)
	value, err := h.opEntry.Execute(context.Background(), sysactor.TypeMemberCreate, sysactor.OperateRequest{
		ChannelID: h.channelID,
		Caller:    harness.Caller{Channel: h.channelID, Actor: parent},
		Anchor:    "clone-request",
		Cause:     message.Root(),
		Payload:   json.RawMessage(`{"decl_id":"clone-source"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	child := value.(map[string]any)["member"].(actor.ActorID)
	if child == parent {
		t.Fatalf("clone reused parent %q", parent)
	}
	members, err := rosterMembersForSource(context.Background(), h.View(), decl)
	if err != nil || len(members) != 2 {
		t.Fatalf("members=%v err=%v", members, err)
	}
	rows, err := h.query.ReadAfterSeq(context.Background(), 0, 128)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if row.Envelope.Type != message.TypeSystemMemberCreated {
			continue
		}
		var payload struct {
			Member string `json:"member"`
			By     struct {
				ForkOf string `json:"fork_of"`
			} `json:"by"`
		}
		_ = json.Unmarshal(row.Envelope.Payload, &payload)
		if payload.Member == string(child) && payload.By.ForkOf == string(parent) {
			found = true
		}
	}
	if !found {
		t.Fatalf("member.created fork narration missing for child=%v parent=%s", child, parent)
	}
}

func TestAgentForkTraversesAgentDoorAndStoreIntoRoster(t *testing.T) {
	const (
		forkDecl   = "fork-path-parent"
		callerDecl = "fork-path-caller"
	)
	resolver := forkPathResolver{forkDecl: forkDecl}
	h, err := Open(completeHomeTestConfig(Config{
		ChannelID: "fork-path-channel", DBPath: t.TempDir() + "/channel.sqlite", Bootstrap: true,
		CompositionResolver: resolver, IntroductionResolver: resolver, ReconcileInterval: time.Hour,
		BootstrapDeclarations: []DeclareRequest{
			routingDeclaration(forkDecl, "fork-capable"),
			routingDeclaration(callerDecl, "routing-live"),
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	parent := routingAgent(t, h, forkDecl)
	caller := routingAgent(t, h, callerDecl)
	term, _ := serverTerm(t, h, caller)
	basis, err := h.controller.PenBasis(caller, term)
	if err != nil {
		t.Fatal(err)
	}
	pen := h.minter.MintAuthority(basis.Run, basis.Kind)
	req, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{
		Type: base.TypeFork, Payload: json.RawMessage(`{"body":{}}`), Audience: message.Audience{parent},
		Cause: message.Root(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := pen.Write(context.Background(), req); err != nil || !out.Accepted() {
		t.Fatalf("fork request write=%+v err=%v", out, err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		members, rosterErr := rosterMembersForSource(context.Background(), h.View(), forkDecl)
		if rosterErr == nil && len(members) == 2 {
			rows, readErr := h.query.ReadAfterSeq(context.Background(), 0, 256)
			if readErr != nil {
				t.Fatal(readErr)
			}
			for _, row := range rows {
				if row.Envelope.Type != message.TypeSystemMemberCreated {
					continue
				}
				var event struct {
					Member actor.ActorID `json:"member"`
					By     struct {
						ForkOf actor.ActorID `json:"fork_of"`
					} `json:"by"`
				}
				if json.Unmarshal(row.Envelope.Payload, &event) == nil && event.By.ForkOf == parent && event.Member != parent {
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	members, _ := rosterMembersForSource(context.Background(), h.View(), forkDecl)
	t.Fatalf("agent.fork did not create and narrate a second roster row: parent=%s members=%v", parent, members)
}
