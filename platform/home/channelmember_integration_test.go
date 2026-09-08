package home

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelmember"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	cmBodyAgentClass = "cm-body-agent"
	cmHostToolClass  = "cm-host-tool"
	cmIdleClass      = "cm-idle"
)

type channelMemberResolver struct{ hub *channelmember.Hub }

func (r channelMemberResolver) BuildClass(ch channel.ID, _ actor.ActorID, class string, raw json.RawMessage) (platform.ActorFactory, bool) {
	switch class {
	case channelmember.SeatClass, channelmember.HandleClass:
		cfg, err := channelmember.ParseConfig(raw)
		if err != nil {
			return platform.ActorFactory{}, false
		}
		if class == channelmember.SeatClass {
			return platform.ActorFactory{Proc: channelmember.SeatDef(r.hub, ch, cfg)}, true
		}
		return platform.ActorFactory{Proc: channelmember.HandleDef(r.hub, ch, cfg)}, true
	case cmBodyAgentClass:
		return platform.ActorFactory{Proc: actorbase.Def{Manifest: introspect.Manifest{
			Class: cmBodyAgentClass, Interfaces: []string{"actor", "agent"},
			Words: map[string]introspect.WordSpec{"body.roundtrip": {Description: "exercise both channel-member directions"}},
		}, New: func() (actorbase.Proc, error) { return bodyAgentProc, nil }}}, true
	case cmHostToolClass:
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) { return echoHostProc, nil }}}, true
	case cmIdleClass:
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error { <-sys.Life().Done(); return nil }, nil
		}}}, true
	}
	return platform.ActorFactory{}, false
}
func (channelMemberResolver) ResolveDeclaration(context.Context, channel.ID, string) (channelspec.DeclarationFacts, error) {
	return channelspec.DeclarationFacts{}, channelspec.ErrDeclarationNotFound
}
func (channelMemberResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return "", false, nil
}
func (channelMemberResolver) ClassPlacement(context.Context, string) (channelspec.PlacementKind, bool, error) {
	return "", false, nil
}
func (channelMemberResolver) AdmitIntroduction(context.Context, channel.ID, channelspec.DeclarationFacts) error {
	return nil
}

func bodyAgentProc(sys actorbase.Sys) error {
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		_, _ = sys.Progress(msg, message.StatusProcessing, map[string]any{"phase": "body"})
		pending, err := sys.Call(msg.Cause(), actor.ActorID(channelmember.HandleSeed), channelmember.HandleCall, map[string]any{
			"target": "host-tool", "type": "host.echo", "payload": map[string]any{"value": "from-body"},
		})
		if err != nil {
			_, _ = sys.Fail(msg, "reverse_call_failed", err.Error())
			continue
		}
		progressDone := make(chan struct{})
		go func() {
			defer close(progressDone)
			for range pending.Progress() {
				_, _ = sys.Progress(msg, message.StatusProcessing, map[string]any{"phase": "host"})
			}
		}()
		terminal, err := pending.Wait(msg.Ctx(), 3*time.Second)
		<-progressDone
		if err != nil {
			_, _ = sys.Fail(msg, "reverse_call_failed", err.Error())
			continue
		}
		var result struct {
			Status string `json:"status"`
			Value  string `json:"value"`
		}
		if json.Unmarshal(terminal.Payload, &result) != nil || result.Status != message.StatusCompleted || result.Value != "from-body" {
			_, _ = sys.Fail(msg, "reverse_call_failed", string(terminal.Payload))
			continue
		}
		_, _ = sys.Reply(msg, map[string]any{"roundtrip": "seat-handle-seat"})
	}
}

func echoHostProc(sys actorbase.Sys) error {
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		var req struct {
			Value string `json:"value"`
		}
		if actorbase.DecodeStrict(msg.Payload, &req) != nil {
			_, _ = sys.Fail(msg, "bad_payload", "value required")
			continue
		}
		_, _ = sys.Progress(msg, message.StatusProcessing, map[string]any{"phase": "host-tool"})
		_, _ = sys.Reply(msg, map[string]any{"value": req.Value})
	}
}

func cmDecl(source, seed string, kind actor.Kind, class string, config json.RawMessage) DeclareRequest {
	return DeclareRequest{SourceDeclID: source, Seed: seed, Kind: kind, Class: class, Config: &config, Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli()}
}

func TestChannelSeatAndHandleCarryBothDirectionsWithSeatAuthority(t *testing.T) {
	hub := channelmember.NewHub()
	resolver := channelMemberResolver{hub: hub}
	hostID, bodyID := channel.ID("host-channel"), channel.ID("body-channel")
	seatRaw, _ := json.Marshal(channelmember.Config{Host: hostID, Body: bodyID})
	handleRaw, _ := json.Marshal(channelmember.Config{Host: hostID, Body: bodyID, Receiver: "body-agent"})
	empty := json.RawMessage(`{}`)

	host, err := Open(completeHomeTestConfig(Config{ChannelID: hostID, DBPath: t.TempDir() + "/host.sqlite", Bootstrap: true, CompositionResolver: resolver, IntroductionResolver: resolver, ReconcileInterval: time.Hour,
		BootstrapDeclarations: []DeclareRequest{
			cmDecl("body-channel", "body", actor.KindChannel, channelmember.SeatClass, seatRaw),
			cmDecl("host-tool", "host-tool", actor.KindTool, cmHostToolClass, empty),
			cmDecl("host-trigger", "host-trigger", actor.KindAgent, cmIdleClass, empty),
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer host.closeInternal("test")
	body, err := Open(completeHomeTestConfig(Config{ChannelID: bodyID, DBPath: t.TempDir() + "/body.sqlite", Bootstrap: true, CompositionResolver: resolver, IntroductionResolver: resolver, ReconcileInterval: time.Hour,
		BootstrapDeclarations: []DeclareRequest{
			cmDecl(channelmember.HandleDeclID, channelmember.HandleSeed, actor.KindTool, channelmember.HandleClass, handleRaw),
			cmDecl("body-agent", "body-agent", actor.KindAgent, cmBodyAgentClass, empty),
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer body.closeInternal("test")

	seat := routingAgent(t, host, "body-channel")
	trigger := routingAgent(t, host, "host-trigger")
	for _, pair := range []struct {
		h  *Home
		id actor.ActorID
	}{{host, seat}, {host, routingAgent(t, host, "host-tool")}, {body, routingAgent(t, body, channelmember.HandleDeclID)}, {body, routingAgent(t, body, "body-agent")}} {
		deadline := time.Now().Add(3 * time.Second)
		for {
			if _, live := pair.h.actors.Stat(pair.id); live {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("actor %s did not become live", pair.id)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	term, _ := serverTerm(t, host, trigger)
	basis, err := host.controller.PenBasis(trigger, term)
	if err != nil {
		t.Fatal(err)
	}
	pen := host.minter.MintAuthority(basis.Run, basis.Kind)
	env, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{Type: "body.roundtrip", Payload: json.RawMessage(`{"body":{}}`), Audience: message.Audience{seat}, Cause: message.Root()})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := pen.Write(context.Background(), env); err != nil || !out.Accepted() {
		t.Fatalf("write=%+v err=%v", out, err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var responses []message.Envelope
	for time.Now().Before(deadline) {
		responses = closureTerminalsFor(t, host, env.ID)
		if len(responses) >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(responses) != 3 {
		t.Fatalf("response count=%d rows=%+v", len(responses), responses)
	}
	var phases []string
	for _, response := range responses[:2] {
		var progress struct {
			Status string `json:"status"`
			Phase  string `json:"phase"`
		}
		_ = json.Unmarshal(response.Payload, &progress)
		if progress.Status != message.StatusProcessing {
			t.Fatalf("progress=%s", response.Payload)
		}
		phases = append(phases, progress.Phase)
	}
	if phases[0] != "body" || phases[1] != "host" {
		t.Fatalf("progress phases=%v", phases)
	}
	var result struct {
		Status    string `json:"status"`
		Roundtrip string `json:"roundtrip"`
	}
	if err := json.Unmarshal(responses[2].Payload, &result); err != nil || result.Status != message.StatusCompleted || result.Roundtrip != "seat-handle-seat" {
		t.Fatalf("terminal=%s err=%v", responses[2].Payload, err)
	}
	for name, h := range map[string]*Home{"host": host, "body": body} {
		rows, err := h.query.ReadAfterSeq(context.Background(), 0, 2000)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, row := range rows {
			if row.Envelope.Kind == message.KindEvent && row.Envelope.Type == channelmember.InboundEvent {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s ledger has no channel-member seam audit", name)
		}
	}

	describe, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{Type: introspect.QueryDescribe, Payload: json.RawMessage(`{"body":{}}`), Audience: message.Audience{seat}, Cause: message.Root()})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := pen.Write(context.Background(), describe); err != nil || !out.Accepted() {
		t.Fatalf("describe write=%+v err=%v", out, err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rows := closureTerminalsFor(t, host, describe.ID)
		if len(rows) == 1 {
			var projection struct {
				Status string                         `json:"status"`
				Words  map[string]introspect.WordSpec `json:"words"`
			}
			if json.Unmarshal(rows[0].Payload, &projection) != nil || projection.Status != message.StatusCompleted || projection.Words["body.roundtrip"].Description == "" {
				t.Fatalf("seat manifest=%s", rows[0].Payload)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("seat manifest did not project body receiver words")
}
