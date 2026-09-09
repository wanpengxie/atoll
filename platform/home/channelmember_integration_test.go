package home

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
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
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	cmBodyAgentClass = "cm-body-agent"
	cmHostToolClass  = "cm-host-tool"
	cmIdleClass      = "cm-idle"
)

type channelMemberResolver struct {
	hub   *channelmember.Hub
	homes *sync.Map
}
type testBodyMembers struct {
	homes *sync.Map
	ch    channel.ID
}

func (m testBodyMembers) MemberOfDeclaration(decl string) (actor.ActorID, error) {
	h, ok := m.homes.Load(m.ch)
	if !ok {
		return "", channelmember.ErrUnreachable
	}
	return h.(*Home).View().MemberOfDeclaration(decl)
}
func (m testBodyMembers) ActorFacts(ctx context.Context, id actor.ActorID) (channelspec.ActorFacts, bool, error) {
	h, ok := m.homes.Load(m.ch)
	if !ok {
		return channelspec.ActorFacts{}, false, channelmember.ErrUnreachable
	}
	return h.(*Home).View().ActorFacts(ctx, id)
}

func (r channelMemberResolver) BuildClass(ch channel.ID, _ actor.ActorID, class string, raw json.RawMessage) (platform.ActorFactory, bool) {
	switch class {
	case channelmember.SeatClass:
		cfg, err := channelmember.ParseSeatConfig(raw)
		if err != nil {
			return platform.ActorFactory{}, false
		}
		return platform.ActorFactory{Proc: channelmember.SeatDef(r.hub, ch, cfg)}, true
	case channelmember.HandleClass:
		cfg, err := channelmember.ParseHandleConfig(raw)
		if err != nil {
			return platform.ActorFactory{}, false
		}
		return platform.ActorFactory{Proc: channelmember.HandleDef(r.hub, ch, cfg, testBodyMembers{r.homes, ch})}, true
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
		if msg.Kind != message.KindRequest {
			continue
		}
		if msg.Type == "body.block" {
			_, _ = sys.Progress(msg, message.StatusProcessing, map[string]any{"phase": "started"})
			<-msg.Ctx().Done()
			_, _ = sys.Emit(behavior.EventSpec{Cause: message.Root(), Type: "body.cancelled", Payload: json.RawMessage(`{}`), Audience: message.Audience{sys.Self()}})
			continue
		}
		_, _ = sys.Progress(msg, message.StatusProcessing, map[string]any{"phase": "body"})
		pending, err := sys.Call(msg.Cause(), actor.ActorID(channelmember.HandleSeed), channelmember.HandleCall, map[string]any{
			"audience": []string{"host-tool"}, "type": "host.echo", "payload": map[string]any{"value": "from-body"},
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
		if json.Unmarshal(canonicalTestBody(terminal.Payload), &result) != nil || result.Status != message.StatusCompleted || result.Value != "from-body" {
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
		if msg.Kind != message.KindRequest {
			continue
		}
		if msg.Type == "host.block" {
			_, _ = sys.Progress(msg, message.StatusProcessing, map[string]any{"phase": "started"})
			<-msg.Ctx().Done()
			_, _ = sys.Emit(behavior.EventSpec{Cause: message.Root(), Type: "host.cancelled", Payload: json.RawMessage(`{}`), Audience: message.Audience{sys.Self()}})
			continue
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
	resolver := channelMemberResolver{hub: hub, homes: &sync.Map{}}
	hostID, bodyID := channel.ID("host-channel"), channel.ID("body-channel")
	seatRaw, _ := json.Marshal(channelmember.SeatConfig{Body: bodyID})
	handleRaw, _ := json.Marshal(channelmember.HandleConfig{Host: hostID, Words: map[string]channelmember.Word{
		"body.roundtrip": {Schema: json.RawMessage(`{"type":"object"}`), Target: "body-agent"},
		"body.block":     {Schema: json.RawMessage(`{"type":"object"}`), Target: "body-agent"},
	}})
	empty := json.RawMessage(`{}`)

	host, err := Open(completeHomeTestConfig(Config{ChannelID: hostID, DBPath: t.TempDir() + "/host.sqlite", Bootstrap: true, CompositionResolver: resolver, IntroductionResolver: resolver, ReconcileInterval: time.Hour,
		BootstrapDeclarations: []DeclareRequest{
			cmDecl("body-channel", string(bodyID), actor.KindChannel, channelmember.SeatClass, seatRaw),
			cmDecl("host-tool", "host-tool", actor.KindTool, cmHostToolClass, empty),
			cmDecl("host-trigger", "host-trigger", actor.KindAgent, cmIdleClass, empty),
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer host.closeInternal("test")
	resolver.homes.Store(hostID, host)
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
	resolver.homes.Store(bodyID, body)

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
	env, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{Type: "body.roundtrip", Payload: canonicalTestPayload(map[string]any{}), Audience: message.Audience{seat}, Cause: message.Root()})
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
		_ = json.Unmarshal(canonicalTestBody(response.Payload), &progress)
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
	if err := json.Unmarshal(canonicalTestBody(responses[2].Payload), &result); err != nil || result.Status != message.StatusCompleted || result.Roundtrip != "seat-handle-seat" {
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

	t.Run("event reaches body with handle identity", func(t *testing.T) {
		event, err := behavior.BuildEvent(time.Now, behavior.EventSpec{Cause: message.Root(), Type: "host.notice", Audience: message.Audience{seat}, Payload: canonicalTestPayload(map[string]any{"notice": "hello"})})
		if err != nil {
			t.Fatal(err)
		}
		if out, err := pen.Write(context.Background(), event); err != nil || !out.Accepted() {
			t.Fatalf("event=%+v %v", out, err)
		}
		handle := routingAgent(t, body, channelmember.HandleDeclID)
		until := time.Now().Add(3 * time.Second)
		for time.Now().Before(until) {
			rows, err := body.query.ReadAfterSeq(context.Background(), 0, 2000)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				if row.Envelope.Type == "host.notice" {
					if row.Envelope.Sender.ID != handle {
						t.Fatalf("sender=%s", row.Envelope.Sender.ID)
					}
					return
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("incoming event lost")
	})
	t.Run("undeclared request rejected before body", func(t *testing.T) {
		request, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{Cause: message.Root(), Type: "private.operation", Audience: message.Audience{seat}, Payload: canonicalTestPayload(map[string]any{})})
		if err != nil {
			t.Fatal(err)
		}
		if out, err := pen.Write(context.Background(), request); err != nil || !out.Accepted() {
			t.Fatalf("request=%+v %v", out, err)
		}
		until := time.Now().Add(3 * time.Second)
		for time.Now().Before(until) {
			responses := closureTerminalsFor(t, host, request.ID)
			if len(responses) > 0 {
				var failure message.Failure
				_ = json.Unmarshal(canonicalTestBody(responses[0].Payload), &failure)
				if failure.ErrorCode != "type_unsupported" {
					t.Fatalf("terminal=%s", responses[0].Payload)
				}
				rows, _ := body.query.ReadAfterSeq(context.Background(), 0, 2000)
				for _, row := range rows {
					if row.Envelope.Type == "private.operation" {
						t.Fatal("undeclared request leaked into body")
					}
				}
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("unsupported request not closed")
	})
	t.Run("body calls host system and emits with seat authority", func(t *testing.T) {
		caller := routingAgent(t, body, "body-agent")
		term, _ := serverTerm(t, body, caller)
		basis, err := body.controller.PenBasis(caller, term)
		if err != nil {
			t.Fatal(err)
		}
		writer := body.minter.MintAuthority(basis.Run, basis.Kind)
		handle := routingAgent(t, body, channelmember.HandleDeclID)
		for _, op := range []struct {
			word, payload, hostType string
			audience                actor.ActorID
		}{
			{channelmember.HandleCall, `{"body":{"type":"system.member.list","audience":["system"],"payload":{}}}`, "system.member.list", actor.SystemActorID},
			{channelmember.HandleEmit, `{"body":{"type":"body.notice","audience":["host-tool"],"payload":{"value":"hello"}}}`, "body.notice", actor.ActorID("host-tool")},
		} {
			request, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{Cause: message.Root(), Type: op.word, Payload: canonicalTestPayload(json.RawMessage(op.payload)), Audience: message.Audience{handle}})
			if err != nil {
				t.Fatal(err)
			}
			if out, err := writer.Write(context.Background(), request); err != nil || !out.Accepted() {
				t.Fatalf("write=%+v %v", out, err)
			}
			until := time.Now().Add(3 * time.Second)
			completed := false
			for time.Now().Before(until) {
				for _, response := range closureTerminalsFor(t, body, request.ID) {
					var result struct {
						Status string `json:"status"`
					}
					_ = json.Unmarshal(canonicalTestBody(response.Payload), &result)
					if result.Status == message.StatusFailed {
						t.Fatalf("reverse operation failed: %s", response.Payload)
					}
					completed = completed || result.Status == message.StatusCompleted
				}
				if completed {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !completed {
				t.Fatal("reverse operation did not complete")
			}
			rows, err := host.query.ReadAfterSeq(context.Background(), 0, 2000)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, row := range rows {
				if row.Envelope.Type == op.hostType && row.Envelope.Sender.ID == seat {
					if len(row.Envelope.Audience) != 1 || row.Envelope.Audience[0] != op.audience {
						t.Fatalf("audience=%v", row.Envelope.Audience)
					}
					found = true
				}
			}
			if !found {
				t.Fatalf("host ledger has no seat-authored %s", op.hostType)
			}
		}
	})
	t.Run("ambiguous declaration is not arbitrarily routed", func(t *testing.T) {
		duplicate, err := body.actors.Introduce(context.Background(), actorctl.IntroduceRequest{DeclID: "body-agent", Seed: "body-agent-second", Kind: actor.KindAgent, Definition: storespec.ActorDefinition{Class: cmBodyAgentClass, Config: empty}, Placement: storespec.NewServerPlacement()})
		if err != nil {
			t.Fatal(err)
		}
		defer endIdentityForFixture(t, body, duplicate.ActorID)
		request, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{Cause: message.Root(), Type: "body.roundtrip", Payload: canonicalTestPayload(map[string]any{}), Audience: message.Audience{seat}})
		if err != nil {
			t.Fatal(err)
		}
		if out, err := pen.Write(context.Background(), request); err != nil || !out.Accepted() {
			t.Fatalf("write=%+v %v", out, err)
		}
		until := time.Now().Add(3 * time.Second)
		for time.Now().Before(until) {
			responses := closureTerminalsFor(t, host, request.ID)
			if len(responses) > 0 {
				var failure message.Failure
				_ = json.Unmarshal(canonicalTestBody(responses[0].Payload), &failure)
				if failure.ErrorCode != "actor_ambiguous" {
					t.Fatalf("ambiguous result=%s", responses[0].Payload)
				}
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("ambiguous target did not close request")
	})
	for _, direction := range []struct {
		name                   string
		from, to               *Home
		caller, target         actor.ActorID
		word, payload, witness string
	}{
		{"host to body", host, body, trigger, seat, "body.block", `{"body":{}}`, "body.cancelled"},
		{"body to host", body, host, routingAgent(t, body, "body-agent"), routingAgent(t, body, channelmember.HandleDeclID), channelmember.HandleCall, `{"body":{"type":"host.block","audience":["host-tool"],"payload":{}}}`, "host.cancelled"},
	} {
		t.Run("cancel "+direction.name, func(t *testing.T) {
			term, _ := serverTerm(t, direction.from, direction.caller)
			basis, err := direction.from.controller.PenBasis(direction.caller, term)
			if err != nil {
				t.Fatal(err)
			}
			writer := direction.from.minter.MintAuthority(basis.Run, basis.Kind)
			request, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{Cause: message.Root(), Type: direction.word, Payload: canonicalTestPayload(json.RawMessage(direction.payload)), Audience: message.Audience{direction.target}})
			if err != nil {
				t.Fatal(err)
			}
			if out, err := writer.Write(context.Background(), request); err != nil || !out.Accepted() {
				t.Fatalf("write=%+v %v", out, err)
			}
			until := time.Now().Add(3 * time.Second)
			for len(closureTerminalsFor(t, direction.from, request.ID)) == 0 && time.Now().Before(until) {
				time.Sleep(5 * time.Millisecond)
			}
			if len(closureTerminalsFor(t, direction.from, request.ID)) == 0 {
				t.Fatal("callee never started")
			}
			direction.from.actors.CancelRequest(direction.target, request.ID)
			until = time.Now().Add(3 * time.Second)
			for time.Now().Before(until) {
				rows, err := direction.to.query.ReadAfterSeq(context.Background(), 0, 2000)
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range rows {
					if row.Envelope.Type == direction.witness {
						return
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Fatal("cancel did not reach opposite callee")
		})
	}
	describe, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{Type: introspect.QueryDescribe, Payload: canonicalTestPayload(map[string]any{}), Audience: message.Audience{seat}, Cause: message.Root()})
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
			if json.Unmarshal(canonicalTestBody(rows[0].Payload), &projection) != nil || projection.Status != message.StatusCompleted || len(projection.Words["body.roundtrip"].InputSchema) == 0 {
				t.Fatalf("seat manifest=%s", rows[0].Payload)
			}
			if _, err := host.actors.Remove(context.Background(), actorctl.RemoveRequest{Target: seat, InitiatorActorID: trigger, Cause: message.Root()}); err != nil {
				t.Fatal(err)
			}
			until := time.Now().Add(3 * time.Second)
			for time.Now().Before(until) {
				_, err := hub.Drive(context.Background(), channelmember.Pair{Host: hostID, Body: bodyID}, channelmember.Request{})
				if errors.Is(err, channelmember.ErrUnreachable) {
					if _, err := body.View().MemberOfDeclaration("body-agent"); err != nil {
						t.Fatalf("seat removal damaged body: %v", err)
					}
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Fatal("removed seat still grants host route")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("seat manifest did not project body receiver words")
}
