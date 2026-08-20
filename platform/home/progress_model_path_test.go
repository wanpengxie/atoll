package home

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

const (
	progressCallerClass   = "progress-model-caller"
	progressReceiverClass = "progress-model-receiver"
)

type progressPathResolver struct{}

func (r progressPathResolver) BuildClass(_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage) (platform.ActorFactory, bool) {
	switch class {
	case progressCallerClass:
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error {
				for {
					msg, err := sys.Recv()
					if err != nil {
						return err
					}
					rv := metatool.ExecuteCallActor(msg.Ctx(), json.RawMessage(`{"actor_id":"progress-target","type":"progress.work","payload":{},"wait":true}`), base.ExecFace(sys, time.Second), metatool.RuntimeContext{
						Trigger: metatool.Trigger{Envelope: msg.Envelope, CorrelationID: msg.CorrelationID},
					})
					_, _ = sys.Reply(msg, rv.Value)
				}
			}, nil
		}}}, true
	case progressReceiverClass:
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error {
				for {
					msg, err := sys.Recv()
					if err != nil {
						return err
					}
					_, _ = sys.Progress(msg, message.StatusReceived, map[string]any{"step": 1})
					_, _ = sys.Progress(msg, message.StatusProcessing, map[string]any{"step": 2})
					_, _ = sys.Reply(msg, map[string]any{"value": "done"})
				}
			}, nil
		}}}, true
	default:
		return routingResolver{}.BuildClass("", "", class, nil)
	}
}

func (progressPathResolver) ResolveDeclaration(ctx context.Context, ch channel.ID, source string) (channelspec.DeclarationFacts, error) {
	switch source {
	case "progress-caller":
		return channelspec.DeclarationFacts{Name: source, Class: progressCallerClass, Config: json.RawMessage(`{}`), Visibility: "public"}, nil
	case "progress-target":
		return channelspec.DeclarationFacts{Name: source, Class: progressReceiverClass, Config: json.RawMessage(`{}`), Visibility: "public"}, nil
	}
	return routingResolver{}.ResolveDeclaration(ctx, ch, source)
}
func (progressPathResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return actor.KindAgent, true, nil
}
func (progressPathResolver) ClassPlacement(context.Context, string) (channelspec.PlacementKind, bool, error) {
	return channelspec.PlacementServer, true, nil
}
func (progressPathResolver) AdmitIntroduction(context.Context, channel.ID, channelspec.DeclarationFacts) error {
	return nil
}

func TestRealJobTableProgressReachesMetatoolInCallerLedgerOrder(t *testing.T) {
	resolver := progressPathResolver{}
	h, err := Open(completeHomeTestConfig(Config{
		ChannelID: "progress-model-channel", DBPath: t.TempDir() + "/channel.sqlite", Bootstrap: true,
		CompositionResolver: resolver, IntroductionResolver: resolver, ReconcileInterval: time.Hour,
		BootstrapDeclarations: []DeclareRequest{
			routingDeclaration("progress-caller", progressCallerClass),
			routingDeclaration("progress-target", progressReceiverClass),
			routingDeclaration("progress-trigger", "routing-live"),
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	model := routingAgent(t, h, "progress-caller")
	target := routingAgent(t, h, "progress-target")
	trigger := routingAgent(t, h, "progress-trigger")
	_, modelSpec := serverTerm(t, h, model)
	_, targetSpec := serverTerm(t, h, target)
	if modelSpec.Class != progressCallerClass || targetSpec.Class != progressReceiverClass {
		t.Fatalf("execution classes model=%q target=%q", modelSpec.Class, targetSpec.Class)
	}
	term, _ := serverTerm(t, h, trigger)
	readyBy := time.Now().Add(3 * time.Second)
	for time.Now().Before(readyBy) {
		_, modelLive := h.actors.Stat(model)
		_, targetLive := h.actors.Stat(target)
		_, triggerLive := h.actors.Stat(trigger)
		if modelLive && targetLive && triggerLive {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, live := h.actors.Stat(model); !live {
		t.Fatal("model actor did not become live")
	}
	if _, live := h.actors.Stat(target); !live {
		t.Fatal("target actor did not become live")
	}
	if _, live := h.actors.Stat(trigger); !live {
		t.Fatal("trigger actor did not become live")
	}
	basis, err := h.controller.PenBasis(trigger, term)
	if err != nil {
		t.Fatal(err)
	}
	pen := h.minter.MintAuthority(basis.Run, basis.Kind)
	request, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{
		Type: "test.observe.progress", Payload: json.RawMessage(`{"body":{}}`), Audience: message.Audience{model}, Visibility: message.VisibilityPublic,
		Cause: message.Root(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := pen.Write(context.Background(), request); err != nil || !out.Accepted() {
		t.Fatalf("trigger write=%+v err=%v", out, err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := h.query.ReadAfterSeq(context.Background(), 0, 512)
		if err != nil {
			t.Fatal(err)
		}
		var modelCall message.ID
		var ledgerSteps []int
		var ledgerStatuses []string
		var observed []any
		for _, row := range rows {
			env := row.Envelope
			if env.Kind == message.KindRequest && env.Sender.ID == model && env.Type == "progress.work" {
				modelCall = env.ID
			}
			if modelCall != "" && env.Kind == message.KindResponse && env.ParentID == modelCall {
				var payload struct {
					Status string `json:"status"`
					Step   int    `json:"step"`
				}
				_ = json.Unmarshal(env.Payload, &payload)
				ledgerStatuses = append(ledgerStatuses, payload.Status)
				if payload.Step != 0 {
					ledgerSteps = append(ledgerSteps, payload.Step)
				}
			}
			if env.Kind == message.KindResponse && env.ParentID == request.ID {
				var payload struct {
					Progress []any `json:"progress_events"`
				}
				if json.Unmarshal(env.Payload, &payload) == nil {
					observed = payload.Progress
				}
			}
		}
		if len(observed) == 2 {
			if len(ledgerStatuses) != 3 || ledgerStatuses[0] != message.StatusReceived || ledgerStatuses[1] != message.StatusProcessing || ledgerStatuses[2] != message.StatusCompleted {
				t.Fatalf("caller ledger statuses=%v", ledgerStatuses)
			}
			if len(ledgerSteps) != 2 || ledgerSteps[0] != 1 || ledgerSteps[1] != 2 {
				t.Fatalf("caller ledger steps=%v", ledgerSteps)
			}
			first := observed[0].(map[string]any)
			second := observed[1].(map[string]any)
			if first["step"] != float64(1) || second["step"] != float64(2) {
				t.Fatalf("metatool observed=%#v", observed)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("real JobTable progress did not reach metatool before the final result")
}
