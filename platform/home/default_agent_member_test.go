package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// The member verdict for set_default_agent is door policy asked of the value
// ledger — the ONE membership authority. An entry-table member (a fork child,
// which has no actor_registry row) must therefore qualify, and an absent id
// must be refused before the authoritative setting event is appended.
func TestSetDefaultAgentAsksTheValueLedger(t *testing.T) {
	h, err := Open(Config{
		ChannelID:            "default-agent-member",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  routingResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: "decl:parent", Class: "routing-live",
			Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent,
			CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	declared, err := h.View().DeclaredInstances(ctx, "decl:parent")
	if err != nil || len(declared) != 1 {
		t.Fatalf("bootstrap parent missing: %v err=%v", declared, err)
	}
	parent := declared[0]

	// A fork child lives in the entry table alone — no durable row.
	desired, err := h.controller.DesiredFor("server", "server")
	if err != nil {
		t.Fatal(err)
	}
	var attempt actorhost.AttemptKey
	for _, d := range desired {
		if d.Actor() == parent {
			attempt = d.Attempt()
		}
	}
	if attempt == "" {
		t.Fatal("parent has no server desired")
	}
	fork, err := h.actors.Fork(ctx, actorctl.ForkRequest{
		CallerActorID: parent, CallerAttempt: attempt,
		RequestID: message.ID("req-child"),
		Spec:      actorcaps.ForkSpec{Kind: actor.KindAgent, Class: "worker"},
	})
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	child := fork.ChildActorID

	setDefault := func(target actor.ActorID) error {
		payload, _ := json.Marshal(map[string]string{"instance_id": string(target)})
		_, err := h.opEntry.Execute(ctx, sysactor.TypeSetDefaultAgent, sysactor.OperateRequest{
			ChannelID: h.channelID, Sender: parent,
			Anchor: "op-" + string(target), Payload: payload,
		})
		return err
	}

	// The entry-table member qualifies.
	if err := setDefault(child); err != nil {
		t.Fatalf("fork child refused as default agent: %v", err)
	}
	id, configured, err := h.View().DefaultAgent(ctx)
	if err != nil || !configured || id != child {
		t.Fatalf("pointer=%q configured=%v err=%v, want %q", id, configured, err, child)
	}

	// An absent id is refused at the door, not by the store.
	err = setDefault("agent:absent/nobody-1")
	var opErr *sysactor.OperateError
	if !errors.As(err, &opErr) || opErr.Code != "member_inactive" {
		t.Fatalf("absent target: err=%v, want member_inactive door refusal", err)
	}
}

func TestSetDefaultAgentStrictPayloadAndExplicitClear(t *testing.T) {
	h, err := Open(Config{
		ChannelID:           "default-agent-payload",
		DBPath:              filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: routingResolver{}, IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval: time.Hour, Bootstrap: true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: "decl:target", Class: "routing-live",
			Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent,
			CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	target := routingAgent(t, h, "decl:target")

	for _, payload := range []string{
		`{}`,
		`{"instance_id":null}`,
		`{"instance_id":1}`,
		`{"instance_id":true}`,
		`{"instance_id":[]}`,
		`{"instance_id":{}}`,
		`{"instance_id":"x","source_decl_id":"decl:target"}`,
		`{"source_decl_id":null}`,
		`{"source_decl_id":""}`,
	} {
		_, err := setDefault(t, h, target, payload)
		var opErr *sysactor.OperateError
		if !errors.As(err, &opErr) || opErr.Code != "bad_payload" {
			t.Fatalf("payload %s: err=%v, want bad_payload", payload, err)
		}
	}
	if id, found, err := h.View().DefaultAgent(context.Background()); err != nil || found || id != "" {
		t.Fatalf("bad payload changed fold: (%q,%v,%v)", id, found, err)
	}

	if _, err := setDefault(t, h, target, `{"instance_id":"`+string(target)+`"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := setDefault(t, h, target, `{"instance_id":""}`); err != nil {
		t.Fatalf("explicit clear: %v", err)
	}
	if id, found, err := h.View().DefaultAgent(context.Background()); err != nil || found || id != "" {
		t.Fatalf("clear fold=(%q,%v,%v)", id, found, err)
	}
}

func TestResolveDefaultSourceFourBranches(t *testing.T) {
	boom := errors.New("query failed")
	tests := []struct {
		name string
		ids  []actor.ActorID
		err  error
		code string
		want actor.ActorID
	}{
		{"error", nil, boom, "unavailable", ""},
		{"none", nil, nil, "member_inactive", ""},
		{"single", []actor.ActorID{"a1"}, nil, "", "a1"},
		{"multiple", []actor.ActorID{"a1", "a2"}, nil, "unavailable", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			broken := 0
			got, opErr := resolveDefaultSource(context.Background(), "decl", func(context.Context, string) ([]actor.ActorID, error) {
				return tt.ids, tt.err
			}, func(count int) { broken = count })
			if got != tt.want {
				t.Fatalf("target=%q want=%q", got, tt.want)
			}
			if tt.code == "" {
				if opErr != nil {
					t.Fatalf("unexpected err=%v", opErr)
				}
			} else if opErr == nil || opErr.Code != tt.code {
				t.Fatalf("err=%v want code=%q", opErr, tt.code)
			}
			if tt.name == "multiple" && broken != 2 {
				t.Fatalf("cardinality alarm=%d want 2", broken)
			}
		})
	}
}
