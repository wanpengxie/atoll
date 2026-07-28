package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/humancell"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type routingResolver struct{}

func (routingResolver) BuildClass(_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage) (platform.ActorFactory, bool) {
	if class != "routing-live" {
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error {
			<-sys.Life().Done()
			return nil
		}, nil
	}}}, true
}

func routingDeclaration(source, class string) DeclareRequest {
	return DeclareRequest{
		SourceDeclID: source, Kind: actor.KindAgent, Class: class,
		Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
	}
}

func openRoutingHomeAt(t *testing.T, name, dbPath string, bootstrap bool, declarations ...DeclareRequest) *Home {
	t.Helper()
	h, err := Open(Config{
		ChannelID: channel.ID(name), DBPath: dbPath,
		CompositionResolver: routingResolver{}, ReconcileInterval: time.Hour,
		IntroductionResolver: inertIntroductionResolver{},
		Bootstrap:            bootstrap, MustExistDB: !bootstrap,
		BootstrapDeclarations: declarations,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func routingAgent(t *testing.T, h *Home, source string) actor.ActorID {
	t.Helper()
	ids, err := h.View().DeclaredInstances(context.Background(), source)
	if err != nil || len(ids) != 1 {
		t.Fatalf("declaration %q: instances=%v err=%v", source, ids, err)
	}
	return ids[0]
}

func setDefault(t *testing.T, h *Home, sender actor.ActorID, payload string) (map[string]any, error) {
	t.Helper()
	value, err := h.opEntry.Execute(context.Background(), sysactor.TypeSetDefaultAgent, sysactor.OperateRequest{
		ChannelID: h.channelID, Sender: sender, Anchor: uuid.NewString(),
		Payload: json.RawMessage(payload),
	})
	if err != nil {
		return nil, err
	}
	return value.(map[string]any), nil
}

func appendAuthoritativeRoutingRow(t *testing.T, h *Home, payload string) {
	t.Helper()
	result, err := h.systemPen.Write(context.Background(), &message.Envelope{
		ID: message.ID(uuid.NewString()), TS: time.Now().UnixMilli(),
		Kind: message.KindEvent, Type: defaultAgentSetEventType,
		Payload: json.RawMessage(payload), Visibility: message.VisibilitySystem,
		Audience: message.Audience{actor.SystemActorID},
	})
	if err != nil || !result.Accepted() {
		t.Fatalf("append authoritative row: result=%+v err=%v", result, err)
	}
}

func TestDefaultAgentFold_SetClearAndReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	h := openRoutingHomeAt(t, "routing-fold", dbPath, true,
		routingDeclaration("decl:one", "routing-live"),
		routingDeclaration("decl:two", "routing-live"),
	)
	ctx := context.Background()
	one := routingAgent(t, h, "decl:one")
	two := routingAgent(t, h, "decl:two")

	if id, found, err := h.View().DefaultAgent(ctx); err != nil || found || id != "" {
		t.Fatalf("fresh fold=(%q,%v,%v), want Unset", id, found, err)
	}
	if _, err := setDefault(t, h, one, `{"instance_id":"`+string(one)+`"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := setDefault(t, h, one, `{"source_decl_id":"decl:two"}`); err != nil {
		t.Fatal(err)
	}
	if id, found, err := h.View().DefaultAgent(ctx); err != nil || !found || id != two {
		t.Fatalf("latest fold=(%q,%v,%v), want %q", id, found, err, two)
	}
	row, found, err := h.query.LatestBySenderAndType(ctx, actor.SystemActorID, defaultAgentSetEventType)
	if err != nil || !found {
		t.Fatalf("latest setting event missing: found=%v err=%v", found, err)
	}
	if row.Envelope.Sender.ID != actor.SystemActorID ||
		row.Envelope.Kind != message.KindEvent ||
		row.Envelope.Visibility != message.VisibilitySystem ||
		len(row.Envelope.Audience) != 1 ||
		row.Envelope.Audience[0] != actor.SystemActorID {
		t.Fatalf("setting envelope contract drifted: %+v", row.Envelope)
	}
	var event map[string]any
	if err := json.Unmarshal(row.Envelope.Payload, &event); err != nil ||
		event["default_agent"] != string(two) || event["set_by"] != string(one) {
		t.Fatalf("setting event=%s err=%v", row.Envelope.Payload, err)
	}
	if err := h.closeInternal("test-reopen"); err != nil {
		t.Fatal(err)
	}

	h = openRoutingHomeAt(t, "routing-fold", dbPath, false)
	if id, found, err := h.View().DefaultAgent(ctx); err != nil || !found || id != two {
		t.Fatalf("reopened fold=(%q,%v,%v), want %q", id, found, err, two)
	}
	if _, err := setDefault(t, h, one, `{"instance_id":""}`); err != nil {
		t.Fatal(err)
	}
	if id, found, err := h.View().DefaultAgent(ctx); err != nil || found || id != "" {
		t.Fatalf("cleared fold=(%q,%v,%v), want Unset", id, found, err)
	}
	if err := h.closeInternal("test"); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultAgentFoldConcurrentWritesMatchLatestLedgerRow(t *testing.T) {
	h := openRoutingHomeAt(t, "routing-linear", filepath.Join(t.TempDir(), "channel.sqlite"), true,
		routingDeclaration("decl:one", "routing-live"),
		routingDeclaration("decl:two", "routing-live"),
	)
	defer func() { _ = h.closeInternal("test") }()
	one := routingAgent(t, h, "decl:one")
	two := routingAgent(t, h, "decl:two")

	before, err := h.query.MaxSeq(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, target := range []actor.ActorID{one, two, one, two, two} {
		wg.Add(1)
		go func(target actor.ActorID) {
			defer wg.Done()
			if _, err := setDefault(t, h, one, `{"instance_id":"`+string(target)+`"}`); err != nil {
				t.Errorf("set %s: %v", target, err)
			}
		}(target)
	}
	wg.Wait()
	after, err := h.query.MaxSeq(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after-before != 5 {
		t.Fatalf("same-value/LWW writes appended %d rows, want 5", after-before)
	}
	row, found, err := h.query.LatestBySenderAndType(
		context.Background(), actor.SystemActorID, defaultAgentSetEventType)
	if err != nil || !found {
		t.Fatalf("latest found=%v err=%v", found, err)
	}
	eventTarget, err := decodeDefaultAgentEvent(row.Envelope.Payload)
	if err != nil {
		t.Fatal(err)
	}
	viewTarget, configured, err := h.View().DefaultAgent(context.Background())
	if err != nil || !configured || viewTarget != eventTarget {
		t.Fatalf("view=(%s,%v,%v) latest=%s", viewTarget, configured, err, eventTarget)
	}
}

func TestDefaultAgentFoldStoreReadFailureIsUnavailable(t *testing.T) {
	h := openRoutingHomeAt(t, "routing-read-failure", filepath.Join(t.TempDir(), "channel.sqlite"), true)
	query := h.query
	if err := h.closeInternal("test"); err != nil {
		t.Fatal(err)
	}
	fold := openDefaultAgentFold(context.Background(), h, query, h.logger)
	if got := fold.snapshot(); got.State != humancell.RoutingUnavailable {
		t.Fatalf("closed-store fold=%+v, want unavailable", got)
	}
}

func TestDefaultAgentFold_InvalidLatestFailsClosedAndForgedSenderIsIgnored(t *testing.T) {
	t.Run("authoritative invalid latest", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
		h := openRoutingHomeAt(t, "routing-invalid", dbPath, true,
			routingDeclaration("decl:one", "routing-live"))
		one := routingAgent(t, h, "decl:one")
		if _, err := setDefault(t, h, one, `{"instance_id":"`+string(one)+`"}`); err != nil {
			t.Fatal(err)
		}
		appendAuthoritativeRoutingRow(t, h, `{"v":1,"set_by":"x"}`)
		if err := h.closeInternal("test-reopen"); err != nil {
			t.Fatal(err)
		}
		h = openRoutingHomeAt(t, "routing-invalid", dbPath, false)
		if id, found, err := h.View().DefaultAgent(context.Background()); id != "" || found ||
			!errors.Is(err, channel.ErrDefaultAgentUnavailable) {
			t.Fatalf("invalid latest folded to (%q,%v,%v)", id, found, err)
		}
		if got := h.defaultAgent.snapshot(); got.State != humancell.RoutingUnavailable {
			t.Fatalf("state=%v want unavailable", got.State)
		}
		_ = h.closeInternal("test")
	})

	t.Run("forged sender", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
		h := openRoutingHomeAt(t, "routing-forged", dbPath, true,
			routingDeclaration("decl:one", "routing-live"))
		one := routingAgent(t, h, "decl:one")
		if _, err := setDefault(t, h, one, `{"instance_id":"`+string(one)+`"}`); err != nil {
			t.Fatal(err)
		}
		env := &message.Envelope{
			ID: message.ID(uuid.NewString()), TS: time.Now().UnixMilli(),
			Kind: message.KindEvent, Type: defaultAgentSetEventType,
			Payload:    json.RawMessage(`{"v":1,"default_agent":"","set_by":"forger"}`),
			Visibility: message.VisibilitySystem,
			Audience:   message.Audience{actor.SystemActorID},
		}
		result, err := h.admittedWriter.WriteAdmitted(context.Background(), storespec.IdentityAdmission{
			ID: one, Kind: actor.KindAgent,
		}, env)
		if err != nil || !result.Accepted() {
			t.Fatalf("forged append result=%+v err=%v", result, err)
		}
		_ = h.closeInternal("test-reopen")
		h = openRoutingHomeAt(t, "routing-forged", dbPath, false)
		if id, found, err := h.View().DefaultAgent(context.Background()); err != nil || !found || id != one {
			t.Fatalf("forged sender changed fold: (%q,%v,%v)", id, found, err)
		}
		_ = h.closeInternal("test")
	})
}

func TestDecodeDefaultAgentEventStrictMatrix(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    actor.ActorID
		ok      bool
	}{
		{"configured", `{"v":1,"default_agent":"a1","set_by":"h1"}`, "a1", true},
		{"clear", `{"v":1,"default_agent":"","set_by":"h1"}`, "", true},
		{"unknown key", `{"v":1,"default_agent":"a1","set_by":"h1","x":true}`, "a1", true},
		{"numeric one", `{"v":1.0,"default_agent":"a1","set_by":"h1"}`, "a1", true},
		{"missing default", `{"v":1,"set_by":"h1"}`, "", false},
		{"null default", `{"v":1,"default_agent":null,"set_by":"h1"}`, "", false},
		{"string version", `{"v":"1","default_agent":"a1","set_by":"h1"}`, "", false},
		{"near one", `{"v":1.0000000000000000001,"default_agent":"a1","set_by":"h1"}`, "", false},
		{"future version", `{"v":2,"default_agent":"a1","set_by":"h1"}`, "", false},
		{"array set by", `{"v":1,"default_agent":"a1","set_by":[]}`, "", false},
		{"empty set by", `{"v":1,"default_agent":"a1","set_by":""}`, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeDefaultAgentEvent(json.RawMessage(tt.payload))
			if (err == nil) != tt.ok || got != tt.want {
				t.Fatalf("decode=%q err=%v, want=%q ok=%v", got, err, tt.want, tt.ok)
			}
		})
	}
}

func TestHarnessNoLongerResolvesEmptyAudience(t *testing.T) {
	h := openRoutingHomeAt(t, "routing-harness", filepath.Join(t.TempDir(), "channel.sqlite"), true,
		routingDeclaration("decl:one", "routing-live"))
	one := routingAgent(t, h, "decl:one")
	if _, err := setDefault(t, h, one, `{"instance_id":"`+string(one)+`"}`); err != nil {
		t.Fatal(err)
	}
	env := &message.Envelope{
		ID: "empty-audience", TS: time.Now().UnixMilli(), Kind: message.KindEvent,
		Type: "routing.probe", Payload: json.RawMessage(`{}`),
	}
	result, err := h.admittedWriter.WriteAdmitted(context.Background(), storespec.IdentityAdmission{
		ID: one, Kind: actor.KindAgent,
	}, env)
	if err != nil || result.RejectReason != harness.HarnessAudienceEmpty ||
		len(env.Audience) != 0 || env.Kind != message.KindEvent {
		t.Fatalf("result=%+v env=%+v err=%v", result, env, err)
	}
	_ = h.closeInternal("test")
}
