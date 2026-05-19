package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/adapters/framework"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/store"
)

type durabilityModule struct {
	mctx *adapter.ModuleContext
}

func (m *durabilityModule) Declares() adapter.Declaration {
	return adapter.Declaration{
		Name:         "durable",
		ActorID:      actor.ActorID("tool:durable"),
		Types:        []string{"durable.request"},
		Binding:      actor.BindingInProcess,
		MaxPendingMs: 10_000,
	}
}

func (m *durabilityModule) Init(_ context.Context, mctx *adapter.ModuleContext) error {
	m.mctx = mctx
	return nil
}

func (m *durabilityModule) Handle(context.Context, *message.Envelope) error  { return nil }
func (m *durabilityModule) OnExternalCallback(context.Context, []byte) error { return nil }
func (m *durabilityModule) Shutdown(context.Context) error                   { return nil }

type durabilityChain struct {
	mu      sync.Mutex
	written []*message.Envelope
}

func (c *durabilityChain) Write(_ context.Context, env *message.Envelope) (khar.WriteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *env
	cp.Payload = append(json.RawMessage(nil), env.Payload...)
	c.written = append(c.written, &cp)
	return khar.WriteResult{MessageID: cp.ID, Seq: int64(len(c.written))}, nil
}

func (c *durabilityChain) Written() []*message.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*message.Envelope, len(c.written))
	copy(out, c.written)
	return out
}

func TestAdapterFrameworkBootRecoverTimersUsesSQLiteStateStore(t *testing.T) {
	ctx := context.Background()
	var now int64 = 1_700_000_000_000
	nowFn := func() int64 { return now }
	clock := func() time.Time { return time.UnixMilli(nowFn()) }

	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "channel.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer db.Close()

	reg := store.NewActorRegistry(db)
	if err := reg.Insert(ctx, actorreg.Record{
		ID:      "tool:durable",
		Kind:    actor.KindTool,
		Binding: actor.BindingInProcess,
	}); err != nil {
		t.Fatalf("seed actor: %v", err)
	}
	types := store.NewTypeRegistry(db, nowFn)
	state := store.NewAdapterStateStore(db, nowFn)
	creds := store.NewAdapterCredentialStore(db, nowFn)
	chain := &durabilityChain{}
	req := &message.Envelope{
		ID:            "req-durable",
		TS:            now,
		TSReceived:    now,
		ChannelID:     channel.ID("ch-durable"),
		Sender:        message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Kind:          message.KindRequest,
		Type:          "durable.request",
		Payload:       json.RawMessage(`{"work":true}`),
		CorrelationID: "req-durable",
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{"tool:durable"},
	}
	lookup := framework.NewMemoryRequestLookup(map[string]*message.Envelope{
		req.ID.String(): req,
	})

	cfg := framework.ManagerConfig{
		ChannelID:       req.ChannelID,
		ActorRegistry:   reg,
		TypeRegistry:    types,
		HarnessChain:    chain,
		RequestLookup:   lookup,
		StateStore:      state,
		CredentialStore: creds,
		Clock:           clock,
	}
	mgr1, err := framework.NewManager(cfg)
	if err != nil {
		t.Fatalf("NewManager #1: %v", err)
	}
	if err := mgr1.Install(ctx, []adapter.Module{&durabilityModule{}}); err != nil {
		t.Fatalf("Install #1: %v", err)
	}
	if err := mgr1.Dispatch(ctx, req); err != nil {
		t.Fatalf("Dispatch #1: %v", err)
	}
	if err := mgr1.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown #1: %v", err)
	}

	now += 20_000
	mgr2, err := framework.NewManager(cfg)
	if err != nil {
		t.Fatalf("NewManager #2: %v", err)
	}
	defer func() { _ = mgr2.Shutdown(context.Background()) }()
	if err := mgr2.Install(ctx, []adapter.Module{&durabilityModule{}}); err != nil {
		t.Fatalf("Install #2: %v", err)
	}
	if err := mgr2.BootRecoverTimers(ctx); err != nil {
		t.Fatalf("BootRecoverTimers: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		written := chain.Written()
		if len(written) > 0 {
			got := written[0]
			if got.Kind != message.KindResponse || got.ParentID != req.ID {
				t.Fatalf("recovered write kind/parent=%s/%s", got.Kind, got.ParentID)
			}
			var payload map[string]any
			if err := json.Unmarshal(got.Payload, &payload); err != nil {
				t.Fatalf("decode response payload: %v", err)
			}
			if payload["reason"] != string(message.TerminalAdapterDefaultTimeout) {
				t.Fatalf("reason=%v want %s", payload["reason"], message.TerminalAdapterDefaultTimeout)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("recovered timer did not fire")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}
