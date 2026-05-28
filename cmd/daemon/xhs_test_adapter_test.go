package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	devicexhs "github.com/wanpengxie/ActOS/adapters/device/xhs"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime"
)

type testXHSConfig struct {
	MaxPendingMs    int64
	ResponsePayload json.RawMessage
	PanicOnHandle   bool
	SkipRespond     bool
}

type testXHSModule struct {
	cfg     testXHSConfig
	maxPend int64
	counter atomic.Int64
	mctx    *adapter.ModuleContext
}

func testXHSActorSeed() actorreg.Record {
	return actorreg.Record{
		ID:          devicexhs.DefaultAdapterActorID,
		Kind:        actor.KindTool,
		Binding:     actor.BindingEmbedded,
		DisplayName: "xhs",
	}
}

func testXHSFactory(cfg testXHSConfig) AdapterModuleFactory {
	return func(_ context.Context, h runtime.ChannelHooks) (adapter.Module, error) {
		if h.ChannelType != XHSCreatorChannelType {
			return nil, nil
		}
		if cfg.MaxPendingMs <= 0 {
			cfg.MaxPendingMs = devicexhs.DefaultMaxPendingMs
		}
		return &testXHSModule{cfg: cfg, maxPend: cfg.MaxPendingMs}, nil
	}
}

func (m *testXHSModule) Declares() adapter.Declaration {
	return adapter.Declaration{
		Name:             devicexhs.AdapterName,
		ActorID:          devicexhs.DefaultAdapterActorID,
		Types:            append([]string(nil), devicexhs.AllTypes...),
		TypeDeclarations: devicexhs.DeclarationTypeDeclarations(),
		Binding:          actor.BindingEmbedded,
		MaxPendingMs:     m.maxPend,
	}
}

func (m *testXHSModule) Init(_ context.Context, mctx *adapter.ModuleContext) error {
	if mctx == nil {
		return errors.New("test xhs: ModuleContext nil")
	}
	if mctx.Respond == nil {
		return errors.New("test xhs: Respond nil")
	}
	m.mctx = mctx
	return nil
}

func (m *testXHSModule) Shutdown(context.Context) error { return nil }

func (m *testXHSModule) Handle(ctx context.Context, env *message.Envelope) error {
	if m.mctx == nil {
		return errors.New("test xhs: Handle before Init")
	}
	if env == nil {
		return errors.New("test xhs: nil envelope")
	}
	if env.Kind != message.KindRequest {
		return fmt.Errorf("test xhs: Handle kind=%s (must be request)", env.Kind)
	}
	if m.cfg.PanicOnHandle {
		panic("test xhs: forced panic")
	}
	if m.cfg.SkipRespond {
		return nil
	}
	payload := m.cfg.ResponsePayload
	if len(payload) == 0 {
		n := m.counter.Add(1)
		payload = json.RawMessage(fmt.Sprintf(
			`{"note_id":"mock-note-%d","url":"https://example.invalid/%d"}`, n, n))
	}
	_, err := m.mctx.Respond(ctx, adapter.CorrelationKey(env.ID), payload, adapter.RespondOptions{
		Status: "completed",
	})
	return err
}

func (m *testXHSModule) OnExternalCallback(context.Context, []byte) error { return nil }
