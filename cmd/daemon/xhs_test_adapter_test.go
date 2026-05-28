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
	// Provisionals is an ordered list of provisional emits the module
	// performs before the final Respond. Each entry pairs a status
	// (Layer 2 core, Layer 3 namespace, or invalid for negative tests)
	// with an optional payload to merge with the status field. Empty
	// slice = no provisional emits (legacy behaviour).
	Provisionals []testXHSProvisional
	// FinalStatus overrides the default "completed" status on the final
	// Respond. Use "failed" + FailedReason for terminal failure tests.
	FinalStatus  string
	FailedReason string
	// EmitTrailingProvisional, when true, causes Handle to emit one
	// extra provisional AFTER the final Respond. This is the
	// "provisional-after-final" zombie path — Step 8 must reject.
	EmitTrailingProvisional bool
	// EmitDuplicateFinal, when true, causes Handle to emit a second
	// final Respond after the first one. Step 8 + the
	// ux_terminal_response_per_request UNIQUE INDEX must reject.
	EmitDuplicateFinal bool
}

// testXHSProvisional describes one provisional response the test
// module should emit before completing.
type testXHSProvisional struct {
	Status  string
	Payload json.RawMessage
}

type testXHSModule struct {
	cfg     testXHSConfig
	maxPend int64
	counter atomic.Int64
	mctx    *adapter.ModuleContext

	// Per-Handle observation seams — read after pollResponse returns to
	// assert the framework rejected our deliberately-illegal emits.
	lastProvisionalErr error
	lastTrailingErr    error
	lastDuplicateErr   error
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

// testXHSFactoryCapture is like testXHSFactory but writes the
// created module into *out on first instantiation so the caller can
// inspect lastProvisionalErr etc after Handle runs.
func testXHSFactoryCapture(cfg testXHSConfig, out **testXHSModule) AdapterModuleFactory {
	return func(_ context.Context, h runtime.ChannelHooks) (adapter.Module, error) {
		if h.ChannelType != XHSCreatorChannelType {
			return nil, nil
		}
		if cfg.MaxPendingMs <= 0 {
			cfg.MaxPendingMs = devicexhs.DefaultMaxPendingMs
		}
		mod := &testXHSModule{cfg: cfg, maxPend: cfg.MaxPendingMs}
		if out != nil {
			*out = mod
		}
		return mod, nil
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

	// Multitype refactor: emit configured provisional responses before
	// the final terminal. Per buildProvisional contract the pending
	// correlation + F3 timer must remain in place across these emits.
	for _, prov := range m.cfg.Provisionals {
		payload := prov.Payload
		if len(payload) == 0 {
			payload = json.RawMessage(`{}`)
		}
		_, perr := m.mctx.Provisional(ctx, adapter.CorrelationKey(env.ID), prov.Status, payload, adapter.ProvisionalOptions{})
		if perr != nil {
			// Record but don't fail Handle — the test asserts on the
			// observable channel log + Provisional return error
			// (currently we let it surface via the manager metrics +
			// a no-op so we still emit the final response, mirroring
			// adapters/device/xhs production behaviour).
			m.lastProvisionalErr = perr
		}
	}

	payload := m.cfg.ResponsePayload
	if len(payload) == 0 {
		n := m.counter.Add(1)
		payload = json.RawMessage(fmt.Sprintf(
			`{"note_id":"mock-note-%d","url":"https://example.invalid/%d"}`, n, n))
	}
	finalStatus := m.cfg.FinalStatus
	if finalStatus == "" {
		finalStatus = "completed"
	}
	respondOpts := adapter.RespondOptions{Status: finalStatus}
	if finalStatus == "failed" && m.cfg.FailedReason != "" {
		respondOpts.Reason = m.cfg.FailedReason
	}
	if _, err := m.mctx.Respond(ctx, adapter.CorrelationKey(env.ID), payload, respondOpts); err != nil {
		return err
	}

	if m.cfg.EmitTrailingProvisional {
		_, err := m.mctx.Provisional(ctx, adapter.CorrelationKey(env.ID), "processing", json.RawMessage(`{}`), adapter.ProvisionalOptions{})
		m.lastTrailingErr = err
	}
	if m.cfg.EmitDuplicateFinal {
		_, err := m.mctx.Respond(ctx, adapter.CorrelationKey(env.ID),
			json.RawMessage(`{"note_id":"dup"}`),
			adapter.RespondOptions{Status: "completed"})
		m.lastDuplicateErr = err
	}
	return nil
}

func (m *testXHSModule) OnExternalCallback(context.Context, []byte) error { return nil }
