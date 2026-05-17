package xhs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Config tunes a Module instance. All fields optional — sane defaults
// apply (DefaultAdapterActorID / DefaultMaxPendingMs / synchronous mock
// reply).
type Config struct {
	// AdapterActorID overrides the actor_registry id this module owns.
	// Empty defaults to DefaultAdapterActorID.
	AdapterActorID actor.ActorID

	// MaxPendingMs overrides the per-type request timeout. Zero
	// defaults to DefaultMaxPendingMs.
	MaxPendingMs int64

	// ResponsePayload, when non-nil, is the JSON payload merged into
	// the response by framework.Respond. Empty falls back to the
	// canonical mock success body
	//   `{"note_id":"mock-note-<n>","url":"https://example.invalid/<n>"}`.
	// The framework adds {status:"completed"} automatically.
	ResponsePayload json.RawMessage

	// PanicOnHandle, when true, makes Handle panic synchronously
	// instead of emitting a response — used by acceptance #4
	// integration test to verify the framework Error Policy emits a
	// failed terminal carrying the panic stack.
	PanicOnHandle bool

	// SkipRespond, when true, makes Handle a no-op (no panic, no
	// Respond). Used by acceptance #5 integration test to verify the
	// framework F3 timer emits adapter_default_timeout reason.
	SkipRespond bool
}

// Module is the T2 in_process xhs scaffold. One instance per channel
// per daemon process. Safe for concurrent invocation by the framework.
type Module struct {
	cfg     Config
	actorID actor.ActorID
	maxPend int64
	counter atomic.Int64
	mctx    *adapter.ModuleContext
}

// New constructs a Module from cfg.
func New(cfg Config) *Module {
	if cfg.AdapterActorID == "" {
		cfg.AdapterActorID = DefaultAdapterActorID
	}
	if cfg.MaxPendingMs <= 0 {
		cfg.MaxPendingMs = DefaultMaxPendingMs
	}
	return &Module{
		cfg:     cfg,
		actorID: cfg.AdapterActorID,
		maxPend: cfg.MaxPendingMs,
	}
}

// Declares satisfies kernel/adapter.Module. Static metadata —
// composition root reads it once during Manager.Install.
func (m *Module) Declares() adapter.Declaration {
	return adapter.Declaration{
		Name:         AdapterName,
		ActorID:      m.actorID,
		Types:        append([]string(nil), AllTypes...),
		TypeSchemas:  declarationTypeSchemas(),
		Binding:      Binding,
		MaxPendingMs: m.maxPend,
	}
}

// Init captures the ModuleContext so Handle can call mctx.Respond.
func (m *Module) Init(_ context.Context, mctx *adapter.ModuleContext) error {
	if mctx == nil {
		return errors.New("xhs(scaffold): Init mctx nil")
	}
	if mctx.Respond == nil {
		return errors.New("xhs(scaffold): Init mctx.Respond nil")
	}
	m.mctx = mctx
	return nil
}

// Shutdown is a no-op — the scaffold holds no resources beyond the
// in-memory counter.
func (m *Module) Shutdown(_ context.Context) error { return nil }

// Handle synchronously emits a success terminal via mctx.Respond. The
// framework dedupes the response against the per-request correlation
// state and registers MarkDone + CancelTimer on success.
func (m *Module) Handle(ctx context.Context, env *message.Envelope) error {
	if m.mctx == nil {
		return errors.New("xhs(scaffold): Handle before Init")
	}
	if env == nil {
		return errors.New("xhs(scaffold): nil envelope")
	}
	if env.Kind != message.KindRequest {
		return fmt.Errorf("xhs(scaffold): Handle kind=%s (must be request)", env.Kind)
	}

	if m.cfg.PanicOnHandle {
		// Surfaced to the framework via timerPolicy.recoverPanic — the
		// caller sees failed terminal payload.reason containing the
		// panic stack (acceptance #4).
		panic("xhs(scaffold): forced panic for acceptance test")
	}
	if m.cfg.SkipRespond {
		// Skip emitting the response — framework F3 timer eventually
		// emits adapter_default_timeout (acceptance #5).
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
	if err != nil {
		return fmt.Errorf("xhs(scaffold): Respond %s: %w", env.ID, err)
	}
	return nil
}

// OnExternalCallback is a no-op for the in_process scaffold — there is
// no external transport to call back from. T3 device adapter replaces
// this with the device_transit.recv decode path.
func (m *Module) OnExternalCallback(_ context.Context, _ []byte) error {
	return nil
}
