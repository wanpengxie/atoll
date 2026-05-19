package framework

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestInProcessBindingFullPath covers the in_process binding end-to-end:
//   - Install verifies actor binding == in_process.
//   - Module Init receives a mctx with DeviceTransit == nil.
//   - Handle responds synchronously; framework writes the response.
func TestInProcessBindingFullPath(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "calc",
			ActorID:      "tool:calc",
			Types:        []string{"calc.add"},
			Binding:      adapter.BindingInProcess,
			MaxPendingMs: 5_000,
		},
		handle: func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
			_, err := mctx.Respond(ctx, env.ID,
				json.RawMessage(`{"sum":42}`),
				adapter.RespondOptions{Status: "completed"})
			return err
		},
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	// In-process modules must NOT have DeviceTransit wired.
	if mod.mctx == nil {
		t.Fatalf("mctx nil")
	}
	if mod.mctx.DeviceTransit != nil {
		t.Fatalf("in_process mctx.DeviceTransit should be nil, got %T", mod.mctx.DeviceTransit)
	}

	req := newTestRequest("channel:test", "agent:a", "calc.add", "req-ip-1")
	req.Audience = []string{"tool:calc"}
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("expected 1 response, got %d", len(written))
	}
	var payload map[string]any
	_ = json.Unmarshal(written[0].Payload, &payload)
	if payload["status"] != "completed" || payload["sum"] != float64(42) {
		t.Fatalf("payload mismatch: %v", payload)
	}
}

// TestViaServerTransitBindingFullPath covers via_server_transit:
//   - Install verifies actor binding == via_server_transit + DeviceTransit injected.
//   - Module Init receives a mctx with DeviceTransit != nil.
//   - Handle calls DeviceTransit.Send (the daemon-transit seam) — not the
//     external HTTP API.
//   - Module eventually responds (via simulated OnExternalCallback path).
func TestViaServerTransitBindingFullPath(t *testing.T) {
	transit := &recordingTransit{}
	clock := newFixedClock(time.Unix(1_700_000_000, 0))
	chain := newFakeChain()
	lookup := NewMemoryRequestLookup(nil)
	registry := newMemoryActorRegistry()
	_ = registry.Insert(context.Background(), actor.Record{
		ID: "tool:xhs", Kind: actor.KindTool, Binding: actor.BindingViaServerTransit,
	})

	var seenFrame *adapter.SendFrame
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.publish"},
			Binding:      adapter.BindingViaServerTransit,
			MaxPendingMs: 5_000,
		},
		handle: func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
			if mctx.DeviceTransit == nil {
				t.Fatalf("via_server_transit mctx.DeviceTransit nil")
			}
			frame := adapter.SendFrame{
				ChannelID:       mctx.ChannelID,
				DeviceSessionID: "device-1",
				Direction:       adapter.DirectionToDevice,
				RequestID:       env.ID,
				Payload:         env.Payload,
			}
			seenFrame = &frame
			_, err := mctx.DeviceTransit.Send(ctx, frame)
			return err
		},
	}
	mgr, err := NewManager(ManagerConfig{
		ChannelID:     "channel:test",
		ActorRegistry: registry,
		HarnessChain:  chain,
		RequestLookup: lookup,
		DeviceTransit: transit,
		Clock:         clock.Now,
		Logger:        &recordingLogger{},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background(), []adapter.Module{mod}); err != nil {
		t.Fatalf("Install: %v", err)
	}
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	// Verify mctx wiring.
	if mod.mctx.DeviceTransit == nil {
		t.Fatalf("DeviceTransit not wired into mctx for via_server_transit")
	}

	req := newTestRequest("channel:test", "agent:a", "xhs.publish", "req-vst-1")
	req.Audience = []string{"tool:xhs"}
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if seenFrame == nil {
		t.Fatalf("expected DeviceTransit.Send to be called")
	}
	if seenFrame.RequestID != "req-vst-1" {
		t.Fatalf("frame.RequestID=%s want req-vst-1", seenFrame.RequestID)
	}
	if seenFrame.Direction != adapter.DirectionToDevice {
		t.Fatalf("frame.Direction=%s want to_device", seenFrame.Direction)
	}
	if len(transit.sent) != 1 {
		t.Fatalf("expected 1 transit.Send call, got %d", len(transit.sent))
	}
	// IMPORTANT: via_server_transit Handle must NOT write to harness
	// directly — the response comes back via OnExternalCallback later.
	if len(chain.Written()) != 0 {
		t.Fatalf("via_server_transit Handle wrote to chain (should be 0): %d", len(chain.Written()))
	}

	// Now simulate the device callback arriving via Manager.OnExternalCallback.
	mod.onCallback = func(ctx context.Context, payload []byte, mctx *adapter.ModuleContext) error {
		// The adapter looks up request_id from its own payload format
		// then calls Respond.
		_, err := mctx.Respond(ctx, "req-vst-1",
			json.RawMessage(`{"note_id":"note_42"}`),
			adapter.RespondOptions{Status: "completed"},
		)
		return err
	}
	if err := mgr.OnExternalCallback(context.Background(), "xhs",
		[]byte(`{"request_id":"req-vst-1","note_id":"note_42"}`)); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}
	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("expected 1 response after callback, got %d", len(written))
	}
	if written[0].Sender.ID != "tool:xhs" {
		t.Fatalf("response sender.id=%s want tool:xhs", written[0].Sender.ID)
	}
	if written[0].ParentID != "req-vst-1" {
		t.Fatalf("response parent_id=%s want req-vst-1", written[0].ParentID)
	}
}

// TestOutboundHTTPBindingMCtxShape sanity-checks the contract for
// outbound_http (already covered end-to-end by feishu_test.go).
func TestOutboundHTTPBindingMCtxShape(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "outbound",
			ActorID:      "tool:outbound",
			Types:        []string{"outbound.x"},
			Binding:      adapter.BindingOutboundHTTP,
			MaxPendingMs: 1_000,
		},
	}
	mgr, _, _, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()
	if mod.mctx == nil {
		t.Fatalf("mctx nil")
	}
	if mod.mctx.DeviceTransit != nil {
		t.Fatalf("outbound_http should not receive DeviceTransit")
	}
	if mod.mctx.HarnessChain == nil {
		t.Fatalf("HarnessChain not wired")
	}
	if mod.mctx.Respond == nil {
		t.Fatalf("Respond not wired")
	}
	if mod.mctx.Correlation == nil {
		t.Fatalf("Correlation not wired")
	}
	if mod.mctx.ErrorPolicy == nil {
		t.Fatalf("ErrorPolicy not wired")
	}
}

// TestViaServerTransitRequiresDeviceTransit asserts that installing a
// via_server_transit module without a DeviceTransit explicitly fails —
// this is the seam guard from manager.go.
func TestViaServerTransitRequiresDeviceTransit(t *testing.T) {
	registry := newMemoryActorRegistry()
	_ = registry.Insert(context.Background(), actor.Record{
		ID: "tool:vst", Kind: actor.KindTool, Binding: actor.BindingViaServerTransit,
	})
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "vst",
			ActorID:      "tool:vst",
			Types:        []string{"vst.send"},
			Binding:      adapter.BindingViaServerTransit,
			MaxPendingMs: 1_000,
		},
	}
	mgr, _ := NewManager(ManagerConfig{
		ChannelID:     "channel:test",
		ActorRegistry: registry,
		HarnessChain:  newFakeChain(),
		RequestLookup: NewMemoryRequestLookup(nil),
		// DeviceTransit deliberately omitted.
	})
	err := mgr.Install(context.Background(), []adapter.Module{mod})
	if err == nil {
		t.Fatalf("expected install error when DeviceTransit missing")
	}
}
