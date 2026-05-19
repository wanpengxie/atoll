package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	deviceframework "github.com/wanpengxie/ActOS/adapters/device/framework"
	"github.com/wanpengxie/ActOS/adapters/xhs"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

// TestIntegration_BindDeviceSession_RoundTrip covers T147 phase-4b
// end-to-end through a real daemon:
//
//  1. Server side sends control.bind_device_session via the mock bus.
//  2. Daemon dispatcher routes to the binder's OnBind hook.
//  3. Binder Upserts the row into the shared SessionStore.
//  4. Daemon emits control.bind_device_session_ack with Accepted=true.
//
// Then exercises unbind:
//
//  5. Server sends control.unbind_device_session.
//  6. Daemon emits the matching ack.
//  7. SessionStore row is deleted.
func TestIntegration_BindDeviceSession_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := deviceframework.NewInMemorySessionStore()
	binder := NewDeviceSessionBinder(store)

	dataDir := t.TempDir()
	cfg := runtime.DaemonConfig{
		DataDir:           dataDir,
		ChannelsDir:       filepath.Join(dataDir, "channels"),
		DaemonID:          "daemon-bind-integ",
		DaemonEpoch:       7,
		UseMockBus:        true,
		NowFn:             nowMs,
		HumanCallerSecret: []byte(integSecret),
		SchedulerPeriod:   50 * time.Millisecond,
		ChannelTemplate: runtime.ChannelTemplate{
			AdapterActorSeeds: []actorreg.Record{xhs.DefaultActorSeed()},
		},
		OnChannelBoot:         wireAdapterFramework(XHSScaffoldFactory(xhs.Config{})),
		OnBindDeviceSession:   binder.OnBind,
		OnUnbindDeviceSession: binder.OnUnbind,
	}
	d, err := runtime.AssembleDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("AssembleDaemon: %v", err)
	}
	if err := d.RunPhases(ctx); err != nil {
		t.Fatalf("RunPhases: %v", err)
	}
	defer func() { _ = d.Close() }()
	srv := d.Bus().ServerSide()

	const sessionID adapter.DeviceSessionID = "sess-integ-1"
	bindBody := transit.BindDeviceSessionBody{
		FrameID:          "frame-bind-integ",
		SessionID:        sessionID,
		ChannelID:        channel.ID("ch-X"),
		DeviceID:         "dev-1",
		DeviceType:       "xhs",
		DaemonID:         "daemon-bind-integ",
		TokenFingerprint: "1234567890abcdef",
		ExpiresAt:        90_000,
		BoundAt:          80_000,
	}
	bindFrame, err := transit.Encode(
		"frame-srv-bind-integ",
		daemonbus.FrameTypeControlBindDeviceSession,
		"server", d.Transit().Epoch(), nowMs(), bindBody,
	)
	if err != nil {
		t.Fatalf("encode bind: %v", err)
	}
	if err := srv.SendToDaemon(ctx, bindFrame); err != nil {
		t.Fatalf("SendToDaemon bind: %v", err)
	}

	ack := waitForAck(t, ctx, srv, daemonbus.FrameTypeControlBindDeviceSessionAck)
	var bindAck transit.BindDeviceSessionAckBody
	if err := transit.DecodePayload(ack, &bindAck); err != nil {
		t.Fatalf("decode bind ack: %v", err)
	}
	if !bindAck.Accepted {
		t.Fatalf("bind ack not accepted: %+v", bindAck)
	}
	if bindAck.SessionID != sessionID {
		t.Errorf("ack.SessionID=%q want %q", bindAck.SessionID, sessionID)
	}

	// Row landed in the shared store.
	row, ok, err := store.Get(ctx, sessionID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if !ok {
		t.Fatal("session row missing after bind ack")
	}
	if row.State != deviceframework.StateReady {
		t.Errorf("row.State=%q want ready", row.State)
	}
	if row.TokenFingerprint != bindBody.TokenFingerprint {
		t.Errorf("fingerprint mismatch: %q vs %q", row.TokenFingerprint, bindBody.TokenFingerprint)
	}

	// Unbind round trip.
	unbindBody := transit.UnbindDeviceSessionBody{
		FrameID:   "frame-unbind-integ",
		SessionID: sessionID,
		ChannelID: bindBody.ChannelID,
		Reason:    "revoked",
	}
	unbindFrame, err := transit.Encode(
		"frame-srv-unbind-integ",
		daemonbus.FrameTypeControlUnbindDeviceSession,
		"server", d.Transit().Epoch(), nowMs(), unbindBody,
	)
	if err != nil {
		t.Fatalf("encode unbind: %v", err)
	}
	if err := srv.SendToDaemon(ctx, unbindFrame); err != nil {
		t.Fatalf("SendToDaemon unbind: %v", err)
	}
	unbindAckFrame := waitForAck(t, ctx, srv, daemonbus.FrameTypeControlUnbindDeviceSessionAck)
	var unbindAck transit.UnbindDeviceSessionAckBody
	if err := transit.DecodePayload(unbindAckFrame, &unbindAck); err != nil {
		t.Fatalf("decode unbind ack: %v", err)
	}
	if !unbindAck.Accepted {
		t.Fatalf("unbind ack not accepted: %+v", unbindAck)
	}
	if _, ok, _ := store.Get(ctx, sessionID); ok {
		t.Error("row still present after unbind ack")
	}
}

// waitForAck drains daemon → server frames until one with the requested
// frame_type appears (skipping heartbeats and other unrelated traffic).
// Fails the test on timeout.
func waitForAck(t *testing.T, ctx context.Context, srv *transit.MockServer, want daemonbus.FrameType) daemonbus.Frame {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("ack %s never arrived", want)
		default:
		}
		recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		f, err := srv.RecvFromDaemon(recvCtx)
		cancel()
		if err != nil {
			t.Fatalf("RecvFromDaemon: %v", err)
		}
		if f.FrameType == want {
			return f
		}
	}
}
