package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	deviceframework "github.com/wanpengxie/ActOS/adapters/device/framework"
	devicexhs "github.com/wanpengxie/ActOS/adapters/device/xhs"
	"github.com/wanpengxie/ActOS/adapters/xhs"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

func TestBuildChannelTemplates_DefaultsToDeviceXHSBinding(t *testing.T) {
	t.Parallel()

	prod := buildChannelTemplates(false)[XHSCreatorChannelType]
	if len(prod.AdapterActorSeeds) != 1 {
		t.Fatalf("prod seeds len=%d want 1", len(prod.AdapterActorSeeds))
	}
	if prod.AdapterActorSeeds[0].Binding != actor.BindingRuntimeInboundViaRelay {
		t.Fatalf("prod actor binding=%q want %q", prod.AdapterActorSeeds[0].Binding, actor.BindingRuntimeInboundViaRelay)
	}

	scaffold := buildChannelTemplates(true)[XHSCreatorChannelType]
	if len(scaffold.AdapterActorSeeds) != 1 {
		t.Fatalf("scaffold seeds len=%d want 1", len(scaffold.AdapterActorSeeds))
	}
	if scaffold.AdapterActorSeeds[0].Binding != actor.BindingEmbedded {
		t.Fatalf("scaffold actor binding=%q want %q", scaffold.AdapterActorSeeds[0].Binding, actor.BindingEmbedded)
	}
}

func TestIntegration_ProductionXHSPublishEmitsDeviceTransitSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	channelsDir := filepath.Join(dataDir, "channels")
	cfg := runtime.DaemonConfig{
		DataDir:           dataDir,
		ChannelsDir:       channelsDir,
		DaemonID:          "daemon-prod-xhs",
		DaemonEpoch:       9,
		UseMockBus:        true,
		NowFn:             nowMs,
		HumanCallerSecret: []byte(integSecret),
		ReplayWindow:      time.Minute,
		SchedulerPeriod:   50 * time.Millisecond,
		ChannelTemplates: map[string]runtime.ChannelTemplate{
			XHSCreatorChannelType: {
				AdapterActorSeeds: []actorreg.Record{DeviceXHSActorSeed()},
				WorkdirSubdirs:    xhs.WorkdirSubdirs(),
				DomainPrompt:      xhs.DomainPrompt(),
			},
		},
		OnChannelBoot: wireAdapterFramework(DeviceXHSFactory(devicexhs.Config{})),
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

	const channelID = "ch-prod-xhs"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human"},
	})

	db := openChannelDBForTest(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()
	assertProductionXHSBindings(t, ctx, db)

	// Simulate the devicebus ws-register lifecycle signal so the
	// adapter's runtime-event state machine treats the device as
	// reachable. In production this frame is pushed by
	// server.gateway.NotifyDeviceLifecycle when the extension's
	// /devicebus WS handshake succeeds.
	pushDeviceLifecycleConnected(t, ctx, srv, channelID, devicexhs.DefaultAdapterActorID)

	// xhs.publish request payload (domain-xhs-spec §1.1) carries
	// title + content for the production extension path. Per Level A
	// (proto-layer0 §1.4.1) the protocol layer does not validate
	// payload contents; payload consistency is enforced by the adapter
	// boundary's per-type allow-lists, not by the harness.
	ack, send := writeRequestAndWaitForDeviceSend(t, ctx, d, srv, channelID, "req-prod-xhs", "user:alice",
		devicexhs.TypePublish, []byte(`{"title":"hello","content":"world"}`))
	if !ack.Accepted {
		t.Fatalf("write_message rejected: reason=%s detail=%s", ack.RejectReason, ack.RejectDetail)
	}
	if send.ChannelID != channel.ID(channelID) {
		t.Errorf("send.ChannelID=%q want %q", send.ChannelID, channelID)
	}
	if send.AdapterActorID != devicexhs.DefaultAdapterActorID {
		t.Errorf("send.AdapterActorID=%q want %q", send.AdapterActorID, devicexhs.DefaultAdapterActorID)
	}
	var transitBody deviceframework.DeviceTransitBody
	if err := json.Unmarshal(send.Body, &transitBody); err != nil {
		t.Fatalf("decode device transit body: %v", err)
	}
	if transitBody.RequestID != ack.MessageID {
		t.Errorf("body.RequestID=%q want canonical ack.MessageID=%q", transitBody.RequestID, ack.MessageID)
	}
	var cmd devicexhs.Command
	if err := json.Unmarshal(transitBody.Payload, &cmd); err != nil {
		t.Fatalf("decode device command: %v", err)
	}
	if cmd.Type != devicexhs.CommandWireType || cmd.Cmd != "publish" {
		t.Fatalf("device command=%+v want command/publish", cmd)
	}
}

func openChannelDBForTest(t *testing.T, channelsDir, channelID string) *sql.DB {
	t.Helper()
	db, err := store.OpenChannel(context.Background(), filepath.Join(channelsDir, channelID, "channel.sqlite"), store.OpenOptions{SkipDDL: true})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	return db
}

func assertProductionXHSBindings(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	reg := store.NewActorRegistry(db)
	rec, ok, err := reg.Lookup(ctx, devicexhs.DefaultAdapterActorID)
	if err != nil {
		t.Fatalf("actor lookup: %v", err)
	}
	if !ok {
		t.Fatalf("actor_registry missing %s", devicexhs.DefaultAdapterActorID)
	}
	if rec.Binding != actor.BindingRuntimeInboundViaRelay {
		t.Fatalf("actor binding=%q want %q", rec.Binding, actor.BindingRuntimeInboundViaRelay)
	}
	var binding string
	if err := db.QueryRowContext(ctx, `SELECT handler_binding FROM type_registry WHERE type=?`, devicexhs.TypePublish).Scan(&binding); err != nil {
		t.Fatalf("type_registry xhs.publish binding: %v", err)
	}
	if binding != string(actor.BindingRuntimeInboundViaRelay) {
		t.Fatalf("type_registry xhs.publish binding=%q want %q", binding, actor.BindingRuntimeInboundViaRelay)
	}
}

// pushDeviceLifecycleConnected pushes a `device_transit.lifecycle`
// frame from the mock server side so the daemon's adapter framework
// projects the device into the "online" state. Mirrors the production
// path where server.gateway.NotifyDeviceLifecycle would emit the same
// frame after a devicebus ws handshake succeeds.
func pushDeviceLifecycleConnected(
	t *testing.T,
	ctx context.Context,
	srv *transit.MockServer,
	channelID string,
	adapterActorID actor.ActorID,
) {
	t.Helper()
	ts := nowMs()
	lf := devicetransit.LifecycleFrame{
		AdapterActorID: adapterActorID,
		ChannelID:      channel.ID(channelID),
		Event:          devicetransit.LifecycleConnected,
		DeviceID:       "device-test",
		Ts:             ts,
	}
	frame, err := transit.Encode(
		"frame-lifecycle-connected",
		daemonbus.FrameTypeDeviceTransitLifecycle,
		"server",
		1,
		ts,
		lf,
	)
	if err != nil {
		t.Fatalf("encode device_transit.lifecycle: %v", err)
	}
	frame.ChannelID = channelID
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatalf("SendToDaemon device_transit.lifecycle: %v", err)
	}
	// Give the dispatcher a moment to apply the state transition before
	// the caller fires the publish request. The adapter framework
	// processes lifecycle synchronously, but the mock daemonbus runs on
	// a goroutine so the next SendToDaemon could otherwise race the
	// state store.
	time.Sleep(50 * time.Millisecond)
}

func writeRequestAndWaitForDeviceSend(
	t *testing.T,
	ctx context.Context,
	d *runtime.Daemon,
	srv *transit.MockServer,
	channelID, requestID, callerActor, envType string,
	payload []byte,
) (transit.WriteMessageAckBody, devicetransit.SendFrame) {
	t.Helper()
	ts := nowMs()
	hc := transit.HumanCaller{
		UserID:        "u1",
		MemberActorID: actor.ActorID(callerActor),
		TS:            ts,
		Nonce:         "nonce-" + requestID,
	}
	hc.ServerToken = transit.SignHumanCaller(
		[]byte(integSecret), channelID, hc.UserID, hc.MemberActorID, hc.TS, hc.Nonce,
	)
	body := transit.WriteMessageBody{
		FrameID:     daemonbus.FrameID("frame-write-" + requestID),
		ChannelID:   channel.ID(channelID),
		HumanCaller: hc,
		EnvelopePartial: message.Envelope{
			ID:         message.ID(requestID),
			Type:       envType,
			Kind:       message.KindRequest,
			Payload:    json.RawMessage(payload),
			Audience:   message.Audience{devicexhs.DefaultAdapterActorID},
			TS:         ts,
			Sender:     message.Sender{ID: actor.ActorID(callerActor)},
			Visibility: message.VisibilityPublic,
		},
	}
	reqFrame, err := transit.Encode("frame-srv-write-"+requestID,
		daemonbus.FrameTypeControlWriteMessage,
		"server", d.Transit().Epoch(), ts, body)
	if err != nil {
		t.Fatalf("encode write_message: %v", err)
	}
	if err := srv.SendToDaemon(ctx, reqFrame); err != nil {
		t.Fatalf("SendToDaemon write_message: %v", err)
	}

	var (
		ack     transit.WriteMessageAckBody
		send    devicetransit.SendFrame
		gotAck  bool
		gotSend bool
	)
	deadline := time.After(5 * time.Second)
	for !gotAck || !gotSend {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for write ack=%v and device_transit.recv=%v", gotAck, gotSend)
		default:
		}
		recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		frame, err := srv.RecvFromDaemon(recvCtx)
		cancel()
		if err != nil {
			t.Fatalf("RecvFromDaemon: %v", err)
		}
		switch frame.FrameKind {
		case daemonbus.FrameTypeControlWriteMessageAck:
			if err := transit.DecodePayload(frame, &ack); err != nil {
				t.Fatalf("decode write ack: %v", err)
			}
			gotAck = true
		case daemonbus.FrameTypeDeviceTransitRecv:
			// impl-layer2 §5.3.2 outbound (adapter → device): daemon
			// adapter pushes `device_transit.recv` toward the server.
			if err := transit.DecodePayload(frame, &send); err != nil {
				t.Fatalf("decode device recv: %v", err)
			}
			gotSend = true
		}
	}
	return ack, send
}
