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
	if prod.AdapterActorSeeds[0].Binding != actor.BindingViaServerTransit {
		t.Fatalf("prod actor binding=%q want %q", prod.AdapterActorSeeds[0].Binding, actor.BindingViaServerTransit)
	}

	scaffold := buildChannelTemplates(true)[XHSCreatorChannelType]
	if len(scaffold.AdapterActorSeeds) != 1 {
		t.Fatalf("scaffold seeds len=%d want 1", len(scaffold.AdapterActorSeeds))
	}
	if scaffold.AdapterActorSeeds[0].Binding != actor.BindingInProcess {
		t.Fatalf("scaffold actor binding=%q want %q", scaffold.AdapterActorSeeds[0].Binding, actor.BindingInProcess)
	}
}

func TestIntegration_ProductionXHSPublishEmitsDeviceTransitSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sessionStore := deviceframework.NewInMemorySessionStore()
	binder := NewDeviceSessionBinder(sessionStore)
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
		SchedulerPeriod:   50 * time.Millisecond,
		ChannelTemplates: map[string]runtime.ChannelTemplate{
			XHSCreatorChannelType: {
				AdapterActorSeeds: []actorreg.Record{DeviceXHSActorSeed()},
				WorkdirSubdirs:    xhs.WorkdirSubdirs(),
				DomainPrompt:      xhs.DomainPrompt(),
			},
		},
		OnChannelBoot:         wireAdapterFramework(DeviceXHSFactory(sessionStore, devicexhs.Config{})),
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

	const channelID = "ch-prod-xhs"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{ActorIDInChannel: "user:alice", Kind: "human"},
	})

	db := openChannelDBForTest(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()
	assertProductionXHSBindings(t, ctx, db)

	const sessionID devicetransit.DeviceSessionID = "sess-prod-xhs"
	bindDeviceSession(t, ctx, d, srv, sessionID, channel.ID(channelID))
	if err := sessionStore.SetState(ctx, sessionID, deviceframework.StateActive, nowMs()); err != nil {
		t.Fatalf("activate session: %v", err)
	}

	ack, send := writeRequestAndWaitForDeviceSend(t, ctx, d, srv, channelID, "req-prod-xhs", "user:alice",
		devicexhs.TypePublish, []byte(`{"title":"hello","device_session_id":"sess-prod-xhs"}`))
	if !ack.Accepted {
		t.Fatalf("write_message rejected: reason=%s detail=%s", ack.RejectReason, ack.RejectDetail)
	}
	if send.ChannelID != channel.ID(channelID) {
		t.Errorf("send.ChannelID=%q want %q", send.ChannelID, channelID)
	}
	if send.DeviceSessionID != sessionID {
		t.Errorf("send.DeviceSessionID=%q want %q", send.DeviceSessionID, sessionID)
	}
	if send.RequestID != ack.MessageID {
		t.Errorf("send.RequestID=%q want canonical ack.MessageID=%q", send.RequestID, ack.MessageID)
	}
	var cmd devicexhs.Command
	if err := json.Unmarshal(send.Payload, &cmd); err != nil {
		t.Fatalf("decode device command: %v", err)
	}
	if cmd.Type != devicexhs.CommandWireType || cmd.Cmd != "publish" {
		t.Fatalf("device command=%+v want command/publish", cmd)
	}
	if _, ok := cmd.Params["device_session_id"]; ok {
		t.Fatalf("device_session_id leaked into device params: %+v", cmd.Params)
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
	if rec.Binding != actor.BindingViaServerTransit {
		t.Fatalf("actor binding=%q want %q", rec.Binding, actor.BindingViaServerTransit)
	}
	var binding string
	if err := db.QueryRowContext(ctx, `SELECT handler_binding FROM type_registry WHERE type=?`, devicexhs.TypePublish).Scan(&binding); err != nil {
		t.Fatalf("type_registry xhs.publish binding: %v", err)
	}
	if binding != string(actor.BindingViaServerTransit) {
		t.Fatalf("type_registry xhs.publish binding=%q want %q", binding, actor.BindingViaServerTransit)
	}
}

func bindDeviceSession(
	t *testing.T,
	ctx context.Context,
	d *runtime.Daemon,
	srv *transit.MockServer,
	sessionID devicetransit.DeviceSessionID,
	channelID channel.ID,
) {
	t.Helper()
	body := transit.BindDeviceSessionBody{
		FrameID:          "frame-bind-" + string(sessionID),
		SessionID:        sessionID,
		ChannelID:        channelID,
		DeviceID:         "device-" + string(sessionID),
		DeviceType:       "xhs",
		DaemonID:         "daemon-prod-xhs",
		TokenFingerprint: "prod123456789abc",
		BoundAt:          nowMs(),
		ExpiresAt:        nowMs() + 60_000,
	}
	frame, err := transit.Encode("frame-srv-bind-"+string(sessionID),
		daemonbus.FrameTypeControlBindDeviceSession,
		"server", d.Transit().Epoch(), nowMs(), body)
	if err != nil {
		t.Fatalf("encode bind: %v", err)
	}
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatalf("SendToDaemon bind: %v", err)
	}
	ackFrame := waitForAck(t, ctx, srv, daemonbus.FrameTypeControlBindDeviceSessionAck)
	var ack transit.BindDeviceSessionAckBody
	if err := transit.DecodePayload(ackFrame, &ack); err != nil {
		t.Fatalf("decode bind ack: %v", err)
	}
	if !ack.Accepted {
		t.Fatalf("bind rejected: %+v", ack)
	}
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
		UserID:           "u1",
		ActorIDInChannel: callerActor,
		TS:               ts,
		Nonce:            "nonce-" + requestID,
	}
	hc.ServerToken = transit.SignHumanCaller(
		[]byte(integSecret), channelID, hc.UserID, hc.ActorIDInChannel, hc.TS, hc.Nonce,
	)
	body := transit.WriteMessageBody{
		FrameID:     "frame-write-" + requestID,
		ChannelID:   channelID,
		HumanCaller: hc,
		EnvelopePartial: message.Envelope{
			ID:         requestID,
			Type:       envType,
			Kind:       message.KindRequest,
			Payload:    json.RawMessage(payload),
			Audience:   []string{string(devicexhs.DefaultAdapterActorID)},
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
			t.Fatalf("timed out waiting for write ack=%v and device_transit.send=%v", gotAck, gotSend)
		default:
		}
		recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		frame, err := srv.RecvFromDaemon(recvCtx)
		cancel()
		if err != nil {
			t.Fatalf("RecvFromDaemon: %v", err)
		}
		switch frame.FrameType {
		case daemonbus.FrameTypeControlWriteMessageAck:
			if err := transit.DecodePayload(frame, &ack); err != nil {
				t.Fatalf("decode write ack: %v", err)
			}
			gotAck = true
		case daemonbus.FrameTypeDeviceTransitSend:
			if err := transit.DecodePayload(frame, &send); err != nil {
				t.Fatalf("decode device send: %v", err)
			}
			gotSend = true
		}
	}
	return ack, send
}
