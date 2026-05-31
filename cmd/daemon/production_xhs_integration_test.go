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
	"github.com/wanpengxie/ActOS/framework/devicetransit"
	"github.com/wanpengxie/ActOS/framework/multiuser/daemonbus"
	"github.com/wanpengxie/ActOS/framework/multiuser/placement"
	"github.com/wanpengxie/ActOS/framework/multiuser/runtime"
	"github.com/wanpengxie/ActOS/framework/multiuser/runtime/transit"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/store"
)

func TestBuildChannelTemplates_XHSCreatorHasNoAdapterSeeds(t *testing.T) {
	t.Parallel()

	// T4 retired the daemon-side xhs adapter; proxy facade now installs
	// itself dynamically when the user proxy daemon arrives. Template
	// keeps WorkdirSubdirs + DomainPrompt (xhs business knowledge) but
	// no longer pre-seeds an adapter actor row (would conflict with the
	// facade's `tool:xhs` install).
	tpl := buildChannelTemplates()[XHSCreatorChannelType]
	if len(tpl.AdapterActorSeeds) != 0 {
		t.Fatalf("AdapterActorSeeds len=%d want 0", len(tpl.AdapterActorSeeds))
	}
	if len(tpl.WorkdirSubdirs) == 0 {
		t.Fatal("WorkdirSubdirs empty; expected xhs subdirs")
	}
	if tpl.DomainPrompt == "" {
		t.Fatal("DomainPrompt empty")
	}
}

func TestIntegration_ProductionXHSPublishUsesProxyFacade(t *testing.T) {
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
		ChannelTemplates:  buildChannelTemplates(),
		OnChannelBoot:     wireAdapterFramework(),
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

	attachProxyXHSActor(t, ctx, d, srv, channelID)
	assertProductionXHSBindings(t, ctx, db)

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
	var forwarded message.Envelope
	if err := json.Unmarshal(transitBody.Payload, &forwarded); err != nil {
		t.Fatalf("decode proxy facade forwarded envelope: %v", err)
	}
	if forwarded.ID != ack.MessageID || forwarded.Type != devicexhs.TypePublish ||
		len(forwarded.Audience) != 1 || forwarded.Audience[0] != devicexhs.DefaultAdapterActorID {
		t.Fatalf("forwarded envelope=%+v want id=%s type=%s audience=%s",
			forwarded, ack.MessageID, devicexhs.TypePublish, devicexhs.DefaultAdapterActorID)
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
	var handlerActor, binding string
	if err := db.QueryRowContext(ctx, `SELECT handler_actor_id, handler_binding FROM type_registry WHERE type=?`, devicexhs.TypePublish).Scan(&handlerActor, &binding); err != nil {
		t.Fatalf("type_registry xhs.publish binding: %v", err)
	}
	if handlerActor != string(devicexhs.DefaultAdapterActorID) {
		t.Fatalf("type_registry xhs.publish actor=%q want %q", handlerActor, devicexhs.DefaultAdapterActorID)
	}
	if binding != string(actor.BindingRuntimeInboundViaRelay) {
		t.Fatalf("type_registry xhs.publish binding=%q want %q", binding, actor.BindingRuntimeInboundViaRelay)
	}
}

func attachProxyXHSActor(
	t *testing.T,
	ctx context.Context,
	d *runtime.Daemon,
	srv *transit.MockServer,
	channelID string,
) {
	t.Helper()
	ts := nowMs()
	capability, err := json.Marshal(map[string]any{
		"name":              devicexhs.AdapterName,
		"types":             devicexhs.AllTypes,
		"type_declarations": devicexhs.DeclarationTypeDeclarations(),
		"max_pending_ms":    devicexhs.DefaultMaxPendingMs,
	})
	if err != nil {
		t.Fatalf("marshal xhs capability: %v", err)
	}
	frame, err := transit.Encode(
		"frame-update-xhs-proxy",
		daemonbus.FrameTypeControlUpdateMembers,
		"server",
		d.Transit().Epoch(),
		ts,
		transit.UpdateMembersBody{
			FrameID:   "frame-update-xhs-proxy",
			ChannelID: channel.ID(channelID),
			Adds: []daemonbus.UpdateMember{{
				MemberActorID: devicexhs.DefaultAdapterActorID,
				Kind:          actor.KindTool,
				Binding:       actor.BindingRuntimeInboundViaRelay,
				DisplayName:   "xhs",
				CapabilitySet: capability,
			}},
		},
	)
	if err != nil {
		t.Fatalf("encode proxy update_members: %v", err)
	}
	frame.ChannelID = channelID
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatalf("SendToDaemon update_members: %v", err)
	}
	for {
		recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		f, err := srv.RecvFromDaemon(recvCtx)
		cancel()
		if err != nil {
			t.Fatalf("RecvFromDaemon update_members ack: %v", err)
		}
		if f.FrameKind != daemonbus.FrameTypeControlUpdateMembersAck {
			continue
		}
		var ack transit.UpdateMembersAckBody
		if err := transit.DecodePayload(f, &ack); err != nil {
			t.Fatalf("decode update_members ack: %v", err)
		}
		if !ack.Accepted {
			t.Fatalf("update_members rejected: %s %s", ack.RejectReason, ack.RejectDetail)
		}
		return
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
