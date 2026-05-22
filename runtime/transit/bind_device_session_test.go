package transit_test

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

// TestDispatcher_BindDeviceSessionRoundTrip covers T147 phase-4b — the
// dispatcher decodes a control.bind_device_session frame, invokes the
// handler, and emits a matching control.bind_device_session_ack so the
// gateway's SendAndAwait wakes up.
func TestDispatcher_BindDeviceSessionRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bus := transit.NewMockBus(64)
	defer func() { _ = bus.Close() }()
	client, err := transit.NewClient(transit.ClientConfig{
		DaemonID: "daemon-A", Transport: bus,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	var received transit.BindDeviceSessionBody
	dispatcher, err := transit.NewDispatcher(transit.DispatcherConfig{
		Client:  client,
		FrameID: atomicFrameID(),
		Handlers: transit.ControlHandlers{
			OnBindDeviceSession: func(_ context.Context, _ daemonbus.Frame, body transit.BindDeviceSessionBody) transit.BindDeviceSessionAckBody {
				received = body
				return transit.BindDeviceSessionAckBody{
					FrameID:         body.FrameID,
					ChannelID:       body.ChannelID,
					BindRequestID:   body.BindRequestID,
					DeviceSessionID: body.DeviceSessionID,
					Result:          daemonbus.DeviceSessionBindAccepted,
				}
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		frame, recvErr := client.Recv(ctx)
		if recvErr != nil {
			done <- recvErr
			return
		}
		done <- dispatcher.Dispatch(ctx, frame)
	}()

	body := transit.BindDeviceSessionBody{
		FrameID:          "frame-bind-1",
		BindRequestID:    "bind-req-1",
		DeviceSessionID:  devicetransit.DeviceSessionID("sess-1"),
		ChannelID:        channel.ID("ch-X"),
		DeviceID:         "dev-A",
		DeviceType:       "xhs.chrome_extension",
		DaemonID:         "daemon-A",
		TokenFingerprint: "deadbeefcafebabe",
		ExpiresAt:        12_345,
		BoundAt:          11_000,
	}
	reqFrame, _ := transit.Encode(
		"frame-srv-bind",
		daemonbus.FrameTypeControlBindDeviceSession,
		"server", client.Epoch(), 0, body,
	)
	server := bus.ServerSide()
	if err := server.SendToDaemon(ctx, reqFrame); err != nil {
		t.Fatal(err)
	}

	select {
	case derr := <-done:
		if derr != nil {
			t.Fatalf("dispatch err: %v", derr)
		}
	case <-ctx.Done():
		t.Fatal("dispatch never returned")
	}

	if received.DeviceSessionID != body.DeviceSessionID {
		t.Errorf("handler received SessionID=%q want %q", received.DeviceSessionID, body.DeviceSessionID)
	}
	if received.TokenFingerprint != body.TokenFingerprint {
		t.Errorf("handler received TokenFingerprint=%q want %q",
			received.TokenFingerprint, body.TokenFingerprint)
	}

	ackFrame, err := server.RecvFromDaemon(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ackFrame.FrameKind != daemonbus.FrameTypeControlBindDeviceSessionAck {
		t.Fatalf("ack frame type = %s", ackFrame.FrameKind)
	}
	var ack transit.BindDeviceSessionAckBody
	if err := transit.DecodePayload(ackFrame, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Result != daemonbus.DeviceSessionBindAccepted {
		t.Errorf("ack.Result=%q: %+v", ack.Result, ack)
	}
	if ack.FrameID != body.FrameID {
		t.Errorf("ack.FrameID=%q want %q", ack.FrameID, body.FrameID)
	}
	if ack.DeviceSessionID != body.DeviceSessionID {
		t.Errorf("ack.DeviceSessionID=%q want %q", ack.DeviceSessionID, body.DeviceSessionID)
	}
}

// TestDispatcher_BindDeviceSessionHandlerMissing covers the nil-safe
// fallback: when ControlHandlers.OnBindDeviceSession is nil, the
// dispatcher MUST still emit a structured result=rejected ack so the
// gateway can distinguish "daemon does not implement bind" from
// "daemon refused bind".
func TestDispatcher_BindDeviceSessionHandlerMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bus := transit.NewMockBus(64)
	defer func() { _ = bus.Close() }()
	client, err := transit.NewClient(transit.ClientConfig{
		DaemonID: "daemon-A", Transport: bus,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	dispatcher, err := transit.NewDispatcher(transit.DispatcherConfig{
		Client:   client,
		FrameID:  atomicFrameID(),
		Handlers: transit.ControlHandlers{}, // OnBindDeviceSession nil
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		frame, recvErr := client.Recv(ctx)
		if recvErr != nil {
			done <- recvErr
			return
		}
		done <- dispatcher.Dispatch(ctx, frame)
	}()

	body := transit.BindDeviceSessionBody{
		FrameID:         "frame-bind-2",
		BindRequestID:   "bind-req-2",
		DeviceSessionID: devicetransit.DeviceSessionID("sess-2"),
		ChannelID:       channel.ID("ch-Y"),
		DeviceID:        "dev-B",
	}
	reqFrame, _ := transit.Encode(
		"frame-srv-bind-2",
		daemonbus.FrameTypeControlBindDeviceSession,
		"server", client.Epoch(), 0, body,
	)
	server := bus.ServerSide()
	if err := server.SendToDaemon(ctx, reqFrame); err != nil {
		t.Fatal(err)
	}

	select {
	case derr := <-done:
		if derr != nil {
			t.Fatalf("dispatch err: %v", derr)
		}
	case <-ctx.Done():
		t.Fatal("dispatch never returned")
	}

	ackFrame, err := server.RecvFromDaemon(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ackFrame.FrameKind != daemonbus.FrameTypeControlBindDeviceSessionAck {
		t.Fatalf("ack frame type = %s", ackFrame.FrameKind)
	}
	var ack transit.BindDeviceSessionAckBody
	if err := transit.DecodePayload(ackFrame, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Result != daemonbus.DeviceSessionBindRejected {
		t.Errorf("ack.Result=%q with nil handler — expected rejected", ack.Result)
	}
	if ack.Reason != transit.DeviceSessionRejectBindInternalError {
		t.Errorf("ack.Reason=%q want %q", ack.Reason, transit.DeviceSessionRejectBindInternalError)
	}
	if ack.DeviceSessionID != body.DeviceSessionID {
		t.Errorf("ack.DeviceSessionID=%q want %q", ack.DeviceSessionID, body.DeviceSessionID)
	}
}

// TestDispatcher_UnbindDeviceSessionRoundTrip mirrors the bind happy
// path for the unbind frame.
func TestDispatcher_UnbindDeviceSessionRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bus := transit.NewMockBus(64)
	defer func() { _ = bus.Close() }()
	client, err := transit.NewClient(transit.ClientConfig{
		DaemonID: "daemon-A", Transport: bus,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	var calledWith transit.UnbindDeviceSessionBody
	dispatcher, err := transit.NewDispatcher(transit.DispatcherConfig{
		Client:  client,
		FrameID: atomicFrameID(),
		Handlers: transit.ControlHandlers{
			OnUnbindDeviceSession: func(_ context.Context, _ daemonbus.Frame, body transit.UnbindDeviceSessionBody) transit.UnbindDeviceSessionAckBody {
				calledWith = body
				return transit.UnbindDeviceSessionAckBody{
					FrameID:         body.FrameID,
					ChannelID:       body.ChannelID,
					DeviceSessionID: body.DeviceSessionID,
					Result:          daemonbus.DeviceSessionBindAccepted,
				}
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		frame, recvErr := client.Recv(ctx)
		if recvErr != nil {
			done <- recvErr
			return
		}
		done <- dispatcher.Dispatch(ctx, frame)
	}()

	body := transit.UnbindDeviceSessionBody{
		FrameID:         "frame-unbind-1",
		DeviceSessionID: devicetransit.DeviceSessionID("sess-1"),
		ChannelID:       channel.ID("ch-X"),
		Reason:          "revoked",
	}
	reqFrame, _ := transit.Encode(
		"frame-srv-unbind",
		daemonbus.FrameTypeControlUnbindDeviceSession,
		"server", client.Epoch(), 0, body,
	)
	server := bus.ServerSide()
	if err := server.SendToDaemon(ctx, reqFrame); err != nil {
		t.Fatal(err)
	}

	select {
	case derr := <-done:
		if derr != nil {
			t.Fatalf("dispatch err: %v", derr)
		}
	case <-ctx.Done():
		t.Fatal("dispatch never returned")
	}

	if calledWith.Reason != "revoked" {
		t.Errorf("handler received Reason=%q want revoked", calledWith.Reason)
	}

	ackFrame, err := server.RecvFromDaemon(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ackFrame.FrameKind != daemonbus.FrameTypeControlUnbindDeviceSessionAck {
		t.Fatalf("ack frame type = %s", ackFrame.FrameKind)
	}
	var ack transit.UnbindDeviceSessionAckBody
	if err := transit.DecodePayload(ackFrame, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Result != daemonbus.DeviceSessionBindAccepted {
		t.Errorf("ack.Result=%q: %+v", ack.Result, ack)
	}
}
