package transit_test

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/framework/multiuser/daemonbus"
	"github.com/wanpengxie/ActOS/framework/multiuser/placement"
	"github.com/wanpengxie/ActOS/framework/multiuser/runtime/transit"
)

// newSenderClient wires a transit.Client over a MockBus and returns
// both halves so tests can drive the server side.
func newSenderClient(t *testing.T) (*transit.Client, *transit.MockServer) {
	t.Helper()
	bus := transit.NewMockBus(64)
	client, err := transit.NewClient(transit.ClientConfig{
		DaemonID:  "daemon-test",
		Transport: bus,
		NowFn:     func() int64 { return time.Now().UnixMilli() },
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	return client, bus.ServerSide()
}

func TestHeartbeatSender_EmitsControlHeartbeat(t *testing.T) {
	client, server := newSenderClient(t)

	var fid atomic.Int64
	frameID := func() string {
		return "hb-" + strconv.FormatInt(fid.Add(1), 10)
	}
	channels := []placement.HeartbeatHeldChannel{
		{ChannelID: "ch-a", OwnerEpoch: 1, FencingToken: "tok-a"},
		{ChannelID: "ch-b", OwnerEpoch: 2, FencingToken: "tok-b"},
	}
	sender, err := transit.NewHeartbeatSender(transit.HeartbeatSenderConfig{
		Client:  client,
		Period:  10 * time.Millisecond,
		FrameID: frameID,
		HeldChannels: func(context.Context) []placement.HeartbeatHeldChannel {
			out := make([]placement.HeartbeatHeldChannel, len(channels))
			copy(out, channels)
			return out
		},
	})
	if err != nil {
		t.Fatalf("NewHeartbeatSender: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sender.Run(ctx) }()

	// Expect at least 2 heartbeat frames within a generous window. The
	// sender emits once immediately then every Period.
	recvCtx, recvCancel := context.WithTimeout(ctx, 2*time.Second)
	defer recvCancel()
	got := 0
	for got < 2 {
		f, err := server.RecvFromDaemon(recvCtx)
		if err != nil {
			t.Fatalf("RecvFromDaemon: %v", err)
		}
		if f.FrameKind != daemonbus.FrameTypeControlHeartbeat {
			t.Fatalf("unexpected frame_type %q (want %q)", f.FrameKind, daemonbus.FrameTypeControlHeartbeat)
		}
		if f.DaemonID != "daemon-test" {
			t.Errorf("frame.DaemonID=%q want daemon-test", f.DaemonID)
		}
		if f.FrameID == "" {
			t.Errorf("frame.FrameID empty")
		}
		var body transit.HeartbeatBody
		if err := json.Unmarshal(f.Payload, &body); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if body.DaemonID != "daemon-test" {
			t.Errorf("body.DaemonID=%q want daemon-test", body.DaemonID)
		}
		if body.HeartbeatSeq <= 0 {
			t.Errorf("body.HeartbeatSeq=%d want > 0", body.HeartbeatSeq)
		}
		if len(body.HeldChannels) != 2 ||
			body.HeldChannels[0].ChannelID != "ch-a" ||
			body.HeldChannels[0].OwnerEpoch != 1 ||
			body.HeldChannels[0].FencingToken != "tok-a" ||
			body.HeldChannels[1].ChannelID != "ch-b" ||
			body.HeldChannels[1].OwnerEpoch != 2 ||
			body.HeldChannels[1].FencingToken != "tok-b" {
			t.Errorf("body.HeldChannels=%v want fencing tuples", body.HeldChannels)
		}
		got++
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Errorf("Run returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not exit after ctx cancel")
	}
}

func TestHeartbeatSender_NilChannelsFnSafe(t *testing.T) {
	client, server := newSenderClient(t)

	sender, err := transit.NewHeartbeatSender(transit.HeartbeatSenderConfig{
		Client:  client,
		Period:  time.Hour, // Period doesn't matter; we drive Emit directly
		FrameID: func() string { return "hb-1" },
		// Channels intentionally nil — must default to no-op (empty list)
	})
	if err != nil {
		t.Fatalf("NewHeartbeatSender: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sender.Emit(ctx); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	f, err := server.RecvFromDaemon(ctx)
	if err != nil {
		t.Fatalf("RecvFromDaemon: %v", err)
	}
	if f.FrameKind != daemonbus.FrameTypeControlHeartbeat {
		t.Fatalf("frame_type=%q", f.FrameKind)
	}
	var body transit.HeartbeatBody
	if err := json.Unmarshal(f.Payload, &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.HeldChannels) != 0 {
		t.Errorf("expected empty held_channels list, got %v", body.HeldChannels)
	}
}

func TestNewHeartbeatSender_Validation(t *testing.T) {
	client, _ := newSenderClient(t)
	cases := []struct {
		name string
		cfg  transit.HeartbeatSenderConfig
	}{
		{"nil client", transit.HeartbeatSenderConfig{FrameID: func() string { return "x" }}},
		{"nil frameID", transit.HeartbeatSenderConfig{Client: client}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := transit.NewHeartbeatSender(c.cfg); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}
