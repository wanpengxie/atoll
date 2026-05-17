package transit_test

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/runtime/transit"
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
	channels := []channel.ID{"ch-a", "ch-b"}
	sender, err := transit.NewHeartbeatSender(transit.HeartbeatSenderConfig{
		Client:  client,
		Period:  10 * time.Millisecond,
		FrameID: frameID,
		Channels: func() []channel.ID {
			out := make([]channel.ID, len(channels))
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
		if f.FrameType != daemonbus.FrameTypeControlHeartbeat {
			t.Fatalf("unexpected frame_type %q (want %q)", f.FrameType, daemonbus.FrameTypeControlHeartbeat)
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
		if len(body.Channels) != 2 || body.Channels[0] != "ch-a" || body.Channels[1] != "ch-b" {
			t.Errorf("body.Channels=%v want [ch-a ch-b]", body.Channels)
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
	if f.FrameType != daemonbus.FrameTypeControlHeartbeat {
		t.Fatalf("frame_type=%q", f.FrameType)
	}
	var body transit.HeartbeatBody
	if err := json.Unmarshal(f.Payload, &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Channels) != 0 {
		t.Errorf("expected empty channels list, got %v", body.Channels)
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
