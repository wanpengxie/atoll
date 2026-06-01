package transit_test

import (
	"context"
	"testing"

	"github.com/wanpengxie/ActOS/framework/multiuser/daemonbus"
	"github.com/wanpengxie/ActOS/framework/multiuser/runtime/transit"
)

func TestHeartbeatTracker_AckReceived(t *testing.T) {
	h := transit.NewHeartbeatTracker()
	if h.LastAckAt() != 0 {
		t.Fatalf("initial LastAckAt = %d, want 0", h.LastAckAt())
	}
	h.AckReceived(100, "f-1")
	if h.LastAckAt() != 100 {
		t.Errorf("after first ack LastAckAt = %d", h.LastAckAt())
	}
	if h.LastFrameID() != "f-1" {
		t.Errorf("LastFrameID = %q", h.LastFrameID())
	}
	// Out-of-order older ack must not regress the watermark.
	h.AckReceived(50, "f-old")
	if h.LastAckAt() != 100 {
		t.Errorf("older ack regressed watermark to %d", h.LastAckAt())
	}
	// Newer ack moves forward.
	h.AckReceived(200, "f-2")
	if h.LastAckAt() != 200 || h.LastFrameID() != "f-2" {
		t.Errorf("state after newer ack: %d/%q", h.LastAckAt(), h.LastFrameID())
	}
}

func TestHeartbeatTracker_Handle(t *testing.T) {
	h := transit.NewHeartbeatTracker()
	nowFn := func() int64 { return 777 }
	handler := h.Handle(nowFn)
	if handler == nil {
		t.Fatal("Handle returned nil")
	}
	frame := daemonbus.Frame{FrameID: "hb-1", FrameKind: daemonbus.FrameTypeControlHeartbeatAck}
	if err := handler(context.Background(), frame); err != nil {
		t.Fatalf("handler err: %v", err)
	}
	if h.LastAckAt() != 777 {
		t.Errorf("LastAckAt = %d want 777", h.LastAckAt())
	}
	if h.LastFrameID() != "hb-1" {
		t.Errorf("LastFrameID = %q want hb-1", h.LastFrameID())
	}
}

func TestHeartbeatTracker_NilSafe(t *testing.T) {
	var h *transit.HeartbeatTracker
	// All methods must tolerate a nil receiver — defensive against
	// pre-assemble paths that read the field early.
	h.AckReceived(1, "")
	if got := h.LastAckAt(); got != 0 {
		t.Errorf("nil LastAckAt=%d", got)
	}
	if got := h.LastFrameID(); got != "" {
		t.Errorf("nil LastFrameID=%q", got)
	}
}
