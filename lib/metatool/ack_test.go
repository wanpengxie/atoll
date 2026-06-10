package metatool_test

import (
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/lib/metatool"
)

func TestResolveFastPathWindow(t *testing.T) {
	tests := []struct {
		name           string
		typeTimeout    time.Duration
		defaultTimeout time.Duration
		waitUnbounded  bool
		want           time.Duration
	}{
		{
			name:           "bounded, type timeout > fast path",
			typeTimeout:    30 * time.Second,
			defaultTimeout: 30 * time.Second,
			waitUnbounded:  false,
			want:           metatool.FastPathWindow,
		},
		{
			name:           "bounded, type timeout < fast path",
			typeTimeout:    5 * time.Second,
			defaultTimeout: 30 * time.Second,
			waitUnbounded:  false,
			want:           5 * time.Second,
		},
		{
			name:           "unbounded uses type timeout",
			typeTimeout:    60 * time.Second,
			defaultTimeout: 30 * time.Second,
			waitUnbounded:  true,
			want:           60 * time.Second,
		},
		{
			name:           "zero type timeout uses default",
			typeTimeout:    0,
			defaultTimeout: 20 * time.Second,
			waitUnbounded:  true,
			want:           20 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := metatool.ResolveFastPathWindow(tt.typeTimeout, tt.defaultTimeout, tt.waitUnbounded)
			if got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAckResult(t *testing.T) {
	ack := metatool.AckDescriptor{
		RequestID: "req-ack",
		Accepted:  true,
		Status:    "accepted",
		EstWaitMs: 15000,
	}
	rv := metatool.AckResult("call_actor", ack)
	if rv.Name != "call_actor" {
		t.Fatalf("expected name call_actor, got %q", rv.Name)
	}
	if rv.Value["status"] != "accepted" {
		t.Fatalf("expected status=accepted, got %v", rv.Value["status"])
	}
	if rv.Value["accepted"] != true {
		t.Fatalf("expected accepted=true, got %v", rv.Value["accepted"])
	}
	if rv.Value["request_id"] != "req-ack" {
		t.Fatalf("expected request_id=req-ack, got %v", rv.Value["request_id"])
	}
}
