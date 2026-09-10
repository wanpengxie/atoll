package channelmember

import (
	"encoding/json"
	"testing"
)

func TestForwardingLimitsDefaultAndOverride(t *testing.T) {
	seat, err := ParseSeatConfig(json.RawMessage(`{"body":"body"}`))
	if err != nil {
		t.Fatal(err)
	}
	if seat.MaxConcurrency != DefaultMaxConcurrency || seat.QueueCapacity != DefaultQueueCapacity {
		t.Fatalf("seat defaults = %d/%d", seat.MaxConcurrency, seat.QueueCapacity)
	}
	handle, err := ParseHandleConfig(json.RawMessage(`{"host":"host","words":{},"max_concurrency":3,"queue_capacity":7}`))
	if err != nil {
		t.Fatal(err)
	}
	if handle.MaxConcurrency != 3 || handle.QueueCapacity != 7 {
		t.Fatalf("handle override = %d/%d", handle.MaxConcurrency, handle.QueueCapacity)
	}
}

func TestForwardingLimitsRejectUnsafeConfig(t *testing.T) {
	for _, raw := range []string{
		`{"body":"body","max_concurrency":-1}`,
		`{"body":"body","max_concurrency":65}`,
		`{"body":"body","queue_capacity":-1}`,
		`{"body":"body","queue_capacity":1025}`,
	} {
		if _, err := ParseSeatConfig(json.RawMessage(raw)); err == nil {
			t.Fatalf("ParseSeatConfig(%s) succeeded", raw)
		}
	}
}
