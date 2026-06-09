package xhs

import "testing"

func TestDescribeTypeMetadataIncludesContractFields(t *testing.T) {
	meta := DescribeTypeMetadata()

	req, ok := meta[TypePublish]
	if !ok {
		t.Fatalf("missing metadata for %s", TypePublish)
	}
	if len(req.AllowedKinds) != 1 || req.AllowedKinds[0] != "request" {
		t.Fatalf("request allowed_kinds = %v; want [request]", req.AllowedKinds)
	}
	if req.MaxPendingMs != DefaultMaxPendingMs {
		t.Fatalf("request max_pending_ms = %d; want %d", req.MaxPendingMs, DefaultMaxPendingMs)
	}

	ev, ok := meta[TypeDeviceOnline]
	if !ok {
		t.Fatalf("missing metadata for %s", TypeDeviceOnline)
	}
	if len(ev.AllowedKinds) != 1 || ev.AllowedKinds[0] != "event" {
		t.Fatalf("event allowed_kinds = %v; want [event]", ev.AllowedKinds)
	}
	if ev.MaxPendingMs != 0 {
		t.Fatalf("event max_pending_ms = %d; want 0", ev.MaxPendingMs)
	}
}
