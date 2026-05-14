package daemonbus

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

// TestFrameTypeClosedSetCardinality asserts the L2 §9.1 closed-set
// cardinality. M1.5 = 4 viewsync + 18 control + 4 device_transit = 26.
//
// Exact values (in spec order) checked by TestAllFrameTypesSpecOrder.
func TestFrameTypeClosedSetCardinality(t *testing.T) {
	t.Parallel()

	if got := len(AllFrameTypes); got != 26 {
		t.Errorf("AllFrameTypes len = %d, want 26 (4 viewsync + 18 control + 4 device_transit)", got)
	}

	// Per-category breakdown — keeps the failure message specific when
	// drift happens.
	var counts = map[Category]int{}
	for _, ft := range AllFrameTypes {
		counts[CategoryOf(ft)]++
	}
	if counts[CategoryViewsync] != 4 {
		t.Errorf("viewsync frame count = %d, want 4", counts[CategoryViewsync])
	}
	if counts[CategoryControl] != 18 {
		t.Errorf("control frame count = %d, want 18", counts[CategoryControl])
	}
	if counts[CategoryDeviceTransit] != 4 {
		t.Errorf("device_transit frame count = %d, want 4", counts[CategoryDeviceTransit])
	}
}

// TestAllFrameTypesSpecOrder asserts the AllFrameTypes slice carries
// the exact L2 §9.1 spec value list in spec order.
func TestAllFrameTypesSpecOrder(t *testing.T) {
	t.Parallel()

	want := []FrameType{
		// view-sync
		FrameTypeViewsyncPush,
		FrameTypeViewsyncAck,
		FrameTypeViewsyncResyncRequest,
		FrameTypeViewsyncResyncResponse,
		// control plane
		FrameTypeControlCreateChannel,
		FrameTypeControlCreateChannelAck,
		FrameTypeControlUnbindChannel,
		FrameTypeControlUnbindChannelAck,
		FrameTypeControlHeartbeat,
		FrameTypeControlHeartbeatAck,
		FrameTypeControlDaemonReclaim,
		FrameTypeControlReclaimAccepted,
		FrameTypeControlReclaimRejected,
		FrameTypeControlRejectChannel,
		FrameTypeControlBindDeviceSession,
		FrameTypeControlBindDeviceSessionAck,
		FrameTypeControlUnbindDeviceSession,
		FrameTypeControlUnbindDeviceSessionAck,
		FrameTypeControlUpdateMembers,
		FrameTypeControlUpdateMembersAck,
		FrameTypeControlWriteMessage,
		FrameTypeControlWriteMessageAck,
		// device transit
		FrameTypeDeviceTransitSend,
		FrameTypeDeviceTransitRecv,
		FrameTypeDeviceTransitAck,
		FrameTypeDeviceTransitError,
	}
	if len(AllFrameTypes) != len(want) {
		t.Fatalf("AllFrameTypes len = %d, want %d", len(AllFrameTypes), len(want))
	}
	for i, ft := range AllFrameTypes {
		if ft != want[i] {
			t.Errorf("AllFrameTypes[%d] = %q, want %q", i, ft, want[i])
		}
	}
}

// TestFrameTypeNamingConvention asserts every frame_type wire form
// follows the L2 §9.1 prefix convention: viewsync.* / control.* /
// device_transit.* — anything outside the three prefixes is a closed-set
// drift.
func TestFrameTypeNamingConvention(t *testing.T) {
	t.Parallel()

	for _, ft := range AllFrameTypes {
		s := string(ft)
		switch {
		case strings.HasPrefix(s, "viewsync."):
		case strings.HasPrefix(s, "control."):
		case strings.HasPrefix(s, "device_transit."):
		default:
			t.Errorf("frame_type %q has no recognized L2 §9.1 prefix", ft)
		}
	}
}

// TestCategoryOfRoundtripping asserts CategoryOf returns the right
// Category for every value in AllFrameTypes.
func TestCategoryOfRoundtripping(t *testing.T) {
	t.Parallel()

	for _, ft := range AllFrameTypes {
		got := CategoryOf(ft)
		var want Category
		s := string(ft)
		switch {
		case strings.HasPrefix(s, "viewsync."):
			want = CategoryViewsync
		case strings.HasPrefix(s, "control."):
			want = CategoryControl
		case strings.HasPrefix(s, "device_transit."):
			want = CategoryDeviceTransit
		}
		if got != want {
			t.Errorf("CategoryOf(%q) = %q, want %q", ft, got, want)
		}
	}

	if CategoryOf("unknown.frame") != "" {
		t.Errorf("CategoryOf(unknown) should return empty string")
	}
}

// TestFrameHeaderFieldSet locks the L2 §9.2 daemonbus mux header field
// set: 5 named header keys + 1 payload. The 5 names match HeaderFields.
//
// This is the verification artifact called out by m1.5-tickets.md §T1
// acceptance criteria — "所有 frame_type 字段集".
func TestFrameHeaderFieldSet(t *testing.T) {
	t.Parallel()

	frame := Frame{
		FrameID:               "f-1",
		FrameType:             FrameTypeControlCreateChannel,
		DaemonID:              "daemon-1",
		DaemonConnectionEpoch: 7,
		SentAt:                1700000000000,
		Payload:               json.RawMessage(`{"x":1}`),
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	var asMap map[string]json.RawMessage
	if err := json.Unmarshal(raw, &asMap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	want := append([]string{}, HeaderFields...)
	want = append(want, "payload")
	sort.Strings(want)

	got := make([]string, 0, len(asMap))
	for k := range asMap {
		got = append(got, k)
	}
	sort.Strings(got)

	if len(got) != len(want) {
		t.Fatalf("frame keys mismatch:\n  got  %v\n  want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("frame key drift at %d: got %q, want %q", i, got[i], want[i])
		}
	}

	// HeaderFields cardinality (5) is part of L2 §9.2 contract — guard
	// against drift on the spec-side enumeration.
	if len(HeaderFields) != 5 {
		t.Errorf("HeaderFields len = %d, want 5", len(HeaderFields))
	}
}

// TestFrameTypeUniqueness asserts no two frame_type values collide.
// Drift / accidental duplication trips the test.
func TestFrameTypeUniqueness(t *testing.T) {
	t.Parallel()

	seen := make(map[FrameType]struct{}, len(AllFrameTypes))
	for _, ft := range AllFrameTypes {
		if _, dup := seen[ft]; dup {
			t.Errorf("duplicate frame_type %q", ft)
		}
		seen[ft] = struct{}{}
	}
}
