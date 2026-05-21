package daemonbus

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

// TestFrameTypeClosedSetCardinality asserts the L2 §9.1 closed-set
// cardinality. M1.5 = 4 viewsync + 19 control + 4 device_transit = 27.
//
// Exact values (in spec order) checked by TestAllFrameTypesSpecOrder.
func TestFrameTypeClosedSetCardinality(t *testing.T) {
	t.Parallel()

	if got := len(AllFrameTypes); got != 27 {
		t.Errorf("AllFrameTypes len = %d, want 27 (4 viewsync + 19 control + 4 device_transit)", got)
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
	if counts[CategoryControl] != 19 {
		t.Errorf("control frame count = %d, want 19", counts[CategoryControl])
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
		FrameTypeControlConnectionAccepted,
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

// TestFrameHeaderFieldSet locks the impl-layer2 §1.3 daemonbus mux
// outer envelope field set: 5 named header keys + 1 payload. The 5
// names match HeaderFields.
//
// Spec-canonical fields (impl-layer2 §1.3): frame_kind / frame_id /
// correlation_frame_id / channel_id / ts / payload.
//
// daemon_id / daemon_connection_epoch live on the Go struct as
// omitempty extras (see R5-20-spec-followup TODO on Frame); they are
// intentionally omitted from this test's input so the marshalled key
// set asserts against the spec-canonical subset.
//
// This is the verification artifact called out by m1.5-tickets.md §T1
// acceptance criteria — "所有 frame_kind 字段集".
func TestFrameHeaderFieldSet(t *testing.T) {
	t.Parallel()

	frame := Frame{
		FrameKind:          FrameTypeControlCreateChannel,
		FrameID:            "f-1",
		CorrelationFrameID: "f-0",
		ChannelID:          "ch-1",
		Ts:                 1700000000000,
		Payload:            json.RawMessage(`{"x":1}`),
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

	// HeaderFields cardinality (5) is part of impl-layer2 §1.3 contract
	// — guard against drift on the spec-side enumeration.
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
