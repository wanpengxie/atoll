package daemonbus

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/framework/multiuser/placement"
)

// TestFrameTypeClosedSetCardinality asserts the impl-layer2 §1.2 closed-set
// cardinality. current = 4 viewsync + 17 control + 4 device_transit = 25.
//
// Exact values (in spec order) checked by TestAllFrameTypesSpecOrder.
func TestFrameTypeClosedSetCardinality(t *testing.T) {
	t.Parallel()

	if got := len(AllFrameTypes); got != 25 {
		t.Errorf("AllFrameTypes len = %d, want 25 (4 viewsync + 17 control + 4 device_transit)", got)
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
	if counts[CategoryControl] != 17 {
		t.Errorf("control frame count = %d, want 17", counts[CategoryControl])
	}
	if counts[CategoryDeviceTransit] != 4 {
		t.Errorf("device_transit frame count = %d, want 4", counts[CategoryDeviceTransit])
	}
}

// TestAllFrameTypesSpecOrder asserts the AllFrameTypes slice carries
// the exact impl-layer2 §1.2 spec value list in spec order.
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
		FrameTypeControlHeldChannelsReport,
		FrameTypeControlHeldChannelsAck,
		FrameTypeControlDaemonReclaim,
		FrameTypeControlReclaimAccepted,
		FrameTypeControlReclaimRejected,
		FrameTypeControlRejectChannel,
		FrameTypeControlUpdateMembers,
		FrameTypeControlUpdateMembersAck,
		FrameTypeControlWriteMessage,
		FrameTypeControlWriteMessageAck,
		// device transit
		FrameTypeDeviceTransitSend,
		FrameTypeDeviceTransitRecv,
		FrameTypeDeviceTransitAck,
		FrameTypeDeviceTransitLifecycle,
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
// follows the impl-layer2 §1.2 prefix convention: viewsync.* / control.* /
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
			t.Errorf("frame_type %q has no recognized impl-layer2 §1.2 prefix", ft)
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

func TestUnbindChannelPayloadABI(t *testing.T) {
	t.Parallel()

	body := UnbindChannelBody{
		ChannelID:  "ch-1",
		OwnerEpoch: 7,
		Reason:     UnbindChannelReasonAbandon,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	got := string(raw)
	for _, key := range []string{`"channel_id"`, `"owner_epoch"`, `"reason"`} {
		if !strings.Contains(got, key) {
			t.Fatalf("UnbindChannelBody json=%s missing %s", got, key)
		}
	}

	ack := UnbindChannelAckBody{
		ChannelID:  "ch-1",
		OwnerEpoch: 7,
		Result:     UnbindChannelRejected,
		Reason:     UnbindChannelRejectOwnerEpochStale,
	}
	raw, err = json.Marshal(ack)
	if err != nil {
		t.Fatal(err)
	}
	got = string(raw)
	for _, key := range []string{`"channel_id"`, `"owner_epoch"`, `"result"`, `"reason"`} {
		if !strings.Contains(got, key) {
			t.Fatalf("UnbindChannelAckBody json=%s missing %s", got, key)
		}
	}
}

// TestFrameHeaderFieldSet locks the impl-layer2 §1.3 daemonbus mux
// outer envelope field set: 8 named header keys + 1 payload. The 8
// names match HeaderFields.
//
// Spec-canonical fields (impl-layer2 §1.3): frame_kind / frame_id /
// correlation_frame_id / request_id / channel_id / daemon_id /
// daemon_connection_epoch / ts / payload.
//
// This is the verification artifact called out by launch-ticket notes §T1
// acceptance criteria — "所有 frame_kind 字段集".
func TestFrameHeaderFieldSet(t *testing.T) {
	t.Parallel()

	frame := Frame{
		FrameKind:             FrameTypeControlCreateChannel,
		FrameID:               "f-1",
		CorrelationFrameID:    "f-0",
		RequestID:             "req-1",
		ChannelID:             "ch-1",
		DaemonID:              placement.DaemonID("daemon-1"),
		DaemonConnectionEpoch: 42,
		Ts:                    1700000000000,
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

	// HeaderFields cardinality (8) is part of impl-layer2 §1.3 contract
	// — guard against drift on the spec-side enumeration.
	if len(HeaderFields) != 8 {
		t.Errorf("HeaderFields len = %d, want 8", len(HeaderFields))
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
