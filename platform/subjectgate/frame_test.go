package subjectgate

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// TestFrameCarriesNoIdentity (DoD-7 帧不携身份锚, 表⑤法度墙): the wire envelope and
// every upstream payload carry NO sender/identity/principal field. A subject's
// identity is bound at the authenticated connection (the槽/cell's own from-log
// authorization), never asserted by the client in a frame — so a client can never
// forge who it is. This is a structural regression guard: adding a sender-shaped
// field to any frame type must fail here.
func TestFrameCarriesNoIdentity(t *testing.T) {
	forbidden := []string{"sender", "identity", "principal", "actor_id", "actorid", "from", "author", "on_behalf", "impersonat"}
	types := []reflect.Type{
		reflect.TypeOf(Frame{}),
		reflect.TypeOf(AttachPayload{}),
		reflect.TypeOf(SubmitPayload{}),
		reflect.TypeOf(ResolvePayload{}),
		reflect.TypeOf(CancelPayload{}),
		reflect.TypeOf(AfterPayload{}),
		reflect.TypeOf(CancelTimerPayload{}),
		reflect.TypeOf(ResourcePayload{}),
		reflect.TypeOf(ObservePayload{}),
		reflect.TypeOf(UnobservePayload{}),
	}
	for _, ty := range types {
		for i := 0; i < ty.NumField(); i++ {
			f := ty.Field(i)
			tag := strings.ToLower(f.Tag.Get("json"))
			name := strings.ToLower(f.Name)
			for _, bad := range forbidden {
				if strings.Contains(tag, bad) || strings.Contains(name, bad) {
					t.Fatalf("%s.%s (json=%q) looks like an identity field — frames must NOT carry sender identity (帧不携身份, DoD-7)",
						ty.Name(), f.Name, f.Tag.Get("json"))
				}
			}
		}
	}
}

func TestNewErrorFrameCarriesFlatContract(t *testing.T) {
	f := NewErrorFrame("ref-7", string(FrameSubmit), CodeForbidden, "denied")
	if f.V != FrameVersion || f.Type != FrameError || f.Ref != "ref-7" {
		t.Fatalf("bad envelope: %#v", f)
	}
	var payload ErrorPayload
	if err := f.DecodePayload(&payload); err != nil {
		t.Fatal(err)
	}
	if payload.Frame != string(FrameSubmit) || payload.Code != CodeForbidden || payload.Detail != "denied" {
		t.Fatalf("bad payload: %#v", payload)
	}
}

// TestFrameRoundTrip round-trips every frame type through tolerant envelope
// parsing and asserts the version bit rides.
func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		typ  FrameType
		load any
	}{
		{"attach", FrameAttach, AttachPayload{Since: map[string]int64{"c1": 7}}},
		{"submit", FrameSubmit, SubmitPayload{ChannelID: "c1", MsgType: "human.message", Kind: "request", Audience: []string{"a"}, Payload: json.RawMessage(`{"x":1}`)}},
		{"resolve", FrameResolve, ResolvePayload{ChannelID: "c1", ReqID: "r1", Decision: "approved"}},
		{"cancel", FrameCancel, CancelPayload{ChannelID: "c1", ReqID: "r1"}},
		{"after", FrameAfter, AfterPayload{ChannelID: "c1", DurationMs: 1000, MsgType: "wake"}},
		{"cancel_timer", FrameCancelTimer, CancelTimerPayload{ChannelID: "c1", TimerID: "t1"}},
		{"resource", FrameResource, ResourcePayload{ChannelID: "c1", Op: ResRead, ResourceID: "res:1"}},
		{"observe", FrameObserve, ObservePayload{ChannelID: "c1"}},
		{"unobserve", FrameUnobserve, UnobservePayload{ChannelID: "c1"}},
		{"feed", FrameFeed, FeedPayload{ChannelID: "c1", Seq: 5, Envelope: json.RawMessage(`{}`)}},
		{"receipt", FrameReceipt, SubmitReceipt{MessageID: "m1"}},
		{"error", FrameError, ErrorPayload{Frame: "submit", Code: CodeBadPayload, Detail: "bad"}},
		{"observe_ended", FrameObserveEnded, ObserveEndedPayload{ChannelID: "c1", Reason: ObserveEndedNowMember}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := NewFrame(tc.typ, "ref-1", tc.load)
			if err != nil {
				t.Fatalf("NewFrame: %v", err)
			}
			b, err := f.Marshal()
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			got, err := ParseEnvelope(b)
			if err != nil {
				t.Fatalf("ParseEnvelope: %v", err)
			}
			if got.V != FrameVersion {
				t.Fatalf("version bit lost: %d", got.V)
			}
			if got.Type != tc.typ || got.Ref != "ref-1" {
				t.Fatalf("envelope mismatch: %+v", got)
			}
		})
	}
}

func TestDirectionalUnknownTypePolicy(t *testing.T) {
	b := []byte(`{"v":2,"frame_type":"teleport","payload":{"future":true}}`)
	if _, err := ParseUpstreamFrame(b); !errors.Is(err, ErrUnknownFrameType) {
		t.Fatalf("upstream unknown kind must fail closed, got %v", err)
	}
	down, err := ParseDownstream(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := down.(UnknownFrame); !ok {
		t.Fatalf("downstream unknown kind must use UnknownFrame, got %T", down)
	}
}

func TestUpstreamUnknownFieldsRejected(t *testing.T) {
	for _, b := range []string{
		`{"v":2,"frame_type":"attach","ref":"r1","unexpected":true,"payload":{}}`,
		`{"v":2,"frame_type":"attach","ref":"r1","payload":{"since":{},"unexpected":true}}`,
		`{"v":2,"frame_type":"submit","ref":"r1","payload":{"channel_id":"c1","msg_type":"m","unexpected":true}}`,
		`{"v":2,"frame_type":"resolve","ref":"r1","payload":{"channel_id":"c1","req_id":"q1","decision":"ok","unexpected":true}}`,
		`{"v":2,"frame_type":"cancel","ref":"r1","payload":{"channel_id":"c1","req_id":"q1","unexpected":true}}`,
		`{"v":2,"frame_type":"after","ref":"r1","payload":{"channel_id":"c1","duration_ms":1,"msg_type":"m","unexpected":true}}`,
		`{"v":2,"frame_type":"cancel_timer","ref":"r1","payload":{"channel_id":"c1","timer_id":"t1","unexpected":true}}`,
		`{"v":2,"frame_type":"resource","ref":"r1","payload":{"channel_id":"c1","op":"read","resource_id":"r1","unexpected":true}}`,
		`{"v":2,"frame_type":"observe","ref":"r1","payload":{"channel_id":"c1","unexpected":true}}`,
		`{"v":2,"frame_type":"unobserve","ref":"r1","payload":{"channel_id":"c1","unexpected":true}}`,
	} {
		f, err := ParseUpstreamFrame([]byte(b))
		if err == nil {
			t.Fatalf("unknown upstream field accepted: %s", b)
		}
		if f.Ref != "r1" {
			t.Fatalf("readable ref must survive validation failure: %#v", f)
		}
	}
}

func TestDownstreamUnknownFieldsIgnored(t *testing.T) {
	b := []byte(`{"v":2,"frame_type":"feed","future_envelope":true,"payload":{"channel_id":"c1","seq":1,"envelope":{},"future_payload":true}}`)
	down, err := ParseDownstream(b)
	if err != nil {
		t.Fatal(err)
	}
	feed, ok := down.(FeedFrame)
	if !ok {
		t.Fatalf("got %T", down)
	}
	var payload FeedPayload
	if err := feed.DecodePayload(&payload); err != nil || payload.ChannelID != "c1" {
		t.Fatalf("downstream additive payload failed: %#v, %v", payload, err)
	}
}

func TestFrameVersionRejected(t *testing.T) {
	// v1 (the pre-连接模型勘误期 envelope) is now refused — v2 is the current version.
	b := []byte(`{"v":1,"frame_type":"attach"}`)
	if _, err := ParseEnvelope(b); err == nil {
		t.Fatal("expected version mismatch to be refused")
	}
}

func TestFrameSizeLimit(t *testing.T) {
	big := strings.Repeat("x", MaxFrameBytes)
	f, err := NewFrame(FrameSubmit, "", SubmitPayload{ChannelID: "c1", MsgType: "m", Payload: json.RawMessage(`"` + big + `"`)})
	if err != nil {
		t.Fatalf("NewFrame: %v", err)
	}
	if _, err := f.Marshal(); err == nil {
		t.Fatal("expected oversize frame Marshal to be refused")
	}
	// ParseEnvelope also refuses oversize bytes.
	over := make([]byte, MaxFrameBytes+1)
	if _, err := ParseEnvelope(over); err == nil {
		t.Fatal("expected oversize ParseEnvelope to be refused")
	}
}

func TestAttachReceiptCarriesContractVersion(t *testing.T) {
	f, err := NewFrame(FrameReceipt, "attach-ref", AttachReceipt{ContractVersion: "1.0"})
	if err != nil {
		t.Fatal(err)
	}
	var got AttachReceipt
	if err := f.DecodePayload(&got); err != nil {
		t.Fatal(err)
	}
	if got.ContractVersion != "1.0" || f.Ref != "attach-ref" {
		t.Fatalf("bad attach receipt: %#v %#v", f, got)
	}
}

// TestRequireChannelID pins the连接模型勘误期 v2 required-field validator (§S1, DoD-4):
// every business frame's channel_id is required — absent / empty / whitespace-only is
// rejected with the typed ErrMissingChannelID (mapped to bad_payload by the upper
// layer). A non-blank id passes. "Required" is a validation layer, not a struct tag: a
// Go decode turns an absent field into an empty string, so the validator — not the
// decoder — is the enforcement point.
func TestRequireChannelID(t *testing.T) {
	rejected := []string{"", " ", "\t", "\n", "  \t \n "}
	for _, cid := range rejected {
		if err := RequireChannelID(cid); !errors.Is(err, ErrMissingChannelID) {
			t.Fatalf("RequireChannelID(%q) must reject with ErrMissingChannelID, got %v", cid, err)
		}
	}
	for _, cid := range []string{"c1", " c1 ", "channel:abc"} {
		if err := RequireChannelID(cid); err != nil {
			t.Fatalf("RequireChannelID(%q) must pass, got %v", cid, err)
		}
	}
}

// The three resource-result forms are read by clients written in another
// language, so what is pinned here is the TEXT, not a Go round-trip. A
// round-trip passes no matter what the fields are called — it marshals and
// unmarshals with the same struct, so it agrees with itself by construction.
// That is how a page of entries reached browsers spelled "ID"/"Kind"/"Ops":
// the entries were json.Marshal'd straight off an internal door struct that
// had no tags, and every Go-side test still passed.
func TestResourceResultsAreSpelledForTheWire(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{
			name:  "outcome",
			value: ResourceOutcome{Status: "ok", ResourceID: "daemon://host/c0/a.png", Ticket: "t-1", Redeem: "http"},
			want:  `{"status":"ok","resource_id":"daemon://host/c0/a.png","ticket":"t-1","redeem":"http"}`,
		},
		{
			name:  "stat",
			value: ResourceStat{Exists: true, Meta: &ResourceMeta{Kind: "file", CreatedAt: 7, CreatedBy: "human:root:1", Size: 9, ModifiedAt: 1787245669712}},
			want:  `{"exists":true,"meta":{"kind":"file","created_at":7,"created_by":"human:root:1","size":9,"modified_at":1787245669712}}`,
		},
		{
			// A device that reports no mtime omits the field rather than sending
			// zero: a file listing that shows "1970" is worse than one that shows
			// nothing where the date should be.
			name:  "stat without a modified time",
			value: ResourceStat{Exists: true, Meta: &ResourceMeta{Kind: "file", Size: 9}},
			want:  `{"exists":true,"meta":{"kind":"file","size":9}}`,
		},
		{
			// A file listing carries its sizes: the device answers them with the
			// names, and dropping them made every reader stat each row to fill
			// in a file list.
			name:  "page",
			value: ResourcePage{Items: []ResourceEntry{{ID: "daemon://host/c0/a.png", Kind: "file", Ops: []string{"read", "write"}, Meta: ResourceMeta{Size: 9, ModifiedAt: 1787245669712}}}, Next: "cur2"},
			want:  `{"items":[{"id":"daemon://host/c0/a.png","kind":"file","ops":["read","write"],"meta":{"size":9,"modified_at":1787245669712}}],"next":"cur2"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(b) != tc.want {
				t.Fatalf("on the wire:\n got %s\nwant %s", b, tc.want)
			}
		})
	}
}
