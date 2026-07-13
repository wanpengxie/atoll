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

// TestFrameRoundTrip round-trips every frame type through Marshal/ParseFrame,
// asserts the version bit rides, and that an unknown frame_type is refused.
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
		{"feed", FrameFeed, FeedPayload{ChannelID: "c1", Seq: 5, Envelope: json.RawMessage(`{}`)}},
		{"receipt", FrameReceipt, SubmitReceipt{MessageID: "m1", Seq: 5}},
		{"error", FrameError, ErrorPayload{Frame: "submit", Code: CodeBadPayload, Detail: "bad"}},
	}
	if len(cases) != len(knownFrameTypes) {
		t.Fatalf("round-trip covers %d frames but %d are known", len(cases), len(knownFrameTypes))
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
			got, err := ParseFrame(b)
			if err != nil {
				t.Fatalf("ParseFrame: %v", err)
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

func TestFrameUnknownTypeRejected(t *testing.T) {
	b := []byte(`{"v":2,"frame_type":"teleport"}`)
	if _, err := ParseFrame(b); err == nil {
		t.Fatal("expected unknown frame_type to be refused")
	}
}

func TestFrameVersionRejected(t *testing.T) {
	// v1 (the pre-连接模型勘误期 envelope) is now refused — v2 is the current version.
	b := []byte(`{"v":1,"frame_type":"attach"}`)
	if _, err := ParseFrame(b); err == nil {
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
	// ParseFrame also refuses oversize bytes.
	over := make([]byte, MaxFrameBytes+1)
	if _, err := ParseFrame(over); err == nil {
		t.Fatal("expected oversize ParseFrame to be refused")
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

// TestResourceOutcomeJSON pins the three resource-result wire forms
// (outcome/stat/page) round-trip (DoD-12).
func TestResourceOutcomeJSON(t *testing.T) {
	out := ResourceOutcome{Status: "ok", Value: json.RawMessage(`{"v":1}`)}
	stat := ResourceStat{Exists: true, Meta: json.RawMessage(`{"size":9}`)}
	page := ResourcePage{Items: []json.RawMessage{json.RawMessage(`{"a":1}`), json.RawMessage(`{"b":2}`)}, Next: "cur2"}
	for _, v := range []any{out, stat, page} {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal %T: %v", v, err)
		}
		switch want := v.(type) {
		case ResourceOutcome:
			var got ResourceOutcome
			if err := json.Unmarshal(b, &got); err != nil || got.Status != want.Status {
				t.Fatalf("outcome round-trip: %v %+v", err, got)
			}
		case ResourceStat:
			var got ResourceStat
			if err := json.Unmarshal(b, &got); err != nil || got.Exists != want.Exists {
				t.Fatalf("stat round-trip: %v %+v", err, got)
			}
		case ResourcePage:
			var got ResourcePage
			if err := json.Unmarshal(b, &got); err != nil || len(got.Items) != 2 || got.Next != "cur2" {
				t.Fatalf("page round-trip: %v %+v", err, got)
			}
		}
	}
}
