package gateway

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/platform/subjectgate"
)

// TestFrameRoundTrip exercises the wire contract through the SAME platform-exported
// aliases the gateway + connector speak (DoD-12 "drivers/gateway 包", S2 申报 #2 的
// follow-up): a marshaled frame re-parses with version + closed frame_type intact,
// and an unknown frame_type / bad version is refused fail-closed.
func TestFrameRoundTrip(t *testing.T) {
	f, err := subjectgate.NewFrame(subjectgate.FrameSubmit, 7, "ref-1", subjectgate.SubmitPayload{
		MsgType: "chat.text", Kind: "event", Payload: json.RawMessage(`{"text":"hi"}`),
	})
	if err != nil {
		t.Fatalf("NewFrame: %v", err)
	}
	b, err := f.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := subjectgate.ParseFrame(b)
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	if got.V != subjectgate.FrameVersion || got.Type != subjectgate.FrameSubmit || got.BindingGen != 7 || got.Ref != "ref-1" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	var sp subjectgate.SubmitPayload
	if err := got.DecodePayload(&sp); err != nil || sp.MsgType != "chat.text" {
		t.Fatalf("payload round-trip: %+v err=%v", sp, err)
	}

	// Unknown frame_type is refused.
	if _, err := subjectgate.ParseFrame([]byte(`{"v":1,"frame_type":"bogus"}`)); err == nil {
		t.Fatal("unknown frame_type must be refused")
	}
	// Bad version is refused.
	if _, err := subjectgate.ParseFrame([]byte(`{"v":2,"frame_type":"submit"}`)); err == nil {
		t.Fatal("unsupported version must be refused")
	}
}

// TestFrameSizeLimit: a serialized frame over the 512KB cap is refused (DoD-12).
func TestFrameSizeLimit(t *testing.T) {
	big := strings.Repeat("x", subjectgate.MaxFrameBytes)
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, 0, "", subjectgate.SubmitPayload{
		MsgType: "chat.text", Payload: json.RawMessage(`"` + big + `"`),
	})
	if _, err := f.Marshal(); err == nil {
		t.Fatal("a frame over MaxFrameBytes must be refused at Marshal")
	}
}

// TestResourceOutcomeJSON: the three resource-result forms serialize as the wire
// contract expects (DoD-12).
func TestResourceOutcomeJSON(t *testing.T) {
	out, _ := json.Marshal(subjectgate.ResourceOutcome{Status: "ok", Value: json.RawMessage(`{"k":1}`)})
	if !strings.Contains(string(out), `"status":"ok"`) {
		t.Fatalf("ResourceOutcome json = %s", out)
	}
	st, _ := json.Marshal(subjectgate.ResourceStat{Exists: true, Meta: json.RawMessage(`{}`)})
	if !strings.Contains(string(st), `"exists":true`) {
		t.Fatalf("ResourceStat json = %s", st)
	}
	pg, _ := json.Marshal(subjectgate.ResourcePage{Items: []json.RawMessage{json.RawMessage(`{"a":1}`)}, Next: "cur"})
	if !strings.Contains(string(pg), `"next":"cur"`) {
		t.Fatalf("ResourcePage json = %s", pg)
	}
}
