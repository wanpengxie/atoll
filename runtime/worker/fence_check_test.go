package worker_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/fencing"
	"github.com/wanpengxie/ActOS/runtime/ipc"
	"github.com/wanpengxie/ActOS/runtime/worker"
)

// TestFenceInvalidErrorFormat — the Error() string MUST surface
// expected/got token/epoch + reason so daemon-side diagnostics can pin
// the worker-exit cause from logs alone.
func TestFenceInvalidErrorFormat(t *testing.T) {
	err := &worker.FenceInvalidError{
		FenceInvalidPayload: ipc.FenceInvalidPayload{
			ExpectedToken: fencing.FencingToken("tok-7"),
			GotToken:      fencing.FencingToken("tok-5"),
			ExpectedEpoch: fencing.DaemonEpoch(12),
			GotEpoch:      fencing.DaemonEpoch(10),
			Reason:        "stale-daemon",
		},
	}
	got := err.Error()
	for _, frag := range []string{"tok-7", "tok-5", "epoch=12", "epoch=10", "stale-daemon"} {
		if !strings.Contains(got, frag) {
			t.Errorf("Error()=%q missing fragment %q", got, frag)
		}
	}
}

// TestFenceFromFrameDecodes — FenceFromFrame returns *FenceInvalidError
// with the decoded payload, accessible via errors.As.
func TestFenceFromFrameDecodes(t *testing.T) {
	payload := ipc.FenceInvalidPayload{
		ExpectedToken: "tok-9",
		GotToken:      "tok-8",
		ExpectedEpoch: 3,
		GotEpoch:      2,
		Reason:        "fence-stale",
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	frame := ipc.Frame{Kind: ipc.KindFenceInvalid, Payload: raw}

	wrapped := worker.FenceFromFrame(frame)
	if wrapped == nil {
		t.Fatal("FenceFromFrame returned nil")
	}

	var fenceErr *worker.FenceInvalidError
	if !errors.As(wrapped, &fenceErr) {
		t.Fatalf("errors.As(*FenceInvalidError) failed for %T", wrapped)
	}
	if fenceErr.ExpectedToken != "tok-9" || fenceErr.GotToken != "tok-8" {
		t.Errorf("tokens=%q/%q want tok-9/tok-8", fenceErr.ExpectedToken, fenceErr.GotToken)
	}
	if fenceErr.ExpectedEpoch != 3 || fenceErr.GotEpoch != 2 {
		t.Errorf("epochs=%d/%d want 3/2", fenceErr.ExpectedEpoch, fenceErr.GotEpoch)
	}
	if fenceErr.Reason != "fence-stale" {
		t.Errorf("Reason=%q want fence-stale", fenceErr.Reason)
	}
}

// TestFenceFromFrameMalformedPayloadStillReturnsError — even with empty
// / corrupt payload bytes the function returns a non-nil error so the
// caller's errors.As branch is exercised. The fields stay zero.
func TestFenceFromFrameMalformedPayload(t *testing.T) {
	frame := ipc.Frame{Kind: ipc.KindFenceInvalid, Payload: []byte("not json")}
	err := worker.FenceFromFrame(frame)
	if err == nil {
		t.Fatal("FenceFromFrame on garbage payload returned nil")
	}
	var fenceErr *worker.FenceInvalidError
	if !errors.As(err, &fenceErr) {
		t.Fatalf("errors.As mismatch on garbage payload, got %T", err)
	}
	// Zero values OK; the test cares that the type is preserved.
	if fenceErr.Reason != "" {
		t.Errorf("Reason=%q want empty on malformed payload", fenceErr.Reason)
	}
}
