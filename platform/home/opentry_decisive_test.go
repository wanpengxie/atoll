package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// Decisive rejections are account truth: malformed payloads and inactive
// senders must commit an anchored started/completed event pair through the
// word's value-op transaction, and a replay of the same request must land on
// the same terminal. The transport gate no longer pre-judges anything.
func TestMemberWordDecisiveRejectionsCommitEventPairs(t *testing.T) {
	h, err := Open(Config{
		ChannelID:           "member-decisive",
		DBPath:              filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: &compositionActivationResolver{},
		ReconcileInterval:   time.Hour,
		Bootstrap:           true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	assertCompleted := func(anchor, digest string, code channel.OperationErrorCode) {
		t.Helper()
		completed, found, err := h.opEntry.admission.LookupCompleted(ctx, channel.MessageCorrelation(anchor), digest)
		if err != nil || !found {
			t.Fatalf("decisive rejection left no completed terminal (found=%v err=%v)", found, err)
		}
		if completed.ErrorCode != code {
			t.Fatalf("completed code=%s want %s", completed.ErrorCode, code)
		}
	}

	// Malformed payload → bad_payload pair keyed by the raw-bytes digest.
	raw := json.RawMessage(`{"instance_id":""}`)
	if _, err := h.opEntry.Execute(ctx, sysactor.TypeRestartActor,
		sysactor.OperateRequest{ChannelID: h.channelID, Sender: "member:someone:1", Anchor: "op-msg-bad", Payload: raw}); err == nil {
		t.Fatal("malformed restart unexpectedly succeeded")
	}
	rawDigest, err := channel.Digest(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	assertCompleted("op-msg-bad", rawDigest, channel.ErrCodeBadPayload)
	// Replay of the same malformed request lands on the same terminal.
	if _, err := h.opEntry.Execute(ctx, sysactor.TypeRestartActor,
		sysactor.OperateRequest{ChannelID: h.channelID, Sender: "member:someone:1", Anchor: "op-msg-bad", Payload: raw}); err == nil {
		t.Fatal("malformed restart replay unexpectedly succeeded")
	}
	assertCompleted("op-msg-bad", rawDigest, channel.ErrCodeBadPayload)

	// Inactive sender → unauthorized_sender pair keyed by the parsed digest.
	payload := map[string]string{"instance_id": "agent:ghost-target:1"}
	encoded, _ := json.Marshal(payload)
	if _, err := h.opEntry.Execute(ctx, sysactor.TypeRestartActor,
		sysactor.OperateRequest{ChannelID: h.channelID, Sender: "member:ghost:1", Anchor: "op-msg-ghost", Payload: encoded}); err == nil {
		t.Fatal("inactive-sender restart unexpectedly succeeded")
	}
	parsedDigest, err := channel.Digest(payload)
	if err != nil {
		t.Fatal(err)
	}
	assertCompleted("op-msg-ghost", parsedDigest, channel.ErrCodeUnauthorizedSender)
}
