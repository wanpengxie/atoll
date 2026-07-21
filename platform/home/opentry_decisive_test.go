package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Member-source rejections are noise: they terminate as the request's failed
// reply and leave ZERO rows in the operation ledger — repeating the same
// garbage grows nothing (no rejection-DDOS through the kernel serial section).
// Only operations that mutate values commit anchored event pairs.
func TestMemberWordRejectionsLeaveNoLedger(t *testing.T) {
	h, err := Open(Config{
		ChannelID:           "member-noise",
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

	declared, err := h.declare(ctx, DeclareRequest{
		SourceDeclID: "decl:probe", Principal: "probe", Class: "probe",
		Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	sender := declared.Row.ID

	// A single-transaction rollback leaves neither half of the pair; absence of
	// the completed terminal (the replay key) proves zero ledger footprint.
	assertNoTerminal := func(anchor, digest string) {
		t.Helper()
		_, found, err := h.opEntry.admission.LookupCompleted(ctx, channel.MessageCorrelation(anchor), digest)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			t.Fatalf("rejection for %q left a completed terminal", anchor)
		}
	}

	// Malformed payload: rejected in the entrance, zero ledger rows, repeats grow nothing.
	raw := json.RawMessage(`{"instance_id":""}`)
	for range 3 {
		if _, err := h.opEntry.Execute(ctx, sysactor.TypeRestartActor,
			sysactor.OperateRequest{ChannelID: h.channelID, Sender: sender, Anchor: "op-msg-bad", Payload: raw}); err == nil {
			t.Fatal("malformed restart unexpectedly succeeded")
		}
	}
	rawDigest, err := channel.Digest(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	assertNoTerminal("op-msg-bad", rawDigest)

	// Decisive in-store rejection (absent target): transaction rolls back whole.
	payload := map[string]string{"instance_id": "agent:absent:1"}
	encoded, _ := json.Marshal(payload)
	for range 3 {
		if _, err := h.opEntry.Execute(ctx, sysactor.TypeRestartActor,
			sysactor.OperateRequest{ChannelID: h.channelID, Sender: sender, Anchor: "op-msg-absent", Payload: encoded}); err == nil {
			t.Fatal("absent-target restart unexpectedly succeeded")
		}
	}
	parsedDigest, err := channel.Digest(payload)
	if err != nil {
		t.Fatal(err)
	}
	assertNoTerminal("op-msg-absent", parsedDigest)
}


