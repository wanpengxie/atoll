package store

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func openVisibleMessages(t *testing.T) (*messages, func(string, string)) {
	t.Helper()
	db, err := openChannelDB(context.Background(), filepath.Join(t.TempDir(), "visible.sqlite"), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	register := func(id, principal string) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO actor_registry(actor_id,actor_kind,principal,class,placement,created_at) VALUES (?,?,?,?,?,1)`, id, "human", principal, "human", "server"); err != nil {
			t.Fatal(err)
		}
	}
	return newMessages(db, nil), register
}

func visibleEnvelope(id string, sender actor.ActorID, visibility message.Visibility, audience ...actor.ActorID) *message.Envelope {
	return &message.Envelope{
		ID: message.ID(id), TS: 1, TSReceived: 1, ChannelID: channel.ID("visible"),
		Sender: message.Sender{Kind: actor.KindHuman, ID: sender}, Kind: message.KindEvent,
		Type: "visible.test", Payload: []byte(`{}`), Visibility: visibility, Audience: message.Audience(audience),
	}
}

func TestReadVisibleAfterSeqFiltersSystemBeforeLimitAndSharesPublicTruth(t *testing.T) {
	ctx := context.Background()
	messages, register := openVisibleMessages(t)
	register("alice", "principal-a")
	register("bob", "principal-b")
	register("carol", "principal-c")

	const batch = 8
	for i := 0; i < 2*batch+1; i++ {
		if _, err := messages.Append(ctx, visibleEnvelope("system-"+string(rune('a'+i)), "alice", message.VisibilitySystem), false, storespec.AppendMetadata{}); err != nil {
			t.Fatal(err)
		}
	}
	firstPublicSeq, err := messages.Append(ctx, visibleEnvelope("public-empty-audience", "alice", message.VisibilityPublic), false, storespec.AppendMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	publicSeq, err := messages.Append(ctx, visibleEnvelope("public", "alice", message.VisibilityPublic), false, storespec.AppendMetadata{})
	if err != nil {
		t.Fatal(err)
	}

	rows, scanned, err := messages.ReadVisibleAfterSeq(ctx, 0, batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Seq != int64(firstPublicSeq.Seq) || rows[1].Seq != int64(publicSeq.Seq) || scanned != int64(publicSeq.Seq) {
		t.Fatalf("bob rows=%v scanned=%d", rowSeqs(rows), scanned)
	}

	rows, _, err = messages.ReadVisibleAfterSeq(ctx, 0, batch)
	if err != nil || len(rows) != 2 {
		t.Fatalf("carol rows=%v err=%v", rowSeqs(rows), err)
	}

	rows, _, err = messages.ReadVisibleAfterSeq(ctx, 0, batch)
	if err != nil || len(rows) != 2 {
		t.Fatalf("sender observer rows=%v err=%v", rowSeqs(rows), err)
	}

	rows, _, err = messages.ReadVisibleAfterSeq(ctx, 0, batch)
	if err != nil || len(rows) != 2 {
		t.Fatalf("audience observer rows=%v err=%v", rowSeqs(rows), err)
	}
}

func TestReadVisibleAfterSeqSnapshotCursorDoesNotSkipLaterInsert(t *testing.T) {
	ctx := context.Background()
	messages, register := openVisibleMessages(t)
	register("alice", "principal-a")
	if _, err := messages.Append(ctx, visibleEnvelope("first", "alice", message.VisibilityPublic), false, storespec.AppendMetadata{}); err != nil {
		t.Fatal(err)
	}
	rows, scanned, err := messages.ReadVisibleAfterSeq(ctx, 0, 100)
	if err != nil || len(rows) != 1 {
		t.Fatalf("first read rows=%v scanned=%d err=%v", rowSeqs(rows), scanned, err)
	}
	second, err := messages.Append(ctx, visibleEnvelope("concurrent-boundary", "alice", message.VisibilityPublic), false, storespec.AppendMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	rows, next, err := messages.ReadVisibleAfterSeq(ctx, scanned, 100)
	if err != nil || len(rows) != 1 || rows[0].Seq != int64(second.Seq) || next != int64(second.Seq) {
		t.Fatalf("boundary read rows=%v next=%d err=%v", rowSeqs(rows), next, err)
	}
}

func TestReadVisibleBeforeSeqBoundsTailAndPagesBackwards(t *testing.T) {
	ctx := context.Background()
	messages, register := openVisibleMessages(t)
	register("alice", "principal-a")

	publicSeqs := make([]int64, 0, 5)
	for i := 0; i < 5; i++ {
		row, err := messages.Append(ctx, visibleEnvelope("public-"+string(rune('a'+i)), "alice", message.VisibilityPublic), false, storespec.AppendMetadata{})
		if err != nil {
			t.Fatal(err)
		}
		publicSeqs = append(publicSeqs, int64(row.Seq))
		if i == 2 {
			if _, err := messages.Append(ctx, visibleEnvelope("system-gap", "alice", message.VisibilitySystem), false, storespec.AppendMetadata{}); err != nil {
				t.Fatal(err)
			}
		}
	}

	rows, head, hasOlder, err := messages.ReadVisibleBeforeSeq(ctx, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rowSeqs(rows), publicSeqs[2:]; !slices.Equal(got, want) {
		t.Fatalf("tail rows=%v want=%v", got, want)
	}
	if head < publicSeqs[len(publicSeqs)-1] || !hasOlder {
		t.Fatalf("tail head=%d hasOlder=%v", head, hasOlder)
	}

	rows, sameHead, hasOlder, err := messages.ReadVisibleBeforeSeq(ctx, rows[0].Seq, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rowSeqs(rows), publicSeqs[:2]; !slices.Equal(got, want) {
		t.Fatalf("older rows=%v want=%v", got, want)
	}
	if sameHead != head || hasOlder {
		t.Fatalf("older head=%d want=%d hasOlder=%v", sameHead, head, hasOlder)
	}
}

func TestReadVisibleBeforeSeqSnapshotHeadHandsOffToForwardRead(t *testing.T) {
	ctx := context.Background()
	messages, register := openVisibleMessages(t)
	register("alice", "principal-a")
	if _, err := messages.Append(ctx, visibleEnvelope("tail", "alice", message.VisibilityPublic), false, storespec.AppendMetadata{}); err != nil {
		t.Fatal(err)
	}
	rows, head, _, err := messages.ReadVisibleBeforeSeq(ctx, 0, 50)
	if err != nil || len(rows) != 1 {
		t.Fatalf("tail rows=%v head=%d err=%v", rowSeqs(rows), head, err)
	}
	later, err := messages.Append(ctx, visibleEnvelope("after-snapshot", "alice", message.VisibilityPublic), false, storespec.AppendMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	rows, next, err := messages.ReadVisibleAfterSeq(ctx, head, 50)
	if err != nil || len(rows) != 1 || rows[0].Seq != int64(later.Seq) || next != int64(later.Seq) {
		t.Fatalf("handoff rows=%v next=%d err=%v", rowSeqs(rows), next, err)
	}
}

func rowSeqs(rows []storespec.StoredRow) []int64 {
	out := make([]int64, len(rows))
	for i := range rows {
		out[i] = rows[i].Seq
	}
	return out
}
