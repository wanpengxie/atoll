package store

import (
	"context"
	"path/filepath"
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

	bob := channel.Reader{ActorID: "bob", Mode: channel.ReaderMember}
	rows, scanned, err := messages.ReadVisibleAfterSeq(ctx, bob, 0, batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Seq != int64(firstPublicSeq.Seq) || rows[1].Seq != int64(publicSeq.Seq) || scanned != int64(publicSeq.Seq) {
		t.Fatalf("bob rows=%v scanned=%d", rowSeqs(rows), scanned)
	}

	carol := channel.Reader{ActorID: "carol", Mode: channel.ReaderMember}
	rows, _, err = messages.ReadVisibleAfterSeq(ctx, carol, 0, batch)
	if err != nil || len(rows) != 2 {
		t.Fatalf("carol rows=%v err=%v", rowSeqs(rows), err)
	}

	senderObserver := channel.Reader{Principal: "principal-a", Mode: channel.ReaderObserver}
	rows, _, err = messages.ReadVisibleAfterSeq(ctx, senderObserver, 0, batch)
	if err != nil || len(rows) != 2 {
		t.Fatalf("sender observer rows=%v err=%v", rowSeqs(rows), err)
	}

	audienceObserver := channel.Reader{Principal: "principal-b", Mode: channel.ReaderObserver}
	rows, _, err = messages.ReadVisibleAfterSeq(ctx, audienceObserver, 0, batch)
	if err != nil || len(rows) != 2 {
		t.Fatalf("audience observer rows=%v err=%v", rowSeqs(rows), err)
	}
}

func TestReadVisibleAfterSeqSnapshotCursorDoesNotSkipLaterInsert(t *testing.T) {
	ctx := context.Background()
	messages, register := openVisibleMessages(t)
	register("alice", "principal-a")
	reader := channel.Reader{ActorID: "alice", Mode: channel.ReaderMember}
	if _, err := messages.Append(ctx, visibleEnvelope("first", "alice", message.VisibilityPublic), false, storespec.AppendMetadata{}); err != nil {
		t.Fatal(err)
	}
	rows, scanned, err := messages.ReadVisibleAfterSeq(ctx, reader, 0, 100)
	if err != nil || len(rows) != 1 {
		t.Fatalf("first read rows=%v scanned=%d err=%v", rowSeqs(rows), scanned, err)
	}
	second, err := messages.Append(ctx, visibleEnvelope("concurrent-boundary", "alice", message.VisibilityPublic), false, storespec.AppendMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	rows, next, err := messages.ReadVisibleAfterSeq(ctx, reader, scanned, 100)
	if err != nil || len(rows) != 1 || rows[0].Seq != int64(second.Seq) || next != int64(second.Seq) {
		t.Fatalf("boundary read rows=%v next=%d err=%v", rowSeqs(rows), next, err)
	}
}

func rowSeqs(rows []storespec.StoredRow) []int64 {
	out := make([]int64, len(rows))
	for i := range rows {
		out[i] = rows[i].Seq
	}
	return out
}
