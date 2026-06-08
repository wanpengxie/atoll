package channelhost_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
	"github.com/wanpengxie/ActOS/server/channelhost"
)

const testChannelID = channel.ID("test-channel")

func openTestStores(t *testing.T) (channelhost.Stores, *runtime.ChannelStores) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "ch.sqlite")
	cs, err := runtime.OpenChannel(context.Background(), dbPath, runtime.OpenChannelOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return channelhost.Stores{
		Log:        cs.Log,
		Query:      cs.Query,
		Requests:   cs.Requests,
		Registry:   cs.Registry,
		Membership: cs.Membership,
		Close:      cs.Close,
	}, cs
}

// newTestHome creates a channelhost with a raw harness chain as writer (the
// v2 assembly root would wrap this in a postCommitWriter, but for unit tests
// the raw chain suffices).
func newTestHome(t *testing.T) (*channelhost.ChannelHome, *harness.Chain) {
	t.Helper()
	ctx := context.Background()
	stores, cs := openTestStores(t)

	chain, err := harness.New(harness.Deps{
		ChannelID:     testChannelID,
		ActorRegistry: cs.Registry,
		Log:           cs.Log,
	})
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}

	home, err := channelhost.New(ctx, channelhost.Config{
		ChannelID: testChannelID,
		Stores:    stores,
		Writer:    chain,
	})
	if err != nil {
		t.Fatalf("channelhost.New: %v", err)
	}
	return home, chain
}

// TestNew_GenesisRegistersSystemActor verifies that channelhost.New registers
// the system actor into membership so its writes pass harness sender validation.
func TestNew_GenesisRegistersSystemActor(t *testing.T) {
	home, _ := newTestHome(t)
	defer func() { _ = home.Close() }()

	actors, err := home.ListActiveActors(context.Background())
	if err != nil {
		t.Fatalf("ListActiveActors: %v", err)
	}
	found := false
	for _, a := range actors {
		if a.ID == actor.SystemActorID {
			found = true
			if a.Kind != actor.KindSystem {
				t.Errorf("system actor kind = %q, want %q", a.Kind, actor.KindSystem)
			}
		}
	}
	if !found {
		t.Fatal("system actor not found after genesis")
	}
}

// TestMaxSeq_EmptyChannel verifies an empty channel has seq 0.
func TestMaxSeq_EmptyChannel(t *testing.T) {
	home, _ := newTestHome(t)
	defer func() { _ = home.Close() }()

	seq, err := home.MaxSeq(context.Background())
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}
	if seq != 0 {
		t.Errorf("MaxSeq = %d, want 0 for empty channel", seq)
	}
}

// TestWriteAndReadBack verifies that writing through the harness chain commits
// an envelope into truth and it can be read back via ReadAfterSeq on channelhost.
func TestWriteAndReadBack(t *testing.T) {
	ctx := context.Background()

	// Open a fresh store with a pre-registered sender.
	dbPath := filepath.Join(t.TempDir(), "ch2.sqlite")
	cs, err := runtime.OpenChannel(ctx, dbPath, runtime.OpenChannelOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}

	senderID := actor.ActorID("user:test")
	_ = cs.Membership.Insert(ctx, storespec.Record{
		ID: senderID, Kind: actor.KindHuman, CreatedAt: time.Now().UnixMilli(),
	})

	chain, err := harness.New(harness.Deps{
		ChannelID:     testChannelID,
		ActorRegistry: cs.Registry,
		Log:           cs.Log,
	})
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}

	home, err := channelhost.New(ctx, channelhost.Config{
		ChannelID: testChannelID,
		Stores: channelhost.Stores{
			Log: cs.Log, Query: cs.Query, Requests: cs.Requests,
			Registry: cs.Registry, Membership: cs.Membership, Close: cs.Close,
		},
		Writer: chain,
	})
	if err != nil {
		t.Fatalf("channelhost.New: %v", err)
	}
	defer func() { _ = home.Close() }()

	env := &message.Envelope{
		ID:         "msg-001",
		TS:         time.Now().UnixMilli(),
		ChannelID:  testChannelID,
		Kind:       message.KindEvent,
		Type:       "test.event",
		Sender:     message.Sender{Kind: actor.KindHuman, ID: senderID},
		Audience:   message.Audience{actor.SystemActorID},
		Visibility: message.VisibilityPublic,
		Payload:    []byte(`{}`),
	}

	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{
		ActorID:   senderID,
		ChannelID: testChannelID,
	})
	// Write through the harness chain directly (v2: the assembly root's
	// postCommitWriter would wrap this; channelhost doesn't own the write path).
	res, err := chain.Write(cctx, env)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !res.Accepted() {
		t.Fatalf("Write rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}
	if res.MessageID != "msg-001" {
		t.Errorf("MessageID = %q, want %q", res.MessageID, "msg-001")
	}

	// Read back via channelhost's query accessor.
	rows, err := home.ReadAfterSeq(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	found := false
	for _, row := range rows {
		if row.Envelope.ID == "msg-001" {
			found = true
			if row.Seq <= 0 {
				t.Errorf("Seq = %d, want > 0", row.Seq)
			}
		}
	}
	if !found {
		t.Errorf("test event msg-001 not found in ReadAfterSeq (got %d rows)", len(rows))
	}
}
