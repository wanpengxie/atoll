package channelhost_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
	"github.com/wanpengxie/ActOS/server/channelhost"
)

const testChannelID = channel.ID("test-channel")

func openTestStores(t *testing.T) channelhost.Stores {
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
	}
}

func newTestHome(t *testing.T) *channelhost.ChannelHome {
	t.Helper()
	ctx := context.Background()
	stores := openTestStores(t)
	home, err := channelhost.New(ctx, channelhost.Config{
		ChannelID: testChannelID,
		Stores:    stores,
	})
	if err != nil {
		t.Fatalf("channelhost.New: %v", err)
	}
	return home
}

// TestNew_GenesisRegistersSystemActor verifies that channelhost.New registers
// the system actor into membership so its writes pass harness sender validation.
func TestNew_GenesisRegistersSystemActor(t *testing.T) {
	home := newTestHome(t)
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
	home := newTestHome(t)
	defer func() { _ = home.Close() }()

	seq, err := home.MaxSeq(context.Background())
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}
	if seq != 0 {
		t.Errorf("MaxSeq = %d, want 0 for empty channel", seq)
	}
}

// TestFanoutWriter_WriteAndReadBack verifies that Dispatch commits an envelope
// into truth and it can be read back via ReadAfterSeq.
func TestFanoutWriter_WriteAndReadBack(t *testing.T) {
	home := newTestHome(t)
	defer func() { _ = home.Close() }()

	ctx := context.Background()

	// Register a sender actor so the harness ACL passes.
	senderID := actor.ActorID("user:test")
	stores := openTestStores(t)
	// Use the home's own stores -- we need to register the sender via membership.
	// Instead, re-open to avoid double-close; use the home's dispatch which
	// requires a registered sender.
	// Simpler: register via ApplyMemberTransitions on a separate home.
	// Actually, let's just write through the home after registering.

	// Create a fresh home with a sender pre-registered.
	dbPath := filepath.Join(t.TempDir(), "ch2.sqlite")
	cs, err := runtime.OpenChannel(ctx, dbPath, runtime.OpenChannelOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	_ = cs.Membership.Insert(ctx, storespec.Record{
		ID: senderID, Kind: actor.KindHuman, CreatedAt: time.Now().UnixMilli(),
	})
	home2, err := channelhost.New(ctx, channelhost.Config{
		ChannelID: testChannelID,
		Stores: channelhost.Stores{
			Log: cs.Log, Query: cs.Query, Requests: cs.Requests,
			Registry: cs.Registry, Membership: cs.Membership, Close: cs.Close,
		},
	})
	if err != nil {
		t.Fatalf("channelhost.New: %v", err)
	}
	defer func() { _ = home2.Close() }()

	_ = stores // not used
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
	res, err := home2.Dispatch(cctx, env)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if !res.Accepted() {
		t.Fatalf("Dispatch rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}
	if res.MessageID != "msg-001" {
		t.Errorf("MessageID = %q, want %q", res.MessageID, "msg-001")
	}

	// Read back.
	rows, err := home2.ReadAfterSeq(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	// Should have at least the system.actor.registered events + our test event.
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

// TestPushHub_NotifyWakesSubscribers verifies that pushHub wakes subscribers
// when a write is committed.
func TestPushHub_NotifyWakesSubscribers(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ch3.sqlite")
	ctx := context.Background()
	cs, err := runtime.OpenChannel(ctx, dbPath, runtime.OpenChannelOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}

	senderID := actor.ActorID("user:push-test")
	_ = cs.Membership.Insert(ctx, storespec.Record{
		ID: senderID, Kind: actor.KindHuman, CreatedAt: time.Now().UnixMilli(),
	})

	home, err := channelhost.New(ctx, channelhost.Config{
		ChannelID: testChannelID,
		Stores: channelhost.Stores{
			Log: cs.Log, Query: cs.Query, Requests: cs.Requests,
			Registry: cs.Registry, Membership: cs.Membership, Close: cs.Close,
		},
	})
	if err != nil {
		t.Fatalf("channelhost.New: %v", err)
	}
	defer func() { _ = home.Close() }()

	sig, unsub := home.Subscribe()
	defer unsub()

	env := &message.Envelope{
		ID:         "msg-push-001",
		TS:         time.Now().UnixMilli(),
		ChannelID:  testChannelID,
		Kind:       message.KindEvent,
		Type:       "test.push",
		Sender:     message.Sender{Kind: actor.KindHuman, ID: senderID},
		Audience:   message.Audience{actor.SystemActorID},
		Visibility: message.VisibilityPublic,
		Payload:    []byte(`{}`),
	}

	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{
		ActorID:   senderID,
		ChannelID: testChannelID,
	})
	_, err = home.Dispatch(cctx, env)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	select {
	case <-sig:
		// OK -- subscriber was woken
	case <-time.After(time.Second):
		t.Fatal("subscriber not woken after Dispatch")
	}
}
