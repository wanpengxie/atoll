package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/store"
)

func TestCatalogPostMember_DaemonActorRegistered_MirrorEventAppended(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("ch-members")
	db, lock, fencing := newMemberTransitionFixture(t, chID)
	reg := store.NewActorRegistry(db)

	err := reg.ApplyMemberTransitions(ctx, chID, []store.MemberActorAdd{{
		ID:          "user:bob",
		Kind:        actor.KindHuman,
		DisplayName: "Bob",
		UserID:      "u-bob",
		Role:        "member",
		At:          1000,
	}}, nil, fencing)
	if err != nil {
		t.Fatalf("ApplyMemberTransitions add: %v", err)
	}
	if rec, ok, err := reg.Lookup(ctx, "user:bob"); err != nil || !ok || !rec.IsActive() {
		t.Fatalf("actor row ok=%v rec=%+v err=%v", ok, rec, err)
	}
	msgs := store.NewMessagesWithLock(db, lock)
	env, ok, err := msgs.FindByID(ctx, chID, "system.actor.registered:user:bob:1000")
	if err != nil || !ok {
		t.Fatalf("registered mirror ok=%v err=%v", ok, err)
	}
	if env.Type != "system.actor.registered" || env.Sender.ID != actor.SystemActorID {
		t.Fatalf("registered mirror env=%+v", env)
	}

	err = reg.ApplyMemberTransitions(ctx, chID, []store.MemberActorAdd{{
		ID:     "user:stale",
		Kind:   actor.KindHuman,
		UserID: "u-stale",
		Role:   "member",
		At:     1001,
	}}, nil, klog.FencingTuple{Token: "wrong", Epoch: fencing.Epoch})
	if err == nil {
		t.Fatal("stale fencing add succeeded")
	}
	if _, ok, err := reg.Lookup(ctx, "user:stale"); err != nil || ok {
		t.Fatalf("stale actor row ok=%v err=%v; actor mutation must roll back with mirror failure", ok, err)
	}
}

func TestCatalogDeleteMember_DaemonActorDeregistered_MirrorEventAppended(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("ch-members-delete")
	db, lock, fencing := newMemberTransitionFixture(t, chID)
	reg := store.NewActorRegistry(db)

	if err := reg.ApplyMemberTransitions(ctx, chID, []store.MemberActorAdd{{
		ID:     "user:bob",
		Kind:   actor.KindHuman,
		UserID: "u-bob",
		Role:   "member",
		At:     1000,
	}}, nil, fencing); err != nil {
		t.Fatalf("seed add: %v", err)
	}
	if err := reg.ApplyMemberTransitions(ctx, chID, nil, []store.MemberActorRemove{{
		ID: "user:bob",
		At: 2000,
	}}, fencing); err != nil {
		t.Fatalf("ApplyMemberTransitions remove: %v", err)
	}
	rec, ok, err := reg.Lookup(ctx, "user:bob")
	if err != nil || !ok || rec.DeregisteredAt != 2000 {
		t.Fatalf("deregistered row ok=%v rec=%+v err=%v", ok, rec, err)
	}
	msgs := store.NewMessagesWithLock(db, lock)
	env, ok, err := msgs.FindByID(ctx, chID, message.ID("system.actor.deregistered:user:bob:2000"))
	if err != nil || !ok {
		t.Fatalf("deregistered mirror ok=%v err=%v", ok, err)
	}
	if env.Type != "system.actor.deregistered" || env.Sender.ID != actor.SystemActorID {
		t.Fatalf("deregistered mirror env=%+v", env)
	}
}

func newMemberTransitionFixture(t *testing.T, chID channel.ID) (*sql.DB, *store.ChannelLock, klog.FencingTuple) {
	t.Helper()
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "channel.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	lock := store.NewChannelLock(db)
	token := placement.FencingToken("member-transition-token")
	if err := lock.Insert(ctx, store.ChannelLockRow{
		ChannelID:    chID,
		FencingToken: token,
		OwnerEpoch:   1,
		DaemonID:     "daemon-test",
		DaemonEpoch:  7,
		AcquiredAt:   1,
		RefreshedAt:  1,
	}); err != nil {
		t.Fatalf("lock insert: %v", err)
	}
	return db, lock, klog.FencingTuple{Token: token, Epoch: 7}
}
