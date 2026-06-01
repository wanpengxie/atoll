package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/fencing"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/store"
)

func TestCatalogPostMember_DaemonActorRegistered_MirrorEventAppended(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("ch-members")
	db, fence, tuple := newMemberTransitionFixture(t, chID)
	reg := store.NewActorRegistryWithObservers(db, fence, store.AppendObserverFuncs{})

	err := reg.ApplyMemberTransitions(ctx, chID, []store.MemberActorAdd{{
		ID:          "user:bob",
		Kind:        actor.KindHuman,
		DisplayName: "Bob",
		UserID:      "u-bob",
		Role:        "member",
		At:          1000,
	}}, nil, tuple)
	if err != nil {
		t.Fatalf("ApplyMemberTransitions add: %v", err)
	}
	if rec, ok, err := reg.Lookup(ctx, "user:bob"); err != nil || !ok || !rec.IsActive() {
		t.Fatalf("actor row ok=%v rec=%+v err=%v", ok, rec, err)
	}
	msgs := store.NewMessagesWithLock(db, fence)
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
	}}, nil, klog.FencingTuple{Token: "wrong", Epoch: tuple.Epoch})
	if err == nil {
		t.Fatal("stale fencing add succeeded")
	}
	if _, ok, err := reg.Lookup(ctx, "user:stale"); err != nil || ok {
		t.Fatalf("stale actor row ok=%v err=%v; actor mutation must roll back with mirror failure", ok, err)
	}
}

func TestProxyHostMetadataEmittedInActorRegisteredMirror(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("ch-proxy-host")
	db, fence, tuple := newMemberTransitionFixture(t, chID)
	reg := store.NewActorRegistryWithObservers(db, fence, store.AppendObserverFuncs{})

	err := reg.ApplyMemberTransitions(ctx, chID, []store.MemberActorAdd{{
		ID:          "tool:kimi",
		Kind:        actor.KindTool,
		Binding:     actor.BindingRuntimeInboundViaRelay,
		DisplayName: "kimi",
		UserID:      "u-proxy",
		Role:        "proxy_daemon",
		At:          1000,
		ProxyHost: store.MemberActorProxyHost{
			DaemonID:   "daemon-proxy",
			DaemonName: "Proxy Laptop",
		},
	}}, nil, tuple)
	if err != nil {
		t.Fatalf("ApplyMemberTransitions add: %v", err)
	}

	msgs := store.NewMessagesWithLock(db, fence)
	env, ok, err := msgs.FindByID(ctx, chID, "system.actor.registered:tool:kimi:1000")
	if err != nil || !ok {
		t.Fatalf("registered mirror ok=%v err=%v", ok, err)
	}
	var payload struct {
		ProxyHost struct {
			DaemonID   string `json:"daemon_id"`
			DaemonName string `json:"daemon_name"`
		} `json:"proxy_host"`
	}
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		t.Fatalf("payload JSON: %v", err)
	}
	if payload.ProxyHost.DaemonID != "daemon-proxy" || payload.ProxyHost.DaemonName != "Proxy Laptop" {
		t.Fatalf("proxy_host=%+v", payload.ProxyHost)
	}
}

func TestCatalogDeleteMember_DaemonActorDeregistered_MirrorEventAppended(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("ch-members-delete")
	db, fence, tuple := newMemberTransitionFixture(t, chID)
	reg := store.NewActorRegistryWithObservers(db, fence, store.AppendObserverFuncs{})

	if err := reg.ApplyMemberTransitions(ctx, chID, []store.MemberActorAdd{{
		ID:     "user:bob",
		Kind:   actor.KindHuman,
		UserID: "u-bob",
		Role:   "member",
		At:     1000,
	}}, nil, tuple); err != nil {
		t.Fatalf("seed add: %v", err)
	}
	if err := reg.ApplyMemberTransitions(ctx, chID, nil, []store.MemberActorRemove{{
		ID: "user:bob",
		At: 2000,
	}}, tuple); err != nil {
		t.Fatalf("ApplyMemberTransitions remove: %v", err)
	}
	rec, ok, err := reg.Lookup(ctx, "user:bob")
	if err != nil || !ok || rec.DeregisteredAt != 2000 {
		t.Fatalf("deregistered row ok=%v rec=%+v err=%v", ok, rec, err)
	}
	msgs := store.NewMessagesWithLock(db, fence)
	env, ok, err := msgs.FindByID(ctx, chID, message.ID("system.actor.deregistered:user:bob:2000"))
	if err != nil || !ok {
		t.Fatalf("deregistered mirror ok=%v err=%v", ok, err)
	}
	if env.Type != "system.actor.deregistered" || env.Sender.ID != actor.SystemActorID {
		t.Fatalf("deregistered mirror env=%+v", env)
	}
}

// newMemberTransitionFixture seeds a fresh channel sqlite and a pure
// fake fence accepting the returned tuple. No framework ChannelLock — the
// subject under test is the substrate ActorRegistry, the fence is just the
// gate it must consult.
func newMemberTransitionFixture(t *testing.T, chID channel.ID) (*sql.DB, store.WriteFence, klog.FencingTuple) {
	t.Helper()
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "channel.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	token := fencing.FencingToken("member-transition-token")
	const epoch = fencing.DaemonEpoch(7)
	return db, fakeFence(token, epoch), klog.FencingTuple{Token: token, Epoch: epoch}
}
