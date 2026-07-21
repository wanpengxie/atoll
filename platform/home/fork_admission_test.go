package home

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestForkAdmissionPublishesEventRunRowStateAndReceipt(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	parent, err := h.admit(ctx, actor.KindHuman, "fork-parent")
	if err != nil {
		t.Fatal(err)
	}
	spec := actorrt.ForkSpec{Kind: actor.KindAgent, Class: "agent.test", NameHint: "worker"}
	child, err := h.forkAdmission(ctx, parent, 1, spec, "nonce-1")
	if err != nil {
		t.Fatal(err)
	}
	prefix := string(parent) + "/worker-"
	if !strings.HasPrefix(string(child), prefix) {
		t.Fatalf("child id %q lacks prefix %q", child, prefix)
	}
	if _, err := uuid.Parse(strings.TrimPrefix(string(child), prefix)); err != nil {
		t.Fatalf("child id does not carry full uuid: %v", err)
	}
	row, ok, err := h.cs.Authority.LookupActive(ctx, child)
	if err != nil || !ok || row.Sponsor != parent || row.CurrentDeclVersion != 1 || row.Class != spec.Class || row.Principal != "" {
		t.Fatalf("fork control row = (%+v,%v,%v)", row, ok, err)
	}
	world, ok, err := h.cs.Authority.WorldOf(ctx, child)
	if err != nil || !ok || world != storespec.WorldRun {
		t.Fatalf("fork world = (%v,%v,%v)", world, ok, err)
	}
	if durable, err := h.cs.DurableHistory.ExistsEver(ctx, child); err != nil || durable {
		t.Fatalf("fork durable history = (%v,%v)", durable, err)
	}
	if _, err := h.stateHandles.Resolve(ctx, storespec.AuthorStamp{ID: child, BirthVersion: 1}); err != nil {
		t.Fatalf("run State missing at publication: %v", err)
	}

	again, err := h.forkAdmission(ctx, parent, 1, spec, "nonce-1")
	if err != nil || again != child {
		t.Fatalf("same nonce = (%q,%v), want %q", again, err, child)
	}
	conflict := spec
	conflict.Config = []byte(`{"different":true}`)
	if _, err := h.forkAdmission(ctx, parent, 1, conflict, "nonce-1"); !errors.Is(err, ErrForkNonceConflict) {
		t.Fatalf("different spec/same nonce err=%v", err)
	}
	second, err := h.forkAdmission(ctx, parent, 1, spec, "nonce-2")
	if err != nil || second == child {
		t.Fatalf("second logical fork = (%q,%v)", second, err)
	}
}

func TestForkMemberReaderUsesActorIdentityWithoutPrincipal(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	parent, err := h.admit(ctx, actor.KindHuman, "fork-reader-parent")
	if err != nil {
		t.Fatal(err)
	}
	child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "agent.test"}, "reader-child")
	if err != nil {
		t.Fatal(err)
	}
	row, active, err := h.controlIndex.LookupActive(ctx, child)
	if err != nil || !active || row.Principal != "" || row.SourceDeclID != "" {
		t.Fatalf("fork identity row=%+v active=%v err=%v", row, active, err)
	}
	reader := channel.Reader{ActorID: child, Mode: channel.ReaderMember}
	if _, _, err := h.View().ReadVisibleAfterSeq(ctx, reader, 0, 10); err != nil {
		t.Fatalf("fork member message read rejected: %v", err)
	}
	if _, err := h.View().Resources().List(ctx, reader, channel.ResourceListQuery{Limit: 10}); err != nil {
		t.Fatalf("fork member resource read rejected: %v", err)
	}
}

func TestForkAdmissionRejectsInvalidSpecAndEndClearsRunAccounts(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	parent, err := h.admit(ctx, actor.KindHuman, "fork-parent-invalid")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bad/name", strings.Repeat("x", 65)} {
		if _, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "agent.test", NameHint: name}, uuid.NewString()); !errors.Is(err, ErrForkSpecInvalid) {
			t.Fatalf("name %q err=%v", name, err)
		}
	}
	child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "agent.test"}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err := (lifecycleEndHandle{home: h, author: storespec.AuthorStamp{ID: parent, BirthVersion: 1}}).End(ctx, child, "test"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := h.cs.Authority.LookupActive(ctx, child); ok {
		t.Fatal("ended fork remains in authority")
	}
	if _, err := h.stateHandles.Resolve(ctx, storespec.AuthorStamp{ID: child, BirthVersion: 1}); !errors.Is(err, accessdoor.ErrStateHandleUnavailable) {
		t.Fatalf("ended fork State resolve = %v", err)
	}
	// A lost spawn_ack may be retried after the committed child has already
	// ended. The receipt outlives the active row for the Home session and must
	// return the original name, never mint a replacement child.
	replayed, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "agent.test"}, "ended-child-receipt")
	if err != nil {
		t.Fatal(err)
	}
	if err := (lifecycleEndHandle{home: h, author: storespec.AuthorStamp{ID: parent, BirthVersion: 1}}).End(ctx, replayed, "ended-before-ack-retry"); err != nil {
		t.Fatal(err)
	}
	again, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "agent.test"}, "ended-child-receipt")
	if err != nil || again != replayed {
		t.Fatalf("ended receipt replay=(%q,%v), want %q", again, err, replayed)
	}
}

func TestParentSuccessorRecoversSponsoredChildAndDespawnsIt(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	parent, err := h.admit(ctx, actor.KindHuman, "recovering-parent")
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(parent)
		return live
	})
	old, _ := h.channel.Cells().CurrentIncarnation(parent)
	child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "agent.test"}, "recover-child")
	if err != nil {
		t.Fatal(err)
	}
	h.channel.Cells().Despawn(old)
	h.pokeReconcile()
	var successor actorrt.Incarnation
	waitHomeCondition(t, func() bool {
		inc, live := h.channel.Cells().CurrentIncarnation(parent)
		if live && inc != old {
			successor = inc
			return true
		}
		return false
	})
	rows, err := h.controlIndex.ListActive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		found = found || (row.ID == child && row.Sponsor == parent)
	}
	if !found {
		t.Fatal("successor could not recover child through sponsor projection")
	}
	handle := newSpawnHandle(h, successor, 1, h.channel.Cells())
	if err := handle.DespawnChild(ctx, child, "recovered_cleanup"); err != nil {
		t.Fatalf("successor despawn: %v", err)
	}
	if _, ok, _ := h.controlIndex.LookupActive(ctx, child); ok {
		t.Fatal("recovered child remains active")
	}
}

func TestForkAcceleratorMissStillReturnsChildAndLevelRingBuildsIt(t *testing.T) {
	resolver := &compositionActivationResolver{}
	resolver.fail.Store(true)
	h, err := Open(Config{
		ChannelID: "fork-accelerator-miss", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: resolver,
		ReconcileInterval:   10 * time.Millisecond, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()
	parent, err := h.admit(ctx, actor.KindHuman, "accelerator-miss-parent")
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(parent)
		return live
	})
	inc, _ := h.channel.Cells().CurrentIncarnation(parent)
	lifecycle := newSpawnHandle(h, inc, 1, h.channel.Cells())
	child, err := lifecycle.Fork(ctx, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "probe"})
	if err != nil || child == "" {
		t.Fatalf("Fork after accelerator miss=(%q,%v)", child, err)
	}
	if _, live := h.channel.Cells().CurrentIncarnation(child); live {
		t.Fatal("missing factory unexpectedly built child")
	}
	if _, active, err := h.controlIndex.LookupActive(ctx, child); err != nil || !active {
		t.Fatalf("accelerator miss rolled back admission: active=%v err=%v", active, err)
	}

	resolver.fail.Store(false)
	now := time.Now().UnixMilli()
	expires := time.Now().Add(time.Minute).UnixMilli()
	write, err := h.systemPen.Write(ctx, &message.Envelope{
		ID: "accelerator-miss-request", Kind: message.KindRequest, Type: "work",
		Audience: message.Audience{child}, Visibility: message.VisibilitySystem,
		TS: now, TSReceived: now, ExpiresAt: &expires,
	})
	if err != nil || !write.Accepted() {
		t.Fatalf("wake request=(%+v,%v)", write, err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(child)
		return live
	})
}
