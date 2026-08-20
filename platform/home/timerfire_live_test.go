package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/timerfire"
)

// With the ownership-cleanup family deleted, a dead author's durable timer is
// stopped by exactly one thing: the fire sink's live membership gate. This
// exercises that gate against the REAL Controller and the REAL harness — a
// live author's fire lands in the message log, an ended author's fire is
// refused as author_not_member and the log stays untouched.
func TestDeadAuthorTimerFireIsRefusedByTheLiveGate(t *testing.T) {
	h, err := Open(Config{
		ChannelID:            "timer-gate",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  routingResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: "decl-author", Seed: "decl-author", Class: "routing-live",
			Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent,
			CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	declared, err := rosterMembersForSource(ctx, h.View(), "decl-author")
	if err != nil || len(declared) != 1 {
		t.Fatalf("bootstrap author missing: %v err=%v", declared, err)
	}
	author := declared[0]

	sink, err := timerfire.New(h.controller, h.admittedWriter)
	if err != nil {
		t.Fatal(err)
	}
	fireEnv := func(id string) *message.Envelope {
		return &message.Envelope{
			ID: message.ID(id), TS: time.Now().UnixMilli(),
			Kind: message.KindEvent, Type: "test.timer.tick",
			Payload:  json.RawMessage(`{}`),
			Audience: message.Audience{author},
			// Self-rooted: the harness now refuses an empty correlation, so a
			// hand-built envelope must spell the root the builder would have.
			CorrelationID: message.ID(id),
		}
	}

	// A live author's fire lands in the log.
	if err := sink.Append(ctx, author, fireEnv("timer:live-1")); err != nil {
		t.Fatalf("live author fire: %v", err)
	}
	// End the author; the identical fire is now refused by the gate and the
	endIdentityForFixture(t, h, author)
	beforeDead, err := h.query.MaxSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = sink.Append(ctx, author, fireEnv("timer:dead-1"))
	var rejected schedule.FireRejected
	if !errors.As(err, &rejected) || rejected.Reason != "author_not_member" {
		t.Fatalf("dead author fire: err=%v, want FireRejected{author_not_member}", err)
	}
	afterDead, err := h.query.MaxSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if afterDead != beforeDead {
		t.Fatalf("a dead author's fire appended %d message rows", afterDead-beforeDead)
	}
}
