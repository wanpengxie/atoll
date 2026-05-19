package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/trigger"
)

type failingDeliverer struct {
	err   error
	calls atomic.Int64
}

func (d *failingDeliverer) Deliver(context.Context, []actor.ActorID, *message.Envelope) error {
	d.calls.Add(1)
	return d.err
}

func TestPostHarnessChainDispatchFailureDoesNotMarkDelivered(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	chID := channel.ID("ch-delivery-error")
	areg := store.NewActorRegistry(db)
	for _, rec := range []actorreg.Record{
		{ID: "user:demo", Kind: actor.KindHuman, CreatedAt: 1},
		{ID: "agent:beta", Kind: actor.KindAgent, CreatedAt: 1},
	} {
		if err := areg.Insert(ctx, rec); err != nil {
			t.Fatalf("seed actor %s: %v", rec.ID, err)
		}
	}

	msgs := store.NewMessages(db)
	chain, err := harness.New(harness.Deps{
		ChannelID:     chID,
		ActorRegistry: areg,
		Log:           msgs,
		NowMs:         func() int64 { return 1000 },
	})
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}
	deliverErr := errors.New("worker push failed")
	deliverer := &failingDeliverer{err: deliverErr}
	gw, err := trigger.New(trigger.Config{
		Registry:  areg,
		Deliverer: deliverer,
		NowFn:     func() int64 { return 1000 },
	})
	if err != nil {
		t.Fatalf("trigger.New: %v", err)
	}

	wrapped := &postHarnessChain{
		chain:    chain,
		gateway:  gw,
		messages: msgs,
		nowFn:    func() int64 { return 1500 },
	}
	writeCtx := harness.CtxWithCaller(ctx, harness.CallerContext{
		ActorID:                 "user:demo",
		ChannelID:               chID,
		AllowProvidedSenderKind: false,
	})
	env := &message.Envelope{
		ID:         "m-delivery-error",
		ChannelID:  string(chID),
		Sender:     message.Sender{ID: "user:demo"},
		Kind:       message.KindEvent,
		Type:       "human.text",
		Payload:    json.RawMessage(`{"text":"hello"}`),
		Visibility: message.VisibilityPublic,
		Audience:   []string{"agent:beta"},
	}

	res, err := wrapped.Write(writeCtx, env)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.RejectReason != "" || res.Deduped {
		t.Fatalf("unexpected write result: %+v", res)
	}
	if deliverer.calls.Load() != 1 {
		t.Fatalf("deliver calls=%d want 1", deliverer.calls.Load())
	}

	got, ok, err := msgs.FindByID(ctx, chID, env.ID)
	if err != nil || !ok {
		t.Fatalf("FindByID ok=%v err=%v", ok, err)
	}
	if got.DeliveredAt != nil {
		t.Fatalf("DeliveredAt=%v want nil after dispatch failure", *got.DeliveredAt)
	}
	if got.DeliveryFailedAt == nil || *got.DeliveryFailedAt != 1500 {
		t.Fatalf("DeliveryFailedAt=%v want 1500", got.DeliveryFailedAt)
	}
	if !strings.Contains(got.LastError, deliverErr.Error()) {
		t.Fatalf("LastError=%q want to contain %q", got.LastError, deliverErr.Error())
	}
	if got.Attempts != 1 {
		t.Fatalf("Attempts=%d want 1", got.Attempts)
	}
}
