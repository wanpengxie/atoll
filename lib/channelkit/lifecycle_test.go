package channelkit

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type blockingPen struct{ entered, release chan struct{} }

func (p blockingPen) Write(context.Context, *message.Envelope) (harness.WriteResult, error) {
	select {
	case <-p.entered:
	default:
		close(p.entered)
	}
	<-p.release
	return harness.WriteResult{}, nil
}

type lifecycleQuery struct{ row storespec.StoredRow }

func (q lifecycleQuery) MaxSeq(context.Context) (int64, error) { return 0, nil }
func (q lifecycleQuery) ReadAfterSeq(context.Context, int64, int) ([]storespec.StoredRow, error) {
	return nil, nil
}
func (q lifecycleQuery) OpenRequestsForActor(context.Context, actor.ActorID) ([]storespec.StoredRow, error) {
	return []storespec.StoredRow{q.row}, nil
}
func (q lifecycleQuery) DistinctOpenRequestReceivers(context.Context) ([]actor.ActorID, error) {
	return nil, nil
}

func TestChannelCloseBoundsStoreIgnoringContext(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	pen := blockingPen{entered: entered, release: release}
	q := lifecycleQuery{row: storespec.StoredRow{Envelope: message.Envelope{
		ID: "req", Kind: message.KindRequest, Audience: message.Audience{"dead"},
	}}}
	c, err := New(Config{ChannelID: "ch", SystemPen: pen, OpenRequests: q,
		ClosedForever: func(context.Context, actor.ActorID) (bool, error) { return true, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	c.OnDown(context.Background(), "dead", actorrt.Incarnation{}, nil)
	<-entered
	c.closeWithin(25 * time.Millisecond)
	if c.Leaked() != 1 {
		t.Fatalf("Leaked = %d, want 1", c.Leaked())
	}
	close(release)
	select {
	case <-c.downDone:
	case <-time.After(time.Second):
		t.Fatal("released consumer did not exit")
	}
}

func TestChannelCloseBeforeStartAndConcurrentClose(t *testing.T) {
	c, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 8
	done := make(chan struct{}, callers)
	for range callers {
		go func() { c.Close(); done <- struct{}{} }()
	}
	for range callers {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Close-before-Start blocked")
		}
	}
	if c.Leaked() != 0 {
		t.Fatalf("Leaked = %d", c.Leaked())
	}
}
