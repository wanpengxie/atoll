package harness

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type slidingAuthority struct{ current atomic.Int64 }

func (a *slidingAuthority) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	return storespec.ActorControlRow{ID: id, Kind: actor.KindAgent, CurrentDeclVersion: a.current.Load(), Placement: storespec.NewServerPlacement()}, true, nil
}
func (a *slidingAuthority) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}
func (a *slidingAuthority) WorldOf(context.Context, actor.ActorID) (storespec.ActorWorld, bool, error) {
	return storespec.WorldDurable, true, nil
}
func (a *slidingAuthority) CheckAuthor(_ context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	if stamp.BirthVersion != a.current.Load() {
		return storespec.AuthorVersionStale, nil
	}
	return storespec.AuthorOK, nil
}

type firstAppendBarrier struct {
	inner   storespec.MessageLog
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *firstAppendBarrier) Append(ctx context.Context, env *message.Envelope, terminal bool) (storespec.AppendResult, error) {
	blocked := false
	l.once.Do(func() {
		blocked = true
		close(l.entered)
	})
	if blocked {
		select {
		case <-l.release:
		case <-ctx.Done():
			return storespec.AppendResult{}, ctx.Err()
		}
	}
	return l.inner.Append(ctx, env, terminal)
}
func (l *firstAppendBarrier) FindByID(ctx context.Context, id message.ID) (*storespec.StoredRow, bool, error) {
	return l.inner.FindByID(ctx, id)
}
func (l *firstAppendBarrier) HasFinalResponse(ctx context.Context, id message.ID) (bool, error) {
	return l.inner.HasFinalResponse(ctx, id)
}

func TestAuthorApplySlidingWindowLetsInFlightWriteFinishThenFencesNext(t *testing.T) {
	cs := newTestStore(t)
	authority := &slidingAuthority{}
	authority.current.Store(1)
	barrier := &firstAppendBarrier{inner: cs.Log, entered: make(chan struct{}), release: make(chan struct{})}
	minter, err := New(Deps{
		ChannelID: testChannelID, Log: barrier, Authority: authority,
		NowMs: func() int64 { return fixedNowMs },
	})
	if err != nil {
		t.Fatal(err)
	}
	pen := minter.Mint("agent:sliding", actor.KindAgent, testChannelID, 1)
	write := func(id message.ID) (WriteResult, error) {
		return pen.Write(context.Background(), &message.Envelope{
			ID: id, TS: fixedNowMs - 1, Kind: message.KindEvent,
			Type: "sliding.probe", Audience: message.Audience{"agent:observer"},
		})
	}
	firstDone := make(chan WriteResult, 1)
	firstErr := make(chan error, 1)
	go func() {
		res, err := write("before-apply")
		firstDone <- res
		firstErr <- err
	}()
	<-barrier.entered // author gate already passed; append is now in flight.
	authority.current.Store(2)
	close(barrier.release)
	first := <-firstDone
	if err := <-firstErr; err != nil || !first.Accepted() {
		t.Fatalf("in-flight pre-apply write=(%+v,%v)", first, err)
	}
	second, err := write("after-apply")
	if err != nil || second.RejectReason != HarnessAuthorVersionStale {
		t.Fatalf("next old-pen write=(%+v,%v)", second, err)
	}
}
