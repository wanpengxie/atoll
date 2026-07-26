package harness

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var errRunStale = errors.New("test: run stale")

type testRunAuthority struct {
	id      actor.ActorID
	allowed atomic.Bool
	calls   atomic.Int64
}

func (a *testRunAuthority) ActorID() actor.ActorID { return a.id }
func (a *testRunAuthority) Admit() error {
	a.calls.Add(1)
	if !a.allowed.Load() {
		return errRunStale
	}
	return nil
}

type authorityAppendBarrier struct {
	inner   storespec.MessageLog
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *authorityAppendBarrier) Append(ctx context.Context, env *message.Envelope, terminal bool) (storespec.AppendResult, error) {
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

func (l *authorityAppendBarrier) FindByID(ctx context.Context, id message.ID) (*storespec.StoredRow, bool, error) {
	return l.inner.FindByID(ctx, id)
}

func (l *authorityAppendBarrier) HasFinalResponse(ctx context.Context, id message.ID) (bool, error) {
	return l.inner.HasFinalResponse(ctx, id)
}

func TestAuthorityPenAdmitsOnceAndLetsAcceptedWriteFinish(t *testing.T) {
	cs := newTestStore(t)
	barrier := &authorityAppendBarrier{
		inner:   cs.Log,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	mint, err := New(Deps{
		ChannelID: testChannelID,
		Log:       barrier,
		Presence:  testAuthority{},
		NowMs:     func() int64 { return fixedNowMs },
	})
	if err != nil {
		t.Fatal(err)
	}
	authority := &testRunAuthority{id: "agent:authority"}
	authority.allowed.Store(true)
	pen := mint.(*minter).MintAuthority(authority, actor.KindAgent)

	done := make(chan error, 1)
	go func() {
		result, err := pen.Write(context.Background(), &message.Envelope{
			ID:       "accepted",
			TS:       fixedNowMs - 1,
			Kind:     message.KindEvent,
			Type:     "authority.accepted",
			Audience: message.Audience{"agent:receiver"},
		})
		if err == nil && !result.Accepted() {
			err = errors.New("write rejected")
		}
		done <- err
	}()
	<-barrier.entered
	authority.allowed.Store(false)
	close(barrier.release)
	if err := <-done; err != nil {
		t.Fatalf("accepted write was re-authorized: %v", err)
	}
	if got := authority.calls.Load(); got != 1 {
		t.Fatalf("admission calls=%d, want 1", got)
	}
	if _, err := pen.Write(context.Background(), &message.Envelope{
		ID:       "stale",
		TS:       fixedNowMs - 1,
		Kind:     message.KindEvent,
		Type:     "authority.stale",
		Audience: message.Audience{"agent:receiver"},
	}); !errors.Is(err, errRunStale) {
		t.Fatalf("next stale write err=%v", err)
	}
	if got := authority.calls.Load(); got != 2 {
		t.Fatalf("admission calls=%d, want one per invocation", got)
	}
}
