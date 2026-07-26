package accessdoor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var errAccessRunStale = errors.New("test: access run stale")

type accessRunAuthority struct {
	id      actor.ActorID
	allowed atomic.Bool
	calls   atomic.Int64
}

func (a *accessRunAuthority) ActorID() actor.ActorID { return a.id }
func (a *accessRunAuthority) Admit() error {
	a.calls.Add(1)
	if !a.allowed.Load() {
		return errAccessRunStale
	}
	return nil
}

type accessBackingAuthority struct{}

func (*accessBackingAuthority) ResourceActorFacts(
	context.Context,
	actor.ActorID,
) (storespec.ResourceActorFacts, error) {
	return storespec.ResourceActorFacts{Active: true}, nil
}

type authorityBlockingReadDriver struct {
	fakeDriver
	entered chan struct{}
	release chan struct{}
}

type blockingStateStore struct {
	fakeStateStore
	entered chan struct{}
	release chan struct{}
}

func (s *blockingStateStore) Read(
	ctx context.Context,
	_ actor.ActorID,
	_ resource.ResourceID,
) ([]byte, bool, error) {
	close(s.entered)
	select {
	case <-s.release:
		return []byte("state"), true, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

func (d *authorityBlockingReadDriver) Read(
	ctx context.Context,
	_ resource.ResourceID,
) ([]byte, bool, error) {
	close(d.entered)
	select {
	case <-d.release:
		return []byte("ok"), true, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

func TestAuthorityAccessAdmitsOnceAndLetsAcceptedInvokeFinish(t *testing.T) {
	backing := &accessBackingAuthority{}
	driver := &authorityBlockingReadDriver{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	minted, err := New(Deps{
		Registry: &fakeRegistry{
			resolveExists: true,
			resolveMeta:   metaKV(),
			actorAllows:   true,
		},
		Drivers:   DriverTable{resourcespec.KindKV: driver},
		Authority: backing,
		State:     &fakeStateStore{},
	})
	if err != nil {
		t.Fatal(err)
	}
	authority := &accessRunAuthority{id: "agent:authority"}
	authority.allowed.Store(true)
	handle := minted.(*minter).MintAuthority(authority)

	done := make(chan struct {
		out Outcome
		err error
	}, 1)
	go func() {
		out, err := handle.Invoke(
			context.Background(), access.OpRead, "resource:authority", nil, nil,
		)
		done <- struct {
			out Outcome
			err error
		}{out: out, err: err}
	}()
	<-driver.entered
	authority.allowed.Store(false)
	close(driver.release)
	result := <-done
	if result.err != nil || !result.out.Accepted() {
		t.Fatalf("accepted Invoke=(%+v,%v)", result.out, result.err)
	}
	if got := authority.calls.Load(); got != 1 {
		t.Fatalf("authority calls=%d, want one", got)
	}
	out, err := handle.Invoke(
		t.Context(), access.OpRead, "resource:authority", nil, nil,
	)
	if err != nil || out.RejectReason != access.OwnerInactive {
		t.Fatalf("next stale Invoke=(%+v,%v)", out, err)
	}
	if got := authority.calls.Load(); got != 2 {
		t.Fatalf("authority calls=%d, want one per invocation", got)
	}

	_, out, err = handle.Open(t.Context(), "file:authority", access.OpRead)
	if err != nil || out.RejectReason != access.OwnerInactive {
		t.Fatalf("stale Open=(%+v,%v)", out, err)
	}
	if _, err := handle.Redeem(t.Context(), FileRoute{}); !errors.Is(err, ErrAuthorInactive) {
		t.Fatalf("stale Redeem err=%v", err)
	}
}

func TestAuthorityStateAdmitsOnceAndLetsAcceptedInvokeFinish(t *testing.T) {
	backing := &accessBackingAuthority{}
	state := &blockingStateStore{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	minted, err := New(Deps{
		Registry:  &fakeRegistry{},
		Drivers:   DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority: backing,
		State:     state,
	})
	if err != nil {
		t.Fatal(err)
	}
	authority := &accessRunAuthority{id: "agent:state-authority"}
	authority.allowed.Store(true)
	handle := minted.(*minter).MintStateAuthority(authority)

	done := make(chan struct {
		out Outcome
		err error
	}, 1)
	go func() {
		out, err := handle.Invoke(
			context.Background(), access.OpRead, "state:authority", nil, nil,
		)
		done <- struct {
			out Outcome
			err error
		}{out: out, err: err}
	}()
	<-state.entered
	authority.allowed.Store(false)
	close(state.release)
	result := <-done
	if result.err != nil || !result.out.Accepted() {
		t.Fatalf("accepted State.Invoke=(%+v,%v)", result.out, result.err)
	}
	if got := authority.calls.Load(); got != 1 {
		t.Fatalf("authority calls=%d, want one", got)
	}
	out, err := handle.Invoke(t.Context(), access.OpRead, "state:authority", nil, nil)
	if err != nil || out.RejectReason != access.OwnerInactive {
		t.Fatalf("next inactive State.Invoke=(%+v,%v)", out, err)
	}
}
