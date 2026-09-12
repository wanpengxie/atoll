package ledgerview

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
)

type testAuthority struct {
	id  actor.ActorID
	err error
}

func (a *testAuthority) ActorID() actor.ActorID { return a.id }
func (a *testAuthority) Admit() error           { return a.err }

type recordingView struct {
	reads int
}

func (v *recordingView) Read(context.Context, actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
	v.reads++
	return actorcaps.LedgerSnapshot{HeadSeq: 7}, nil
}

func (v *recordingView) ReadVisibleAfterSeq(context.Context, int64, int) ([]actorcaps.LedgerRow, int64, error) {
	v.reads++
	return nil, 8, nil
}

func (v *recordingView) ReadVisibleBeforeSeq(context.Context, int64, int) ([]actorcaps.LedgerRow, int64, bool, error) {
	v.reads++
	return nil, 6, true, nil
}

func (v *recordingView) Session(context.Context, string, message.ID) ([]actorcaps.LedgerRow, error) {
	v.reads++
	return nil, nil
}

func (v *recordingView) BuildSession(context.Context, actorcaps.LedgerSnapshot, string, message.ID) ([]actorcaps.LedgerRow, error) {
	v.reads++
	return nil, nil
}

func (v *recordingView) Tail(context.Context, int64, int) ([]actorcaps.LedgerRow, int64, error) {
	v.reads++
	return nil, 0, nil
}

func TestBoundViewChecksAuthorityOnEveryOperation(t *testing.T) {
	inner := &recordingView{}
	minter, err := New(inner)
	if err != nil {
		t.Fatal(err)
	}
	authority := &testAuthority{id: "agent:test:1"}
	view := minter.MintAuthority(authority)

	if _, err := view.Read(t.Context(), actorcaps.LedgerRead{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := view.ReadVisibleAfterSeq(t.Context(), 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := view.ReadVisibleBeforeSeq(t.Context(), 0, 1); err != nil {
		t.Fatal(err)
	}
	if inner.reads != 3 {
		t.Fatalf("inner reads=%d want 3", inner.reads)
	}

	stale := errors.New("stale run")
	authority.err = stale
	if _, err := view.Read(t.Context(), actorcaps.LedgerRead{}); !errors.Is(err, stale) {
		t.Fatalf("Read err=%v want %v", err, stale)
	}
	if _, _, err := view.ReadVisibleAfterSeq(t.Context(), 0, 1); !errors.Is(err, stale) {
		t.Fatalf("After err=%v want %v", err, stale)
	}
	if _, _, _, err := view.ReadVisibleBeforeSeq(t.Context(), 0, 1); !errors.Is(err, stale) {
		t.Fatalf("Before err=%v want %v", err, stale)
	}
	if inner.reads != 3 {
		t.Fatalf("stale handle reached inner: reads=%d", inner.reads)
	}
}

func TestNewAndMintRejectMissingInputs(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("New(nil) err=%v", err)
	}
	minter, err := New(&recordingView{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := minter.MintAuthority(nil).Read(t.Context(), actorcaps.LedgerRead{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil authority err=%v", err)
	}
}
