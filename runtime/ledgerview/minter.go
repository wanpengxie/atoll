package ledgerview

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/capauth"
)

var ErrInvalidInput = errors.New("ledgerview: invalid mint input")

// Minter is the sole outward mint face for ledger views.
type Minter interface {
	MintAuthority(capauth.Authority) actorcaps.LedgerView
}

type minter struct {
	inner actorcaps.LedgerView
}

func New(inner actorcaps.LedgerView) (Minter, error) {
	if inner == nil {
		return nil, ErrInvalidInput
	}
	return &minter{inner: inner}, nil
}

func (m *minter) MintAuthority(authority capauth.Authority) actorcaps.LedgerView {
	if m == nil || m.inner == nil || authority == nil || authority.ActorID() == "" {
		return rejectedView{err: ErrInvalidInput}
	}
	return boundView{inner: m.inner, authority: authority}
}

type boundView struct {
	inner     actorcaps.LedgerView
	authority capauth.Authority
}

func (v boundView) admit() error {
	if v.authority == nil {
		return ErrInvalidInput
	}
	return v.authority.Admit()
}

func (v boundView) Read(ctx context.Context, req actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
	if err := v.admit(); err != nil {
		return actorcaps.LedgerSnapshot{}, err
	}
	return v.inner.Read(ctx, req)
}

func (v boundView) ReadVisibleAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]actorcaps.LedgerRow, int64, error) {
	if err := v.admit(); err != nil {
		return nil, afterSeq, err
	}
	return v.inner.ReadVisibleAfterSeq(ctx, afterSeq, limit)
}

func (v boundView) ReadVisibleBeforeSeq(ctx context.Context, beforeSeq int64, limit int) ([]actorcaps.LedgerRow, int64, bool, error) {
	if err := v.admit(); err != nil {
		return nil, beforeSeq, false, err
	}
	return v.inner.ReadVisibleBeforeSeq(ctx, beforeSeq, limit)
}

type rejectedView struct {
	err error
}

func (v rejectedView) Read(context.Context, actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
	return actorcaps.LedgerSnapshot{}, v.err
}

func (v rejectedView) ReadVisibleAfterSeq(context.Context, int64, int) ([]actorcaps.LedgerRow, int64, error) {
	return nil, 0, v.err
}

func (v rejectedView) ReadVisibleBeforeSeq(context.Context, int64, int) ([]actorcaps.LedgerRow, int64, bool, error) {
	return nil, 0, false, v.err
}

var _ actorcaps.LedgerView = boundView{}
var _ actorcaps.LedgerView = rejectedView{}
