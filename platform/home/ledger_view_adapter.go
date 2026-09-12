package home

import (
	"context"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
)

// actorLedgerView is the temporary actor-facing shape adapter while Home's
// cross-membrane cursor methods still return storage rows. The platform step
// removes that storage vocabulary and this adapter with it.
type actorLedgerView struct {
	View
}

func (v actorLedgerView) ReadVisibleAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]actorcaps.LedgerRow, int64, error) {
	rows, scanned, err := v.View.ReadVisibleAfterSeq(ctx, afterSeq, limit)
	if err != nil {
		return nil, scanned, err
	}
	return actorLedgerRows(rows), scanned, nil
}

func (v actorLedgerView) ReadVisibleBeforeSeq(ctx context.Context, beforeSeq int64, limit int) ([]actorcaps.LedgerRow, int64, bool, error) {
	rows, head, older, err := v.View.ReadVisibleBeforeSeq(ctx, beforeSeq, limit)
	if err != nil {
		return nil, head, older, err
	}
	return actorLedgerRows(rows), head, older, nil
}

var _ actorcaps.LedgerView = actorLedgerView{}
