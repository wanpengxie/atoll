package home

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
)

// Read owns pagination and its hard budget. Filtering never hides storage work.
// Reading backwards pins the first head: concurrent appends cannot enter it.
func (v View) Read(ctx context.Context, q actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
	if q.MaxRows < 0 || q.MaxBytes < 0 {
		return actorcaps.LedgerSnapshot{}, fmt.Errorf("negative ledger read limit")
	}
	maxRows, maxBytes := actorcaps.MaxLedgerRows, actorcaps.MaxLedgerBytes
	if q.MaxRows > 0 {
		maxRows = min(maxRows, q.MaxRows)
	}
	if q.MaxBytes > 0 {
		maxBytes = min(maxBytes, q.MaxBytes)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var out actorcaps.LedgerSnapshot
	before, count, bytes := int64(0), 0, 0
	for {
		if err := ctx.Err(); err != nil {
			return actorcaps.LedgerSnapshot{}, err
		}
		if count == maxRows {
			return actorcaps.LedgerSnapshot{}, actorcaps.ErrLedgerLimit
		}
		limit := min(256, maxRows-count)
		page, head, older, err := v.visible.ReadVisibleBeforeSeq(ctx, before, limit)
		if err != nil {
			return actorcaps.LedgerSnapshot{}, err
		}
		if before == 0 {
			out.HeadSeq = head
		}
		if len(page) > limit {
			return actorcaps.LedgerSnapshot{}, actorcaps.ErrLedgerLimit
		}
		if len(page) == 0 && older {
			return actorcaps.LedgerSnapshot{}, fmt.Errorf("ledger read made no progress")
		}
		for i := len(page) - 1; i >= 0; i-- {
			row := page[i]
			if row.Seq <= 0 || row.Seq > out.HeadSeq || (before > 0 && row.Seq >= before) || (i > 0 && page[i-1].Seq >= row.Seq) {
				return actorcaps.LedgerSnapshot{}, fmt.Errorf("invalid ledger ordering")
			}
			count++
			bytes += len(row.Envelope.Payload)
			if bytes > maxBytes {
				return actorcaps.LedgerSnapshot{}, actorcaps.ErrLedgerLimit
			}
			out.Rows = append(out.Rows, actorcaps.LedgerRow{Envelope: row.Envelope, Seq: row.Seq, IsTerminal: row.IsTerminal})
		}
		if !older {
			break
		}
		before = page[0].Seq
	}
	if err := ctx.Err(); err != nil {
		return actorcaps.LedgerSnapshot{}, err
	}
	slices.Reverse(out.Rows)
	return out, nil
}
