package home

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type boundedLedgerStub struct {
	storespec.VisibleMessageQuery
	total, read, calls int
	page               func(context.Context, int64, int) ([]storespec.StoredRow, int64, bool, error)
}

func (s *boundedLedgerStub) ReadVisibleBeforeSeq(ctx context.Context, before int64, limit int) ([]storespec.StoredRow, int64, bool, error) {
	s.calls++
	if s.page != nil {
		return s.page(ctx, before, limit)
	}
	end := s.total
	if before != 0 {
		end = int(before) - 1
	}
	start := max(0, end-limit)
	rows := make([]storespec.StoredRow, 0, end-start)
	for i := start + 1; i <= end; i++ {
		rows = append(rows, storespec.StoredRow{Seq: int64(i), Envelope: message.Envelope{ID: message.ID(fmt.Sprint(i)), Payload: []byte(`{"_context":{"session":"other"},"body":{}}`)}})
	}
	s.read += len(rows)
	return rows, int64(s.total), start > 0, nil
}

func TestLedgerViewHardScanLimit(t *testing.T) {
	for _, cap := range []int{1000, 4096} {
		for _, total := range []int{0, cap, 1_000_000} {
			s := &boundedLedgerStub{total: total}
			out, err := (View{visible: s}).Read(t.Context(), actorcaps.LedgerRead{MaxRows: cap})
			if total > cap {
				if !errors.Is(err, actorcaps.ErrLedgerLimit) || out.Rows != nil || out.HeadSeq != 0 {
					t.Fatalf("partial read returned: %+v %v", out, err)
				}
			} else if err != nil || len(out.Rows) != total || out.HeadSeq != int64(total) {
				t.Fatalf("bounded read rejected: %+v %v", out, err)
			}
			if s.read != min(cap, total) || s.calls > max(1, (cap+255)/256) {
				t.Fatalf("unbounded scan: rows=%d calls=%d", s.read, s.calls)
			}
		}
	}
}

func TestLedgerViewPinsHeadAcrossConcurrentAppends(t *testing.T) {
	s := &boundedLedgerStub{total: 300}
	base := &boundedLedgerStub{total: 300}
	s.page = func(ctx context.Context, before int64, limit int) ([]storespec.StoredRow, int64, bool, error) {
		if before != 0 {
			base.total = 301
		}
		return base.ReadVisibleBeforeSeq(ctx, before, limit)
	}
	out, err := (View{visible: s}).Read(t.Context(), actorcaps.LedgerRead{})
	if err != nil || out.HeadSeq != 300 || len(out.Rows) != 300 || out.Rows[0].Seq != 1 || out.Rows[299].Seq != 300 {
		t.Fatalf("snapshot changed: head=%d rows=%d err=%v", out.HeadSeq, len(out.Rows), err)
	}
}

func TestLedgerViewFailuresNeverReturnPartialHistory(t *testing.T) {
	for _, mode := range []string{"storage", "empty-page", "ordering", "bytes", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			s := &boundedLedgerStub{total: 300}
			base := &boundedLedgerStub{total: 300}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s.page = func(ctx context.Context, before int64, limit int) ([]storespec.StoredRow, int64, bool, error) {
				if before == 0 {
					return base.ReadVisibleBeforeSeq(ctx, before, limit)
				}
				switch mode {
				case "storage":
					return nil, 0, false, errors.New("read failed")
				case "empty-page":
					return nil, 300, true, nil
				case "ordering":
					return []storespec.StoredRow{{Seq: before}}, 300, true, nil
				case "bytes":
					return []storespec.StoredRow{{Seq: 1, Envelope: message.Envelope{Payload: []byte(strings.Repeat("x", actorcaps.MaxLedgerBytes))}}}, 300, false, nil
				default:
					cancel()
					return nil, 300, false, nil
				}
			}
			out, err := (View{visible: s}).Read(ctx, actorcaps.LedgerRead{})
			if err == nil || out.Rows != nil || out.HeadSeq != 0 || s.calls != 2 {
				t.Fatalf("partial history escaped: rows=%d head=%d calls=%d err=%v", len(out.Rows), out.HeadSeq, s.calls, err)
			}
		})
	}
}

func TestLedgerLimitsCannotBeRaisedAndCancelledReadsDoNotQuery(t *testing.T) {
	s := &boundedLedgerStub{total: 1_000_000}
	_, err := (View{visible: s}).Read(t.Context(), actorcaps.LedgerRead{MaxRows: 1_000_000, MaxBytes: 1 << 30})
	if !errors.Is(err, actorcaps.ErrLedgerLimit) || s.read != 4096 {
		t.Fatalf("raised hard cap: reads=%d err=%v", s.read, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s = &boundedLedgerStub{total: 100}
	_, err = (View{visible: s}).Read(ctx, actorcaps.LedgerRead{})
	if !errors.Is(err, context.Canceled) || s.calls != 0 {
		t.Fatalf("expired read queried storage: calls=%d err=%v", s.calls, err)
	}
}
