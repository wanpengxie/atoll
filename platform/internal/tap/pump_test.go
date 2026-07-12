package tap

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type pumpQuery struct {
	read func(context.Context, int64, int) ([]storespec.StoredRow, error)
}

func (q pumpQuery) MaxSeq(context.Context) (int64, error) { return 0, nil }
func (q pumpQuery) ReadAfterSeq(ctx context.Context, seq int64, limit int) ([]storespec.StoredRow, error) {
	return q.read(ctx, seq, limit)
}
func (q pumpQuery) OpenRequestsForActor(context.Context, actor.ActorID) ([]storespec.StoredRow, error) {
	return nil, nil
}
func (q pumpQuery) DistinctOpenRequestReceivers(context.Context) ([]actor.ActorID, error) {
	return nil, nil
}

func TestOpenPumpCloseCancelsDrainAndIsIdempotent(t *testing.T) {
	entered := make(chan struct{})
	q := pumpQuery{read: func(ctx context.Context, _ int64, _ int) ([]storespec.StoredRow, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	p := OpenPump(NewSignal(), q, 0, func(storespec.StoredRow) error { return nil }, nil)
	<-entered
	done := make(chan struct{})
	go func() { p.Close(); p.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the in-store read")
	}
	if p.Leaked() != 0 {
		t.Fatalf("Leaked = %d", p.Leaked())
	}
}

func TestOpenPumpCloseReturnsSilent(t *testing.T) {
	var reads atomic.Int64
	q := pumpQuery{read: func(context.Context, int64, int) ([]storespec.StoredRow, error) {
		reads.Add(1)
		return nil, nil
	}}
	sig := NewSignal()
	p := OpenPump(sig, q, 0, func(storespec.StoredRow) error { return nil }, nil)
	deadline := time.Now().Add(time.Second)
	for reads.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	p.Close()
	before := reads.Load()
	for range 10 {
		sig.Notify()
	}
	time.Sleep(20 * time.Millisecond)
	if got := reads.Load(); got != before {
		t.Fatalf("reader called after Close: before=%d after=%d", before, got)
	}
}

func TestOpenPumpCloseBoundsReaderIgnoringContext(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	q := pumpQuery{read: func(context.Context, int64, int) ([]storespec.StoredRow, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil, nil
	}}
	p := OpenPump(NewSignal(), q, 0, func(storespec.StoredRow) error { return nil }, nil)
	<-entered
	started := time.Now()
	p.closeWithin(25 * time.Millisecond)
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("bounded Close took %v", elapsed)
	}
	if p.Leaked() != 1 {
		t.Fatalf("Leaked = %d, want 1", p.Leaked())
	}
	close(release)
	select {
	case <-p.done:
	case <-time.After(time.Second):
		t.Fatal("released pump did not exit")
	}
}

// TestPumpHandleErrorGatesCursorAndRetriesSameRow locks the cursor-gate
// contract (投递红线的 at-least-once 半): a handle error stops the cursor AT
// that row, and the next wake retries the SAME row — the gate is live
// machinery, not a dead branch.
func TestPumpHandleErrorGatesCursorAndRetriesSameRow(t *testing.T) {
	all := []storespec.StoredRow{{Seq: 1}, {Seq: 2}}
	q := pumpQuery{read: func(_ context.Context, after int64, _ int) ([]storespec.StoredRow, error) {
		var out []storespec.StoredRow
		for _, row := range all {
			if row.Seq > after {
				out = append(out, row)
			}
		}
		return out, nil
	}}
	var mu sync.Mutex
	var seen []int64
	var failedOnce bool
	sig := NewSignal()
	p := OpenPump(sig, q, 0, func(row storespec.StoredRow) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, row.Seq)
		if row.Seq == 1 && !failedOnce {
			failedOnce = true
			return errors.New("consumer gates the cursor")
		}
		return nil
	}, nil)
	defer p.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		done := len(seen) >= 3
		got := append([]int64(nil), seen...)
		mu.Unlock()
		if done {
			want := []int64{1, 1, 2}
			for i, seq := range want {
				if got[i] != seq {
					t.Fatalf("handle sequence = %v, want %v (row 1 retried, then advance)", got, want)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gated row never retried: seen %v", got)
		}
		sig.Notify() // next wake retries from the gated cursor
		time.Sleep(10 * time.Millisecond)
	}
}

// TestOpenPumpConcurrentCloseWaitsForCompletion locks 公理 3's completion
// semantics on the Pump: N concurrent Close callers all participate in or
// wait for the ONE real teardown — nobody returns while the winning closer is
// still inside its bounded join, and the abandon incident counts once.
func TestOpenPumpConcurrentCloseWaitsForCompletion(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	q := pumpQuery{read: func(context.Context, int64, int) ([]storespec.StoredRow, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil, nil
	}}
	p := OpenPump(NewSignal(), q, 0, func(storespec.StoredRow) error { return nil }, nil)
	<-entered
	done := make(chan struct{}, 2)
	go func() { p.closeWithin(100 * time.Millisecond); done <- struct{}{} }()
	go func() { p.closeWithin(100 * time.Millisecond); done <- struct{}{} }()
	select {
	case <-done:
		t.Fatal("a Close returned before the one real teardown converged")
	case <-time.After(50 * time.Millisecond):
	}
	for range 2 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("a Close caller did not converge with the one teardown")
		}
	}
	if p.Leaked() != 1 {
		t.Fatalf("Leaked = %d, want one incident", p.Leaked())
	}
	close(release)
	select {
	case <-p.done:
	case <-time.After(time.Second):
		t.Fatal("released pump did not exit")
	}
}

// TestOpenPumpAbandonedReaderRowsNeverReachHandle locks the abandoned pump's
// silence promise (弃证的写路径半): a reader that ignores ctx parks past the
// bounded Close, then RECOVERS and returns rows — those rows must never reach
// handle, because the owner has moved on to tearing down the very things
// handle touches (cells, stores).
func TestOpenPumpAbandonedReaderRowsNeverReachHandle(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	q := pumpQuery{read: func(context.Context, int64, int) ([]storespec.StoredRow, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return []storespec.StoredRow{{Seq: 1}}, nil
	}}
	var handled atomic.Int64
	p := OpenPump(NewSignal(), q, 0, func(storespec.StoredRow) error { handled.Add(1); return nil }, nil)
	<-entered
	p.closeWithin(25 * time.Millisecond)
	if p.Leaked() != 1 {
		t.Fatalf("Leaked = %d, want 1", p.Leaked())
	}
	close(release) // the parked reader recovers and hands rows to the leaked goroutine
	select {
	case <-p.done:
	case <-time.After(time.Second):
		t.Fatal("released pump did not exit")
	}
	if got := handled.Load(); got != 0 {
		t.Fatalf("handle called %d times after abandoned Close, want 0", got)
	}
}

// TestOpenPumpCloseNeverAbandonsMidHandle drives the check→handle straddle
// window itself: the drain goroutine is parked INSIDE a handle call while a
// bounded Close times out. Close must wait out that one in-flight handle
// (the fence's mutex), and once Close has returned no further row may reach
// handle — the exact interleave a bare ctx re-check cannot exclude.
func TestOpenPumpCloseNeverAbandonsMidHandle(t *testing.T) {
	rows := []storespec.StoredRow{{Seq: 1}, {Seq: 2}}
	fed := make(chan struct{})
	q := pumpQuery{read: func(context.Context, int64, int) ([]storespec.StoredRow, error) {
		select {
		case <-fed:
			select {} // park forever: the leaked goroutine stays alive past Close
		default:
			close(fed)
			return rows, nil
		}
	}}
	entered, release := make(chan struct{}), make(chan struct{})
	var handled atomic.Int64
	p := OpenPump(NewSignal(), q, 0, func(storespec.StoredRow) error {
		if handled.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	}, nil)
	<-entered // drain is inside handle(row 1)
	closed := make(chan struct{})
	go func() { p.closeWithin(25 * time.Millisecond); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while a handle call was in flight")
	case <-time.After(100 * time.Millisecond):
	}
	close(release) // handle 1 completes; Close seals and returns
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the in-flight handle completed")
	}
	// Row 2 may legally slip in iff drain won the fence mutex BEFORE Close's
	// seal (that call precedes Close's return). What must hold: the count
	// freezes — once sealed, no row ever reaches handle again.
	time.Sleep(50 * time.Millisecond)
	frozen := handled.Load()
	if frozen > 2 {
		t.Fatalf("handled = %d, want at most the one pre-seal race row", frozen)
	}
	time.Sleep(50 * time.Millisecond)
	if got := handled.Load(); got != frozen {
		t.Fatalf("handle called after Close sealed: %d -> %d", frozen, got)
	}
	if p.Leaked() != 1 {
		t.Fatalf("Leaked = %d, want 1", p.Leaked())
	}
}
