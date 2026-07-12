package tap

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/runtime/storespec"
)

// readBatch caps one ReadAfterSeq page. A drained read (< readBatch rows) ends
// the catch-up loop until the next wake. Physical buffer parameter, no semantics.
const readBatch = 256

// Pump is one subscriber's cursor pump over committed truth: it waits on the
// Signal, reads forward from its own seq cursor via ReadAfterSeq, hands each row
// to handle in seq order, and advances the cursor past handled rows. The signal
// is lossy — a coalesced wake just means the next read catches every new seq, so
// signal merging is harmless. Correctness is the cursor read.
//
// handle's error is the cursor's gate: a returned error STOPS the cursor at that
// row, so the next wake retries it (at-least-once for handles that need it). A
// best-effort handle (cell delivery) simply never returns an error — it observes
// the row and returns nil, so the cursor always advances (push-mailbox semantics:
// a not-hosted/full mailbox is observed, not retried).
type Pump struct {
	sig    *Signal
	reader storespec.MessageQuery
	handle func(storespec.StoredRow) error
	logger *slog.Logger
	cursor int64

	cancelSub func()
	wake      <-chan struct{}
	done      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeDone chan struct{}
	leaked    atomic.Int64

	// handleMu/handleSealed fence handle invocation against Close's bounded
	// abandon (公理 7 通用款的 per-component instance): a ctx check alone
	// leaves a check→call window — the drain goroutine can pass the check,
	// park, and only resume after Close has abandoned and returned. The seal
	// and the call share one critical section, and the abandon path seals
	// BEFORE returning (blocking at most one strictly non-blocking handle),
	// so "Close 返回后 handle=0" holds absolutely, not probabilistically.
	handleMu     sync.Mutex
	handleSealed bool
}

// errPumpSealed stops drain at the current row when Close has abandoned the
// pump — same control flow as a handle gating the cursor (neither advances).
var errPumpSealed = errors.New("tap: pump sealed by bounded close")

// OpenPump builds and starts a pump that reads rows with seq > from, advancing past each row
// handle accepts. logger surfaces read faults (a failed ReadAfterSeq is the only
// fault the pump itself can hit); nil → discard. The pump does not start until
// Start is called.
func OpenPump(sig *Signal, reader storespec.MessageQuery, from int64,
	handle func(storespec.StoredRow) error, logger *slog.Logger) *Pump {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pump{
		sig:    sig,
		reader: reader,
		handle: handle,
		logger: logger,
		cursor:    from,
		done:      make(chan struct{}),
		closeDone: make(chan struct{}),
		ctx:       ctx,
		cancel:    cancel,
	}
	p.wake, p.cancelSub = p.sig.Subscribe()
	go p.run()
	return p
}

func (p *Pump) run() {
	defer close(p.done)
	// Initial drain: cover the window between the from cursor and Subscribe.
	p.drain()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.wake:
			p.drain()
		}
	}
}

// drain reads forward until the store is caught up (a short page = drained) or a
// handle gates the cursor. A read fault leaves the cursor put — the next wake
// retries from the same position.
func (p *Pump) drain() {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}
		rows, err := p.reader.ReadAfterSeq(p.ctx, p.cursor, readBatch)
		if err != nil {
			p.logger.Error("tap.pump.read_failed", "cursor", p.cursor, "err", err)
			return
		}
		// Re-check ctx AFTER the read returns: a reader that ignores ctx can park
		// past Close's bounded abandon and hand rows back to a goroutine the owner
		// already gave up on — those rows must never reach handle (the abandoned
		// pump's silence promise: Home tears down cells/stores right after the
		// abandon, and a late handle call would touch them).
		if p.ctx.Err() != nil {
			return
		}
		if len(rows) == 0 {
			return
		}
		for _, row := range rows {
			if err := p.guardedHandle(row); err != nil {
				// Cursor gated at this row: stop here, retry on next wake. This IS
				// the at-least-once delivery contract's physical implementation
				// (the same skeleton as a Kafka consumer offset / WAL apply
				// cursor / replication cursor) — not dead code. Today's only
				// handle (cell delivery) is best-effort and never returns an
				// error, so this branch is currently unexercised; that is the
				// CONSUMER's choice to forgo retry, not evidence the gate itself
				// is unneeded. Any future reliable consumer (audit log, external
				// export, indexer) needs exactly this gate the moment it exists —
				// do NOT remove it as an apparently-dead branch.
				return
			}
			p.cursor = row.Seq
		}
		if len(rows) < readBatch {
			return
		}
	}
}

// guardedHandle invokes handle under the admission fence: sealed → the row is
// refused (errPumpSealed, cursor stays put, drain stops). The fast-path ctx
// checks in drain remain, but only this critical section is the proof.
func (p *Pump) guardedHandle(row storespec.StoredRow) error {
	p.handleMu.Lock()
	defer p.handleMu.Unlock()
	if p.handleSealed {
		return errPumpSealed
	}
	return p.handle(row)
}

// Close stops the pump loop and unsubscribes from the signal.
func (p *Pump) Close() {
	p.closeWithin(5 * time.Second)
}

func (p *Pump) closeWithin(timeout time.Duration) {
	p.closeOnce.Do(func() {
		// Completion semantics (公理 3, per-component): closeDone is closed by
		// defer so it survives a teardown panic — a later Close never returns
		// before the one real teardown has fully converged (or panicked out),
		// decoupling "done" from Once's own burnt flag.
		defer close(p.closeDone)
		p.cancel()
		if p.cancelSub != nil {
			p.cancelSub()
		}
		select {
		case <-p.done:
		case <-time.After(timeout):
			// Seal the handle fence BEFORE returning: the leaked goroutine may
			// still be parked anywhere (even past a ctx check), but it can never
			// again enter handle. Taking the mutex waits out at most one
			// in-flight strictly non-blocking handle call — bounded.
			p.handleMu.Lock()
			p.handleSealed = true
			p.handleMu.Unlock()
			// Bounded abandon proof: the pump can only read and invoke the strictly
			// non-blocking delivery handle (write path); the fence above makes the
			// write path unreachable after this return, and the pump has no
			// actor/goroutine production capability.
			p.leaked.Add(1)
			p.logger.Error("tap.pump.join_timeout", "timeout", timeout,
				"safety", "reader/handle cannot produce actors; writes remain cursor-gated")
		}
	})
	<-p.closeDone // 后到者一律等 closeDone (公理 3)
}

func (p *Pump) Leaked() int64 { return p.leaked.Load() }
