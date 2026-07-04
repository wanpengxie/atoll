package tap

import (
	"context"
	"log/slog"

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
	stop      chan struct{}
}

// NewPump builds a pump that reads rows with seq > from, advancing past each row
// handle accepts. logger surfaces read faults (a failed ReadAfterSeq is the only
// fault the pump itself can hit); nil → discard. The pump does not start until
// Start is called.
func NewPump(sig *Signal, reader storespec.MessageQuery, from int64,
	handle func(storespec.StoredRow) error, logger *slog.Logger) *Pump {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Pump{
		sig:    sig,
		reader: reader,
		handle: handle,
		logger: logger,
		cursor: from,
		done:   make(chan struct{}),
		stop:   make(chan struct{}),
	}
}

// Start subscribes to the signal and launches the pump loop. It runs one initial
// drain (so rows committed between the from cursor and subscription are not
// missed) then blocks on wakes.
func (p *Pump) Start() {
	p.wake, p.cancelSub = p.sig.Subscribe()
	go p.run()
}

func (p *Pump) run() {
	defer close(p.done)
	// Initial drain: cover the window between the from cursor and Subscribe.
	p.drain()
	for {
		select {
		case <-p.stop:
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
	ctx := context.Background()
	for {
		select {
		case <-p.stop:
			return
		default:
		}
		rows, err := p.reader.ReadAfterSeq(ctx, p.cursor, readBatch)
		if err != nil {
			p.logger.Error("tap.pump.read_failed", "cursor", p.cursor, "err", err)
			return
		}
		if len(rows) == 0 {
			return
		}
		for _, row := range rows {
			if err := p.handle(row); err != nil {
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

// Close stops the pump loop and unsubscribes from the signal.
func (p *Pump) Close() {
	close(p.stop)
	if p.cancelSub != nil {
		p.cancelSub()
	}
	<-p.done
}
