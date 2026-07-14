package home

import (
	"errors"
	"time"
)

// Close tears down the channel home by quiescing PRODUCERS before the CONSUMERS
// they feed, so no still-live producer can enqueue work into an already-dead sink:
//
//  1. reconcile ticker — stops the SpawnIfAbsent/activation producer.
//  2. link acceptor    — stops port producers (their schedule/access wire arms)
//     and all external compute traffic.
//  3. delivery tap      — stops delivering messages that would drive a cell to
//     schedule/emit anew.
//  4. cells             — stops every in-proc cell (the Schedule/emit producers);
//     their goroutines are joined.
//  5. schedule engine   — only NOW, with every schedule PRODUCER gone, is the time
//     engine's run loop stopped. Stopping it earlier (the old "engine first" order)
//     left a window where a still-live cell/port could Schedule() an in-memory
//     (incarnation-bind) timer into a dead run loop, silently losing it.
//  6. channel stores    — the DB the engine fired into (FireSink→pen→log) and cells
//     persisted to, torn down last.
//
// No deadlock: cell shutdown never blocks on the engine (Schedule/Cancel only
// insert into mem + post a wake, never join the run loop), and engine.Close only
// joins its own run goroutine (fireDue's Append/Revive never block on the
// already-stopped cells).
func (h *Home) Close() error { return h.closeInternal("normal") }

func (h *Home) closeInternal(reason string) error {
	return h.closeInternalWithin(reason, 5*time.Second)
}

func (h *Home) closeInternalWithin(reason string, reconcileTimeout time.Duration) error {
	h.closeOnce.Do(func() {
		started := time.Now()
		defer close(h.closeDone)
		// Publish the lifecycle fence as the first close action. Public mutation
		// entry points check closed before touching durable state, so no scheduler
		// handoff or fault checkpoint between close entry and Runtime.Seal can leave
		// a store-write window open.
		h.closed.Store(true)
		var errs []error
		addErr := func(err error) {
			if err != nil {
				errs = append(errs, err)
			}
		}
		var teardownPanic any
		guard := func(name string, fn func()) {
			defer func() {
				if p := recover(); p != nil {
					if teardownPanic == nil {
						teardownPanic = p
					}
					h.logger.Error("home.teardown.panic", "step", name, "panic", p)
				}
			}()
			fn()
		}
		step := func(name string, fn func()) {
			guard(name+".checkpoint", func() {
				if err := h.faults.checkpoint(name); err != nil {
					errs = append(errs, err)
				}
			})
			guard(name, fn)
		}
		// Step ZERO seals the construction authority immediately after the public
		// lifecycle fence above.
		step("close.seal", func() {
			if h.channel != nil && h.channel.Cells() != nil {
				h.channel.Cells().Seal()
			}
		})
		guard("state.closing.checkpoint", func() { _ = h.faults.checkpoint("state.closing") })
		step("close.reconcile", func() {
			if h.reconcileStop == nil {
				return
			}
			h.reconcileStop()
			if h.reconcileDone == nil {
				return
			}
			select {
			case <-h.reconcileDone:
			case <-time.After(reconcileTimeout):
				h.reconcileLeaked.Add(1)
				h.logger.Error("home.reconcile.join_timeout", "timeout", reconcileTimeout,
					"safety", "runtime admission sealed; late writes hit closed stores")
			}
		})
		step("close.links", func() {
			if h.links != nil {
				addErr(h.links.Close())
			}
		})
		step("close.delivery", func() {
			if h.delivery != nil {
				h.delivery.Close()
			}
		})
		step("close.cells", func() {
			if h.channel == nil {
				return
			}
			rt := h.channel.Cells()
			if rt != nil {
				rt.StopAll()
				if leaked := rt.DrainZombies(0); len(leaked) > 0 {
					h.logger.Warn("home.close.zombies_leaked", "channel", h.channelID, "count", len(leaked), "actors", leaked)
				}
			}
			h.channel.Close()
		})
		step("close.engine", func() {
			if h.engine != nil {
				h.engine.Close()
			}
		})
		step("close.stores", func() {
			if h.cs != nil {
				addErr(h.cs.Close())
			}
		})
		h.closeErr = errors.Join(errs...)
		leaked := h.reconcileLeaked.Load()
		if h.links != nil {
			leaked += h.links.Leaked()
		}
		if h.delivery != nil {
			leaked += h.delivery.Leaked()
		}
		// zombieTotal is the runtime's LIFETIME zombie count — a cumulative
		// account, not this close's delta — so it is reported as its own field
		// rather than folded into leaked (which sums per-instance close deltas).
		var zombieTotal int64
		if h.channel != nil {
			leaked += h.channel.Leaked()
			if h.channel.Cells() != nil {
				zombieTotal = h.channel.Cells().LeakedTotal()
			}
		}
		if h.engine != nil {
			leaked += h.engine.Leaked()
		}
		_ = h.faults.checkpoint("state.closed")
		step("close.end", func() {})
		h.logger.Info("platform.home.closed", "channel", h.channelID, "reason", reason,
			"cleanup_errors", len(errs), "leaked", leaked, "zombie_total", zombieTotal,
			"duration", time.Since(started))
		if teardownPanic != nil {
			panic(teardownPanic)
		}
	})
	<-h.closeDone
	return h.closeErr
}
