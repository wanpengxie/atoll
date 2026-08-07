package home

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// closeBarrierDefault is each barrier's own budget when the caller brought no
// deadline: lifecycle verbs (rollback, Destroy, duplicate-open shutdown) own a
// bounded close regardless of what their request context is doing.
const closeBarrierDefault = 5 * time.Second

func (h *Home) closeInternal(reason string) error {
	return h.closeInternalUnder(reason, context.Background())
}

// closeInternalUnder runs the close sequence under the caller's budget. A ctx
// without a deadline keeps the historical shape — each barrier gets its own
// closeBarrierDefault — so verb-path closes stay bounded on their own terms
// while a process shutdown threads one shared budget through every Home.
func (h *Home) closeInternalUnder(reason string, ctx context.Context) error {
	h.closed.Store(true)
	joinCtx, joinCancel := quiesceContext(ctx)
	defer joinCancel()

	// This is a safety barrier, not an advisory cleanup step. A timeout only
	// means the caller stopped waiting; admitted commands still own Controller
	// and Store until commandOwner reaches its permanent drained level. Do not
	// consume closeOnce or tear down any dependency before that happens.
	if h.actors != nil {
		if err := h.actors.Quiesce(joinCtx); err != nil {
			return err
		}
	}

	h.closeOnce.Do(func() {
		defer close(h.closeDone)
		// Draining commands and retiring physical resources are separate
		// barriers. A command that used most of the join budget must not hand an
		// already-expired context to Host/SystemKernel shutdown: teardown must
		// actually run, so an exhausted caller budget falls back to the barrier
		// default here. At most one Home can be inside this block when a shared
		// budget dies — every Home behind it fails its Quiesce first — so the
		// overrun this floor buys is one barrier, not one per Home.
		barrierCtx, cancel := teardownContext(ctx)
		defer cancel()
		var faults []error

		if h.reconcileStop != nil {
			h.reconcileStop()
			if h.reconcileDone != nil {
				select {
				case <-h.reconcileDone:
				case <-barrierCtx.Done():
					faults = append(faults, barrierCtx.Err())
				}
			}
		}
		if h.delivery != nil {
			h.delivery.Close()
		}
		if h.engine != nil {
			h.engine.Close()
		}
		if h.actors != nil {
			faults = appendIfError(faults, h.actors.close(barrierCtx))
		}
		h.closeErr = errors.Join(faults...)
		h.logger.Info("platform.home.closed",
			"channel", h.channelID, "reason", reason,
			"cleanup_errors", len(faults),
		)
	})
	<-h.closeDone

	return errors.Join(h.closeErr, h.closeStoreUnder(ctx))
}

// quiesceContext is the pre-teardown barrier's budget: the caller's context
// verbatim when it brought a deadline — an exhausted shared budget fails this
// barrier immediately and leaves the Home honestly un-closed for a retry —
// and the barrier default when it brought none.
func quiesceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(context.Background(), closeBarrierDefault)
}

// teardownContext is the one-shot teardown barrier's budget: the caller's
// context while it still has time, the barrier default once it is spent —
// this block must actually run, so it never receives an already-dead context.
func teardownContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > 0 && ctx.Err() == nil {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(context.Background(), closeBarrierDefault)
}

// closeStoreUnder closes the physical stores within the caller's budget.
// Store close remains retryable after the one-shot runtime teardown, and it
// cannot be interrupted — sqlite does not take a context — so an expired
// budget stops waiting and says so while the close keeps running to
// completion. The mutex serializes retries behind the abandoned attempt, and
// whichever attempt finishes marks the level; process death is what reclaims
// a close that never returns.
func (h *Home) closeStoreUnder(ctx context.Context) error {
	if h.closeStore == nil || h.storeCloseDone.Load() {
		return nil
	}
	done := make(chan error, 1)
	go func() {
		h.storeCloseMu.Lock()
		defer h.storeCloseMu.Unlock()
		if h.storeCloseDone.Load() {
			done <- nil
			return
		}
		err := h.closeStore()
		if err == nil {
			h.storeCloseDone.Store(true)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("platform: close stores: %w", err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("platform: store close abandoned: budget expired while the close is still running")
	}
}

func appendIfError(in []error, err error) []error {
	if err != nil {
		return append(in, err)
	}
	return in
}
