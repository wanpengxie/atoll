package home

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (h *Home) closeInternal(reason string) error {
	return h.closeInternalWithin(reason, 5*time.Second)
}

func (h *Home) closeInternalWithin(reason string, timeout time.Duration) error {
	h.closed.Store(true)
	joinCtx, joinCancel := context.WithTimeout(context.Background(), timeout)
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
		// already-expired context to Host/SystemKernel shutdown.
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		var faults []error

		if h.reconcileStop != nil {
			h.reconcileStop()
			if h.reconcileDone != nil {
				select {
				case <-h.reconcileDone:
				case <-ctx.Done():
					faults = append(faults, ctx.Err())
				}
			}
		}
		if h.links != nil {
			faults = appendIfError(faults, h.links.Close())
		}
		if h.delivery != nil {
			h.delivery.Close()
		}
		if h.engine != nil {
			h.engine.Close()
		}
		if h.actors != nil {
			faults = appendIfError(faults, h.actors.close(ctx))
		}
		h.closeErr = errors.Join(faults...)
		h.logger.Info("platform.home.closed",
			"channel", h.channelID, "reason", reason,
			"cleanup_errors", len(faults),
		)
	})
	<-h.closeDone

	// Store close remains retryable after the one-shot runtime teardown.
	if h.closeStore != nil && !h.storeCloseDone.Load() {
		h.storeCloseMu.Lock()
		defer h.storeCloseMu.Unlock()
		if !h.storeCloseDone.Load() {
			if err := h.closeStore(); err != nil {
				return errors.Join(h.closeErr, fmt.Errorf("platform: close stores: %w", err))
			}
			h.storeCloseDone.Store(true)
		}
	}
	return h.closeErr
}

func appendIfError(in []error, err error) []error {
	if err != nil {
		return append(in, err)
	}
	return in
}
