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
	h.closeOnce.Do(func() {
		defer close(h.closeDone)
		h.closed.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		var faults []error

		// Command admission closes first. Every command admitted before this
		// point retains ownership through Store commit and Controller
		// publication before any Host is sealed.
		if h.actors != nil {
			faults = appendIfError(faults, h.actors.Quiesce(ctx))
		}
		if h.reconcileStop != nil {
			h.reconcileStop()
			if h.reconcileDone != nil {
				select {
				case <-h.reconcileDone:
				case <-ctx.Done():
					h.reconcileLeaked.Add(1)
					faults = append(faults, ctx.Err())
				}
			}
		}
		if h.links != nil {
			faults = appendIfError(faults, h.links.Close())
		}
		if h.deliveryStop != nil {
			h.deliveryStop()
		}
		if h.delivery != nil {
			h.delivery.Close()
		}
		if h.engine != nil {
			h.engine.Close()
		}
		if h.actors != nil {
			// ChannelActors closes managed Host, Controller, then the
			// SystemKernel exact Unit last.
			faults = appendIfError(faults, h.actors.Close(ctx))
		}
		h.closeErr = errors.Join(faults...)
		h.logger.Info("platform.home.closed",
			"channel", h.channelID, "reason", reason,
			"cleanup_errors", len(faults),
		)
	})
	<-h.closeDone

	// Store close remains retryable after the one-shot runtime teardown.
	if h.cs != nil && !h.storeCloseDone.Load() {
		h.storeCloseMu.Lock()
		defer h.storeCloseMu.Unlock()
		if !h.storeCloseDone.Load() {
			if err := h.cs.Close(); err != nil {
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
