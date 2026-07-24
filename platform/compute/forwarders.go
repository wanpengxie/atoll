package compute

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
)

type storageHostForwarder struct {
	host     StorageHost
	logger   *slog.Logger
	interval time.Duration
	mu       sync.RWMutex
	client   storageControlClient
}

type storageControlClient interface {
	SendReconcilePull(context.Context, []string) (link.ReconcilePullReply, error)
	SendReclaimAck(context.Context, string) (link.ReclaimAckReply, error)
}

const scrubberPumpInterval = 60 * time.Second

func newStorageHostForwarder(
	host StorageHost,
	logger *slog.Logger,
	interval time.Duration,
) *storageHostForwarder {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if interval <= 0 {
		interval = scrubberPumpInterval
	}
	return &storageHostForwarder{host: host, logger: logger, interval: interval}
}

func (f *storageHostForwarder) Rebind(client storageControlClient) {
	f.mu.Lock()
	f.client = client
	f.mu.Unlock()
}

func (f *storageHostForwarder) current() storageControlClient {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.client
}

func (f *storageHostForwarder) handleAlloc(request link.AllocRequest) link.AllocReply {
	if f.host == nil {
		return link.AllocReply{OK: false, Reason: "compute: no storage host wired"}
	}
	if err := f.host.Alloc(request.Coord, request.Dir); err != nil {
		return link.AllocReply{OK: false, Reason: err.Error()}
	}
	return link.AllocReply{OK: true}
}

func (f *storageHostForwarder) pump(ctx context.Context) {
	if f.host == nil {
		return
	}
	f.pass(ctx)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.pass(ctx)
		}
	}
}

func (f *storageHostForwarder) pass(ctx context.Context) {
	dialer := f.current()
	if dialer == nil {
		return
	}
	reply, err := dialer.SendReconcilePull(ctx, f.host.ActiveWriteCoords())
	if err != nil {
		f.logger.Warn("platform.compute.storage_reconcile_pull_failed", "err", err)
		return
	}
	if reply.Reason != "" {
		f.logger.Warn("platform.compute.storage_reconcile_pull_rejected", "reason", reply.Reason)
		return
	}
	resources := make([]StorageResourceCoord, 0, len(reply.Resources))
	for _, row := range reply.Resources {
		resources = append(resources, StorageResourceCoord{Coord: row.Coord})
	}
	reservations := make([]StorageReservationCoord, 0, len(reply.PendingReservations))
	for _, row := range reply.PendingReservations {
		reservations = append(reservations, StorageReservationCoord{
			ReservationID: row.ReservationID,
			Coord:         row.Coord,
		})
	}
	tombstones := make([]StorageTombstoneCoord, 0, len(reply.PendingTombstones))
	for _, row := range reply.PendingTombstones {
		tombstones = append(tombstones, StorageTombstoneCoord{
			TombstoneID: row.TombstoneID,
			Coord:       row.Coord,
		})
	}
	f.host.Reconcile(
		ctx,
		resources,
		reservations,
		tombstones,
		func(callCtx context.Context, tombstoneID string) (bool, error) {
			current := f.current()
			if current == nil {
				return false, fmt.Errorf("compute: no live link for tombstone %q", tombstoneID)
			}
			result, err := current.SendReclaimAck(callCtx, tombstoneID)
			return result.Found, err
		},
	)
}
