package home

import "context"

// DaemonObligationCounts returns the durable resource obligations still named
// after daemonID. It is an observation-only retirement aid: callers use it for
// edge logging after world-layer revocation, never to decide or roll it back.
func (h *Home) daemonObligationCounts(ctx context.Context, daemonID string) (resources, reservations, tombstones int, err error) {
	if h.closed.Load() {
		return 0, 0, 0, ErrClosed
	}
	resourceRows, err := h.cs.Outbox.ListByPlacementDaemon(ctx, daemonID)
	if err != nil {
		return 0, 0, 0, err
	}
	reservationRows, err := h.cs.Outbox.ListReservationsByDaemon(ctx, daemonID)
	if err != nil {
		return 0, 0, 0, err
	}
	tombstoneRows, err := h.cs.Outbox.ListTombstonesByDaemon(ctx, daemonID)
	if err != nil {
		return 0, 0, 0, err
	}
	return len(resourceRows), len(reservationRows), len(tombstoneRows), nil
}
