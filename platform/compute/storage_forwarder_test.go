package compute

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
)

type forwarderClient struct {
	reply link.ReconcilePullReply
	err   error
	calls int
}

func (c *forwarderClient) SendReconcilePull(context.Context, []string) (link.ReconcilePullReply, error) {
	c.calls++
	return c.reply, c.err
}

func (*forwarderClient) SendReclaimAck(context.Context, string) (link.ReclaimAckReply, error) {
	return link.ReclaimAckReply{}, nil
}

type forwarderHost struct {
	mu    sync.Mutex
	calls int
}

func (*forwarderHost) Alloc(string, bool) error    { return nil }
func (*forwarderHost) ActiveWriteCoords() []string { return nil }
func (h *forwarderHost) Reconcile(
	context.Context,
	[]StorageResourceCoord,
	[]StorageReservationCoord,
	[]StorageTombstoneCoord,
	StorageReclaimAckFunc,
) {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
}

func (h *forwarderHost) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func TestStorageHostForwarderCleanReplyReconciles(t *testing.T) {
	client := &forwarderClient{}
	host := &forwarderHost{}
	forwarder := newStorageHostForwarder(host, slog.New(slog.DiscardHandler), time.Second)
	forwarder.Rebind(client)
	forwarder.pass(context.Background())
	if client.calls != 1 || host.callCount() != 1 {
		t.Fatalf("pulls=%d reconciles=%d, want 1/1", client.calls, host.callCount())
	}
}

func TestStorageHostForwarderFailureOrRejectDoesNotReconcile(t *testing.T) {
	for _, client := range []*forwarderClient{
		{err: errors.New("offline")},
		{reply: link.ReconcilePullReply{Reason: "rejected"}},
	} {
		host := &forwarderHost{}
		forwarder := newStorageHostForwarder(host, slog.New(slog.DiscardHandler), time.Second)
		forwarder.Rebind(client)
		forwarder.pass(context.Background())
		if client.calls != 1 || host.callCount() != 0 {
			t.Fatalf("pulls=%d reconciles=%d, want 1/0", client.calls, host.callCount())
		}
	}
}
