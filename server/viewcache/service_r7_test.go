package viewcache

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/server/store"
)

func newR7Service(t *testing.T) *Service {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "vc.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewService(db)
}

func r7Frame(seq viewsync.Seq) viewsync.PushFrame {
	return viewsync.PushFrame{
		ChannelID: channel.ID("ch-r7"),
		Seq:       seq,
		MessageID: message.ID("r7-m-" + r7Itoa(int64(seq))),
		Envelope: message.Envelope{
			ID:         message.ID("r7-m-" + r7Itoa(int64(seq))),
			TS:         int64(seq) * 1000,
			ChannelID:  "ch-r7",
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:r7"},
			Kind:       message.KindEvent,
			Type:       "agent.text",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{message.AudienceWildcard},
			Seq:        int64(seq),
		},
	}
}

func r7Itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

func TestChannelBuffer_PerChannelCap_OverflowTriggersResyncRequired(t *testing.T) {
	svc := newR7Service(t)
	ctx := context.Background()

	if _, err := svc.Apply(ctx, r7Frame(1)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var got viewsync.ApplyResult
	overflowSeq := viewsync.Seq(defaultPendingFrameCap + 3)
	for seq := viewsync.Seq(3); seq <= overflowSeq; seq++ {
		res, err := svc.Apply(ctx, r7Frame(seq))
		if err != nil {
			t.Fatalf("Apply seq=%d: %v", seq, err)
		}
		got = res
	}
	if got.Outcome != viewsync.ApplyOutcomeResyncRequired {
		t.Fatalf("overflow outcome=%q want resync_required", got.Outcome)
	}

	buf := svc.bufferFor("ch-r7")
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if len(buf.pending) != 0 || buf.pendingBytes != 0 {
		t.Fatalf("pending buffer len=%d bytes=%d want cleared", len(buf.pending), buf.pendingBytes)
	}
}

type blockingR7Resyncer struct {
	calls   atomic.Int32
	called  chan [2]viewsync.Seq
	release chan struct{}
}

func (r *blockingR7Resyncer) RequestResync(
	ctx context.Context,
	channelID channel.ID,
	since, until viewsync.Seq,
) ([]viewsync.ResyncMessage, error) {
	r.calls.Add(1)
	r.called <- [2]viewsync.Seq{since, until}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.release:
		return nil, nil
	}
}

func TestGapResync_OverlappingWindows_Coalesced(t *testing.T) {
	svc := newR7Service(t)
	ctx := context.Background()
	resyncer := &blockingR7Resyncer{
		called:  make(chan [2]viewsync.Seq, 4),
		release: make(chan struct{}),
	}
	svc.SetResyncer(resyncer)

	if _, err := svc.Apply(ctx, r7Frame(1)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := svc.Apply(ctx, r7Frame(3)); err != nil {
		t.Fatalf("first gap: %v", err)
	}
	select {
	case got := <-resyncer.called:
		if got != [2]viewsync.Seq{2, 2} {
			t.Fatalf("first resync window=%v want [2 2]", got)
		}
	case <-time.After(time.Second):
		t.Fatal("first resync not started")
	}

	if _, err := svc.Apply(ctx, r7Frame(5)); err != nil {
		t.Fatalf("overlapping gap: %v", err)
	}
	select {
	case got := <-resyncer.called:
		t.Fatalf("overlapping gap fired another resync window=%v", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(resyncer.release)
	if got := resyncer.calls.Load(); got != 1 {
		t.Fatalf("resync calls=%d want 1", got)
	}
}

func TestGapResync_RateLimitedCooldown(t *testing.T) {
	oldNow := nowMs
	now := int64(1000)
	nowMs = func() int64 { return now }
	t.Cleanup(func() { nowMs = oldNow })

	svc := newR7Service(t)
	ctx := context.Background()
	calls := make(chan [2]viewsync.Seq, 4)
	svc.SetFireResyncForTest(func(channelID channel.ID, since, until viewsync.Seq) {
		calls <- [2]viewsync.Seq{since, until}
	})

	if _, err := svc.Apply(ctx, r7Frame(1)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := svc.Apply(ctx, r7Frame(3)); err != nil {
		t.Fatalf("first gap: %v", err)
	}
	select {
	case got := <-calls:
		if got != [2]viewsync.Seq{2, 2} {
			t.Fatalf("first window=%v want [2 2]", got)
		}
	default:
		t.Fatal("first gap did not fire resync")
	}

	if _, err := svc.Apply(ctx, r7Frame(5)); err != nil {
		t.Fatalf("cooldown gap: %v", err)
	}
	select {
	case got := <-calls:
		t.Fatalf("cooldown gap fired resync window=%v", got)
	default:
	}

	now += gapResyncCooldown.Milliseconds() + 1
	if _, err := svc.Apply(ctx, r7Frame(6)); err != nil {
		t.Fatalf("post-cooldown gap: %v", err)
	}
	select {
	case got := <-calls:
		if got != [2]viewsync.Seq{2, 5} {
			t.Fatalf("post-cooldown window=%v want [2 5]", got)
		}
	default:
		t.Fatal("post-cooldown gap did not fire resync")
	}
}
