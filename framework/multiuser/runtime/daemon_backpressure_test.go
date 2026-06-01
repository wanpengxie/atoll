package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/framework/multiuser/runtime/transit"
	"github.com/wanpengxie/ActOS/framework/multiuser/viewsync"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

type backpressureOutbox struct {
	chID channel.ID
	mu   sync.Mutex
}

func (o *backpressureOutbox) ChannelID() channel.ID { return o.chID }

func (o *backpressureOutbox) PendingPage(context.Context, int) ([]viewsync.PushFrame, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return nil, nil
}

func (o *backpressureOutbox) MarkPushed(context.Context, viewsync.Seq, int64) error { return nil }
func (o *backpressureOutbox) ResetPushed(context.Context, viewsync.Seq) error       { return nil }
func (o *backpressureOutbox) AckUpTo(context.Context, viewsync.Seq) error           { return nil }
func (o *backpressureOutbox) PendingCount(context.Context) (int, error)             { return 0, nil }

func TestViewsyncBackpressure_DoesNotResumeOnWallClock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	bus := transit.NewMockBus(8)
	defer func() { _ = bus.Close() }()
	client, err := transit.NewClient(transit.ClientConfig{DaemonID: "daemon-bp", Transport: bus})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	chID := channel.ID("ch-backpressure")
	pusher, err := transit.NewPusher(transit.PusherConfig{
		Outbox:    &backpressureOutbox{chID: chID},
		Client:    client,
		Cursors:   transit.NewCursorTracker(),
		FrameID:   func() string { return "bp-frame" },
		PollEvery: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	d := &Daemon{
		channels: map[channel.ID]*channelRuntime{},
		runCtx:   runCtx,
	}
	cr := &channelRuntime{channelID: chID, pusher: pusher}
	d.channels[chID] = cr
	defer func() {
		runCancel()
		d.wg.Wait()
	}()

	activeCtx, activeCancel := context.WithCancel(ctx)
	cr.pushMu.Lock()
	cr.pausePush = activeCancel
	cr.pushMu.Unlock()

	d.pauseChannelPushForBackpressure(viewsync.AckFrame{
		ChannelID:       chID,
		LastReceivedSeq: 7,
		Accepted:        false,
		RejectReason:    viewsync.RejectReasonViewsyncResyncBackpressure,
	})
	select {
	case <-activeCtx.Done():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("backpressure did not cancel active pusher")
	}
	if waitPusherActive(cr, 650*time.Millisecond) {
		t.Fatal("pusher resumed on wall-clock timer without cursor advance or resync completion")
	}

	d.resumeChannelPushAfterBackpressure(viewsync.AckFrame{
		ChannelID:       chID,
		LastReceivedSeq: 7,
		Accepted:        true,
	})
	if waitPusherActive(cr, 100*time.Millisecond) {
		t.Fatal("pusher resumed before accepted ack advanced past rejected cursor")
	}

	d.resumeChannelPushAfterBackpressure(viewsync.AckFrame{
		ChannelID:       chID,
		LastReceivedSeq: 8,
		Accepted:        true,
	})
	if !waitPusherActive(cr, 500*time.Millisecond) {
		t.Fatal("pusher did not resume after accepted ack advanced past rejected cursor")
	}

	d.pauseChannelPushForBackpressure(viewsync.AckFrame{
		ChannelID:       chID,
		LastReceivedSeq: 9,
		Accepted:        false,
		RejectReason:    viewsync.RejectReasonViewsyncResyncBackpressure,
	})
	if waitPusherActive(cr, 50*time.Millisecond) {
		t.Fatal("pusher still marked active after second backpressure pause")
	}

	d.resumeChannelPushAfterBackpressure(viewsync.AckFrame{
		ChannelID:       chID,
		LastReceivedSeq: 9,
		Accepted:        true,
		ResyncCompleted: true,
	})
	if !waitPusherActive(cr, 500*time.Millisecond) {
		t.Fatal("pusher did not resume on explicit resync completion ack")
	}
}

func waitPusherActive(cr *channelRuntime, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		cr.pushMu.Lock()
		active := cr.pausePush != nil
		cr.pushMu.Unlock()
		if active {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}
