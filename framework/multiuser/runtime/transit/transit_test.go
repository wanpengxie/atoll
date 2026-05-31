package transit_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/framework/multiuser/daemonbus"
	multistore "github.com/wanpengxie/ActOS/framework/multiuser/runtime/store"
	"github.com/wanpengxie/ActOS/framework/multiuser/runtime/transit"
	"github.com/wanpengxie/ActOS/framework/multiuser/viewsync"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/store"
)

const testChannelID = channel.ID("ch-1")

// TestViewSync_E2E covers acceptance gate #2 (T3):
//
//	write message -> outbox -> push -> mock ack -> outbox GC
func TestViewSync_E2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	outbox := multistore.NewViewSyncOutbox(db, testChannelID)
	msgs := store.NewMessagesWithObservers(db, nil, outbox)

	// Insert 3 messages.
	for i := 0; i < 3; i++ {
		env := newEnvelope(i + 1)
		if _, err := msgs.Append(ctx, env, klog.FencingTuple{}); err != nil {
			t.Fatalf("append #%d: %v", i+1, err)
		}
	}

	// Daemon-side transit wired with mock bus.
	bus := transit.NewMockBus(64)
	defer func() { _ = bus.Close() }()

	client, err := transit.NewClient(transit.ClientConfig{
		DaemonID:  "daemon-A",
		Transport: bus,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	cursors := transit.NewCursorTracker()
	frameIDs := atomicFrameID()
	pusher, err := transit.NewPusher(transit.PusherConfig{
		Outbox:  outbox,
		Client:  client,
		Cursors: cursors,
		FrameID: frameIDs,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Drain everything in one batch.
	sent, err := pusher.Drain(ctx)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if sent != 3 {
		t.Errorf("expected 3 frames pushed, got %d", sent)
	}

	// Mock server receives the 3 push frames in seq ASC.
	server := bus.ServerSide()
	for i := 1; i <= 3; i++ {
		f, err := server.RecvFromDaemon(ctx)
		if err != nil {
			t.Fatalf("server recv #%d: %v", i, err)
		}
		if f.FrameKind != daemonbus.FrameTypeViewsyncPush {
			t.Fatalf("frame #%d type = %s", i, f.FrameKind)
		}
		var pf viewsync.PushFrame
		if err := transit.DecodePayload(f, &pf); err != nil {
			t.Fatal(err)
		}
		if pf.Seq != viewsync.Seq(i) {
			t.Errorf("frame #%d: seq=%d expected %d", i, pf.Seq, i)
		}
	}

	// Server sends ack for last_received_seq=3.
	ackHandler, err := transit.NewAckHandler(outbox, cursors)
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := ackHandler.Handle(ctx, viewsync.AckFrame{
		ChannelID:       testChannelID,
		LastReceivedSeq: viewsync.LastReceivedSeq(3),
		Accepted:        true,
	})
	if err != nil {
		t.Fatalf("ack handle: %v", err)
	}
	if !advanced {
		t.Error("ack should advance cursor")
	}

	// Outbox should be empty after GC.
	pending, _ := outbox.PendingPage(ctx, 10)
	if len(pending) != 0 {
		t.Errorf("after ack GC expected 0 outbox rows, got %d", len(pending))
	}
	hi, ok, _ := outbox.HighestSeq(ctx)
	if ok {
		t.Errorf("outbox should be empty, got highest=%d", hi)
	}

	// Idempotent ack — second call returns advanced=false, no error.
	advanced2, err := ackHandler.Handle(ctx, viewsync.AckFrame{
		ChannelID:       testChannelID,
		LastReceivedSeq: viewsync.LastReceivedSeq(3),
		Accepted:        true,
	})
	if err != nil || advanced2 {
		t.Errorf("repeat ack: advanced=%v err=%v", advanced2, err)
	}
}

func TestAckHandler_MuxOwnerEpochStale_PausesChannelPumpAndReconciles(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	outbox := multistore.NewViewSyncOutbox(db, testChannelID)
	msgs := store.NewMessagesWithObservers(db, nil, outbox)
	for i := 0; i < 2; i++ {
		if _, err := msgs.Append(ctx, newEnvelope(i+1), klog.FencingTuple{}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := outbox.MarkPushed(ctx, 1, 100); err != nil {
		t.Fatal(err)
	}
	if err := outbox.MarkPushed(ctx, 2, 101); err != nil {
		t.Fatal(err)
	}
	var reconciled atomic.Int32
	handler, err := transit.NewAckHandlerWithRejectHandler(outbox, transit.NewCursorTracker(), func(ctx context.Context, ack viewsync.AckFrame) error {
		if ack.ChannelID != testChannelID || ack.RejectReason != viewsync.RejectReasonMuxOwnerEpochStale {
			t.Fatalf("stale hook ack=%+v", ack)
		}
		reconciled.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := handler.Handle(ctx, viewsync.AckFrame{
		ChannelID:    testChannelID,
		Accepted:     false,
		RejectReason: viewsync.RejectReasonMuxOwnerEpochStale,
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if advanced {
		t.Fatal("negative ack must not advance cursor")
	}
	if reconciled.Load() != 1 {
		t.Fatalf("stale hook calls=%d want 1", reconciled.Load())
	}
	var pushedRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM view_sync_outbox WHERE status='pushed'`).Scan(&pushedRows); err != nil {
		t.Fatal(err)
	}
	if pushedRows != 2 {
		t.Fatalf("stale reject reset pushed rows; pushed=%d want 2 until reclaim resumes pump", pushedRows)
	}
}

func TestViewsyncBackpressure_PausesDaemonPusher(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	outbox := multistore.NewViewSyncOutbox(db, testChannelID)
	msgs := store.NewMessagesWithObservers(db, nil, outbox)
	for i := 0; i < 2; i++ {
		if _, err := msgs.Append(ctx, newEnvelope(i+1), klog.FencingTuple{}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := outbox.MarkPushed(ctx, 1, 100); err != nil {
		t.Fatal(err)
	}
	if err := outbox.MarkPushed(ctx, 2, 101); err != nil {
		t.Fatal(err)
	}

	var paused atomic.Int32
	handler, err := transit.NewAckHandlerWithRejectHandler(outbox, transit.NewCursorTracker(), func(ctx context.Context, ack viewsync.AckFrame) error {
		if ack.ChannelID != testChannelID || ack.RejectReason != viewsync.RejectReasonViewsyncResyncBackpressure {
			t.Fatalf("backpressure hook ack=%+v", ack)
		}
		paused.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := handler.Handle(ctx, viewsync.AckFrame{
		ChannelID:    testChannelID,
		Accepted:     false,
		RejectReason: viewsync.RejectReasonViewsyncResyncBackpressure,
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if advanced {
		t.Fatal("backpressure reject must not advance cursor")
	}
	if paused.Load() != 1 {
		t.Fatalf("pause hook calls=%d want 1", paused.Load())
	}
	var pushedRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM view_sync_outbox WHERE status='pushed'`).Scan(&pushedRows); err != nil {
		t.Fatal(err)
	}
	if pushedRows != 2 {
		t.Fatalf("backpressure reject reset pushed rows; pushed=%d want 2 until retry", pushedRows)
	}
}

func TestAckHandler_TransientReject_ResetsPushed(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	outbox := multistore.NewViewSyncOutbox(db, testChannelID)
	msgs := store.NewMessagesWithObservers(db, nil, outbox)
	for i := 0; i < 2; i++ {
		if _, err := msgs.Append(ctx, newEnvelope(i+1), klog.FencingTuple{}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := outbox.MarkPushed(ctx, 1, 100); err != nil {
		t.Fatal(err)
	}
	if err := outbox.MarkPushed(ctx, 2, 101); err != nil {
		t.Fatal(err)
	}
	handler, err := transit.NewAckHandler(outbox, transit.NewCursorTracker())
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := handler.Handle(ctx, viewsync.AckFrame{
		ChannelID:    testChannelID,
		Accepted:     false,
		RejectReason: viewsync.RejectReason("transient_internal"),
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if advanced {
		t.Fatal("negative ack must not advance cursor")
	}
	pending, err := outbox.PendingPage(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("retryable rows=%d want 2", len(pending))
	}
	var pushedRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM view_sync_outbox WHERE status='pushed'`).Scan(&pushedRows); err != nil {
		t.Fatal(err)
	}
	if pushedRows != 0 {
		t.Fatalf("transient reject left pushed rows=%d want 0", pushedRows)
	}
}

// TestGap_ResyncFillsHole covers acceptance gate #3 (T3):
//
//	deliberately skip one frame in the mock server -> server detects gap
//	-> sends viewsync.resync_request -> daemon ServeResync -> messages
//	applied in order on server side.
func TestGap_ResyncFillsHole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	outbox := multistore.NewViewSyncOutbox(db, testChannelID)
	msgs := store.NewMessagesWithObservers(db, nil, outbox)

	// Insert 5 messages.
	for i := 0; i < 5; i++ {
		env := newEnvelope(i + 1)
		if _, err := msgs.Append(ctx, env, klog.FencingTuple{}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Push them all.
	bus := transit.NewMockBus(64)
	defer func() { _ = bus.Close() }()
	client, _ := transit.NewClient(transit.ClientConfig{DaemonID: "daemon-A", Transport: bus})
	_, _ = client.Connect(ctx)
	cursors := transit.NewCursorTracker()
	pusher, _ := transit.NewPusher(transit.PusherConfig{
		Outbox: outbox, Client: client, Cursors: cursors, FrameID: atomicFrameID(),
	})
	if _, err := pusher.Drain(ctx); err != nil {
		t.Fatal(err)
	}

	server := bus.ServerSide()

	// "Server" applies: pretend it got frames 1, 2, 4, 5 (dropped 3 in
	// transit). We model this by reading all 5 from the bus but only
	// noting which seq we "applied". Then we ask daemon to resync seq 3.
	applied := make(map[viewsync.Seq]bool)
	for i := 0; i < 5; i++ {
		f, err := server.RecvFromDaemon(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var pf viewsync.PushFrame
		_ = transit.DecodePayload(f, &pf)
		if pf.Seq == 3 {
			continue // simulate lost frame
		}
		applied[pf.Seq] = true
	}
	if applied[3] {
		t.Fatal("seq=3 should be lost in this test")
	}

	// Daemon-side resync handler.
	resyncServer, err := transit.NewResyncServer(outbox)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := resyncServer.ServeResync(ctx, viewsync.ResyncRequest{
		ChannelID: testChannelID,
		SinceSeq:  3,
		UntilSeq:  3,
	})
	if err != nil {
		t.Fatalf("ServeResync: %v", err)
	}
	if len(resp.Messages) != 1 || resp.Messages[0].Seq != 3 {
		t.Fatalf("resync resp: %+v", resp.Messages)
	}
	// Apply on (mock) server side: order preserved (single entry here).
	applied[resp.Messages[0].Seq] = true

	// Verify all 5 are now applied in seq order.
	for i := viewsync.Seq(1); i <= 5; i++ {
		if !applied[i] {
			t.Errorf("seq=%d missing after resync", i)
		}
	}

	// Also: ServeResync with multi-seq range returns ASC order.
	resp2, err := resyncServer.ServeResync(ctx, viewsync.ResyncRequest{
		ChannelID: testChannelID,
		SinceSeq:  2,
		UntilSeq:  4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp2.Messages) != 3 {
		t.Fatalf("multi-range resync: %d msgs", len(resp2.Messages))
	}
	for i := 1; i < len(resp2.Messages); i++ {
		if resp2.Messages[i].Seq <= resp2.Messages[i-1].Seq {
			t.Errorf("non-ascending resync at %d", i)
		}
	}
}

// TestDispatcher_FrameIDGenerated verifies the dispatcher emits a
// resync_response after handling a resync_request from the mock server.
func TestDispatcher_ResyncRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	db, _ := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	defer func() { _ = db.Close() }()

	outbox := multistore.NewViewSyncOutbox(db, testChannelID)
	msgs := store.NewMessagesWithObservers(db, nil, outbox)
	if _, err := msgs.Append(ctx, newEnvelope(1), klog.FencingTuple{}); err != nil {
		t.Fatal(err)
	}

	bus := transit.NewMockBus(64)
	defer func() { _ = bus.Close() }()
	client, _ := transit.NewClient(transit.ClientConfig{DaemonID: "daemon-A", Transport: bus})
	_, _ = client.Connect(ctx)

	resyncServer, _ := transit.NewResyncServer(outbox)

	dispatcher, err := transit.NewDispatcher(transit.DispatcherConfig{
		Client:  client,
		FrameID: atomicFrameID(),
		Handlers: transit.ControlHandlers{
			OnViewsyncResyncRequest: resyncServer.ServeResync,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Drive one Recv→Dispatch in a goroutine.
	done := make(chan error, 1)
	go func() {
		frame, recvErr := client.Recv(ctx)
		if recvErr != nil {
			done <- recvErr
			return
		}
		done <- dispatcher.Dispatch(ctx, frame)
	}()

	// Server sends resync_request.
	server := bus.ServerSide()
	req := viewsync.ResyncRequest{ChannelID: testChannelID, SinceSeq: 1, UntilSeq: 1}
	reqFrame, _ := transit.Encode("frame-X", daemonbus.FrameTypeViewsyncResyncRequest, "server", client.Epoch(), 0, req)
	if err := server.SendToDaemon(ctx, reqFrame); err != nil {
		t.Fatal(err)
	}

	// Wait for daemon Dispatch to finish.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("dispatch: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("dispatch timeout")
	}

	// Server should now have received a viewsync.resync_response.
	resp, err := server.RecvFromDaemon(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if resp.FrameKind != daemonbus.FrameTypeViewsyncResyncResponse {
		t.Errorf("unexpected response frame: %s", resp.FrameKind)
	}
	var rr viewsync.ResyncResponse
	_ = transit.DecodePayload(resp, &rr)
	if len(rr.Messages) != 1 || rr.Messages[0].Seq != 1 {
		t.Errorf("resync response: %+v", rr)
	}
}

func TestMessagesByRange_PagedResponse(t *testing.T) {
	ctx := context.Background()
	reader := &pagedResyncReader{chID: testChannelID}
	resyncServer, err := transit.NewResyncServer(reader)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := resyncServer.ServeResync(ctx, viewsync.ResyncRequest{
		ChannelID: testChannelID,
		SinceSeq:  1,
		UntilSeq:  1200,
	})
	if err != nil {
		t.Fatalf("ServeResync: %v", err)
	}
	if len(resp.Messages) != 1200 {
		t.Fatalf("messages=%d want 1200", len(resp.Messages))
	}
	want := []resyncRange{{1, 500}, {501, 1000}, {1001, 1200}}
	if len(reader.ranges) != len(want) {
		t.Fatalf("ranges=%v want %v", reader.ranges, want)
	}
	for i := range want {
		if reader.ranges[i] != want[i] {
			t.Fatalf("range[%d]=%v want %v", i, reader.ranges[i], want[i])
		}
	}
	for i, msg := range resp.Messages {
		wantSeq := viewsync.Seq(i + 1)
		if msg.Seq != wantSeq {
			t.Fatalf("message[%d].Seq=%d want %d", i, msg.Seq, wantSeq)
		}
	}
}

// helpers ---------------------------------------------------------------

type resyncRange struct {
	since viewsync.Seq
	until viewsync.Seq
}

type pagedResyncReader struct {
	chID   channel.ID
	ranges []resyncRange
}

func (r *pagedResyncReader) ChannelID() channel.ID { return r.chID }

func (r *pagedResyncReader) MessagesByRange(
	ctx context.Context,
	since, until viewsync.Seq,
) ([]viewsync.ResyncMessage, error) {
	r.ranges = append(r.ranges, resyncRange{since: since, until: until})
	if got := until - since + 1; got > viewsync.Seq(transit.MaxResyncChunkSize) {
		return nil, errors.New("range exceeded chunk size")
	}
	out := make([]viewsync.ResyncMessage, 0, int(until-since+1))
	for seq := since; seq <= until; seq++ {
		env := newEnvelope(int(seq))
		env.Seq = int64(seq)
		out = append(out, viewsync.ResyncMessage{
			Seq:       seq,
			MessageID: env.ID,
			Envelope:  *env,
		})
	}
	return out, nil
}

func atomicFrameID() transit.FrameIDGen {
	var n atomic.Int64
	return func() string {
		i := n.Add(1)
		return "frame-" + itoa(int(i))
	}
}

func newEnvelope(seq int) *message.Envelope {
	return &message.Envelope{
		ID:         message.ID("m-" + itoa(seq)),
		TS:         int64(1000 + seq),
		TSReceived: int64(1000 + seq),
		ChannelID:  testChannelID,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Kind:       message.KindEvent,
		Type:       "tick",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:channel-agent"},
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
