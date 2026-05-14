package transit_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/transit"
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

	msgs := store.NewMessages(db)
	outbox := store.NewViewSyncOutbox(db, testChannelID)

	// Insert 3 messages.
	for i := 0; i < 3; i++ {
		env := newEnvelope(i + 1)
		if _, err := msgs.Append(ctx, env); err != nil {
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
		if f.FrameType != daemonbus.FrameTypeViewsyncPush {
			t.Fatalf("frame #%d type = %s", i, f.FrameType)
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
	})
	if err != nil || advanced2 {
		t.Errorf("repeat ack: advanced=%v err=%v", advanced2, err)
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

	msgs := store.NewMessages(db)
	outbox := store.NewViewSyncOutbox(db, testChannelID)

	// Insert 5 messages.
	for i := 0; i < 5; i++ {
		env := newEnvelope(i + 1)
		if _, err := msgs.Append(ctx, env); err != nil {
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

	msgs := store.NewMessages(db)
	outbox := store.NewViewSyncOutbox(db, testChannelID)
	if _, err := msgs.Append(ctx, newEnvelope(1)); err != nil {
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
	reqFrame, _ := transit.Encode("frame-X", daemonbus.FrameTypeViewsyncResyncRequest, "server", 0, 0, req)
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
	if resp.FrameType != daemonbus.FrameTypeViewsyncResyncResponse {
		t.Errorf("unexpected response frame: %s", resp.FrameType)
	}
	var rr viewsync.ResyncResponse
	_ = transit.DecodePayload(resp, &rr)
	if len(rr.Messages) != 1 || rr.Messages[0].Seq != 1 {
		t.Errorf("resync response: %+v", rr)
	}
}

// helpers ---------------------------------------------------------------

func atomicFrameID() transit.FrameIDGen {
	var n atomic.Int64
	return func() string {
		i := n.Add(1)
		return "frame-" + itoa(int(i))
	}
}

func newEnvelope(seq int) *message.Envelope {
	return &message.Envelope{
		ID:         "m-" + itoa(seq),
		TS:         int64(1000 + seq),
		TSReceived: int64(1000 + seq),
		ChannelID:  string(testChannelID),
		Sender:     message.Sender{Kind: message.SenderAgent, ID: "agent:a"},
		Kind:       message.KindEvent,
		Type:       "tick",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   []string{"*"},
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
