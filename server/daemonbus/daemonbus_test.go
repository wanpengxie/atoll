package daemonbus_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coagent-ai/coagent/kernel/channel"
	kerneldaemonbus "github.com/coagent-ai/coagent/kernel/daemonbus"
	"github.com/coagent-ai/coagent/kernel/message"
	"github.com/coagent-ai/coagent/kernel/placement"
	"github.com/coagent-ai/coagent/kernel/viewsync"
	"github.com/coagent-ai/coagent/server/daemonbus"
	"github.com/coagent-ai/coagent/server/store"
)

func newSvc(t *testing.T) *daemonbus.Service {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "db.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return daemonbus.NewService(db, daemonbus.Config{SharedSecret: "test-secret"})
}

// pipeTransport is an in-memory bidirectional transport: send via one
// channel, receive via the other. Used by both "daemon side" and
// "server side" of the test (each side wires opposite read/write
// channels).
type pipeTransport struct {
	in   chan kerneldaemonbus.Frame
	out  chan kerneldaemonbus.Frame
	once sync.Once
	done chan struct{}
}

func newPipePair() (server, daemon *pipeTransport) {
	a := make(chan kerneldaemonbus.Frame, 16)
	b := make(chan kerneldaemonbus.Frame, 16)
	done := make(chan struct{})
	server = &pipeTransport{in: a, out: b, done: done}
	daemon = &pipeTransport{in: b, out: a, done: done}
	return
}

func (p *pipeTransport) ReadFrame(ctx context.Context) (kerneldaemonbus.Frame, error) {
	select {
	case f, ok := <-p.in:
		if !ok {
			return kerneldaemonbus.Frame{}, errors.New("closed")
		}
		return f, nil
	case <-p.done:
		return kerneldaemonbus.Frame{}, errors.New("closed")
	case <-ctx.Done():
		return kerneldaemonbus.Frame{}, ctx.Err()
	}
}
func (p *pipeTransport) WriteFrame(ctx context.Context, f kerneldaemonbus.Frame) error {
	select {
	case p.out <- f:
		return nil
	case <-p.done:
		return errors.New("closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *pipeTransport) Close() error {
	p.once.Do(func() { close(p.done) })
	return nil
}

// TestRegisterAndIssueEpoch covers the daemons table + epoch counter.
func TestRegisterAndIssueEpoch(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()

	if err := svc.RegisterDaemon(ctx, placement.DaemonID("d1"), "localhost", "v0", 32, "test-secret"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	if err := svc.RegisterDaemon(ctx, placement.DaemonID("d1"), "localhost", "v0", 32, "wrong"); err != daemonbus.ErrDaemonAuthFailed {
		t.Errorf("auth wrong err=%v want ErrDaemonAuthFailed", err)
	}
	e1, err := svc.IssueConnectionEpoch(ctx, placement.DaemonID("d1"))
	if err != nil || e1 != 1 {
		t.Errorf("epoch1=%d err=%v", e1, err)
	}
	e2, _ := svc.IssueConnectionEpoch(ctx, placement.DaemonID("d1"))
	if e2 != 2 {
		t.Errorf("epoch2=%d want 2", e2)
	}
	if _, err := svc.IssueConnectionEpoch(ctx, placement.DaemonID("ghost")); err != daemonbus.ErrDaemonNotRegistered {
		t.Errorf("ghost err=%v want ErrDaemonNotRegistered", err)
	}
}

// TestDispatchPushAndAck routes a viewsync.push through the dispatch
// loop and confirms the handler is invoked + a viewsync.ack is sent
// back with the correct cursor.
func TestDispatchPushAndAck(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d1"), 5, svr)

	pushed := make(chan viewsync.PushFrame, 1)
	handlers := daemonbus.Handlers{
		OnPush: func(ctx context.Context, c *daemonbus.Connection, f viewsync.PushFrame) (viewsync.LastReceivedSeq, error) {
			pushed <- f
			return viewsync.LastReceivedSeq(int64(f.Seq)), nil
		},
	}

	runErr := make(chan error, 1)
	go func() { runErr <- conn.Run(ctx, handlers) }()

	// "daemon side" sends viewsync.push frame.
	pushFrame := viewsync.PushFrame{
		ChannelID: channel.ID("ch-A"), Seq: 7, MessageID: "m-7",
		Envelope: message.Envelope{
			ID: "m-7", TS: 1, ChannelID: "ch-A",
			Sender: message.Sender{Kind: message.SenderAgent, ID: "a"},
			Kind:   message.KindEvent, Type: "agent.text",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic, Audience: []string{"*"},
		},
	}
	rawPayload, _ := json.Marshal(pushFrame)
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID: "p-1", FrameType: kerneldaemonbus.FrameTypeViewsyncPush,
		DaemonID: "d1", DaemonConnectionEpoch: 5, Payload: rawPayload,
	}); err != nil {
		t.Fatalf("write push: %v", err)
	}

	select {
	case f := <-pushed:
		if int64(f.Seq) != 7 || f.MessageID != "m-7" {
			t.Errorf("push handler got wrong frame: %+v", f)
		}
	case <-ctx.Done():
		t.Fatal("OnPush never called")
	}

	// "daemon side" reads back the ack from server.
	ack, err := dmn.ReadFrame(ctx)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if ack.FrameType != kerneldaemonbus.FrameTypeViewsyncAck {
		t.Errorf("ack frame_type=%q", ack.FrameType)
	}

	var ackBody viewsync.AckFrame
	if err := json.Unmarshal(ack.Payload, &ackBody); err != nil {
		t.Fatalf("unmarshal ack: %v", err)
	}
	if int64(ackBody.LastReceivedSeq) != 7 {
		t.Errorf("ack seq=%d want 7", ackBody.LastReceivedSeq)
	}

	conn.Close()
	<-runErr
}

// TestStaleEpochDropped ensures frames with an older epoch are
// silently dropped (L2 §9.4).
func TestStaleEpochDropped(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d1"), 5, svr)

	pushed := make(chan struct{}, 1)
	handlers := daemonbus.Handlers{
		OnPush: func(ctx context.Context, c *daemonbus.Connection, f viewsync.PushFrame) (viewsync.LastReceivedSeq, error) {
			pushed <- struct{}{}
			return 0, nil
		},
	}

	go func() { _ = conn.Run(ctx, handlers) }()

	pushFrame := viewsync.PushFrame{ChannelID: "ch-A", Seq: 1, MessageID: "m-1"}
	raw, _ := json.Marshal(pushFrame)
	// Frame epoch = 3, server epoch = 5 → stale → dropped.
	_ = dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID: "p-stale", FrameType: kerneldaemonbus.FrameTypeViewsyncPush,
		DaemonID: "d1", DaemonConnectionEpoch: 3, Payload: raw,
	})

	select {
	case <-pushed:
		t.Fatal("stale-epoch push frame was dispatched")
	case <-time.After(150 * time.Millisecond):
	}

	conn.Close()
}
