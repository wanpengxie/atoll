package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/ipc"
	"github.com/wanpengxie/ActOS/runtime/worker"
)

// fakeDaemon wires the daemon end of an io.Pipe pair into an ipc.Codec
// + a small dispatcher loop that pattern-matches incoming Frame.Kind
// and writes the corresponding reply. The test goroutine asserts what
// the client stamped on outbound frames.
type fakeDaemon struct {
	codec *ipc.Codec
	t     *testing.T

	// Captured copies of the last write_message frame, so the test can
	// verify reply stamping (channel id / worker id / fencing token /
	// daemon epoch).
	lastWriteFrame chan ipc.Frame
}

func newFakeDaemon(t *testing.T, r io.Reader, w io.Writer) *fakeDaemon {
	return &fakeDaemon{
		codec:          ipc.NewCodec(r, w),
		t:              t,
		lastWriteFrame: make(chan ipc.Frame, 4),
	}
}

func (d *fakeDaemon) loop(ctx context.Context, ack ipc.HandshakeAckPayload) {
	for {
		frame, err := d.codec.Read()
		if err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		switch frame.Kind {
		case ipc.KindHandshake:
			payload, _ := json.Marshal(ack)
			_ = d.codec.Write(ipc.Frame{
				ID:      frame.ID,
				Kind:    ipc.KindHandshakeAck,
				Payload: payload,
			})
		case ipc.KindWriteMessage:
			d.lastWriteFrame <- frame
			res, _ := json.Marshal(ipc.WriteMessageResult{Seq: 42, Deduped: false})
			rp, _ := json.Marshal(ipc.ReplyPayload{OK: true, Result: res})
			_ = d.codec.Write(ipc.Frame{
				ID:      frame.ID,
				Kind:    ipc.KindReply,
				Payload: rp,
			})
		case ipc.KindHeartbeat:
			rp, _ := json.Marshal(ipc.ReplyPayload{OK: true})
			_ = d.codec.Write(ipc.Frame{
				ID:      frame.ID,
				Kind:    ipc.KindReply,
				Payload: rp,
			})
		case ipc.KindShutdown:
			_ = d.codec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindShutdownAck})
			return
		}
	}
}

// TestIPCClient_HandshakeStampsOutboundFrames covers the L1 §11.6 contract:
// after Handshake completes, every outbound non-handshake frame MUST be
// stamped with channelID / workerID / fencingToken / daemonEpoch.
func TestIPCClient_HandshakeStampsOutboundFrames(t *testing.T) {
	t.Parallel()
	workerR, daemonW := io.Pipe()
	daemonR, workerW := io.Pipe()
	t.Cleanup(func() {
		_ = workerR.Close()
		_ = workerW.Close()
		_ = daemonR.Close()
		_ = daemonW.Close()
	})

	daemon := newFakeDaemon(t, daemonR, daemonW)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go daemon.loop(ctx, ipc.HandshakeAckPayload{
		WorkerID:     "worker-XYZ",
		ChannelID:    channel.ID("ch-1"),
		FencingToken: placement.FencingToken("tok-11"),
		DaemonEpoch:  placement.DaemonEpoch(7),
	})

	client := worker.NewIPCClient(workerR, workerW)
	client.Start(ctx)
	t.Cleanup(client.Stop)

	ack, err := client.Handshake(ctx, "lease-A")
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	if ack.WorkerID != "worker-XYZ" {
		t.Errorf("WorkerID=%q want worker-XYZ", ack.WorkerID)
	}

	// Send a write_message — daemon will record the frame.
	if _, err := client.WriteMessage(ctx, messageEnvelopeStub()); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	select {
	case frame := <-daemon.lastWriteFrame:
		if frame.ChannelID != "ch-1" {
			t.Errorf("ChannelID=%q want ch-1", frame.ChannelID)
		}
		if frame.WorkerID != "worker-XYZ" {
			t.Errorf("WorkerID=%q want worker-XYZ", frame.WorkerID)
		}
		if frame.FencingToken != "tok-11" {
			t.Errorf("FencingToken=%q want tok-11", frame.FencingToken)
		}
		if frame.DaemonEpoch != 7 {
			t.Errorf("DaemonEpoch=%d want 7", frame.DaemonEpoch)
		}
		if frame.ID == "" {
			t.Error("FrameID empty (stamp lost)")
		}
	case <-ctx.Done():
		t.Fatal("write_message frame not observed by daemon")
	}
}

// TestIPCClient_WriteMessageFenceInvalidReturnsTypedError covers the fence
// path: when daemon replies with a fence_invalid frame, WriteMessage MUST
// surface *FenceInvalidError so the worker main loop can errors.As.
func TestIPCClient_WriteMessageFenceInvalidReturnsTypedError(t *testing.T) {
	t.Parallel()
	workerR, daemonW := io.Pipe()
	daemonR, workerW := io.Pipe()
	t.Cleanup(func() {
		_ = workerR.Close()
		_ = workerW.Close()
		_ = daemonR.Close()
		_ = daemonW.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	daemonCodec := ipc.NewCodec(daemonR, daemonW)
	go func() {
		for {
			frame, err := daemonCodec.Read()
			if err != nil {
				return
			}
			switch frame.Kind {
			case ipc.KindHandshake:
				ack, _ := json.Marshal(ipc.HandshakeAckPayload{
					WorkerID:     "worker-1",
					ChannelID:    "ch-1",
					FencingToken: "tok-1",
					DaemonEpoch:  1,
				})
				_ = daemonCodec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindHandshakeAck, Payload: ack})
			case ipc.KindWriteMessage:
				payload, _ := json.Marshal(ipc.FenceInvalidPayload{
					ExpectedToken: "tok-2", GotToken: "tok-1",
					ExpectedEpoch: 2, GotEpoch: 1,
					Reason: "stale",
				})
				_ = daemonCodec.Write(ipc.Frame{
					ID:      frame.ID,
					Kind:    ipc.KindFenceInvalid,
					Payload: payload,
				})
				return
			}
		}
	}()

	client := worker.NewIPCClient(workerR, workerW)
	client.Start(ctx)
	t.Cleanup(client.Stop)

	if _, err := client.Handshake(ctx, "lease-A"); err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	_, err := client.WriteMessage(ctx, messageEnvelopeStub())
	if err == nil {
		t.Fatal("WriteMessage should fail on fence_invalid reply")
	}
	var fenceErr *worker.FenceInvalidError
	if !errors.As(err, &fenceErr) {
		t.Fatalf("err=%T want *FenceInvalidError", err)
	}
	if fenceErr.Reason != "stale" {
		t.Errorf("Reason=%q want stale", fenceErr.Reason)
	}
}

// TestIPCClient_SendAfterStopFails — once Stop() is called, subsequent
// sends MUST fail rather than block forever.
func TestIPCClient_SendAfterStopFails(t *testing.T) {
	t.Parallel()
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()
	t.Cleanup(func() {
		_ = r1.Close()
		_ = w1.Close()
		_ = r2.Close()
		_ = w2.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := worker.NewIPCClient(r1, w2)
	client.Start(ctx)
	client.Stop()

	// Heartbeat after stop should not hang.
	err := client.Heartbeat(ctx, 1)
	if err == nil {
		t.Error("Heartbeat after Stop() returned nil err")
	}

	// Suppress unused-variable warnings — pipe ends are cleaned by t.Cleanup.
	_ = w1
	_ = r2
}

// TestIPCClient_HandshakeMismatchSurfaceError — daemon returns a non-ack
// kind; client should fail loudly rather than silently advance.
func TestIPCClient_HandshakeMismatchSurfaceError(t *testing.T) {
	t.Parallel()
	workerR, daemonW := io.Pipe()
	daemonR, workerW := io.Pipe()
	t.Cleanup(func() {
		_ = workerR.Close()
		_ = workerW.Close()
		_ = daemonR.Close()
		_ = daemonW.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	daemonCodec := ipc.NewCodec(daemonR, daemonW)
	go func() {
		frame, err := daemonCodec.Read()
		if err != nil {
			return
		}
		// Reply with the wrong kind on purpose.
		_ = daemonCodec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindReply})
	}()

	client := worker.NewIPCClient(workerR, workerW)
	client.Start(ctx)
	t.Cleanup(client.Stop)

	_, err := client.Handshake(ctx, "lease-A")
	if err == nil {
		t.Fatal("Handshake should fail on non-ack reply")
	}
}

// messageEnvelopeStub returns a minimal valid envelope payload — every
// WriteMessage test reuses this to keep focus on the IPC layer (the
// fake daemon doesn't validate envelope contents).
func messageEnvelopeStub() message.Envelope {
	return message.Envelope{
		ID:         "m-test",
		ChannelID:  "ch-1",
		Type:       "tick",
		Visibility: message.VisibilityPublic,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Kind:       message.KindEvent,
		Payload:    json.RawMessage(`{}`),
		Audience:   message.Audience{"agent:channel-agent"},
	}
}

// TestIPCClient_TriggersDeliversDaemonPush covers M1.6-T1 P3: an
// unsolicited KindTrigger frame pushed by the daemon MUST be decoded
// and surfaced through IPCClient.Triggers() so the worker's Bridge can
// react. The daemon-supplied envelope / correlation_id / cursor must
// round-trip intact, and the worker must reserve local trigger-buffer
// capacity before acking the daemon.
func TestIPCClient_TriggersDeliversDaemonPush(t *testing.T) {
	t.Parallel()
	workerR, daemonW := io.Pipe()
	daemonR, workerW := io.Pipe()
	t.Cleanup(func() {
		_ = workerR.Close()
		_ = workerW.Close()
		_ = daemonR.Close()
		_ = daemonW.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	daemonCodec := ipc.NewCodec(daemonR, daemonW)
	// Daemon side: respond to handshake, then push a KindTrigger frame.
	daemonDone := make(chan error, 1)
	go func() {
		defer close(daemonDone)
		for {
			frame, err := daemonCodec.Read()
			if err != nil {
				daemonDone <- err
				return
			}
			if frame.Kind != ipc.KindHandshake {
				continue
			}
			ack, _ := json.Marshal(ipc.HandshakeAckPayload{
				WorkerID:     "worker-T",
				ChannelID:    "ch-T",
				FencingToken: "tok-5",
				DaemonEpoch:  3,
			})
			_ = daemonCodec.Write(ipc.Frame{
				ID: frame.ID, Kind: ipc.KindHandshakeAck, Payload: ack,
			})

			// Now push two triggers — second one to prove the buffer
			// can hold a backlog while the bridge processes the first.
			for i := 0; i < 2; i++ {
				env := messageEnvelopeStub()
				env.ID = "m-trigger"
				if i == 1 {
					env.ID = "m-trigger-2"
				}
				payload, _ := json.Marshal(ipc.TriggerPayload{
					Envelope:      env,
					CorrelationID: "corr-1",
					Cursor:        int64(100 + i),
				})
				_ = daemonCodec.Write(ipc.Frame{
					ID:      "push-trigger",
					Kind:    ipc.KindTrigger,
					Payload: payload,
				})
				ackFrame, err := daemonCodec.Read()
				if err != nil {
					daemonDone <- err
					return
				}
				if ackFrame.Kind != ipc.KindTriggerAck {
					daemonDone <- errors.New("unexpected trigger ack kind")
					return
				}
				if ackFrame.ChannelID != "ch-T" || ackFrame.WorkerID != "worker-T" ||
					ackFrame.FencingToken != "tok-5" || ackFrame.DaemonEpoch != 3 {
					daemonDone <- errors.New("trigger ack stamp mismatch")
					return
				}
				var ack ipc.TriggerAckPayload
				if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
					daemonDone <- err
					return
				}
				if !ack.Accepted {
					daemonDone <- errors.New("trigger ack rejected")
					return
				}
				if ack.Cursor != int64(100+i) {
					daemonDone <- errors.New("trigger ack cursor mismatch")
					return
				}
			}
			return
		}
	}()

	client := worker.NewIPCClient(workerR, workerW)
	client.Start(ctx)
	t.Cleanup(client.Stop)

	if _, err := client.Handshake(ctx, "lease-T"); err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	triggers := client.Triggers()
	got := 0
	deadline := time.After(2 * time.Second)
	for got < 2 {
		select {
		case <-deadline:
			t.Fatalf("only %d triggers seen", got)
		case payload, ok := <-triggers:
			if !ok {
				t.Fatalf("trigger channel closed early at %d", got)
			}
			if payload.CorrelationID != "corr-1" {
				t.Errorf("CorrelationID=%q want corr-1", payload.CorrelationID)
			}
			if payload.Envelope.ID == "" {
				t.Error("envelope id empty")
			}
			if payload.Cursor < 100 {
				t.Errorf("Cursor=%d want >=100", payload.Cursor)
			}
			got++
		}
	}
	if dropped := client.TriggerDropCount(); dropped != 0 {
		t.Errorf("unexpected drops=%d", dropped)
	}
	select {
	case err := <-daemonDone:
		if err != nil {
			t.Fatalf("daemon side: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("daemon side did not finish")
	}
}

func TestIPCClient_TriggersNackWhenBufferFull(t *testing.T) {
	t.Parallel()
	workerR, daemonW := io.Pipe()
	daemonR, workerW := io.Pipe()
	t.Cleanup(func() {
		_ = workerR.Close()
		_ = workerW.Close()
		_ = daemonR.Close()
		_ = daemonW.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	daemonCodec := ipc.NewCodec(daemonR, daemonW)
	client := worker.NewIPCClient(workerR, workerW)
	client.Start(ctx)
	t.Cleanup(client.Stop)

	handshakeDone := make(chan struct{})
	go func() {
		frame, err := daemonCodec.Read()
		if err != nil {
			t.Errorf("read handshake: %v", err)
			close(handshakeDone)
			return
		}
		ack, _ := json.Marshal(ipc.HandshakeAckPayload{
			WorkerID:     "worker-T",
			ChannelID:    "ch-T",
			FencingToken: "tok-5",
			DaemonEpoch:  3,
		})
		_ = daemonCodec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindHandshakeAck, Payload: ack})
		close(handshakeDone)
	}()
	if _, err := client.Handshake(ctx, "lease-T"); err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	<-handshakeDone

	const triggerBufferSize = 32
	for i := 0; i < triggerBufferSize+1; i++ {
		env := messageEnvelopeStub()
		env.ID = message.ID("m-buffer")
		payload, _ := json.Marshal(ipc.TriggerPayload{
			Envelope: env,
			Cursor:   int64(i + 1),
		})
		if err := daemonCodec.Write(ipc.Frame{
			ID:      "push-buffer",
			Kind:    ipc.KindTrigger,
			Payload: payload,
		}); err != nil {
			t.Fatalf("write trigger %d: %v", i, err)
		}
		ackFrame, err := daemonCodec.Read()
		if err != nil {
			t.Fatalf("read ack %d: %v", i, err)
		}
		if ackFrame.Kind != ipc.KindTriggerAck {
			t.Fatalf("ack kind=%s want %s", ackFrame.Kind, ipc.KindTriggerAck)
		}
		var ack ipc.TriggerAckPayload
		if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
			t.Fatalf("decode ack %d: %v", i, err)
		}
		if i < triggerBufferSize {
			if !ack.Accepted {
				t.Fatalf("ack %d accepted=false: %+v", i, ack)
			}
			continue
		}
		if ack.Accepted {
			t.Fatalf("overflow ack accepted=true: %+v", ack)
		}
		if ack.Reason != "trigger_buffer_full" {
			t.Fatalf("overflow reason=%q want trigger_buffer_full", ack.Reason)
		}
	}

	if dropped := client.TriggerDropCount(); dropped != 1 {
		t.Fatalf("TriggerDropCount=%d want 1", dropped)
	}
	for i := 0; i < triggerBufferSize; i++ {
		select {
		case <-client.Triggers():
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("drain trigger %d timed out", i)
		}
	}
}
