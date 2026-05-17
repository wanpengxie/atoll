package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/ipc"
	"github.com/wanpengxie/ActOS/runtime/worker"
)

// fakeBridgeDaemon mirrors fakeDaemon (from ipc_client_test) but layers
// in: trigger push + write_message collection.
type fakeBridgeDaemon struct {
	codec *ipc.Codec

	writeFrames chan ipc.Frame
	doneAcks    chan struct{}
}

func newFakeBridgeDaemon(r io.Reader, w io.Writer) *fakeBridgeDaemon {
	return &fakeBridgeDaemon{
		codec:       ipc.NewCodec(r, w),
		writeFrames: make(chan ipc.Frame, 32),
		doneAcks:    make(chan struct{}, 1),
	}
}

func (d *fakeBridgeDaemon) writeFakeAck(frame ipc.Frame) {
	res, _ := json.Marshal(ipc.WriteMessageResult{Seq: 1, Deduped: false})
	rp, _ := json.Marshal(ipc.ReplyPayload{OK: true, Result: res})
	_ = d.codec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindReply, Payload: rp})
}

// loop services a fresh daemon side: handshake → push N triggers →
// echo write_message replies until worker shuts down or stops.
func (d *fakeBridgeDaemon) loop(ctx context.Context, ack ipc.HandshakeAckPayload, triggers int) {
	pushed := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		frame, err := d.codec.Read()
		if err != nil {
			return
		}
		switch frame.Kind {
		case ipc.KindHandshake:
			payload, _ := json.Marshal(ack)
			_ = d.codec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindHandshakeAck, Payload: payload})

			// Push N triggers right after handshake. Bridge will react
			// in-order; daemon side captures each WriteMessage frame.
			if !pushed {
				pushed = true
				for i := 0; i < triggers; i++ {
					env := message.Envelope{
						ID:         "trig-" + string(rune('a'+i)),
						ChannelID:  string(ack.ChannelID),
						Type:       "human.text",
						Sender:     message.Sender{Kind: message.SenderHuman, ID: "user:alice"},
						Visibility: message.VisibilityPublic,
						Kind:       message.KindEvent,
						Audience:   []string{string(ack.WorkerActorID)},
						Payload:    json.RawMessage(`{"text":"hi"}`),
					}
					payload, _ := json.Marshal(ipc.TriggerPayload{
						Envelope:      env,
						CorrelationID: "corr-X",
						Cursor:        int64(i + 1),
					})
					_ = d.codec.Write(ipc.Frame{
						ID:      "push-" + env.ID,
						Kind:    ipc.KindTrigger,
						Payload: payload,
					})
				}
			}
		case ipc.KindWriteMessage:
			d.writeFrames <- frame
			d.writeFakeAck(frame)
		case ipc.KindHeartbeat:
			rp, _ := json.Marshal(ipc.ReplyPayload{OK: true})
			_ = d.codec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindReply, Payload: rp})
		case ipc.KindShutdown:
			_ = d.codec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindShutdownAck})
			select {
			case d.doneAcks <- struct{}{}:
			default:
			}
			return
		}
	}
}

// TestMockBridge_ReactAndExitOnMaxTurns covers M1.6-T1 acceptance #5:
//   - bridge reacts to each trigger with one agent.text envelope
//   - hitting MaxTurns appends a final next_action=done envelope and
//     returns nil, so Runtime.Run shuts down cleanly.
func TestMockBridge_ReactAndExitOnMaxTurns(t *testing.T) {
	t.Parallel()
	workerR, daemonW := io.Pipe()
	daemonR, workerW := io.Pipe()
	t.Cleanup(func() {
		_ = workerR.Close()
		_ = workerW.Close()
		_ = daemonR.Close()
		_ = daemonW.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	daemon := newFakeBridgeDaemon(daemonR, daemonW)
	ack := ipc.HandshakeAckPayload{
		WorkerID:      "worker-MB",
		ChannelID:     channel.ID("ch-MB"),
		WorkerActorID: "agent:channel-agent",
		FencingToken:  placement.FencingToken(1),
		DaemonEpoch:   placement.DaemonEpoch(1),
	}
	go daemon.loop(ctx, ack, 2)

	bridge := worker.NewMockBridge()
	bridge.MaxTurns = 2

	rt, err := worker.New(worker.Config{
		LeaseID:        "lease-mb",
		In:             workerR,
		Out:            workerW,
		HeartbeatEvery: time.Hour, // suppress heartbeat for the unit test
		Bridge:         bridge,
	})
	if err != nil {
		t.Fatalf("worker.New: %v", err)
	}

	runDone := make(chan error, 1)
	var runOnce sync.Once
	go func() {
		err := rt.Run(ctx)
		runOnce.Do(func() { runDone <- err })
	}()

	// Expect three write_message frames: 2 reactions + 1 terminal.
	collected := make([]ipc.Frame, 0, 3)
	deadline := time.After(3 * time.Second)
	for len(collected) < 3 {
		select {
		case f := <-daemon.writeFrames:
			collected = append(collected, f)
		case <-deadline:
			t.Fatalf("collected %d write frames, want 3", len(collected))
		}
	}

	// First two are per-trigger reactions; third is the terminal
	// next_action=done.
	for i, f := range collected {
		var wp ipc.WriteMessagePayload
		if err := json.Unmarshal(f.Payload, &wp); err != nil {
			t.Fatalf("frame %d decode: %v", i, err)
		}
		if wp.Envelope.Sender.ID != "agent:channel-agent" {
			t.Errorf("frame %d sender.id=%q want agent:channel-agent", i, wp.Envelope.Sender.ID)
		}
		if wp.Envelope.Sender.Kind != message.SenderAgent {
			t.Errorf("frame %d sender.kind=%q", i, wp.Envelope.Sender.Kind)
		}
		if i < 2 {
			if wp.Envelope.CorrelationID != "corr-X" {
				t.Errorf("reaction %d correlation=%q", i, wp.Envelope.CorrelationID)
			}
			if wp.Envelope.ParentID == "" {
				t.Errorf("reaction %d parent_id empty", i)
			}
		} else {
			var body map[string]any
			if err := json.Unmarshal(wp.Envelope.Payload, &body); err != nil {
				t.Fatalf("terminal payload decode: %v", err)
			}
			if body["next_action"] != "done" {
				t.Errorf("terminal payload next_action=%v", body["next_action"])
			}
		}
	}

	// Bridge returned → Runtime should Shutdown and exit.
	select {
	case <-daemon.doneAcks:
	case <-time.After(2 * time.Second):
		t.Fatal("daemon never observed shutdown frame")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Runtime.Run never returned")
	}
}
