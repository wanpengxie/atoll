package worker_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
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
				go d.pushTriggers(ctx, ack, triggers)
			}
		case ipc.KindWriteMessage:
			d.writeFrames <- frame
			d.writeFakeAck(frame)
		case ipc.KindTriggerAck:
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

func (d *fakeBridgeDaemon) pushTriggers(ctx context.Context, ack ipc.HandshakeAckPayload, triggers int) {
	for i := 0; i < triggers; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		env := message.Envelope{
			ID:         message.ID("trig-" + string(rune('a'+i))),
			ChannelID:  ack.ChannelID,
			Type:       "human.text",
			Sender:     message.Sender{Kind: actor.KindHuman, ID: "user:alice"},
			Visibility: message.VisibilityPublic,
			Kind:       message.KindEvent,
			Audience:   message.Audience{ack.WorkerActorID},
			Payload:    json.RawMessage(`{"text":"hi"}`),
		}
		payload, _ := json.Marshal(ipc.TriggerPayload{
			Envelope:      env,
			CorrelationID: "corr-X",
			Cursor:        int64(i + 1),
		})
		if err := d.codec.Write(ipc.Frame{
			ID:      "push-" + string(env.ID),
			Kind:    ipc.KindTrigger,
			Payload: payload,
		}); err != nil {
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
		FencingToken:  placement.FencingToken("tok-1"),
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
		if wp.Envelope.Sender.Kind != actor.KindAgent {
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

// TestMockBridge_DomainPromptLog covers M1.6-T5 phase-3 acceptance B3:
// when the daemon spawn env carries COAGENT_DOMAIN_PROMPT (the L4 §2.4
// xhs-creator segment), the bridge emits exactly one
// `mock_bridge: domain_prompt_loaded ...` line to PromptLogWriter on
// Run start. The line carries the worker id, channel_type, prompt
// length, and a short sha256 prefix so log scans can correlate the
// prompt-cache key without echoing the full prompt body.
func TestMockBridge_DomainPromptLog(t *testing.T) {
	t.Parallel()

	const prompt = "你是 xhs 内容创作 agent。\n业务约束：禁止重复 publish。"
	const channelType = "xhs-creator"
	sum := sha256.Sum256([]byte(prompt))
	wantSHA := hex.EncodeToString(sum[:8])

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
		WorkerID:      "worker-PROMPT",
		ChannelID:     channel.ID("ch-PROMPT"),
		WorkerActorID: "agent:channel-agent",
		FencingToken:  placement.FencingToken("tok-1"),
		DaemonEpoch:   placement.DaemonEpoch(1),
	}
	// One trigger → bridge reacts, MaxTurns=1 fires terminal, Run exits.
	go daemon.loop(ctx, ack, 1)

	logBuf := &bytes.Buffer{}
	envLookup := func(key string) string {
		switch key {
		case worker.EnvKeyChannelType:
			return channelType
		case worker.EnvKeyDomainPrompt:
			return prompt
		}
		return ""
	}
	bridge := worker.NewMockBridge()
	bridge.MaxTurns = 1 // exit after the single trigger
	bridge.PromptLogWriter = logBuf
	bridge.EnvLookup = envLookup

	rt, err := worker.New(worker.Config{
		LeaseID:        "lease-prompt",
		In:             workerR,
		Out:            workerW,
		HeartbeatEvery: time.Hour,
		Bridge:         bridge,
	})
	if err != nil {
		t.Fatalf("worker.New: %v", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- rt.Run(ctx) }()

	select {
	case <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Runtime.Run never returned")
	}

	got := logBuf.String()
	if !strings.Contains(got, "mock_bridge: domain_prompt_loaded ") {
		t.Errorf("log missing prompt-loaded breadcrumb; got=%q", got)
	}
	if !strings.Contains(got, "worker=worker-PROMPT") {
		t.Errorf("log missing worker id; got=%q", got)
	}
	if !strings.Contains(got, "channel_type=\"xhs-creator\"") {
		t.Errorf("log missing channel_type; got=%q", got)
	}
	if !strings.Contains(got, "sha256="+wantSHA) {
		t.Errorf("log missing sha256=%s; got=%q", wantSHA, got)
	}
}

// TestMockBridge_NoDomainPromptLog asserts the legacy / group-channel
// path: when COAGENT_DOMAIN_PROMPT is unset the bridge emits a
// `no_domain_prompt` counterpart so the absence is still observable.
// Keeps acceptance B3 grep deterministic (positive for xhs-creator,
// negative for generic channels — no silent omission).
func TestMockBridge_NoDomainPromptLog(t *testing.T) {
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
		WorkerID:      "worker-GROUP",
		ChannelID:     channel.ID("ch-GROUP"),
		WorkerActorID: "agent:channel-agent",
		FencingToken:  placement.FencingToken("tok-1"),
		DaemonEpoch:   placement.DaemonEpoch(1),
	}
	go daemon.loop(ctx, ack, 1)

	logBuf := &bytes.Buffer{}
	bridge := worker.NewMockBridge()
	bridge.MaxTurns = 1
	bridge.PromptLogWriter = logBuf
	// Force empty env so the test doesn't leak host COAGENT_* state.
	bridge.EnvLookup = func(string) string { return "" }

	rt, err := worker.New(worker.Config{
		LeaseID:        "lease-nogrp",
		In:             workerR,
		Out:            workerW,
		HeartbeatEvery: time.Hour,
		Bridge:         bridge,
	})
	if err != nil {
		t.Fatalf("worker.New: %v", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- rt.Run(ctx) }()

	select {
	case <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Runtime.Run never returned")
	}

	got := logBuf.String()
	if !strings.Contains(got, "mock_bridge: no_domain_prompt ") {
		t.Errorf("log missing no_domain_prompt breadcrumb; got=%q", got)
	}
	if strings.Contains(got, "domain_prompt_loaded") {
		t.Errorf("legacy path must not emit domain_prompt_loaded; got=%q", got)
	}
}
