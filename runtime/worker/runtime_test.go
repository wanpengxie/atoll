package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/runtime/ipc"
	"github.com/wanpengxie/ActOS/runtime/worker"
)

func TestRuntime_FenceInvalidHeartbeatCancelsBridge(t *testing.T) {
	t.Parallel()

	workerR, daemonW := io.Pipe()
	daemonR, workerW := io.Pipe()
	t.Cleanup(func() {
		_ = workerR.Close()
		_ = workerW.Close()
		_ = daemonR.Close()
		_ = daemonW.Close()
	})

	daemonCodec := ipc.NewCodec(daemonR, daemonW)
	heartbeatSeen := make(chan struct{}, 1)
	go func() {
		for {
			frame, err := daemonCodec.Read()
			if err != nil {
				return
			}
			switch frame.Kind {
			case ipc.KindHandshake:
				ack, _ := json.Marshal(ipc.HandshakeAckPayload{
					WorkerID:      "worker-fenced",
					ChannelID:     channel.ID("ch-fenced"),
					WorkerActorID: "agent:fenced",
					FencingToken:  1,
					DaemonEpoch:   1,
				})
				_ = daemonCodec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindHandshakeAck, Payload: ack})
			case ipc.KindHeartbeat:
				payload, _ := json.Marshal(ipc.FenceInvalidPayload{
					ExpectedToken: 2,
					GotToken:      1,
					ExpectedEpoch: 2,
					GotEpoch:      1,
					Reason:        "stale worker",
				})
				_ = daemonCodec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindFenceInvalid, Payload: payload})
				select {
				case heartbeatSeen <- struct{}{}:
				default:
				}
			case ipc.KindShutdown:
				_ = daemonCodec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindShutdownAck})
				return
			}
		}
	}()

	bridgeExited := make(chan error, 1)
	rt, err := worker.New(worker.Config{
		LeaseID:        "lease-fenced",
		In:             workerR,
		Out:            workerW,
		HeartbeatEvery: 10 * time.Millisecond,
		Bridge: worker.BridgeFunc(func(ctx context.Context, _ *worker.IPCClient) error {
			<-ctx.Done()
			err := ctx.Err()
			bridgeExited <- err
			return err
		}),
	})
	if err != nil {
		t.Fatalf("New runtime: %v", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- rt.Run(context.Background()) }()

	select {
	case <-heartbeatSeen:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("heartbeat was not observed")
	}

	select {
	case err := <-bridgeExited:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Bridge.Run err=%v want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Bridge.Run did not exit after fence_invalid heartbeat")
	}

	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Runtime.Run err=%v want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Runtime.Run did not return after fence_invalid heartbeat")
	}
}
