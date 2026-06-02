package kimi

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior/futurereg"
	"github.com/wanpengxie/ActOS/lib/behavior/futurereg/contract"
)

// contractFakeIPC is a no-op IPCFacade for the contract harness: the worker
// caller's Submit registers the future then writes the request envelope through
// IPC. The contract scenarios only care that the write is accepted (the
// register-before-write ordering, subscribe-before-send §3.2); the response is
// fed back through Deliver to simulate the daemon → worker Triggers() stream.
type contractFakeIPC struct {
	written chan message.Envelope
}

func newContractFakeIPC() *contractFakeIPC {
	// generous buffer so concurrent Submits never block the harness
	return &contractFakeIPC{written: make(chan message.Envelope, 1024)}
}

func (f *contractFakeIPC) ChannelID() channel.ID           { return "ch-contract" }
func (f *contractFakeIPC) WorkerID() string                { return "worker-contract" }
func (f *contractFakeIPC) WorkerActorID() actor.ActorID    { return "agent:worker-contract" }
func (f *contractFakeIPC) Triggers() <-chan TriggerPayload { return nil }
func (f *contractFakeIPC) WriteEnvelope(_ context.Context, env message.Envelope) error {
	select {
	case f.written <- env:
	default:
	}
	return nil
}

// kimiWorkerTransport binds the shared contract scenarios to the kimi
// worker-side caller helper (bridgeCaller). It is "transport B" in the
// cross-transport contract harness (spec §7): the SAME futurereg core, reached
// through the IPC-fed worker caller surface (Submit register-then-write /
// Await / Deliver-as-Triggers / Watch / Abandon / Pending) rather than the
// in-daemon registry directly.
type kimiWorkerTransport struct {
	caller *bridgeCaller
	ipc    *contractFakeIPC
}

// Register maps to the worker caller's Submit: it registers the future BEFORE
// writing the request envelope through IPC (subscribe-before-send). We discard
// the ack — the contract is about the futurereg semantics, not ack rendering.
func (tr *kimiWorkerTransport) Register(id message.ID) {
	req := message.Envelope{
		ID:        id,
		ChannelID: "ch-contract",
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "agent:worker-contract"},
		Kind:      message.KindRequest,
		Type:      "contract.ask",
		Audience:  message.Audience{"tool:contract-target"},
	}
	if _, err := tr.caller.Submit(context.Background(), tr.ipc, req, 30_000, true); err != nil {
		panic("kimiWorkerTransport.Register: Submit failed: " + err.Error())
	}
}

// Deliver feeds one inbound response into the worker caller, exactly as
// routeTriggers does when the daemon → worker Triggers() stream carries a
// response back.
func (tr *kimiWorkerTransport) Deliver(env *message.Envelope) futurereg.Disposition {
	return tr.caller.Deliver(env)
}

// Await drives the worker caller's bounded Await: ok=false,err=nil = window
// elapsed (fast-path degrade to ack), err!=nil = hard wait error.
func (tr *kimiWorkerTransport) Await(ctx context.Context, id message.ID, timeout time.Duration) (*message.Envelope, bool, error) {
	return tr.caller.Await(ctx, id, timeout)
}

func (tr *kimiWorkerTransport) Watch(id message.ID) (futurereg.Watcher, error) {
	return tr.caller.Watch(id)
}

func (tr *kimiWorkerTransport) Abandon(id message.ID) { tr.caller.Abandon(id) }

func (tr *kimiWorkerTransport) Pending() []message.ID { return tr.caller.Pending() }

// TestContractKimiWorkerTransport runs the shared futurereg contract scenario
// table against the kimi worker-side caller helper. The in-daemon transport
// runs the SAME table in kernel/adapter/futurereg; matching results across both
// prove the futurereg sync/async semantics do not drift between the in-daemon
// router and the cross-process worker caller (spec §7 / §10 drift risk).
func TestContractKimiWorkerTransport(t *testing.T) {
	contract.Run(t, func(t *testing.T) contract.Transport {
		return &kimiWorkerTransport{
			caller: newBridgeCaller(),
			ipc:    newContractFakeIPC(),
		}
	})
}
