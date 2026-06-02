package workerhost

import (
	"github.com/wanpengxie/ActOS/runtime/fence"
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// Fence validates that an inbound IPC frame from a worker carries the
// fencing_token + daemon_epoch the daemon recorded for its lease.
//
// Returns:
//   - ok=true, zero payload — write allowed; daemon should process the frame.
//   - ok=false, payload — caller should reply with IPCFenceInvalid +
//     FenceInvalidPayload so the worker exits.
func Fence(
	frame ipc.Frame,
	expectedToken fence.FencingToken,
	expectedEpoch fence.DaemonEpoch,
) (bool, ipc.FenceInvalidPayload) {
	if frame.FencingToken == expectedToken && frame.DaemonEpoch == expectedEpoch {
		return true, ipc.FenceInvalidPayload{}
	}
	return false, ipc.FenceInvalidPayload{
		ExpectedToken: expectedToken,
		GotToken:      frame.FencingToken,
		ExpectedEpoch: expectedEpoch,
		GotEpoch:      frame.DaemonEpoch,
		Reason:        "fencing or daemon_epoch mismatch — daemon restarted or lease lost",
	}
}
