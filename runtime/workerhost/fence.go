package workerhost

import (
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// Fence validates that an inbound IPC frame from a worker carries the
// worker-LEASE token the host assigned at spawn (v2 instance fence — guards
// against a zombie / reconnecting worker, NOT against channel-log writers,
// since the channel has a single writer by construction).
//
// Returns:
//   - ok=true, zero payload — frame allowed; host should process it.
//   - ok=false, payload — caller should reply with KindFenceInvalid +
//     FenceInvalidPayload so the worker exits.
func Fence(frame ipc.Frame, expectedToken string) (bool, ipc.FenceInvalidPayload) {
	if frame.LeaseToken == expectedToken {
		return true, ipc.FenceInvalidPayload{}
	}
	return false, ipc.FenceInvalidPayload{
		ExpectedToken: expectedToken,
		GotToken:      frame.LeaseToken,
		Reason:        "worker-lease token mismatch — stale / zombie worker",
	}
}
