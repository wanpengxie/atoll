package worker

import (
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// FenceInvalidError signals the worker MUST exit. A typed error so the
// worker main loop can catch it via errors.As + call os.Exit explicitly.
type FenceInvalidError struct {
	ipc.FenceInvalidPayload
}

// Error implements error.
func (e *FenceInvalidError) Error() string {
	return fmt.Sprintf(
		"worker: fence invalid (expected token=%q epoch=%d, got token=%q epoch=%d): %s",
		e.ExpectedToken, e.ExpectedEpoch, e.GotToken, e.GotEpoch, e.Reason,
	)
}

// FenceFromFrame decodes an IPCFenceInvalid frame into a typed error.
// Returns a *FenceInvalidError so callers can errors.As on it.
func FenceFromFrame(frame ipc.Frame) error {
	var payload ipc.FenceInvalidPayload
	_ = json.Unmarshal(frame.Payload, &payload)
	return &FenceInvalidError{FenceInvalidPayload: payload}
}
