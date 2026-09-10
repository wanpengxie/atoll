package agentlooper

import (
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
)

// fail supplies this actor's recovery guidance; the tool adapter only relays it.
func fail(sys actorbase.Sys, msg actorbase.Msg, code, detail string, extra ...map[string]any) (message.ID, error) {
	fields := map[string]any{}
	switch code {
	case "busy":
		fields["recovery_hint"] = "This Looper is already executing the session; wait for that turn to finish."
	case "context_limit", "context_invalid":
		fields["recovery_hint"] = "The history cannot build a complete context within the read limit. Start a new session through the Controller."
	case "operation_mismatch", "assignment_conflict", "stale_assignment":
		fields["recovery_hint"] = "The command does not match this Looper's current assignment. Do not resend the stale command."
	}
	for _, more := range extra {
		for key, value := range more {
			fields[key] = value
		}
	}
	return sys.Fail(msg, code, detail, fields)
}

// callFailure keeps message delivery correlation distinct from model tool IDs.
type callFailure struct {
	Code      string
	Detail    string
	RequestID string
	cause     error
	Response  actorbase.Msg
}

func (e *callFailure) Error() string { return e.Code + ": " + e.Detail }
func (e *callFailure) Unwrap() error { return e.cause }
