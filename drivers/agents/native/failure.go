package native

import (
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
)

// fail supplies this actor's recovery guidance; the tool adapter only relays it.
func fail(sys actorbase.Sys, msg actorbase.Msg, code, detail string, extra ...map[string]any) (message.ID, error) {
	fields := map[string]any{}
	switch code {
	case "scope_required":
		fields["recovery_hint"] = "Specify a session; use session=new on agent.ask to start a new conversation."
	case "session_capacity", "capacity", "busy":
		fields["recovery_hint"] = "Wait for current work to settle before resending; retain the same submission_key when retrying an ask."
	case "submission_conflict", "operation_conflict":
		fields["recovery_hint"] = "The key already identifies different input. Reuse it only for the identical operation."
	case "work_not_found", "session_not_found":
		fields["recovery_hint"] = "Read agent.status or agent.session.list on this Controller and use an existing identifier."
	case "work_closed", "session_archived":
		fields["recovery_hint"] = "Read the completed result; use a new ask for follow-up work."
	case "context_limit", "context_invalid":
		fields["recovery_hint"] = "Start a new session with session=new; this session's context cannot be reconstructed within the limit."
	case "context_unavailable":
		fields["recovery_hint"] = "The requested source context is unavailable. Start a new session without related_work_id, or select an available source."
	}
	for _, more := range extra {
		for key, value := range more {
			fields[key] = value
		}
	}
	return sys.Fail(msg, code, detail, fields)
}
