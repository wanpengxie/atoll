package hostcommon

import "github.com/wanpengxie/atoll/runtime/actorrt"

// OutcomeString names an actorrt.Outcome for structured logging (an observation
// label, not a semantic branch — the handle does not act differently per kind).
func OutcomeString(o actorrt.Outcome) string {
	switch o {
	case actorrt.Delivered:
		return "delivered"
	case actorrt.NotHosted:
		return "not_hosted"
	case actorrt.MailboxFull:
		return "mailbox_full"
	case actorrt.Stopped:
		return "stopped"
	default:
		return "unknown"
	}
}
