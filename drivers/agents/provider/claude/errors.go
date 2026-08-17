package claude

import (
	"regexp"
	"strings"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

var invalidResumePattern = regexp.MustCompile(`(?i)no conversation found`)
var nativeSecretPattern = regexp.MustCompile(`(?i)(bearer\s+)[^\s,;]+|((?:authorization|api[_-]?key|token|password)\s*[:=]\s*)[^\s,;]+`)

func redactNative(s string) string { return nativeSecretPattern.ReplaceAllString(s, "$1$2[redacted]") }

func classifyInterruptReply(action driverproto.ActionToken, target driverproto.WorkerTurnTarget, reply controlReply) driverproto.ControlOutcome {
	out := driverproto.ControlOutcome{Action: action, Target: target, Verdict: driverproto.ControlAccepted, Disposition: driverproto.KeepWorker}
	if reply.Success {
		return out
	}
	out.Verdict = driverproto.ControlRejected
	out.Detail = strings.TrimSpace(reply.Error)
	if out.Detail == "" {
		out.Detail = "interrupt rejected"
	}
	return out
}
