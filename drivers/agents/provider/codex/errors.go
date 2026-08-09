package codex

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

var invalidResumePattern = regexp.MustCompile(`(?i)no rollout found|thread not found|conversation not found|is archived|(thread|rollout).*(not found|missing|invalid)|does not exist`)
var noActivePattern = regexp.MustCompile(`(?i)no active turn|turn.*not active`)
var nativeSecretPattern = regexp.MustCompile(`(?i)(bearer\s+)[^\s,;]+|((?:authorization|api[_-]?key|token|password)\s*[:=]\s*)[^\s,;]+`)

func redactNative(s string) string { return nativeSecretPattern.ReplaceAllString(s, "$1$2[redacted]") }

func isInvalidResumeError(err error) bool {
	return err != nil && invalidResumePattern.MatchString(err.Error())
}
func classifyControlOutcome(req driverproto.ControlRequest, err error) driverproto.ControlOutcome {
	out := driverproto.ControlOutcome{Action: req.Action, Target: req.Target, Verdict: driverproto.ControlAccepted, Disposition: driverproto.KeepWorker}
	if err == nil {
		return out
	}
	out.Detail = err.Error()
	var re *rpcError
	if errors.As(err, &re) {
		if bytesContains(re.Data, []byte("activeTurnNotSteerable")) && req.Kind == driverproto.ControlSteer {
			out.Verdict = driverproto.ControlNotSteerable
			return out
		}
		if noActivePattern.MatchString(err.Error()) {
			out.Verdict = driverproto.ControlTargetGone
			return out
		}
		out.Verdict = driverproto.ControlRejected
		return out
	}
	out.Verdict, out.Disposition = driverproto.ControlRejected, driverproto.RetireWorker
	return out
}
func bytesContains(raw json.RawMessage, needle []byte) bool {
	return strings.Contains(string(raw), string(needle))
}
