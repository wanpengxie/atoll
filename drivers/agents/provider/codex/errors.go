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
func classifyControl(err error, kind driverproto.ControlKind) driverproto.ControlResult {
	if err == nil {
		return driverproto.ControlAccept(driverproto.KeepWorker)
	}
	var re *rpcError
	if errors.As(err, &re) {
		if bytesContains(re.Data, []byte("activeTurnNotSteerable")) && kind == driverproto.ControlSteer {
			return driverproto.NotSteerable(err.Error(), driverproto.KeepWorker)
		}
		if noActivePattern.MatchString(err.Error()) {
			return driverproto.TargetGone(err.Error(), driverproto.KeepWorker)
		}
		return driverproto.ControlReject(driverproto.FailureProvider, err.Error(), driverproto.KeepWorker)
	}
	return driverproto.ControlUncertain(driverproto.FailureTransport, err.Error())
}
func bytesContains(raw json.RawMessage, needle []byte) bool {
	return strings.Contains(string(raw), string(needle))
}
