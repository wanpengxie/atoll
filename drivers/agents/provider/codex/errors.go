package codex

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/wanpengxie/atoll/drivers/agents/base"
)

var invalidResumePattern = regexp.MustCompile(`(?i)no rollout found|thread not found|conversation not found|is archived|(thread|rollout).*(not found|missing|invalid)|does not exist`)
var closingPattern = regexp.MustCompile(`(?i)thread.*clos(?:ing|ed)`)
var noActivePattern = regexp.MustCompile(`(?i)no active turn|turn.*not active`)
var mismatchPattern = regexp.MustCompile(`(?i)expected.*turn|turn.*mismatch`)
var emptyPattern = regexp.MustCompile(`(?i)empty.*input|input.*empty`)

func isInvalidResumeError(err error) bool {
	return err != nil && invalidResumePattern.MatchString(err.Error())
}
func isClosingError(err error) bool { return err != nil && closingPattern.MatchString(err.Error()) }
func controlVerdict(err error) base.ControlVerdict {
	if err == nil {
		return base.ControlAccepted
	}
	var re *rpcError
	if errors.As(err, &re) && bytesContains(re.Data, []byte("activeTurnNotSteerable")) {
		return base.ControlNotSteerable
	}
	s := err.Error()
	switch {
	case noActivePattern.MatchString(s):
		return base.ControlNoActiveTurn
	case mismatchPattern.MatchString(s):
		return base.ControlMismatch
	case emptyPattern.MatchString(s):
		return base.ControlEmptyInput
	default:
		return base.ControlRPCError
	}
}
func bytesContains(raw json.RawMessage, needle []byte) bool {
	return strings.Contains(string(raw), string(needle))
}
