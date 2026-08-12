package portal

import "github.com/wanpengxie/atoll/platform/lagoon"

// portalErrorCode is the complete portal wire-error closed set. No handler may
// emit an ad-hoc code outside these values; lagoon errors enter through the
// explicit map below.
type portalErrorCode string

const (
	codeInvalidCredentials portalErrorCode = "invalid_credentials"
	codeNotAuthenticated   portalErrorCode = "not_authenticated"
	codeInvalidArgs        portalErrorCode = "invalid_args"
	codeInternalError      portalErrorCode = "internal_error"
	codeUnavailable        portalErrorCode = "unavailable"
	codeBadPayload         portalErrorCode = "bad_payload"
	codeNotFound           portalErrorCode = "not_found"
	codeConflictExists     portalErrorCode = "conflict_exists"
	codePermissionDenied   portalErrorCode = "permission_denied"
	codeReserved           portalErrorCode = "reserved"
	codeResultUnknown      portalErrorCode = "result_unknown"
)

func mapLagoonCode(code lagoon.ErrorCode) (portalErrorCode, bool) {
	switch code {
	case lagoon.CodeInvalidArgs:
		return codeInvalidArgs, true
	case lagoon.CodeNotFound:
		return codeNotFound, true
	case lagoon.CodeConflictExists:
		return codeConflictExists, true
	case lagoon.CodePermissionDenied:
		return codePermissionDenied, true
	case lagoon.CodeReserved:
		return codeReserved, true
	case lagoon.CodeResultUnknown:
		return codeResultUnknown, true
	default:
		return "", false
	}
}
