package channelspec

// SpaceErrorCode is the closed error vocabulary of the space-tool contract.
// The contract crosses the membrane — the membrane's read faces, the portal's
// forwarding gates and the space-tool frame codec all speak these codes — so
// the vocabulary lives in the boundary leaf package rather than inside any
// single party's package.
type SpaceErrorCode string

const (
	SpaceForbidden             SpaceErrorCode = "forbidden"
	SpaceDeclNotFound          SpaceErrorCode = "decl_not_found"
	SpaceResourceNotFound      SpaceErrorCode = "resource_not_found"
	SpaceCapabilityUnavailable SpaceErrorCode = "capability_unavailable"
	SpaceChannelUnavailable    SpaceErrorCode = "channel_unavailable"
	SpaceUnavailable           SpaceErrorCode = "space_unavailable"
	SpaceInvalidRequest        SpaceErrorCode = "invalid_request"
	SpaceConflict              SpaceErrorCode = "conflict"
)

var spaceErrorCodes = [...]SpaceErrorCode{
	SpaceForbidden, SpaceDeclNotFound, SpaceResourceNotFound,
	SpaceCapabilityUnavailable, SpaceChannelUnavailable, SpaceUnavailable,
	SpaceInvalidRequest, SpaceConflict,
}

type SpaceError struct {
	Code   SpaceErrorCode
	Detail string
}

func (e *SpaceError) Error() string {
	if e.Detail == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Detail
}
