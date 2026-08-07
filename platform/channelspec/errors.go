package channelspec

// RealmErrorCode is the closed error vocabulary of the realm-tool contract.
// The contract crosses the membrane — the membrane's read faces, the app's
// forwarding gates and the realm-tool frame codec all speak these codes — so
// the vocabulary lives in the boundary leaf package rather than inside any
// single party's package.
type RealmErrorCode string

const (
	RealmForbidden             RealmErrorCode = "forbidden"
	RealmDeclNotFound          RealmErrorCode = "decl_not_found"
	RealmResourceNotFound      RealmErrorCode = "resource_not_found"
	RealmCapabilityUnavailable RealmErrorCode = "capability_unavailable"
	RealmChannelUnavailable    RealmErrorCode = "channel_unavailable"
	RealmUnavailable           RealmErrorCode = "realm_unavailable"
	RealmInvalidRequest        RealmErrorCode = "invalid_request"
	RealmConflict              RealmErrorCode = "conflict"
)

var realmErrorCodes = [...]RealmErrorCode{
	RealmForbidden, RealmDeclNotFound, RealmResourceNotFound,
	RealmCapabilityUnavailable, RealmChannelUnavailable, RealmUnavailable,
	RealmInvalidRequest, RealmConflict,
}

type RealmError struct {
	Code   RealmErrorCode
	Detail string
}

func (e *RealmError) Error() string {
	if e.Detail == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Detail
}
