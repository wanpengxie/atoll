package channel

import (
	"encoding/json"
	"io"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
)

type ReaderMode string

const (
	ReaderMember   ReaderMode = "member"
	ReaderObserver ReaderMode = "observer"
)

// Reader is the closed identity shape used by channel-visible read views.
// Member readers carry their active channel actor; observer readers carry the
// realm-authenticated principal whose perspective is being represented.
type Reader struct {
	Principal string        `json:"principal"`
	ActorID   actor.ActorID `json:"actor_id,omitempty"`
	Mode      ReaderMode    `json:"mode"`
}

func (r Reader) Valid() bool {
	switch r.Mode {
	case ReaderMember:
		return r.Principal != "" && r.ActorID != ""
	case ReaderObserver:
		return r.Principal != "" && r.ActorID == ""
	default:
		return false
	}
}

type ActorFacts struct {
	Principal string     `json:"principal"`
	Kind      actor.Kind `json:"kind"`
	Active    bool       `json:"active"`
}

type ResourceListQuery struct {
	Prefix string `json:"prefix,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

type ResourceMeta struct {
	ID                resource.ResourceID `json:"id"`
	Kind              string              `json:"kind"`
	CreatedBy         actor.ActorID       `json:"created_by"`
	CreatedAt         int64               `json:"created_at"`
	PlacementKind     string              `json:"placement_kind,omitempty"`
	PlacementDaemonID string              `json:"placement_daemon_id,omitempty"`
	Dir               bool                `json:"dir,omitempty"`
	SourceChannelID   ID                  `json:"source_channel_id,omitempty"`
	SourceResourceID  resource.ResourceID `json:"source_resource_id,omitempty"`
}

type ResourcePage struct {
	Items []ResourceMeta `json:"items"`
	Next  string         `json:"next,omitempty"`
}

type ResourceFetch struct {
	Meta ResourceMeta
	Body io.ReadCloser
}

type ResourceRef struct {
	ChannelID  ID                  `json:"channel_id"`
	ResourceID resource.ResourceID `json:"resource_id"`
}

type DeclSummary struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Owner      string `json:"owner"`
	Visibility string `json:"visibility"`
	Class      string `json:"class"`
}

type DeclDetail struct {
	DeclSummary
	Config json.RawMessage `json:"config,omitempty"`
}

type DeclSpec struct {
	Name       string          `json:"name"`
	Class      string          `json:"class"`
	Visibility string          `json:"visibility"`
	Config     json.RawMessage `json:"config,omitempty"`
}

type IntroduceOpts struct{}

type OperationView struct {
	Ref        string          `json:"ref"`
	Family     string          `json:"family"`
	Status     string          `json:"status"`
	Op         string          `json:"op,omitempty"`
	ResultJSON json.RawMessage `json:"result_json,omitempty"`
	ErrorCode  string          `json:"error_code,omitempty"`
	CreatedAt  int64           `json:"created_at"`
	DoneAt     *int64          `json:"done_at,omitempty"`
}

type Requester struct {
	ActorID   actor.ActorID `json:"actor_id"`
	ChannelID ID            `json:"channel_id"`
	RequestID string        `json:"request_id"`
}

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

var AllRealmErrorCodes = []RealmErrorCode{
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

type ErrResultUnknown struct{ Ref string }

func (e *ErrResultUnknown) Error() string { return "result_unknown: " + e.Ref }
