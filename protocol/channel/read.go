package channel

import (
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
// space-authenticated principal whose perspective is being represented.
type Reader struct {
	Principal string        `json:"principal,omitempty"`
	ActorID   actor.ActorID `json:"actor_id,omitempty"`
	Mode      ReaderMode    `json:"mode"`
}

func (r Reader) Valid() bool {
	switch r.Mode {
	case ReaderMember:
		return r.Principal == "" && r.ActorID != ""
	case ReaderObserver:
		return r.Principal != "" && r.ActorID == ""
	default:
		return false
	}
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
