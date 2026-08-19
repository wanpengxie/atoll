package gateway

import "github.com/wanpengxie/atoll/protocol/actor"

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
