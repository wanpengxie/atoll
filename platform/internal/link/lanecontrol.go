package link

import (
	"fmt"

	"github.com/wanpengxie/atoll/protocol/access"
)

type ResolveCoordRequest struct {
	RequestID string `json:"request_id"`
	Token     string `json:"token"`
}

type ResolveCoordReply struct {
	RequestID     string           `json:"request_id"`
	OK            bool             `json:"ok"`
	Coord         string           `json:"coord,omitempty"`
	Mode          access.Operation `json:"mode,omitempty"`
	ReservationID string           `json:"reservation_id,omitempty"`
	Reason        string           `json:"reason,omitempty"`
}

func (m ResolveCoordRequest) validate() error {
	if err := requiredControlField("resolve_coord.request_id", m.RequestID); err != nil {
		return err
	}
	return requiredControlField("resolve_coord.token", m.Token)
}

func (m ResolveCoordReply) validate() error {
	if err := requiredControlField("resolve_coord_reply.request_id", m.RequestID); err != nil {
		return err
	}
	if !m.OK {
		return requiredControlField("resolve_coord_reply.reason", m.Reason)
	}
	if err := requiredControlField("resolve_coord_reply.coord", m.Coord); err != nil {
		return err
	}
	if m.Mode != access.OpRead && m.Mode != access.OpWrite {
		return fmt.Errorf("link: resolve_coord_reply.mode must be read or write")
	}
	return nil
}
