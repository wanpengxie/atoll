package link

import (
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// Declaration is the actor identity a daemon ships on attach so the home can
// register it into membership (the daemon holds NO truth — registration is
// home-side). Type/catalog is domain, not link wire (type non-first-class):
// link carries only the structural triple the registry needs.
type Declaration struct {
	ActorID actor.ActorID `json:"actor_id"`
	Kind    actor.Kind    `json:"kind"`
	Binding actor.Binding `json:"binding"`
	Epoch   int64         `json:"epoch"`
}

// AttachRequest is the stream-0 control message a daemon sends to join a
// channel home: the party identity + the actor streams it will open. The FIRST
// send joins the link; a daemon whose desired set changes may send it again
// (Reattach, §S-P8) — each send carries the FULL current declared set, never an
// increment, so the home's re-diff is idempotent and self-correcting (a dropped
// declaration simply is not in the next send). It carries NO credential —
// authentication is an app-layer concern resolved on the WS upgrade (the URL's
// ?key= query) before the connection ever reaches the Acceptor; the link layer
// is auth-agnostic (it does not care who the peer is, only its ResolveFunc
// differs). A credential field here would be a dead leak of an app concern into
// the wire vocabulary.
type AttachRequest struct {
	Proto        int           `json:"proto"`
	ComputeID    string        `json:"compute_id"`
	Declarations []Declaration `json:"declarations"`
}

// AttachReply is the home's stream-0 response: the assigned channel and the
// accept verdict.
type AttachReply struct {
	ChannelID channel.ID `json:"channel_id"`
	Accepted  bool       `json:"accepted"`
	Reason    string     `json:"reason,omitempty"`
	// DaemonID is the AUTHORITATIVE compute id the home just counted this
	// link online under (期11 spec §4.7's "AttachReply 增 daemonID 回传") —
	// the pre-authenticated daemonID the Acceptor received from the app
	// layer, or (dev/self-declared mode, daemonID=="") the ComputeID the
	// daemon itself sent. Today a daemon that dials with no explicit
	// ComputeID gets a random uuid from compute.Run and never learns whether
	// the home overrode it (Acceptor.handleAttach's computeID var already
	// does exactly that override when daemonID != ""). The daemon updates
	// its OWN identity on receipt (Dialer.DaemonID) — replacing the random
	// uuid — because per-channel resource root paths, AllocRequest routing,
	// and reservation/tombstone ownership all need this SAME value to be the
	// one unambiguous authority, not a value the daemon merely hoped the
	// home would agree with.
	DaemonID string `json:"daemon_id,omitempty"`
}

// controlKind tags one stream-0 control payload (the link control plane is
// JSON; actor streams are native ipc). Exactly two messages: attach and its
// reply — the whole party-level negotiation.
type controlKind string

const (
	ctrlAttach      controlKind = "attach"
	ctrlAttachReply controlKind = "attach_reply"
	ctrlPlanPull    controlKind = "plan_pull"
	ctrlPlanReply   controlKind = "plan_reply"
)

type PlanPull struct {
	BoundID string `json:"bound_id"`
}

type PlanReply struct {
	Actors []platform.PlanActor `json:"actors"`
	Error  string               `json:"error,omitempty"`
}

// controlFrame is the stream-0 envelope: one kind, one optional payload each.
type controlFrame struct {
	RequestID   string         `json:"request_id,omitempty"`
	Kind        controlKind    `json:"kind"`
	Attach      *AttachRequest `json:"attach,omitempty"`
	AttachReply *AttachReply   `json:"attach_reply,omitempty"`
	PlanPull    *PlanPull      `json:"plan_pull,omitempty"`
	PlanReply   *PlanReply     `json:"plan_reply,omitempty"`
}

func encodeControl(f controlFrame) ([]byte, error) { return json.Marshal(f) }

func decodeControl(b []byte) (controlFrame, error) {
	var f controlFrame
	if err := json.Unmarshal(b, &f); err != nil {
		return controlFrame{}, fmt.Errorf("link: decode control: %w", err)
	}
	return f, nil
}
