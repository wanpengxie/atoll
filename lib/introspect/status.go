package introspect

import "encoding/json"

// QueryStatus — what is this actor's LIVE operational state right now (its
// volatile self-snapshot), as opposed to actor.describe's static capability
// surface. The request payload is a StatusRequest (currently empty: the full
// self-snapshot is the only form). An actor answers with a Status whose snapshot
// map it fills itself; introspect守结构不守词汇 — it never interprets the keys.
//
// This is the additive status self-answer foreshadowed in introspect.go: it
// exists because a concrete actor (the device adapters) has non-trivial live
// state worth surfacing proactively — the device-attach online/offline flag —
// that the send→terminal outcome alone cannot answer (an actor with no in-flight
// request still has a knowable online state). Pain-driven, not pre-built.
const QueryStatus = "actor.status"

// StatusRequest is the actor.status request payload. Empty = the full
// self-snapshot. It mirrors DescribeRequest's selector shape so the convention
// is uniform, though no selector axis is defined yet.
type StatusRequest struct{}

// Status is the actor.status answer: the actor's identity plus an OPAQUE live
// snapshot. The snapshot map is the actor's own vocabulary (e.g.
// {"device_online": true}) — introspect owns the envelope (actor_id + the
// snapshot slot) but never the keys inside it, exactly as the substrate守结构
// 不守词汇. The JSON key is "status_snapshot" (not "status") so it never
// collides with the behavior-layer terminal `status` field that the response
// wrapper merges in alongside it.
type Status struct {
	// ActorID is the actor's registry id (e.g. "tool:xhs").
	ActorID string `json:"actor_id"`
	// Snapshot is the actor-filled opaque live state. nil → an empty object.
	Snapshot map[string]any `json:"status_snapshot"`
}

// AnswerStatus builds the actor.status answer for the given actor id and its
// self-supplied live snapshot. This is the ONE standard constructor every actor
// routes its status self-answer through, so the answer shape never drifts from
// the convention (mirrors AnswerDescribe). A nil snapshot is normalised to an
// empty object so the wire shape always carries the slot.
func AnswerStatus(actorID string, snapshot map[string]any) Status {
	if snapshot == nil {
		snapshot = map[string]any{}
	}
	return Status{ActorID: actorID, Snapshot: snapshot}
}

// ParseStatusRequest decodes an actor.status request payload. A nil/empty
// payload is the full-snapshot request. Mirrors ParseDescribeRequest.
func ParseStatusRequest(payload []byte) (StatusRequest, error) {
	var req StatusRequest
	if len(payload) == 0 {
		return req, nil
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return StatusRequest{}, err
	}
	return req, nil
}
