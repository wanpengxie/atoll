package store

import "fmt"

// Visibility constants matching the v4 messages.visibility CHECK enum.
const (
	VisibilityPublic  = "public"
	VisibilityPrivate = "private"
	VisibilitySystem  = "system"
)

// Kind constants matching the v4 messages.kind CHECK enum.
const (
	KindEvent    = "event"
	KindRequest  = "request"
	KindResponse = "response"
)

// TypeMapping is the m1.3-v4-foundation-spec §4.1.3 rewrite output for one
// `(old_payload_type, body.type)` pair.
//
// `OverrideVisibility` / `OverrideAudienceSelf` / `PayloadOverlay` /
// `DocRefsFromBodyField` together describe the per-row mutation needed
// on top of the default audience/visibility/payload derivation.
type TypeMapping struct {
	// NewType is the v4 envelope.type the old row maps to (e.g.
	// "human.text", "xhs.publish.requested").
	NewType string

	// Kind is the v4 envelope.kind (event / request / response).
	Kind string

	// IsTerminal is the store-derived is_terminal column (0 or 1).
	// Spec rule: dispatch.completed / failed / rejected → 1; else 0.
	IsTerminal int

	// OverrideVisibility, when non-empty, forces this exact visibility
	// regardless of the default rule (default rule = "private if old
	// audience='self', else public").
	OverrideVisibility string

	// OverrideAudienceSelf, when true, rewrites the audience to
	// [<sender.id>] regardless of the old audience string. Used by
	// self.memo (always private to author).
	OverrideAudienceSelf bool

	// DocRefsFromBodyField, when non-empty, names a payload.body field
	// whose value gets lifted into the new `doc_refs` JSON array.
	// e.g. "doc_ref", "path".
	DocRefsFromBodyField string

	// PayloadOverlay, when non-nil, is shallow-merged into the new
	// payload before write — e.g. system.notice gets severity='info'.
	PayloadOverlay map[string]any
}

// dispatchOpMap normalizes the legacy `body.type` (xhs.<op>) to the v4
// canonical mid-segment. Most are 1:1 with body.type, three operations
// get an extra segment (note.fetch / note.record / recent.fetch /
// cookie.sync) per §4.1.3.
var dispatchOpMap = map[string]string{
	"xhs.publish":       "xhs.publish",
	"xhs.search":        "xhs.search",
	"xhs.get_note":      "xhs.note.fetch",
	"xhs.record_note":   "xhs.note.record",
	"xhs.get_my_recent": "xhs.recent.fetch",
	"xhs.sync_cookie":   "xhs.cookie.sync",
}

// Dropped legacy payload_type values. They map to *no* v4 type — the
// migration counts them in MigrationReport.DroppedTypes but does not
// fail the run. Per §4.1.3 these are explicit drops, not unknowns.
var droppedTypes = map[string]struct{}{
	"dispatch.self_check_due": {},
	"cron.tick":               {},
}

// MapType resolves the v4 mapping for a (oldPayloadType, bodyType) pair.
// bodyType is only consulted for the dispatch.* family.
//
// Returns (mapping, dropped, err):
//   - dropped=true & err=nil   → caller MUST skip this row + count it
//   - mapping populated & err=nil → caller proceeds with the mapping
//   - err != nil               → unknown combination, abort migration
func MapType(oldPayloadType, bodyType string) (TypeMapping, bool, error) {
	if _, ok := droppedTypes[oldPayloadType]; ok {
		return TypeMapping{}, true, nil
	}

	switch oldPayloadType {
	case "user.text":
		return TypeMapping{NewType: "human.text", Kind: KindEvent}, false, nil

	case "agent.text":
		return TypeMapping{NewType: "agent.text", Kind: KindEvent}, false, nil

	case "agent.progress":
		// Spec: merge into agent.text + visibility=system.
		return TypeMapping{
			NewType:            "agent.text",
			Kind:               KindEvent,
			OverrideVisibility: VisibilitySystem,
		}, false, nil

	case "system.notice":
		return TypeMapping{
			NewType: "system.event",
			Kind:    KindEvent,
			PayloadOverlay: map[string]any{
				"severity": "info",
			},
		}, false, nil

	case "system.heartbeat":
		return TypeMapping{NewType: "system.heartbeat", Kind: KindEvent}, false, nil

	case "channel.presence_changed":
		return TypeMapping{
			NewType: "system.event",
			Kind:    KindEvent,
			PayloadOverlay: map[string]any{
				"severity": "info",
				"kind":     "presence_changed",
			},
		}, false, nil

	case "channel.config.updated":
		return TypeMapping{
			NewType:              "file.updated",
			Kind:                 KindEvent,
			DocRefsFromBodyField: "path",
		}, false, nil

	case "dispatch.start":
		op, ok := dispatchOpMap[bodyType]
		if !ok {
			return TypeMapping{}, false, fmt.Errorf(
				"unknown dispatch.start body.type %q", bodyType,
			)
		}
		return TypeMapping{NewType: op + ".requested", Kind: KindRequest}, false, nil

	case "dispatch.accepted":
		op, ok := dispatchOpMap[bodyType]
		if !ok {
			return TypeMapping{}, false, fmt.Errorf(
				"unknown dispatch.accepted body.type %q", bodyType,
			)
		}
		// accepted is an ack event, not a terminal response.
		return TypeMapping{NewType: op + ".accepted", Kind: KindEvent}, false, nil

	case "dispatch.completed":
		op, ok := dispatchOpMap[bodyType]
		if !ok {
			return TypeMapping{}, false, fmt.Errorf(
				"unknown dispatch.completed body.type %q", bodyType,
			)
		}
		return TypeMapping{
			NewType:    op + ".completed",
			Kind:       KindResponse,
			IsTerminal: 1,
		}, false, nil

	case "dispatch.failed":
		op, ok := dispatchOpMap[bodyType]
		if !ok {
			return TypeMapping{}, false, fmt.Errorf(
				"unknown dispatch.failed body.type %q", bodyType,
			)
		}
		return TypeMapping{
			NewType:    op + ".failed",
			Kind:       KindResponse,
			IsTerminal: 1,
		}, false, nil

	case "dispatch.rejected":
		op, ok := dispatchOpMap[bodyType]
		if !ok {
			return TypeMapping{}, false, fmt.Errorf(
				"unknown dispatch.rejected body.type %q", bodyType,
			)
		}
		return TypeMapping{
			NewType:    op + ".rejected",
			Kind:       KindResponse,
			IsTerminal: 1,
		}, false, nil

	case "task.opened":
		return TypeMapping{
			NewType:              "file.created",
			Kind:                 KindEvent,
			DocRefsFromBodyField: "doc_ref",
		}, false, nil

	case "task.appended":
		return TypeMapping{
			NewType:              "file.updated",
			Kind:                 KindEvent,
			DocRefsFromBodyField: "doc_ref",
		}, false, nil

	case "task.closed":
		return TypeMapping{
			NewType:              "file.updated",
			Kind:                 KindEvent,
			DocRefsFromBodyField: "doc_ref",
			PayloadOverlay: map[string]any{
				"status": "completed",
			},
		}, false, nil

	case "self.memo":
		return TypeMapping{
			NewType:              "agent.text",
			Kind:                 KindEvent,
			OverrideVisibility:   VisibilityPrivate,
			OverrideAudienceSelf: true,
		}, false, nil

	case "workdir.changed":
		return TypeMapping{
			NewType:              "file.updated",
			Kind:                 KindEvent,
			DocRefsFromBodyField: "path",
		}, false, nil
	}

	return TypeMapping{}, false, fmt.Errorf("unmapped legacy payload_type %q", oldPayloadType)
}
