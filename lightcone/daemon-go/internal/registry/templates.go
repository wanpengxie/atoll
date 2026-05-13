package registry

import (
	"encoding/json"
)

// XHSCreatorAdapterActorID is the canonical sender.id for the
// xhs-creator template's tool adapter (L4 §2.1.1). Every request-type
// row in `XHSCreatorTypes()` resolves to this actor; the channel
// bootstrap saga (L2 §1.4.7 step 6) is expected to seed it with
// actor_kind='tool' / actor_binding='daemon_rpc' BEFORE calling
// Install with these rows.
const XHSCreatorAdapterActorID = "tool:xhs-adapter"

// xhsCreatorDomain is the `type_registry.domain` value all xhs-creator
// rows ship with. Kept as a const so the test suite + future template
// loaders don't drift on the literal.
const xhsCreatorDomain = "xhs"

// xhsAdapterTimeoutMs is the L4 §2 ad-hoc choice for adapter Ad-2
// (max_pending_ms). 30s is long enough for a Chrome-extension round
// trip but short enough to keep stuck adapters from blocking the
// channel for hours. Per-request override path is out of scope for
// M1.3.
const xhsAdapterTimeoutMs int64 = 30_000

// XHSCreatorTypes returns the six L4 §2.2 business type_registry rows
// for the xhs-creator channel template. Used by:
//
//   - the channel bootstrap saga via a future "template loader" step
//     (out of scope for T5 — see ticket §Out of scope), and
//   - the type_test.go happy-path test for Install (acts as the
//     normative reference doc for the schemas).
//
// Caller is responsible for ensuring `XHSCreatorAdapterActorID` is
// already registered in actor_registry before passing the slice to
// Install — Phase 1's actor seeding step belongs to the saga, not
// to this package.
//
// All five request-allowed types share `handler_actor_id =
// XHSCreatorAdapterActorID` and `handler_binding = daemon_rpc`
// (Chrome-extension adapter lives in the daemon process). The lone
// event-only type (`xhs.note.archived`) is emitted by the channel
// agent, has no handler, and uses `in_worker_bus` binding (it's
// produced from inside the worker after the publish round-trip).
//
// JSON Schema dialect: Draft 2020-12. The `$schema` URI is omitted —
// the jsonschema/v5 compiler treats absent `$schema` as 2020-12 by
// default (the compiler's documented contract).
func XHSCreatorTypes() []TypeRow {
	maxPending := xhsAdapterTimeoutMs
	return []TypeRow{
		// ------------------------------------------------------------------
		// 1) xhs.publish — agent -> tool:xhs-adapter, fields: title,content[,tags,images]
		// ------------------------------------------------------------------
		{
			Type:           "xhs.publish",
			AllowedKinds:   []string{"request", "response"},
			HandlerBinding: HandlerBindingDaemonRPC,
			MaxPendingMs:   &maxPending,
			HandlerActorID: XHSCreatorAdapterActorID,
			Domain:         xhsCreatorDomain,
			SchemasByKind: mustJSON(map[string]any{
				"request": map[string]any{
					"type":                 "object",
					"required":             []string{"title", "content"},
					"additionalProperties": false,
					"properties": map[string]any{
						"title":   map[string]any{"type": "string"},
						"content": map[string]any{"type": "string"},
						"tags": map[string]any{
							"type":  "array",
							"items": map[string]any{"type": "string"},
						},
						"images": map[string]any{
							"type":  "array",
							"items": map[string]any{"type": "string"},
						},
					},
				},
				"response": xhsResponseSchema(map[string]any{
					"note_id":     map[string]any{"type": "string"},
					"url":         map[string]any{"type": "string"},
					"device_id":   map[string]any{"type": "string"},
					"retry_after": map[string]any{"type": "integer", "minimum": 0},
				}),
			}),
		},

		// ------------------------------------------------------------------
		// 2) xhs.search — agent -> tool, fields: query[,limit]
		// ------------------------------------------------------------------
		{
			Type:           "xhs.search",
			AllowedKinds:   []string{"request", "response"},
			HandlerBinding: HandlerBindingDaemonRPC,
			MaxPendingMs:   &maxPending,
			HandlerActorID: XHSCreatorAdapterActorID,
			Domain:         xhsCreatorDomain,
			SchemasByKind: mustJSON(map[string]any{
				"request": map[string]any{
					"type":                 "object",
					"required":             []string{"query"},
					"additionalProperties": false,
					"properties": map[string]any{
						"query": map[string]any{"type": "string"},
						"limit": map[string]any{"type": "integer", "minimum": 1},
					},
				},
				"response": xhsResponseSchema(map[string]any{
					"results": map[string]any{
						"type":  "array",
						"items": map[string]any{"type": "object"},
					},
				}),
			}),
		},

		// ------------------------------------------------------------------
		// 3) xhs.note.fetch — agent -> tool, fields: note_id
		// ------------------------------------------------------------------
		{
			Type:           "xhs.note.fetch",
			AllowedKinds:   []string{"request", "response"},
			HandlerBinding: HandlerBindingDaemonRPC,
			MaxPendingMs:   &maxPending,
			HandlerActorID: XHSCreatorAdapterActorID,
			Domain:         xhsCreatorDomain,
			SchemasByKind: mustJSON(map[string]any{
				"request": map[string]any{
					"type":                 "object",
					"required":             []string{"note_id"},
					"additionalProperties": false,
					"properties": map[string]any{
						"note_id": map[string]any{"type": "string"},
					},
				},
				"response": xhsResponseSchema(map[string]any{
					"note": map[string]any{"type": "object"},
				}),
			}),
		},

		// ------------------------------------------------------------------
		// 4) xhs.recent.fetch — agent -> tool, fields: [limit]
		// ------------------------------------------------------------------
		{
			Type:           "xhs.recent.fetch",
			AllowedKinds:   []string{"request", "response"},
			HandlerBinding: HandlerBindingDaemonRPC,
			MaxPendingMs:   &maxPending,
			HandlerActorID: XHSCreatorAdapterActorID,
			Domain:         xhsCreatorDomain,
			SchemasByKind: mustJSON(map[string]any{
				"request": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"properties": map[string]any{
						"limit": map[string]any{"type": "integer", "minimum": 1},
					},
				},
				"response": xhsResponseSchema(map[string]any{
					"notes": map[string]any{
						"type":  "array",
						"items": map[string]any{"type": "object"},
					},
				}),
			}),
		},

		// ------------------------------------------------------------------
		// 5) xhs.cookie.sync — agent -> tool, fields: (empty)
		// ------------------------------------------------------------------
		{
			Type:           "xhs.cookie.sync",
			AllowedKinds:   []string{"request", "response"},
			HandlerBinding: HandlerBindingDaemonRPC,
			MaxPendingMs:   &maxPending,
			HandlerActorID: XHSCreatorAdapterActorID,
			Domain:         xhsCreatorDomain,
			SchemasByKind: mustJSON(map[string]any{
				"request": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"properties":           map[string]any{},
				},
				"response": xhsResponseSchema(nil),
			}),
		},

		// ------------------------------------------------------------------
		// 6) xhs.note.archived — agent-emitted event, no handler
		// ------------------------------------------------------------------
		// in_worker_bus binding: emitted from inside the worker after
		// the publish loop archives the note (L4 §2.1.2). No
		// handler_actor_id (no concrete receiver — pure broadcast).
		{
			Type:           "xhs.note.archived",
			AllowedKinds:   []string{"event"},
			HandlerBinding: HandlerBindingInWorkerBus,
			Domain:         xhsCreatorDomain,
			SchemasByKind: mustJSON(map[string]any{
				"event": map[string]any{
					"type":                 "object",
					"required":             []string{"note_id", "archive_path"},
					"additionalProperties": false,
					"properties": map[string]any{
						"note_id":      map[string]any{"type": "string"},
						"archive_path": map[string]any{"type": "string"},
					},
				},
			}),
		},
	}
}

// xhsResponseSchema builds the canonical xhs-* response schema with the
// shared {status, reason} pair plus the per-type success fields the
// caller supplies via `extra`.
//
// Critical contract: `reason` MUST be `{"type": "string"}` (not an
// enum) — otherwise the §3.7 platform fallback emits would fail
// validation (L2 §1.4.2 normative "reason 字段必须 type: string，不能 enum
// 收窄"). This helper guarantees the rule across all five types.
//
// `status` is an enum {"completed", "failed"} per L4 §2.2 — both
// success and failure are explicit-only; never empty.
func xhsResponseSchema(extra map[string]any) map[string]any {
	props := map[string]any{
		"status": map[string]any{
			"type": "string",
			"enum": []string{"completed", "failed"},
		},
		"reason": map[string]any{"type": "string"},
	}
	for k, v := range extra {
		props[k] = v
	}
	return map[string]any{
		"type":                 "object",
		"required":             []string{"status"},
		"additionalProperties": false,
		"properties":           props,
	}
}

// mustJSON marshals a Go value to JSON or panics — only used in this
// file for package-initialization-style template construction, where a
// failure means a code typo, not a runtime input issue.
func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic("registry: mustJSON: " + err.Error())
	}
	return b
}
