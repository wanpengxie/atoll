package kimi

import (
	"encoding/json"
	"time"
)

// types.go is the inward (channel) domain face. Unlike xhs (many business types,
// each statically mapped to one device verb), kimi serves a SINGLE
// request/response type — `kimi.command` — whose device verb is drawn
// DYNAMICALLY from the request payload's `action` field. The closed set lives in
// the action allowlist below, not in the type set.

// TypeCommand is the one request type this adapter serves inward.
const TypeCommand = "kimi.command"

// commandDeadline bounds one browser primitive. Browser actions are sub-second
// to a few seconds; navigate + page load can be slow. A single 60s budget covers
// them all — there is no xhs.publish-style minutes-long operation here. A request
// past this deadline is reaped into a timeout failure.
const commandDeadline = 60 * time.Second

// actions is the closed set of browser primitives the extension understands.
// `action` is taken from the request payload; an action outside this set is
// rejected (invalid_action) before anything reaches the device.
var actions = map[string]struct{}{
	"navigate":      {},
	"find_tab":      {},
	"snapshot":      {},
	"click":         {},
	"fill":          {},
	"evaluate":      {},
	"screenshot":    {},
	"network":       {},
	"upload":        {},
	"save_as_pdf":   {},
	"list_tabs":     {},
	"close_tab":     {},
	"close_session": {},
}

// isAction reports whether name is in the closed browser-primitive set.
func isAction(name string) bool {
	_, ok := actions[name]
	return ok
}

// commandPayload is the inward shape of a kimi.command request: the verb plus
// its opaque arguments. args passes through to the device verbatim as the frame
// params — the adapter does not interpret it.
type commandPayload struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}
