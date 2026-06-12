package xhs

import "time"

// types.go is the inward (channel) type closed set plus its private mapping to
// the outward device command verbs and per-type wait budgets. The type names
// are BUSINESS language (publish/search/note/recent) — the device verbs
// (get-note, get-my-recent) never leak inward; that translation is exactly the
// adapter's job (anti-corruption layer).

// The closed set of request types this adapter serves inward.
const (
	TypePublish     = "xhs.publish"
	TypeSearch      = "xhs.search"
	TypeNoteFetch   = "xhs.note.fetch"
	TypeRecentFetch = "xhs.recent.fetch"
)

// deadlines: publish is a long site action (image upload + post); the rest are
// quick reads. Layered per §3.3 / §6.2 of the adapter spec — a request past its
// deadline is reaped into a timeout failure.
const (
	publishDeadline = 600 * time.Second
	shortDeadline   = 30 * time.Second
)

// typeSpec binds an inward type to its device verb and wait budget.
type typeSpec struct {
	cmd      string
	deadline time.Duration
}

// typeSpecs is the private inward→outward mapping. A type absent here is
// unsupported (Receive fails it with type_unsupported).
var typeSpecs = map[string]typeSpec{
	TypePublish:     {cmd: "publish", deadline: publishDeadline},
	TypeSearch:      {cmd: "search", deadline: shortDeadline},
	TypeNoteFetch:   {cmd: "get-note", deadline: shortDeadline},
	TypeRecentFetch: {cmd: "get-my-recent", deadline: shortDeadline},
}

// lookupType resolves an inward type to its spec. ok=false ⇒ unsupported.
func lookupType(t string) (typeSpec, bool) {
	s, ok := typeSpecs[t]
	return s, ok
}
