package xhs

import (
	"sort"
	"time"

	"github.com/wanpengxie/atoll/drivers/tools/plugindevice"
)

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

// The endpoint words. They are the adapter's OWN words, not the plugin's: they
// are answered locally and stay answerable when no plugin is attached, which is
// exactly when someone needs to move the endpoint.
const (
	TypeListenSet = "xhs.listen.set"
	TypeListenGet = "xhs.listen.get"
)

// deadlines: publish is a long site action (image upload + post); the rest are
// quick reads. A request past its deadline is reaped into a timeout failure.
const (
	publishDeadline = 600 * time.Second
	shortDeadline   = 30 * time.Second
)

// typeSpecs is the private inward→outward mapping. A type absent here is
// unsupported (Receive fails it with type_unsupported).
var typeSpecs = map[string]plugindevice.Spec{
	TypePublish:     {Cmd: "publish", Deadline: publishDeadline},
	TypeSearch:      {Cmd: "search", Deadline: shortDeadline},
	TypeNoteFetch:   {Cmd: "get-note", Deadline: shortDeadline},
	TypeRecentFetch: {Cmd: "get-my-recent", Deadline: shortDeadline},
}

// lookupType resolves an inward type to its spec. ok=false ⇒ unsupported.
func lookupType(t string) (plugindevice.Spec, bool) {
	s, ok := typeSpecs[t]
	return s, ok
}

// supportedTypes is what a type_unsupported failure names back, derived from
// the mapping itself so the list can never drift from what is actually served.
func supportedTypes() []string {
	out := make([]string, 0, len(typeSpecs)+2)
	for t := range typeSpecs {
		out = append(out, t)
	}
	out = append(out, TypeListenSet, TypeListenGet)
	sort.Strings(out)
	return out
}
