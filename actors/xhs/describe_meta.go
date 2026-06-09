package xhs

import "github.com/wanpengxie/ActOS/lib/introspect"

// DescribeTypeMetadata returns the xhs type metadata in the introspect
// contract shape, deriving allowed_kinds / max_pending_ms from the closed
// type sets: request/response types get the domain default wait budget;
// event-only rows are event-scoped and advertise no pending budget.
func DescribeTypeMetadata() map[string]introspect.TypeMeta {
	out := make(map[string]introspect.TypeMeta, len(typeMeta))
	for name, meta := range typeMeta {
		kind := "request"
		maxPending := DefaultMaxPendingMs
		for _, v := range EventOnlyTypes {
			if v == name {
				kind = "event"
				maxPending = 0
				break
			}
		}
		meta.AllowedKinds = []string{kind}
		meta.MaxPendingMs = maxPending
		out[name] = meta
	}
	return out
}
