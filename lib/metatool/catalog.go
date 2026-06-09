package metatool

import (
	"sort"

	"github.com/wanpengxie/ActOS/lib/introspect"
)

// FormatCatalog projects an introspect.Catalog (the substrate's actor.list
// response) into the grouped JSON shape the LLM consumes.
func FormatCatalog(catalog introspect.Catalog) map[string]any {
	out := make([]map[string]any, 0, len(catalog.Actors))
	for _, a := range catalog.Actors {
		entry := map[string]any{
			"actor_id": a.ID,
			"kind":     a.Kind,
			"present":  a.Present,
		}
		if a.Binding != "" {
			entry["binding"] = a.Binding
		}
		if a.UptimeMs > 0 {
			entry["uptime_ms"] = a.UptimeMs
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		ii, _ := out[i]["actor_id"].(string)
		jj, _ := out[j]["actor_id"].(string)
		return ii < jj
	})
	return map[string]any{"actors": out}
}
