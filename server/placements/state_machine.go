package placements

import "github.com/coagent-ai/coagent/kernel/placement"

// parseState converts a wire-form string into placement.State. Any
// unrecognised value returns an empty placement.State which
// SQLStore.ListByState then surfaces as a SQL filter that matches no
// rows — safer than panicking on user input.
func parseState(s string) placement.State {
	switch placement.State(s) {
	case placement.StateCreating, placement.StateActive, placement.StateOrphan, placement.StateStale:
		return placement.State(s)
	}
	return ""
}
