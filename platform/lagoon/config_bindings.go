package lagoon

import (
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// Bind only fields explicitly selected by the recipe. This is configuration
// materialization, not relation discovery: it never inspects an actor class.
func bindCreationConfig(raw json.RawMessage, bindings map[string]string, parent channel.ID) (json.RawMessage, error) {
	fields := map[string]json.RawMessage{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, err
		}
	}
	if fields == nil {
		fields = map[string]json.RawMessage{}
	}
	for field, source := range bindings {
		if field == "" || source != "parent_channel_id" {
			return nil, fmt.Errorf("unsupported creation binding %q: %q", field, source)
		}
		fields[field], _ = json.Marshal(parent)
	}
	return json.Marshal(fields)
}
