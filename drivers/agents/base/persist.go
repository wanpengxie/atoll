package base

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

const ResumeSeedKey = "agent.resume-seed"
const ObsCheckpointDrop actorrt.ObsKind = "agentbase.checkpoint_drop"

func readSeed(sys actorbase.Sys) []byte {
	out, err := sys.State().Get(resource.ResourceID(ResumeSeedKey))
	if err != nil || !out.Found || len(out.Value) == 0 {
		return nil
	}
	return append([]byte(nil), out.Value...)
}

// persistSeed intentionally has no coordinator, fence, retry, or ordering
// state. Resume self-healing is the only reliability mechanism for this
// low-frequency opaque checkpoint.
func persistSeed(sys actorbase.Sys, value []byte) {
	value = append([]byte(nil), value...)
	go func() {
		out, err := sys.State().Put(resource.ResourceID(ResumeSeedKey), value)
		if err == nil && out.Accepted() {
			return
		}
		detail := map[string]any{"key": ResumeSeedKey, "reject_reason": string(out.RejectReason)}
		if err != nil {
			detail["error"] = err.Error()
		}
		raw, _ := json.Marshal(detail)
		_ = sys.PublishObs(ObsCheckpointDrop, raw)
	}()
}
