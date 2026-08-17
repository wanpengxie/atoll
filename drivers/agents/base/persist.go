package base

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

const ResumeSeedKey = "agent.resume-seed"
const ObsCheckpointDrop actorrt.ObsKind = "agentbase.checkpoint_drop"

// readSeed retries while the state door cannot yet say: a daemon-hosted actor
// boots exactly while its outbound link is coming up, and the door answers
// OutcomeUnknown (transport stage) until it is. Reading once there silently
// starts a fresh provider session and loses the one the actor had. A resolved
// "no seed" (accepted-but-empty or resource_not_found) is definitive and
// returns at once; only the unknown/error verdicts are retried, inside the same
// budget catch-up uses, so an unavailable link never delays boot beyond it.
// seedReader is the slice of Sys readSeed needs; Sys satisfies it.
type seedReader interface {
	State() actorbase.StateHandle
	Self() actor.ActorID
}

func readSeed(ctx context.Context, sys seedReader) []byte {
	deadline := time.Now().Add(catchupQueryBudget)
	for attempt := 0; ; attempt++ {
		out, err := sys.State().Get(resource.ResourceID(ResumeSeedKey))
		switch {
		case err == nil && out.Accepted():
			if !out.Found || len(out.Value) == 0 {
				return nil
			}
			if attempt > 0 {
				slog.Info("agent resume seed read after link came up", "actor", sys.Self(), "attempts", attempt+1)
			}
			return append([]byte(nil), out.Value...)
		case err == nil && out.RejectReason == access.ResourceNotFound:
			return nil
		}
		if time.Now().After(deadline) {
			slog.Warn("agent resume seed unreadable; starting a fresh session", "actor", sys.Self(), "error", err, "reject", out.RejectReason)
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(catchupRetryInterval):
		}
	}
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
