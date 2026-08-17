package base

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

const ResumeSeedKey = "agent.resume-seed"
const SelectionKey = "agent.selection"
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
	return readState(ctx, sys, ResumeSeedKey)
}

func readState(ctx context.Context, sys seedReader, key string) []byte {
	deadline := time.Now().Add(catchupQueryBudget)
	for attempt := 0; ; attempt++ {
		out, err := sys.State().Get(resource.ResourceID(key))
		switch {
		case err == nil && out.Accepted():
			if !out.Found || len(out.Value) == 0 {
				return nil
			}
			if attempt > 0 {
				slog.Info("agent state read after link came up", "actor", sys.Self(), "key", key, "attempts", attempt+1)
			}
			return append([]byte(nil), out.Value...)
		case err == nil && out.RejectReason == access.ResourceNotFound:
			return nil
		}
		if time.Now().After(deadline) {
			slog.Warn("agent state unreadable; using default", "actor", sys.Self(), "key", key, "error", err, "reject", out.RejectReason)
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(catchupRetryInterval):
		}
	}
}

func readSelection(ctx context.Context, sys seedReader, spec runtimeproto.Spec) runtimeproto.TurnOptions {
	var selection runtimeproto.TurnOptions
	if raw := readState(ctx, sys, SelectionKey); len(raw) != 0 && json.Unmarshal(raw, &selection) == nil {
		for _, candidate := range spec.Selections {
			if candidate == selection {
				return selection
			}
		}
	}
	if spec.DefaultSelection >= 0 && spec.DefaultSelection < len(spec.Selections) {
		return spec.Selections[spec.DefaultSelection]
	}
	return runtimeproto.TurnOptions{}
}

// persistSeed intentionally has no coordinator, fence, retry, or ordering
// state. Resume self-healing is the only reliability mechanism for this
// low-frequency opaque checkpoint.
func persistSeed(sys actorbase.Sys, value []byte) {
	persistState(sys, ResumeSeedKey, value)
}

func persistSelection(sys actorbase.Sys, selection runtimeproto.TurnOptions) {
	value, _ := json.Marshal(selection)
	persistState(sys, SelectionKey, value)
}

func persistState(sys actorbase.Sys, key string, value []byte) {
	value = append([]byte(nil), value...)
	go func() {
		out, err := sys.State().Put(resource.ResourceID(key), value)
		if err == nil && out.Accepted() {
			return
		}
		detail := map[string]any{"key": key, "reject_reason": string(out.RejectReason)}
		if err != nil {
			detail["error"] = err.Error()
		}
		raw, _ := json.Marshal(detail)
		_ = sys.PublishObs(ObsCheckpointDrop, raw)
	}()
}
