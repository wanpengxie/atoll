package home

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sync"

	"github.com/wanpengxie/atoll/platform/internal/humancell"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const defaultAgentSetEventType = "channel.routing.default_set"

// defaultAgentFold is the in-memory projection of the latest authoritative
// routing event. Its single lock is also the setting linearization lock: a
// writer holds it across durable append, Accepted confirmation, and projection
// update, so readers can never observe a value ahead of the ledger.
type defaultAgentFold struct {
	mu    sync.RWMutex
	state humancell.RoutingSnapshot
	home  *Home
}

func openDefaultAgentFold(
	ctx context.Context,
	home *Home,
	query storespec.MessageQuery,
	logger *slog.Logger,
) *defaultAgentFold {
	fold := &defaultAgentFold{
		home:  home,
		state: humancell.RoutingSnapshot{State: humancell.RoutingUnset},
	}
	row, found, err := query.LatestBySenderAndType(ctx, actor.SystemActorID, defaultAgentSetEventType)
	if err != nil {
		fold.state.State = humancell.RoutingUnavailable
		logger.Warn("platform.routing.fold_unavailable", "error", err)
		return fold
	}
	if !found {
		return fold
	}
	target, err := decodeDefaultAgentEvent(row.Envelope.Payload)
	if err != nil {
		fold.state.State = humancell.RoutingUnavailable
		logger.Warn("platform.routing.authoritative_row_invalid",
			"seq", row.Seq, "message_id", row.Envelope.ID, "error", err)
		return fold
	}
	if target == "" {
		return fold
	}
	fold.state = humancell.RoutingSnapshot{
		State: humancell.RoutingConfigured, Target: target,
	}
	return fold
}

func (f *defaultAgentFold) snapshot() humancell.RoutingSnapshot {
	if f == nil {
		return humancell.RoutingSnapshot{State: humancell.RoutingUnavailable}
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state
}

func (f *defaultAgentFold) set(
	ctx context.Context,
	target actor.ActorID,
	setBy actor.ActorID,
) error {
	if f == nil || f.home == nil {
		return errors.New("platform: default-agent fold unavailable")
	}
	if setBy == "" {
		return errors.New("platform: default-agent setter required")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.home.emitSystemEvent(ctx, defaultAgentSetEventType, map[string]any{
		"v": 1, "default_agent": target, "set_by": setBy,
	}); err != nil {
		return err
	}
	if target == "" {
		f.state = humancell.RoutingSnapshot{State: humancell.RoutingUnset}
	} else {
		f.state = humancell.RoutingSnapshot{
			State: humancell.RoutingConfigured, Target: target,
		}
	}
	return nil
}

// decodeDefaultAgentEvent applies the event contract without using a struct:
// absent, null, or wrongly typed fields must not collapse into Go zero values.
func decodeDefaultAgentEvent(payload json.RawMessage) (actor.ActorID, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return "", fmt.Errorf("decode object: %w", err)
	}
	if fields == nil {
		return "", errors.New("payload must be an object")
	}
	rawVersion, ok := fields["v"]
	if !ok || bytes.Equal(bytes.TrimSpace(rawVersion), []byte("null")) {
		return "", errors.New("v must be a non-null JSON number")
	}
	decoder := json.NewDecoder(bytes.NewReader(rawVersion))
	decoder.UseNumber()
	var encodedVersion any
	if err := decoder.Decode(&encodedVersion); err != nil {
		return "", errors.New("v must be a JSON number")
	}
	version, ok := encodedVersion.(json.Number)
	if !ok {
		return "", errors.New("v must be a JSON number")
	}
	value, ok := new(big.Rat).SetString(version.String())
	if !ok || value.Cmp(big.NewRat(1, 1)) != 0 {
		return "", errors.New("unsupported routing event version")
	}
	defaultAgent, err := requiredJSONString(fields, "default_agent")
	if err != nil {
		return "", err
	}
	setBy, err := requiredJSONString(fields, "set_by")
	if err != nil {
		return "", err
	}
	if setBy == "" {
		return "", errors.New("set_by must be non-empty")
	}
	return actor.ActorID(defaultAgent), nil
}

func requiredJSONString(fields map[string]json.RawMessage, key string) (string, error) {
	raw, ok := fields[key]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", fmt.Errorf("%s must be a non-null JSON string", key)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s must be a JSON string", key)
	}
	return value, nil
}
