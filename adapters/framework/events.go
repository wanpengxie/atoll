package framework

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

const (
	systemEventCorrelationLost     = "correlation_lost"
	systemEventTimerTerminalFailed = "adapter_timer_terminal_failed"
	orphanCallbackPayloadKind      = "orphan_callback"
	eventPayloadSchemaObject       = `{"type":"object"}`
)

var eventSeq atomic.Uint64

// OrphanCallbackType returns the per-adapter event type used when an external
// callback cannot be correlated to a pending request.
func OrphanCallbackType(adapterName string) string {
	return fmt.Sprintf("adapter.%s.orphan_callback", adapterName)
}

// OrphanCallbackEvent describes one uncorrelatable external callback.
type OrphanCallbackEvent struct {
	AdapterName    string
	AdapterActorID actor.ActorID
	ChannelID      channel.ID
	Chain          harness.Chain
	Clock          func() time.Time
	CorrelationID  string
	Detail         string
	Payload        []byte
}

// EmitOrphanCallbackEvents writes the L1 §6.5 observability pair:
// adapter.<name>.orphan_callback plus system.event(kind=correlation_lost).
// A nil Chain is treated as no-op so lower-level adapter unit tests can omit
// the harness seam.
func EmitOrphanCallbackEvents(ctx context.Context, ev OrphanCallbackEvent) error {
	if ev.Chain == nil {
		return nil
	}
	now := eventNow(ev.Clock)
	adapterPayload := map[string]any{
		"kind":           orphanCallbackPayloadKind,
		"adapter":        ev.AdapterName,
		"correlation_id": ev.CorrelationID,
		"detail":         ev.Detail,
	}
	if len(ev.Payload) > 0 {
		adapterPayload["payload"] = string(ev.Payload)
	}
	if err := writeEvent(ctx, ev.Chain, eventEnvelope{
		Type:      OrphanCallbackType(ev.AdapterName),
		ChannelID: ev.ChannelID,
		Sender:    message.Sender{Kind: actor.KindTool, ID: ev.AdapterActorID},
		Now:       now,
		Payload:   adapterPayload,
	}); err != nil {
		return err
	}
	return writeEvent(ctx, ev.Chain, eventEnvelope{
		Type:      "system.event",
		ChannelID: ev.ChannelID,
		Sender:    message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Now:       now,
		Payload: map[string]any{
			"severity":       "warn",
			"kind":           systemEventCorrelationLost,
			"adapter":        ev.AdapterName,
			"correlation_id": ev.CorrelationID,
			"detail":         ev.Detail,
		},
	})
}

type timerTerminalFailedEvent struct {
	AdapterName string
	ChannelID   channel.ID
	Chain       harness.Chain
	Clock       func() time.Time
	RequestID   string
	Err         error
}

func emitTimerTerminalFailedEvent(ctx context.Context, ev timerTerminalFailedEvent) error {
	if ev.Chain == nil {
		return nil
	}
	detail := ""
	if ev.Err != nil {
		detail = ev.Err.Error()
	}
	return writeEvent(ctx, ev.Chain, eventEnvelope{
		Type:      "system.event",
		ChannelID: ev.ChannelID,
		Sender:    message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Now:       eventNow(ev.Clock),
		Payload: map[string]any{
			"severity":   "error",
			"kind":       systemEventTimerTerminalFailed,
			"adapter":    ev.AdapterName,
			"request_id": ev.RequestID,
			"detail":     detail,
		},
	})
}

type eventEnvelope struct {
	Type      string
	ChannelID channel.ID
	Sender    message.Sender
	Now       int64
	Payload   map[string]any
}

func writeEvent(ctx context.Context, chain harness.Chain, ev eventEnvelope) error {
	body, err := json.Marshal(ev.Payload)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(body)
	seq := eventSeq.Add(1)
	env := &message.Envelope{
		ID:         fmt.Sprintf("event:%s:%d:%d:%s", ev.Type, ev.Now, seq, hex.EncodeToString(hash[:])[:16]),
		TS:         ev.Now,
		TSReceived: ev.Now,
		ChannelID:  string(ev.ChannelID),
		Sender:     ev.Sender,
		Kind:       message.KindEvent,
		Type:       ev.Type,
		Payload:    body,
		Visibility: message.VisibilitySystem,
		Audience:   []string{"*"},
	}
	res, err := chain.Write(ctx, env)
	if err != nil {
		return fmt.Errorf("framework: write event %s: %w", ev.Type, err)
	}
	if res.RejectReason != "" {
		return fmt.Errorf("framework: write event %s rejected: %s (%s)", ev.Type, res.RejectReason, res.RejectDetail)
	}
	return nil
}

func eventNow(clock func() time.Time) int64 {
	if clock == nil {
		return time.Now().UnixMilli()
	}
	return clock().UnixMilli()
}
