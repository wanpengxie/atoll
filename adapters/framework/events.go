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
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

const (
	systemEventCorrelationLost     = "correlation_lost"
	systemEventTimerTerminalFailed = "adapter_timer_terminal_failed"
	orphanCallbackPayloadKind      = "orphan_callback"
)

var eventSeq atomic.Uint64

// orphanCallbackType returns the per-adapter event type used when an external
// callback cannot be correlated to a pending request.
func orphanCallbackType(adapterName string) string {
	return fmt.Sprintf("adapter.%s.orphan_callback", adapterName)
}

// orphanCallbackEvent describes one uncorrelatable external callback.
type orphanCallbackEvent struct {
	AdapterName    string
	AdapterActorID actor.ActorID
	ChannelID      channel.ID
	Chain          harness.Chain
	Clock          func() time.Time
	CorrelationID  string
	Detail         string
	Payload        []byte
}

// emitOrphanCallbackEvents writes the L1 §6.5 adapter observability event:
// adapter.<name>.orphan_callback. Callers that need a system event emit it
// explicitly after this succeeds.
func emitOrphanCallbackEvents(ctx context.Context, ev orphanCallbackEvent) error {
	if ev.Chain == nil {
		return nil
	}
	adapterPayload := map[string]any{
		"kind":           orphanCallbackPayloadKind,
		"adapter":        ev.AdapterName,
		"correlation_id": ev.CorrelationID,
		"detail":         ev.Detail,
	}
	if len(ev.Payload) > 0 {
		adapterPayload["payload"] = string(ev.Payload)
	}
	raw, err := json.Marshal(adapterPayload)
	if err != nil {
		return err
	}
	_, err = writeEvent(ctx, ev.Chain, eventEnvelope{
		Type:       orphanCallbackType(ev.AdapterName),
		ChannelID:  ev.ChannelID,
		Sender:     message.Sender{Kind: actor.KindTool, ID: ev.AdapterActorID},
		Now:        eventNow(ev.Clock),
		PayloadRaw: raw,
	})
	return err
}

func emitCorrelationLostSystemEvent(ctx context.Context, chain harness.Chain, ev orphanCallbackEvent) error {
	if chain == nil {
		return nil
	}
	_, err := writeEvent(ctx, chain, eventEnvelope{
		Type:      "core.system_event",
		ChannelID: ev.ChannelID,
		Sender:    message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Now:       eventNow(ev.Clock),
		Payload: map[string]any{
			"severity":       "warn",
			"kind":           systemEventCorrelationLost,
			"adapter":        ev.AdapterName,
			"correlation_id": ev.CorrelationID,
			"detail":         ev.Detail,
		},
	})
	return err
}

type eventEnvelope struct {
	Type       string
	ChannelID  channel.ID
	Sender     message.Sender
	Now        int64
	Payload    map[string]any
	PayloadRaw json.RawMessage
	Visibility message.Visibility
	Audience   message.Audience
}

func writeEvent(ctx context.Context, chain harness.Chain, ev eventEnvelope) (message.ID, error) {
	body := append(json.RawMessage(nil), ev.PayloadRaw...)
	if len(body) == 0 {
		var err error
		body, err = json.Marshal(ev.Payload)
		if err != nil {
			return "", err
		}
	}
	hash := sha256.Sum256(body)
	seq := eventSeq.Add(1)
	visibility := ev.Visibility
	if visibility == "" {
		visibility = message.VisibilityPrivate
	}
	audience := ev.Audience
	if len(audience) == 0 {
		audience = message.Audience{actor.SystemActorID}
	}
	// Round-3 cluster F: visibility=system was removed from the
	// proto-layer0 §2.4 closed set. Ops events (correlation_lost /
	// adapter_timer_terminal_failed) now follow §4.1.3 informative
	// guidance for ops events: visibility=private + audience=[channel
	// system actor] so they stay out of user view caches.
	envID := message.ID(fmt.Sprintf("event:%s:%d:%d:%s", ev.Type, ev.Now, seq, hex.EncodeToString(hash[:])[:16]))
	env := &message.Envelope{
		ID:         envID,
		TS:         ev.Now,
		TSReceived: ev.Now,
		ChannelID:  ev.ChannelID,
		Sender:     ev.Sender,
		Kind:       message.KindEvent,
		Type:       ev.Type,
		Payload:    body,
		Visibility: visibility,
		Audience:   audience,
	}
	res, err := chain.Write(ctx, env)
	if err != nil {
		return "", fmt.Errorf("framework: write event %s: %w", ev.Type, err)
	}
	if res.RejectReason != "" {
		return "", fmt.Errorf("framework: write event %s rejected: %s (%s)", ev.Type, res.RejectReason, res.RejectDetail)
	}
	return envID, nil
}

func buildEmitEvent(cfg respondConfig, decl adapter.Declaration) adapter.EmitEventFunc {
	return func(ctx context.Context, eventType string, payload json.RawMessage, opts adapter.EmitEventOptions) (message.ID, error) {
		if eventType == "" {
			return "", fmt.Errorf("framework: EmitEvent type required")
		}
		if eventType == orphanCallbackType(decl.Name) {
			return "", fmt.Errorf("framework: EmitEvent type %s is framework-owned; use ReportOrphanCallback", eventType)
		}
		if !declAllowsKind(decl, eventType, message.KindEvent) {
			return "", fmt.Errorf("framework: EmitEvent type %s is not event-capable for adapter %s", eventType, decl.Name)
		}
		if len(payload) == 0 {
			payload = json.RawMessage(`{}`)
		}
		now := eventNow(cfg.clock)
		env := eventEnvelope{
			Type:       eventType,
			ChannelID:  cfg.channelID,
			Sender:     message.Sender{Kind: actor.KindTool, ID: cfg.adapterActorID},
			Now:        now,
			PayloadRaw: append(json.RawMessage(nil), payload...),
			Visibility: opts.Visibility,
			Audience:   opts.Audience,
		}
		return writeEvent(ctx, cfg.chain, env)
	}
}

func buildReportOrphanCallback(cfg respondConfig, decl adapter.Declaration) adapter.OrphanCallbackFunc {
	return func(ctx context.Context, report adapter.OrphanCallbackReport) error {
		return emitOrphanCallbackEvents(ctx, orphanCallbackEvent{
			AdapterName:    decl.Name,
			AdapterActorID: decl.ActorID,
			ChannelID:      cfg.channelID,
			Chain:          cfg.chain,
			Clock:          cfg.clock,
			CorrelationID:  report.CorrelationID,
			Detail:         report.Detail,
			Payload:        append([]byte(nil), report.Payload...),
		})
	}
}

func eventNow(clock func() time.Time) int64 {
	if clock == nil {
		return time.Now().UnixMilli()
	}
	return clock().UnixMilli()
}
