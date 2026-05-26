package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
	"github.com/wanpengxie/ActOS/server/catalog"
	"github.com/wanpengxie/ActOS/server/daemonbus"
	"github.com/wanpengxie/ActOS/server/devicebus"
	"github.com/wanpengxie/ActOS/server/httperr"
	"github.com/wanpengxie/ActOS/server/identity"
	"github.com/wanpengxie/ActOS/server/placements"
)

const (
	controlJSONBodyLimit      = 64 << 10
	writeMessageJSONBodyLimit = 1 << 20
	maxWriteMessageAudience   = 256
)

// DaemonbusHandlers wires the daemonbus dispatch hooks to gateway-
// level subsystem services. Implements daemonbus.HandlersProvider.
func (a *App) DaemonbusHandlers() daemonbus.Handlers {
	return daemonbus.Handlers{
		OnPush: func(ctx context.Context, conn *daemonbus.Connection, frame viewsync.PushFrame) (viewsync.AckFrame, error) {
			ok, err := a.placements.ValidatePushFencing(ctx, frame.ChannelID, conn.DaemonID, frame.OwnerEpoch, frame.FencingToken)
			if err != nil {
				return viewsync.AckFrame{}, err
			}
			if !ok {
				return viewsync.AckFrame{
					ChannelID:    frame.ChannelID,
					Accepted:     false,
					RejectReason: viewsync.RejectReasonMuxOwnerEpochStale,
				}, nil
			}
			res, err := a.viewcache.Apply(ctx, frame)
			if err != nil {
				return viewsync.AckFrame{}, err
			}
			if res.Outcome == viewsync.ApplyOutcomeResyncRequired {
				return viewsync.AckFrame{
					ChannelID:       frame.ChannelID,
					LastReceivedSeq: res.LastReceivedSeq,
					Accepted:        false,
					RejectReason:    viewsync.RejectReasonViewsyncResyncBackpressure,
				}, nil
			}
			// FIX-T5 fan-out: if a buffered gap just closed, Apply
			// returns the just-newly-contiguous frames (current +
			// previously buffered) in ApplyResult.DrainedMessages —
			// fan-out every one in seq ASC order so the front-end
			// doesn't miss messages that arrived out of order.
			// Otherwise, plain contiguous → fan-out current frame.
			// Duplicate / gap → no fan-out (gap path also fires a
			// resync request inside viewcache.Apply so the missing
			// closed interval is recovered without waiting for a
			// client reconnect).
			switch {
			case len(res.DrainedMessages) > 0:
				for _, df := range res.DrainedMessages {
					a.pushhub.PushMessage(df.ChannelID, df.Seq, df.Envelope)
				}
			case res.Outcome == viewsync.ApplyOutcomeContiguous:
				a.pushhub.PushMessage(frame.ChannelID, frame.Seq, frame.Envelope)
			}
			return viewsync.AckFrame{
				ChannelID:       frame.ChannelID,
				LastReceivedSeq: res.LastReceivedSeq,
				Accepted:        true,
			}, nil
		},
		OnCreateChannelAck: func(ctx context.Context, conn *daemonbus.Connection, ack placement.CreateChannelAck) error {
			ok, err := a.placements.Activate(ctx, ack, placement.ConnectionEpoch(conn.ConnectionEpoch))
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
			orphanOK, orphanErr := a.placements.OrphanCreating(ctx, ack.ChannelID, ack.CreateRequestID)
			unbindErr := a.sendUnbindChannel(ctx, conn, ack.ChannelID, ack.OwnerEpoch, kerneldaemonbus.UnbindChannelReasonAbandon)
			switch {
			case orphanErr != nil && unbindErr != nil:
				return fmt.Errorf("gateway: create_channel_ack CAS rejected for %s; orphan: %v; unbind: %w", ack.ChannelID, orphanErr, unbindErr)
			case orphanErr != nil:
				return fmt.Errorf("gateway: create_channel_ack CAS rejected for %s; orphan: %w", ack.ChannelID, orphanErr)
			case unbindErr != nil:
				return fmt.Errorf("gateway: create_channel_ack CAS rejected for %s; unbind: %w", ack.ChannelID, unbindErr)
			default:
				return fmt.Errorf("gateway: create_channel_ack CAS rejected for %s; orphaned=%v rollback_sent=true", ack.ChannelID, orphanOK)
			}
		},
		OnRejectChannel: func(ctx context.Context, conn *daemonbus.Connection, rej placement.RejectChannel) error {
			_, err := a.placements.RejectCreate(ctx, rej)
			return err
		},
		OnHeartbeat: func(ctx context.Context, conn *daemonbus.Connection, payload daemonbus.HeartbeatPayload) (placement.HeartbeatAckPayload, error) {
			if payload.DaemonID != "" && payload.DaemonID != conn.DaemonID {
				return placement.HeartbeatAckPayload{}, fmt.Errorf("daemonbus: heartbeat daemon_id %q does not match authenticated conn %q", payload.DaemonID, conn.DaemonID)
			}
			if err := a.daemonbus.RecordHeartbeat(ctx, conn.DaemonID); err != nil {
				return placement.HeartbeatAckPayload{}, err
			}
			a.retryRollbackIntentsAsync(conn, "placement.rollback_retry_failed_during_heartbeat", false)
			diff, err := a.placements.ObserveHeartbeat(ctx, conn.DaemonID, payload.HeldChannels)
			if err != nil {
				return placement.HeartbeatAckPayload{}, err
			}
			return placement.HeartbeatAckPayload{
				HeartbeatSeq:  payload.HeartbeatSeq,
				PlacementDiff: diff,
			}, nil
		},
		OnHeldChannelsReport: func(ctx context.Context, conn *daemonbus.Connection, req placement.HeldChannelsReport) error {
			// FIX-T4: req.DaemonID must match the WS-authenticated
			// Connection.DaemonID. A daemon must never speak for
			// another daemon — without this guard a hostile / buggy
			// daemon could report placements it never owned by
			// forging the payload-level daemon_id.
			if req.DaemonID != conn.DaemonID {
				return fmt.Errorf("daemonbus: held_channels_report daemon_id %q does not match authenticated conn %q", req.DaemonID, conn.DaemonID)
			}
			out := make([]placement.HeldChannelsDecision, 0, len(req.Channels))
			acceptedChannels := make([]channel.ID, 0, len(req.Channels))
			for _, ch := range req.Channels {
				ok, reason, err := a.placements.AcceptHeldChannel(ctx, ch.ChannelID, conn.DaemonID, ch, placement.ConnectionEpoch(conn.ConnectionEpoch))
				if err != nil {
					return err
				}
				if ok {
					out = append(out, placement.HeldChannelsDecision{ChannelID: ch.ChannelID, Accepted: true})
					acceptedChannels = append(acceptedChannels, ch.ChannelID)
				} else {
					if reason == "" {
						reason = "fencing mismatch"
					}
					out = append(out, placement.HeldChannelsDecision{ChannelID: ch.ChannelID, Accepted: false, Reason: reason})
				}
			}
			_, err := conn.SendFrame(ctx, kerneldaemonbus.FrameTypeControlHeldChannelsAck, placement.HeldChannelsAck{
				DaemonID:  req.DaemonID,
				Decisions: out,
			})
			if err != nil {
				return err
			}
			for _, chID := range acceptedChannels {
				a.syncChannelMembersForChannelAsync(string(chID))
			}
			return nil
		},
		OnDeviceTransitRecv: func(ctx context.Context, conn *daemonbus.Connection, frame kerneldaemonbus.Frame) error {
			// device_transit.recv = daemon adapter → server → device
			// (impl-layer2 §5.3.2). The daemon-side adapter pushes a
			// recv frame whose payload should be relayed to the device
			// WS keyed by channel_id + adapter_actor_id. The WS wire is the
			// flat devicebus.DeviceFrame so we unwrap SendFrame.Body
			// (opaque to kernel) into the WS schema before relaying.
			var sf devicetransit.SendFrame
			if err := json.Unmarshal(frame.Payload, &sf); err != nil {
				return fmt.Errorf("gateway: decode device_transit.recv: %w", err)
			}
			var body deviceFrameBody
			if len(sf.Body) > 0 {
				if err := json.Unmarshal(sf.Body, &body); err != nil {
					return fmt.Errorf("gateway: decode device_transit.recv body: %w", err)
				}
			}
			df := devicebus.DeviceFrame{
				Direction:     body.Direction,
				ActorID:       string(sf.AdapterActorID),
				ChannelID:     string(sf.ChannelID),
				RequestID:     body.RequestID,
				ParentID:      body.ParentID,
				CorrelationID: body.CorrelationID,
				Payload:       body.Payload,
				ExpiresAt:     body.ExpiresAt,
			}
			err := a.devicebus.SendFrameToActor(ctx, sf.ChannelID, sf.AdapterActorID, df)
			if errors.Is(err, devicebus.ErrRegistrationNotFound) {
				return a.synthesizeDeviceUnreachableCallback(ctx, sf, body)
			}
			return err
		},
	}
}

// synthesizeDeviceUnreachableCallback emits a failed device_transit.send
// callback back to the originating daemon adapter when no live extension
// is registered for the target (channel_id, actor_id). The adapter
// processes it as a normal failure response envelope, so the LLM caller
// gets a clean `device_not_bound` terminal instead of a torn-down
// daemonbus connection.
func (a *App) synthesizeDeviceUnreachableCallback(
	ctx context.Context,
	sf devicetransit.SendFrame,
	body deviceFrameBody,
) error {
	cbPayload, err := synthesizeDeviceUnreachablePayload(sf, body)
	if err != nil {
		return fmt.Errorf("gateway: marshal synthetic unreachable callback: %w", err)
	}
	df := devicebus.DeviceFrame{
		Direction:     "send",
		ActorID:       string(sf.AdapterActorID),
		ChannelID:     string(sf.ChannelID),
		RequestID:     body.RequestID,
		ParentID:      body.ParentID,
		CorrelationID: body.CorrelationID,
		Payload:       cbPayload,
		ExpiresAt:     body.ExpiresAt,
	}
	return a.ForwardDeviceFrame(ctx, df, sf.AdapterActorID)
}

func synthesizeDeviceUnreachablePayload(sf devicetransit.SendFrame, body deviceFrameBody) (json.RawMessage, error) {
	if req, ok := decodeProxyFacadeRequestEnvelope(body.Payload); ok {
		return synthesizeProxyFacadeUnreachableResponse(sf, req)
	}
	return synthesizeLegacyDeviceUnreachableCallback(body)
}

func decodeProxyFacadeRequestEnvelope(raw json.RawMessage) (message.Envelope, bool) {
	if len(raw) == 0 {
		return message.Envelope{}, false
	}
	var env message.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return message.Envelope{}, false
	}
	if env.ID == "" || env.Kind != message.KindRequest || env.Type == "" || env.Sender.ID == "" {
		return message.Envelope{}, false
	}
	return env, true
}

func synthesizeProxyFacadeUnreachableResponse(sf devicetransit.SendFrame, req message.Envelope) (json.RawMessage, error) {
	const (
		code = "device_not_bound"
		msg  = "no proxy daemon is registered for this channel + adapter actor"
	)
	payloadFields := map[string]any{
		"status":     "failed",
		"reason":     string(message.TerminalReceiverUnavailable),
		"error_code": code,
		"detail":     msg,
	}
	if len(req.Audience) > 0 {
		payloadFields["missing_actor_id"] = req.Audience[0].String()
	}
	payload, err := json.Marshal(payloadFields)
	if err != nil {
		return nil, err
	}
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = req.ID
	}
	visibility := req.Visibility
	if visibility == "" {
		visibility = message.VisibilityPublic
	}
	channelID := req.ChannelID
	if channelID == "" {
		channelID = sf.ChannelID
	}
	resp := message.Envelope{
		ID:            message.ID("response:" + req.ID.String() + ":sys-" + string(message.TerminalReceiverUnavailable)),
		TS:            time.Now().UnixMilli(),
		ChannelID:     channelID,
		Sender:        message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:          message.KindResponse,
		Type:          req.Type,
		Payload:       payload,
		ParentID:      req.ID,
		CorrelationID: correlationID,
		Visibility:    visibility,
		Audience:      message.Audience{req.Sender.ID},
	}
	return json.Marshal(resp)
}

func synthesizeLegacyDeviceUnreachableCallback(body deviceFrameBody) (json.RawMessage, error) {
	code := "device_not_bound"
	msg := "no proxy daemon is registered for this channel + adapter actor"
	return json.Marshal(map[string]any{
		// Callback.correlation_id matches the envelope.id (carried as
		// request_id on the device_transit wire), not the chain
		// correlation_id. Adapter framework uses this to look up the
		// pending request entry; mismatch falls through as orphan.
		"correlation_id": body.RequestID,
		"status":         "error",
		"error": map[string]any{
			"code":    code,
			"message": msg,
		},
	})
}

// OnChannelCreated implements catalog.PlacementHook. Channel bind sends the
// full initial member set in control.create_channel, so create-time catalog
// rows do not need an additional update_members frame.
func (a *App) OnChannelCreated(ctx context.Context, ch catalog.Channel, members []catalog.ChannelMember) error {
	return nil
}

// OnChannelMembersChanged implements catalog.PlacementHook by mirroring
// catalog membership transitions into the owning daemon's actor_registry.
func (a *App) OnChannelMembersChanged(ctx context.Context, channelID string, adds []catalog.ChannelMember, removes []string) error {
	if channelID == "" || (len(adds) == 0 && len(removes) == 0) {
		return nil
	}
	daemonID, ok, err := a.placements.ResolveDaemonForChannel(ctx, channel.ID(channelID))
	if err != nil {
		return err
	}
	if !ok {
		// Unbound channels get the full member set through the later
		// control.create_channel initial_members payload.
		return nil
	}
	conn, ok := a.daemonbus.ConnectionFor(daemonID)
	if !ok {
		pkgLogger.Warn().
			Str("event", "gateway.update_members_rejected").
			Str("request_id", requestctx.RequestID(ctx)).
			Str("channel_id", channelID).
			Str("daemon_id", string(daemonID)).
			Str("reason", "daemon_not_connected").
			Msg("update_members frame not sent")
		return fmt.Errorf("gateway: update_members: daemon %s not connected", daemonID)
	}
	body := kerneldaemonbus.UpdateMembersBody{
		FrameID:   kerneldaemonbus.FrameID(uuid.NewString()),
		ChannelID: channel.ID(channelID),
		Adds:      make([]kerneldaemonbus.UpdateMember, 0, len(adds)),
		Removes:   make([]actor.ActorID, 0, len(removes)),
	}
	for _, m := range adds {
		display := ""
		if usr, err := a.identity.GetUser(ctx, m.UserID); err == nil {
			display = usr.DisplayName
			if display == "" {
				display = usr.Email
			}
		}
		body.Adds = append(body.Adds, kerneldaemonbus.UpdateMember{
			UserID:        kerneldaemonbus.UserID(m.UserID),
			MemberActorID: actor.ActorID(m.MemberActorID),
			Kind:          actor.KindHuman,
			Role:          m.Role,
			DisplayName:   display,
		})
	}
	for _, id := range removes {
		if id != "" {
			body.Removes = append(body.Removes, actor.ActorID(id))
		}
	}
	pkgLogger.Info().
		Str("event", "gateway.update_members_send").
		Str("request_id", requestctx.RequestID(ctx)).
		Str("channel_id", channelID).
		Str("daemon_id", string(daemonID)).
		Str("frame_id", string(body.FrameID)).
		Int("add_count", len(body.Adds)).
		Int("remove_count", len(body.Removes)).
		Msg("sending update_members frame")
	ackFrame, err := conn.SendAndAwait(ctx, kerneldaemonbus.FrameTypeControlUpdateMembers, body)
	if err != nil {
		pkgLogger.Warn().Err(err).
			Str("event", "gateway.update_members_failed").
			Str("request_id", requestctx.RequestID(ctx)).
			Str("channel_id", channelID).
			Str("daemon_id", string(daemonID)).
			Str("frame_id", string(body.FrameID)).
			Msg("update_members send failed")
		return fmt.Errorf("gateway: update_members send: %w", err)
	}
	if ackFrame.FrameKind != kerneldaemonbus.FrameTypeControlUpdateMembersAck {
		pkgLogger.Warn().
			Str("event", "gateway.update_members_rejected").
			Str("request_id", requestctx.RequestID(ctx)).
			Str("channel_id", channelID).
			Str("daemon_id", string(daemonID)).
			Str("frame_id", string(body.FrameID)).
			Str("ack_frame_kind", string(ackFrame.FrameKind)).
			Str("reason", "unexpected_ack_frame").
			Msg("update_members ack rejected")
		return fmt.Errorf("gateway: update_members unexpected ack frame %s", ackFrame.FrameKind)
	}
	var ack kerneldaemonbus.UpdateMembersAckBody
	if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
		pkgLogger.Warn().Err(err).
			Str("event", "gateway.update_members_rejected").
			Str("request_id", requestctx.RequestID(ctx)).
			Str("channel_id", channelID).
			Str("daemon_id", string(daemonID)).
			Str("frame_id", string(body.FrameID)).
			Str("reason", "ack_decode_failed").
			Msg("update_members ack decode failed")
		return fmt.Errorf("gateway: update_members ack decode: %w", err)
	}
	if !ack.Accepted {
		if ack.RejectReason == "" {
			ack.RejectReason = "rejected"
		}
		pkgLogger.Warn().
			Str("event", "gateway.update_members_rejected").
			Str("request_id", requestctx.RequestID(ctx)).
			Str("channel_id", channelID).
			Str("daemon_id", string(daemonID)).
			Str("frame_id", string(body.FrameID)).
			Str("reason", ack.RejectReason).
			Str("detail", ack.RejectDetail).
			Msg("update_members ack rejected")
		return fmt.Errorf("gateway: update_members rejected: %s %s", ack.RejectReason, ack.RejectDetail)
	}
	pkgLogger.Info().
		Str("event", "gateway.update_members_accepted").
		Str("request_id", requestctx.RequestID(ctx)).
		Str("channel_id", channelID).
		Str("daemon_id", string(daemonID)).
		Str("frame_id", string(body.FrameID)).
		Msg("update_members ack accepted")
	return nil
}

// NotifyProxyDaemonReady implements devicebus.ProxyDaemonNotifier. It reuses
// control.update_members to make proxy-hosted tool actors visible to the cloud
// daemon actor_registry before cmd/daemon installs the matching facade module.
func (a *App) NotifyProxyDaemonReady(ctx context.Context, d devicebus.Daemon, ready devicebus.DaemonReadyInput) error {
	if d.ChannelID == "" || len(ready.Actors) == 0 {
		return nil
	}
	body := kerneldaemonbus.UpdateMembersBody{
		FrameID:   kerneldaemonbus.FrameID(uuid.NewString()),
		ChannelID: d.ChannelID,
		Adds:      make([]kerneldaemonbus.UpdateMember, 0, len(ready.Actors)),
	}
	for _, readyActor := range ready.Actors {
		if readyActor.ActorID == "" {
			continue
		}
		body.Adds = append(body.Adds, kerneldaemonbus.UpdateMember{
			UserID:        kerneldaemonbus.UserID(d.OwnerID),
			MemberActorID: readyActor.ActorID,
			Kind:          actor.KindTool,
			Binding:       actor.BindingRuntimeInboundViaRelay,
			Role:          "proxy_daemon",
			DisplayName:   string(readyActor.ActorID),
			CapabilitySet: readyActor.CapabilitySet,
			ProxyHost: &kerneldaemonbus.ProxyHost{
				DaemonID:   d.ID,
				DaemonName: d.Name,
			},
		})
	}
	return a.sendProxyUpdateMembers(ctx, body)
}

func (a *App) NotifyProxyDaemonOffline(ctx context.Context, d devicebus.Daemon, actors []actor.ActorID) error {
	if d.ChannelID == "" || len(actors) == 0 {
		return nil
	}
	body := kerneldaemonbus.UpdateMembersBody{
		FrameID:   kerneldaemonbus.FrameID(uuid.NewString()),
		ChannelID: d.ChannelID,
		Removes:   append([]actor.ActorID(nil), actors...),
	}
	return a.sendProxyUpdateMembers(ctx, body)
}

func (a *App) sendProxyUpdateMembers(ctx context.Context, body kerneldaemonbus.UpdateMembersBody) error {
	conn, err := a.daemonbus.ConnectionForChannel(ctx, string(body.ChannelID))
	if err != nil {
		return err
	}
	ackFrame, err := conn.SendAndAwait(ctx, kerneldaemonbus.FrameTypeControlUpdateMembers, body)
	if err != nil {
		return err
	}
	if ackFrame.FrameKind != kerneldaemonbus.FrameTypeControlUpdateMembersAck {
		return fmt.Errorf("gateway: proxy update_members unexpected ack %s", ackFrame.FrameKind)
	}
	var ack kerneldaemonbus.UpdateMembersAckBody
	if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
		return fmt.Errorf("gateway: decode proxy update_members ack: %w", err)
	}
	if !ack.Accepted {
		return fmt.Errorf("gateway: proxy update_members rejected: %s %s", ack.RejectReason, ack.RejectDetail)
	}
	return nil
}

func (a *App) reclaimPlacement(ctx context.Context, p placement.Placement) error {
	conn, ok, err := a.reclaimCandidate(ctx, p.DaemonID, nil)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("gateway: reclaim no connected daemon candidate for %s", p.ChannelID)
	}
	reserved, req, ok, err := a.placements.ReserveReclaim(
		ctx,
		p.ChannelID,
		conn.DaemonID,
		placement.ConnectionEpoch(conn.ConnectionEpoch),
	)
	if err != nil || !ok {
		return err
	}
	reclaimCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ackFrame, err := conn.SendAndAwait(reclaimCtx, kerneldaemonbus.FrameTypeControlDaemonReclaim, req)
	if err != nil {
		if rollbackErr := a.reclaimRollback(ctx, conn, reserved, "daemon_reclaim_send_failed"); rollbackErr != nil {
			return fmt.Errorf("gateway: daemon_reclaim send: %w; rollback: %v", err, rollbackErr)
		}
		return fmt.Errorf("gateway: daemon_reclaim send: %w", err)
	}
	switch ackFrame.FrameKind {
	case kerneldaemonbus.FrameTypeControlReclaimAccepted:
		var ack placement.ReclaimAccepted
		if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
			if rollbackErr := a.reclaimRollback(ctx, conn, reserved, "reclaim_accepted_decode_failed"); rollbackErr != nil {
				return fmt.Errorf("gateway: reclaim_accepted decode: %w; rollback: %v", err, rollbackErr)
			}
			return fmt.Errorf("gateway: reclaim_accepted decode: %w", err)
		}
		if ack.ChannelID != reserved.ChannelID ||
			ack.CreateRequestID != reserved.CreateRequestID ||
			ack.NewOwnerEpoch != reserved.OwnerEpoch {
			if rollbackErr := a.reclaimRollback(ctx, conn, reserved, "reclaim_accepted_mismatch"); rollbackErr != nil {
				return fmt.Errorf("gateway: reclaim_accepted mismatch for %s; rollback: %v", reserved.ChannelID, rollbackErr)
			}
			return fmt.Errorf("gateway: reclaim_accepted mismatch for %s", reserved.ChannelID)
		}
		ok, err := a.placements.ActivateReclaim(
			ctx,
			ack,
			conn.DaemonID,
			placement.ConnectionEpoch(conn.ConnectionEpoch),
		)
		if err != nil {
			return err
		}
		if ok {
			_ = a.deleteRollbackIntent(ctx, placementRollbackIntent{
				ChannelID:       reserved.ChannelID,
				CreateRequestID: reserved.CreateRequestID,
				OwnerEpoch:      reserved.OwnerEpoch,
			})
			a.daemonbus.MarkReclaimAssigned(conn.DaemonID)
			return nil
		}
		if rollbackErr := a.reclaimRollback(ctx, conn, reserved, "reclaim_accepted_cas_rejected"); rollbackErr != nil {
			return fmt.Errorf("gateway: reclaim_accepted CAS rejected for %s; rollback: %v", reserved.ChannelID, rollbackErr)
		}
		return fmt.Errorf("gateway: reclaim_accepted CAS rejected for %s", reserved.ChannelID)
	case kerneldaemonbus.FrameTypeControlReclaimRejected:
		var rej placement.ReclaimRejected
		if err := json.Unmarshal(ackFrame.Payload, &rej); err != nil {
			if rollbackErr := a.reclaimRollback(ctx, conn, reserved, "reclaim_rejected_decode_failed"); rollbackErr != nil {
				return fmt.Errorf("gateway: reclaim_rejected decode: %w; rollback: %v", err, rollbackErr)
			}
			return fmt.Errorf("gateway: reclaim_rejected decode: %w", err)
		}
		if rej.ChannelID != reserved.ChannelID || rej.CreateRequestID != reserved.CreateRequestID {
			if rollbackErr := a.reclaimRollback(ctx, conn, reserved, "reclaim_rejected_mismatch"); rollbackErr != nil {
				return fmt.Errorf("gateway: reclaim_rejected mismatch for %s; rollback: %v", reserved.ChannelID, rollbackErr)
			}
			return fmt.Errorf("gateway: reclaim_rejected mismatch for %s", reserved.ChannelID)
		}
		if rollbackErr := a.reclaimRollback(ctx, conn, reserved, "reclaim_rejected_"+string(rej.Reason)); rollbackErr != nil {
			return fmt.Errorf("gateway: reclaim_rejected %q for %s; rollback: %v", rej.Reason, reserved.ChannelID, rollbackErr)
		}
		if rej.Reason != placement.ReclaimRejectStoreMissing &&
			rej.Reason != placement.ReclaimRejectCompletenessCheckFailed &&
			rej.Reason != placement.ReclaimRejectOwnerEpochInvalid &&
			rej.Reason != placement.ReclaimRejectInternalError {
			return fmt.Errorf("gateway: reclaim_rejected unknown reason %q for %s", rej.Reason, reserved.ChannelID)
		}
		return nil
	default:
		if rollbackErr := a.reclaimRollback(ctx, conn, reserved, "reclaim_unexpected_ack_frame"); rollbackErr != nil {
			return fmt.Errorf("gateway: reclaim unexpected ack frame %s; rollback: %v", ackFrame.FrameKind, rollbackErr)
		}
		return fmt.Errorf("gateway: reclaim unexpected ack frame %s", ackFrame.FrameKind)
	}
}

func (a *App) reclaimCandidate(ctx context.Context, previousOwner placement.DaemonID, excluded map[placement.DaemonID]bool) (*daemonbus.Connection, bool, error) {
	metrics, err := a.daemonbus.ConnectedConnectionMetrics(ctx)
	if err != nil {
		return nil, false, err
	}
	filter := func(allowPrevious bool) []daemonbus.ConnectionMetrics {
		out := make([]daemonbus.ConnectionMetrics, 0, len(metrics))
		for _, m := range metrics {
			if m.Connection == nil || m.Connection.IsClosed() {
				continue
			}
			if excluded != nil && excluded[m.DaemonID] {
				continue
			}
			if !allowPrevious && m.DaemonID == previousOwner {
				continue
			}
			if m.Capacity > 0 && m.ActiveChannels >= m.Capacity {
				continue
			}
			out = append(out, m)
		}
		return out
	}
	candidates := filter(false)
	if len(candidates) == 0 {
		candidates = filter(true)
	}
	if len(candidates) == 0 {
		return nil, false, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].ActiveChannels != candidates[j].ActiveChannels {
			return candidates[i].ActiveChannels < candidates[j].ActiveChannels
		}
		if candidates[i].LastReclaimAt != candidates[j].LastReclaimAt {
			return candidates[i].LastReclaimAt < candidates[j].LastReclaimAt
		}
		return candidates[i].DaemonID < candidates[j].DaemonID
	})
	chosen := candidates[0]
	return chosen.Connection, true, nil
}

func (a *App) sendUnbindChannel(
	ctx context.Context,
	conn *daemonbus.Connection,
	channelID channel.ID,
	ownerEpoch placement.OwnerEpoch,
	reason kerneldaemonbus.UnbindChannelReason,
) error {
	if conn == nil || channelID == "" || ownerEpoch <= 0 {
		return nil
	}
	_, err := conn.SendFrame(ctx, kerneldaemonbus.FrameTypeControlUnbindChannel, kerneldaemonbus.UnbindChannelBody{
		ChannelID:  channelID,
		OwnerEpoch: ownerEpoch,
		Reason:     reason,
	})
	return err
}

// NotifyDeviceLifecycle implements devicebus.LifecycleNotifier. It
// pushes a `device_transit.lifecycle` daemonbus frame to the daemon
// owning the channel so the adapter framework can project the
// device-state machine.
//
// At-least-once delivery is best-effort: if the owning daemon is not
// connected the frame is dropped silently (the daemon's reclaim path
// will resync the connection state on reconnect — this signal is an
// optimization, not closure-critical).
func (a *App) NotifyDeviceLifecycle(
	ctx context.Context,
	channelID channel.ID,
	actorID actor.ActorID,
	event devicetransit.LifecycleEvent,
	deviceID string,
	detail string,
) {
	if channelID == "" || actorID == "" {
		return
	}
	conn, err := a.daemonbus.ConnectionForChannel(ctx, string(channelID))
	if err != nil {
		// Daemon offline / channel not bound — drop. The composition
		// root will re-emit a "connected" lifecycle on the next
		// channel boot via the actor-token routing record.
		pkgLogger.Debug().
			Str("event", "gateway.device_lifecycle_dropped").
			Str("channel_id", string(channelID)).
			Str("actor_id", string(actorID)).
			Str("lifecycle_event", string(event)).
			Err(err).
			Msg("daemon connection unavailable; device lifecycle dropped")
		return
	}
	payload := devicetransit.LifecycleFrame{
		AdapterActorID: actorID,
		ChannelID:      channelID,
		Event:          event,
		DeviceID:       deviceID,
		Ts:             time.Now().UnixMilli(),
		Detail:         detail,
	}
	if _, err := conn.SendFrame(ctx, kerneldaemonbus.FrameTypeDeviceTransitLifecycle, payload); err != nil {
		pkgLogger.Warn().
			Str("event", "gateway.device_lifecycle_send_failed").
			Str("channel_id", string(channelID)).
			Str("actor_id", string(actorID)).
			Str("lifecycle_event", string(event)).
			Err(err).
			Msg("device lifecycle frame send failed")
	}
}

// ForwardDeviceFrame implements devicebus.TransitForwarder by wrapping
// the flat /devicebus wire frame into a devicetransit.SendFrame and
// pushing it through the daemonbus mux.
//
// Direction: device → server → daemon adapter. Per impl-layer2 §5.3.1,
// this inbound direction rides on `device_transit.send`. The daemon
// framework decodes SendFrame.Body as deviceFrameBody (mirror of
// adapters/device/framework.DeviceTransitBody serial shape).
func (a *App) ForwardDeviceFrame(ctx context.Context, frame devicebus.DeviceFrame, adapterActorID actor.ActorID) error {
	body, err := json.Marshal(deviceFrameBody{
		Direction:     frame.Direction,
		RequestID:     frame.RequestID,
		ParentID:      frame.ParentID,
		CorrelationID: frame.CorrelationID,
		Payload:       frame.Payload,
		ExpiresAt:     frame.ExpiresAt,
	})
	if err != nil {
		return fmt.Errorf("gateway: marshal device_transit.send body: %w", err)
	}
	sf := devicetransit.SendFrame{
		ChannelID:      channel.ID(frame.ChannelID),
		AdapterActorID: adapterActorID,
		Body:           body,
	}
	conn, err := a.daemonbus.ConnectionForChannel(ctx, frame.ChannelID)
	if err != nil {
		return err
	}
	_, err = conn.SendFrame(ctx, kerneldaemonbus.FrameTypeDeviceTransitSend, sf)
	return err
}

// deviceFrameBody mirrors adapters/device/framework.DeviceTransitBody on
// the wire so server can wrap/unwrap the opaque devicetransit.SendFrame.Body
// without importing adapters/** (architecturally forbidden by go-arch-lint).
type deviceFrameBody struct {
	Direction     string          `json:"direction"`
	RequestID     string          `json:"request_id,omitempty"`
	ParentID      string          `json:"parent_id,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	ExpiresAt     int64           `json:"expires_at,omitempty"`
}

func (a *App) correlationForParent(ctx context.Context, channelID string, parentID message.ID) message.ID {
	parent, ok, err := a.viewcache.MessageByID(ctx, channel.ID(channelID), parentID.String())
	if err != nil || !ok {
		return parentID
	}
	if parent.Envelope.CorrelationID != "" {
		return parent.Envelope.CorrelationID
	}
	return parent.Envelope.ID
}

// buildEngine wires the gin router. Routes are mounted by each
// subsystem; this is the only place that knows the URL layout so
// the surface stays auditable.
func buildEngine(a *App) *gin.Engine {
	r := gin.New()
	r.Use(requestIDMiddleware())
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	api := r.Group("/api")

	a.identity.RegisterPublicRoutes(api)

	auth := api.Group("/")
	auth.Use(a.identity.AuthMiddleware())
	a.identity.RegisterAuthRoutes(auth)
	a.catalog.RegisterRoutes(auth, a.identity)
	a.placements.RegisterRoutes(auth)
	a.viewcache.RegisterRoutes(auth)
	a.devicebus.RegisterRoutes(auth)

	// Channel-level orchestration: provisioning + message write.
	auth.POST("/workspaces/:wsID/channels/:chID/bind", httperr.MaxBodyBytes(controlJSONBodyLimit), a.handleBindChannel)
	auth.POST("/channels/:chID/messages", httperr.MaxBodyBytes(writeMessageJSONBodyLimit), a.handleWriteMessage)

	r.GET("/ws", a.pushhub.HandleWS(a.identity))
	r.GET("/daemonbus", a.daemonbus.HandleWS(a))
	r.GET("/devicebus/v2/connect", a.devicebus.HandleWSV2(a))

	// SPA static serving: when UIDistDir is configured, serve the
	// pnpm-build artifact at "/" plus a NoRoute fallback to index.html
	// so client-side routes (e.g. /channel/123) still hand off to the
	// SPA. API/WS prefixes are excluded so missing endpoints continue
	// to return a JSON 404 (audit-friendly).
	if dir := a.cfg.UIDistDir; dir != "" {
		r.Static("/assets", filepath.Join(dir, "assets"))
		// /downloads/* must revalidate every fetch so cloudflare / browser
		// caches can't pin a stale extension zip — see commit message for
		// the cloudflare-cached-zip-broke-manifest.key incident.
		downloadsDir := filepath.Join(dir, "downloads")
		r.GET("/downloads/*filepath", func(c *gin.Context) {
			path, ok := safeDownloadPath(downloadsDir, c.Param("filepath"))
			if !ok {
				c.AbortWithStatus(http.StatusNotFound)
				return
			}
			c.Header("Cache-Control", "no-cache, must-revalidate")
			c.File(path)
		})
		r.StaticFile("/favicon.svg", filepath.Join(dir, "favicon.svg"))
		indexPath := filepath.Join(dir, "index.html")
		r.NoRoute(func(c *gin.Context) {
			p := c.Request.URL.Path
			if strings.HasPrefix(p, "/api/") || p == "/ws" || p == "/daemonbus" || strings.HasPrefix(p, "/devicebus") {
				c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
				return
			}
			// Missing static download must not fall back to index.html;
			// returning the SPA shell would mask 404s and break clients
			// that expect a real binary at /downloads/<file>.
			if strings.HasPrefix(p, "/downloads/") {
				c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
				return
			}
			if c.Request.Method != http.MethodGet {
				c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
				return
			}
			c.File(indexPath)
		})
	}
	return r
}

// ----------------------------------------------------------------------
// Channel bind: catalog channel + placements.Reserve + send
// control.create_channel to daemon
// ----------------------------------------------------------------------

type bindChannelReq struct {
	DaemonID string `json:"daemon_id" binding:"required"`
}

func (a *App) handleBindChannel(c *gin.Context) {
	var req bindChannelReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	u := identity.UserFrom(c)

	// Verify caller is a channel member.
	ch, _, err := a.catalog.GetChannel(c.Request.Context(), c.Param("chID"), u.ID)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}

	members, err := a.catalog.ListChannelMembers(c.Request.Context(), ch.ID)
	if err != nil {
		httperr.Internal(c, "gateway.bind_channel.list_members", err)
		return
	}
	displayFn := func(uid string) string {
		usr, _ := a.identity.GetUser(c.Request.Context(), uid)
		if usr.DisplayName != "" {
			return usr.DisplayName
		}
		return usr.Email
	}
	initial := catalog.InitialMembersFor(members, displayFn)

	daemonID := placement.DaemonID(req.DaemonID)
	conn, ok := a.daemonbus.ConnectionFor(daemonID)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "daemon not connected"})
		return
	}

	// M1.6-T5 phase-2 — thread the channel-template key (catalog.Channel.Type)
	// into the placement reserve so daemon's bootstrap saga can resolve
	// the matching ChannelTemplate (actor seeds / workdir subdirs / domain
	// prompt). Legacy group channels carry Type="group", which the daemon
	// treats as the no-template default.
	_, createReq, err := a.placements.ReserveWith(
		c.Request.Context(),
		channel.ID(ch.ID),
		daemonID,
		placement.ConnectionEpoch(conn.ConnectionEpoch),
		initial,
		placements.ReserveOptions{ChannelType: ch.Type},
	)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	// Fire-and-forget the create_channel frame — daemon's ACK arrives
	// via the dispatch loop's OnCreateChannelAck hook which advances
	// placement to active.
	if _, err := conn.SendFrame(c.Request.Context(), kerneldaemonbus.FrameTypeControlCreateChannel, createReq); err != nil {
		httperr.Internal(c, "gateway.bind_channel.send_create", err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"channel_id":        ch.ID,
		"daemon_id":         string(daemonID),
		"create_request_id": string(createReq.CreateRequestID),
	})
}

// ----------------------------------------------------------------------
// Write message: human-caller token → daemonbus control.write_message
// ----------------------------------------------------------------------

type writeMessageReq struct {
	// R4-3: caller MUST supply envelope.id (L0 §1.1 sender-provided).
	// This drives L1 §2.3 harness Step 3 dedupe so retries with the
	// same id + same content collapse to one append. The gateway no
	// longer fabricates an id on the caller's behalf — proto-layer3
	// §1.8.1 / §1.8.3.
	ID            string          `json:"id"          binding:"required"`
	Type          string          `json:"type"        binding:"required"`
	Payload       json.RawMessage `json:"payload"     binding:"required"`
	ParentID      string          `json:"parent_id"`
	CorrelationID string          `json:"correlation_id"`
	Audience      []string        `json:"audience"`
	Visibility    string          `json:"visibility"`
	// FIX-T8: caller may supply kind explicitly. When omitted the
	// gateway fills the L1 §1.1 default for core types (and leaves
	// it empty for business types — daemon harness will reject on
	// step 5 with `harness_kind_not_allowed`).
	Kind string `json:"kind"`
	// TS is the sender-provided envelope timestamp (ms epoch). Part of
	// the L1 §2.3 canonical_hash domain (kernel/message/canonical.go
	// envelopeHashInput) — callers MUST resend the same TS when
	// retrying an idempotent write; otherwise the second POST trips
	// harness_id_duplicate_conflict at Step 3. Server stamps
	// time.Now() only when TS is omitted (0).
	TS int64 `json:"ts"`
}

func (a *App) handleWriteMessage(c *gin.Context) {
	// impl-layer3 §1.8.1 normative: write-message endpoint MUST
	// fail-closed reject unknown top-level fields (HTTP 400 with the
	// `harness_envelope_unknown_field` reason — the same reason the
	// daemon harness Step 2 would surface if the daemon decoded a
	// fattened envelope). The default gin ShouldBindJSON silently
	// accepts unknown fields, so we decode manually with
	// DisallowUnknownFields and then run binding's required-field
	// validation explicitly.
	var req writeMessageReq
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		if isJSONUnknownFieldError(err) {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":         string(message.HarnessEnvelopeUnknownField),
				"reject_reason": string(message.HarnessEnvelopeUnknownField),
				"reject_detail": err.Error(),
			})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validateWriteMessageRequired(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(req.Audience) > maxWriteMessageAudience {
		c.JSON(http.StatusBadRequest, gin.H{"error": "audience_too_large"})
		return
	}
	u := identity.UserFrom(c)
	channelID := c.Param("chID")

	member, err := a.catalog.GetChannelMember(c.Request.Context(), channelID, u.ID)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}

	// FIX-T8: server-side early kind normalize + request audience
	// validation. Caller may supply `kind` explicitly; otherwise we
	// apply the L1 §1.1 default. kind-locked core types (e.g.
	// core.system_event) reject caller overrides. kind=request frames
	// must carry exactly one concrete audience (L1 §10.2 step 5).
	kind, ok := resolveKind(req.Type, message.Kind(req.Kind))
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": string(message.HarnessKindNotAllowedForType)})
		return
	}
	if kind == message.KindRequest {
		if len(req.Audience) != 1 || req.Audience[0] == "" || req.Audience[0] == "*" {
			c.JSON(http.StatusBadRequest, gin.H{"error": string(message.HarnessRequestAudienceInvalid)})
			return
		}
	}

	conn, err := a.daemonbus.ConnectionForChannel(c.Request.Context(), channelID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}

	ts := req.TS
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}
	nonce, err := newNonce()
	if err != nil {
		httperr.Internal(c, "gateway.write_message.nonce", err)
		return
	}
	caller := kerneldaemonbus.HumanCaller{
		UserID:        kerneldaemonbus.UserID(u.ID),
		MemberActorID: actor.ActorID(member.MemberActorID),
		TS:            ts,
		Nonce:         nonce,
		ServerToken:   a.signHumanCaller(channelID, u.ID, member.MemberActorID, ts, nonce),
	}
	vis := message.VisibilityPublic
	if req.Visibility != "" {
		vis = message.Visibility(req.Visibility)
	}
	audience := make(message.Audience, 0, len(req.Audience))
	for _, id := range req.Audience {
		audience = append(audience, actor.ActorID(id))
	}
	envelope := message.Envelope{
		// R4-3: id is caller-supplied (L0 §1.1 sender-provided);
		// drives L1 §2.3 harness Step 3 dedupe. The gateway does
		// not fabricate id on the caller's behalf.
		ID:            message.ID(req.ID),
		Type:          req.Type,
		ChannelID:     channel.ID(channelID),
		Sender:        message.Sender{Kind: actor.KindHuman, ID: actor.ActorID(member.MemberActorID)},
		Kind:          kind,
		Payload:       req.Payload,
		ParentID:      message.ID(req.ParentID),
		CorrelationID: message.ID(req.CorrelationID),
		Audience:      audience,
		Visibility:    vis,
		TS:            ts,
	}
	if kind == message.KindResponse && envelope.ParentID != "" && envelope.CorrelationID == "" {
		envelope.CorrelationID = a.correlationForParent(c.Request.Context(), channelID, envelope.ParentID)
	}
	body := kerneldaemonbus.WriteMessageBody{
		FrameID:         kerneldaemonbus.FrameID(uuid.NewString()),
		ChannelID:       channel.ID(channelID),
		HumanCaller:     caller,
		EnvelopePartial: envelope,
	}
	ack, err := conn.SendAndAwait(c.Request.Context(), kerneldaemonbus.FrameTypeControlWriteMessage, body)
	if err != nil {
		if respondDaemonbusAwaitError(c, err) {
			return
		}
		httperr.Internal(c, "gateway.write_message.send", err)
		return
	}

	// M1.6-T5 phase-4: deserialize the daemon-side ack body so callers
	// (coagent ask/emit/answer + downstream agents) receive the actual
	// envelope.id / seq / dedupe / reject_reason rather than just the
	// frame transport metadata. Best-effort: if the ack payload is the
	// legacy "no body" shape, fall through with the historical
	// {frame_id, daemon_ack_id} response so existing browser-side flows
	// keep working unchanged.
	resp := gin.H{
		"frame_id":      body.FrameID,
		"daemon_ack_id": ack.FrameID,
	}
	if len(ack.Payload) > 0 {
		var ackBody kerneldaemonbus.WriteMessageAckBody
		if err := json.Unmarshal(ack.Payload, &ackBody); err == nil {
			if ackBody.MessageID != "" {
				resp["message_id"] = ackBody.MessageID
				// L1 §1.5 contract:
				//   kind=request  → correlation_id = envelope.id
				//   kind=event    → correlation_id = envelope.id
				//   kind=response → correlation_id = parent's id (not
				//                   accessible here without a store
				//                   lookup; omit and let downstream
				//                   resolve via parent_id).
				if kind == message.KindRequest || kind == message.KindEvent {
					resp["correlation_id"] = ackBody.MessageID
				} else if envelope.CorrelationID != "" {
					resp["correlation_id"] = envelope.CorrelationID
				}
			}
			if ackBody.Seq > 0 {
				resp["seq"] = ackBody.Seq
			}
			if ackBody.Deduped {
				resp["deduped"] = true
			}
			if ackBody.RejectReason != "" {
				resp["reject_reason"] = ackBody.RejectReason
				resp["reject_detail"] = ackBody.RejectDetail
				c.JSON(writeMessageRejectStatus(ackBody.RejectReason), resp)
				return
			}
			resp["accepted"] = ackBody.Accepted
		}
	}
	c.JSON(http.StatusOK, resp)
}

func writeMessageRejectStatus(reason string) int {
	for _, r := range message.AllHarnessRejectReasons {
		if reason == r.String() {
			if status := r.HTTPStatus(); status != 0 {
				return status
			}
			return http.StatusConflict
		}
	}
	return http.StatusConflict
}

func respondDaemonbusAwaitError(c *gin.Context, err error) bool {
	switch {
	case errors.Is(err, daemonbus.ErrPendingAwaitLimitExceeded):
		c.JSON(http.StatusTooManyRequests, gin.H{"error": err.Error()})
		return true
	case errors.Is(err, daemonbus.ErrSendAndAwaitTimeout):
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": err.Error()})
		return true
	case errors.Is(err, daemonbus.ErrConnectionClosed):
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return true
	default:
		return false
	}
}

func safeDownloadPath(root, param string) (string, bool) {
	rel := strings.TrimPrefix(param, "/")
	if rel == "" || filepath.IsAbs(rel) {
		return "", false
	}
	clean := filepath.Clean(rel)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", false
	}
	rootReal, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", false
	}
	candidate := filepath.Join(rootReal, clean)
	candidateReal, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", false
	}
	info, err := os.Stat(candidateReal)
	if err != nil || info.IsDir() {
		return "", false
	}
	relToRoot, err := filepath.Rel(rootReal, candidateReal)
	if err != nil || relToRoot == ".." || strings.HasPrefix(relToRoot, ".."+string(os.PathSeparator)) || filepath.IsAbs(relToRoot) {
		return "", false
	}
	return candidateReal, true
}

// isJSONUnknownFieldError reports whether err is the error
// encoding/json's Decoder returns when DisallowUnknownFields fires.
// The stdlib does not export a typed error, so we match the canonical
// prefix string the decoder emits ("json: unknown field"). All other
// JSON shape errors (missing field, type mismatch, syntax) fall through
// to the generic 400 branch.
func isJSONUnknownFieldError(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), "json: unknown field")
}

// validateWriteMessageRequired enforces the L3 §1.8.1 required-field
// set without going through gin's validator. R5-16 swapped
// ShouldBindJSON for a json.Decoder with DisallowUnknownFields, so the
// `binding:"required"` struct tags no longer fire automatically.
func validateWriteMessageRequired(req *writeMessageReq) error {
	if req.ID == "" {
		return fmt.Errorf("writeMessageReq.ID is required")
	}
	if req.Type == "" {
		return fmt.Errorf("writeMessageReq.Type is required")
	}
	if len(req.Payload) == 0 {
		return fmt.Errorf("writeMessageReq.Payload is required")
	}
	return nil
}

// signHumanCaller produces the HMAC token consumed by daemon when it
// receives control.write_message. The daemon-side recomputation uses the
// same structured input encoding: each string field is length-prefixed and
// ts is fixed-width big-endian, so embedded delimiters cannot cause field
// confusion.
func (a *App) signHumanCaller(channelID, userID, actorID string, ts int64, nonce string) string {
	mac := hmac.New(sha256.New, []byte(a.cfg.HumanCallerSecret))
	mac.Write([]byte("coagent-human-caller-v2"))
	writeHumanCallerField(mac, channelID)
	writeHumanCallerField(mac, userID)
	writeHumanCallerField(mac, actorID)
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(ts))
	mac.Write(tsBuf[:])
	writeHumanCallerField(mac, nonce)
	return hex.EncodeToString(mac.Sum(nil))
}

func writeHumanCallerField(w io.Writer, value string) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(value)))
	_, _ = w.Write(lenBuf[:])
	_, _ = w.Write([]byte(value))
}

// nonceReader is the entropy source for newNonce. Defaults to
// crypto/rand.Reader; tests swap in a failing reader to assert the
// 500 path of FIX-T8 phase-4.
var nonceReader io.Reader = rand.Reader

// newNonce returns a fresh 16-byte hex nonce, propagating any error
// from the entropy source. The caller MUST surface the error to the
// HTTP client (500) — FIX-T8: silently signing with all-zero bytes
// would defeat the HumanCaller replay-protection token.
func newNonce() (string, error) {
	buf := make([]byte, 16)
	if _, err := io.ReadFull(nonceReader, buf); err != nil {
		return "", fmt.Errorf("gateway: read nonce entropy: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
