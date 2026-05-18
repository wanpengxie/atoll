package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	kadapter "github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/server/catalog"
	"github.com/wanpengxie/ActOS/server/daemonbus"
	"github.com/wanpengxie/ActOS/server/devicebus"
	"github.com/wanpengxie/ActOS/server/identity"
	"github.com/wanpengxie/ActOS/server/placements"
)

// DaemonbusHandlers wires the daemonbus dispatch hooks to gateway-
// level subsystem services. Implements daemonbus.HandlersProvider.
func (a *App) DaemonbusHandlers() daemonbus.Handlers {
	return daemonbus.Handlers{
		OnPush: func(ctx context.Context, conn *daemonbus.Connection, frame viewsync.PushFrame) (viewsync.LastReceivedSeq, error) {
			res, err := a.viewcache.Apply(ctx, frame)
			if err != nil {
				return 0, err
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
			return res.LastReceivedSeq, nil
		},
		OnCreateChannelAck: func(ctx context.Context, conn *daemonbus.Connection, ack placement.CreateChannelAck) error {
			_, err := a.placements.Activate(ctx, ack, placement.ConnectionEpoch(conn.ConnectionEpoch))
			return err
		},
		OnHeartbeat: func(ctx context.Context, conn *daemonbus.Connection, payload daemonbus.HeartbeatPayload) error {
			if err := a.daemonbus.RecordHeartbeat(ctx, conn.DaemonID); err != nil {
				return err
			}
			for _, chID := range payload.Channels {
				_ = a.placements.Heartbeat(ctx, chID, conn.DaemonID)
			}
			return nil
		},
		OnReclaim: func(ctx context.Context, conn *daemonbus.Connection, req placement.ReclaimRequest) error {
			// FIX-T4: req.DaemonID must match the WS-authenticated
			// Connection.DaemonID. A daemon must never speak for
			// another daemon — without this guard a hostile / buggy
			// daemon could reclaim placements it never owned by
			// forging the payload-level daemon_id.
			if req.DaemonID != conn.DaemonID {
				return fmt.Errorf("daemonbus: reclaim daemon_id %q does not match authenticated conn %q", req.DaemonID, conn.DaemonID)
			}
			out := make([]placement.ReclaimDecision, 0, len(req.Channels))
			for _, ch := range req.Channels {
				ok, err := a.placements.AcceptReclaim(ctx, ch.ChannelID, conn.DaemonID, ch, placement.ConnectionEpoch(conn.ConnectionEpoch))
				if err != nil {
					return err
				}
				if ok {
					out = append(out, placement.ReclaimDecision{ChannelID: ch.ChannelID, Accepted: true})
				} else {
					out = append(out, placement.ReclaimDecision{ChannelID: ch.ChannelID, Accepted: false, Reason: "fencing mismatch"})
				}
			}
			ft := kerneldaemonbus.FrameTypeControlReclaimAccepted
			_, err := conn.SendFrame(ctx, ft, map[string]any{
				"daemon_id": string(req.DaemonID),
				"decisions": out,
			})
			return err
		},
		OnDeviceTransitSend: func(ctx context.Context, conn *daemonbus.Connection, frame kerneldaemonbus.Frame) error {
			// T147 §A-S1 — daemon serialises the adapter.SendFrame
			// (kernel/adapter/transit.go) as the daemonbus payload, so
			// we MUST decode the same shape. The earlier wiring decoded
			// devicebus.DeviceFrame which silently drops fields the
			// daemon includes (Direction enum, ExpiresAt) and yields an
			// empty DeviceSessionID when the JSON keys differ — every
			// adapter push got routed to "" and dropped.
			var sf kadapter.SendFrame
			if err := json.Unmarshal(frame.Payload, &sf); err != nil {
				return fmt.Errorf("gateway: decode device_transit.send: %w", err)
			}
			// Translate to the device-WS wire shape (devicebus.DeviceFrame
			// carries the simpler json used between server and the Chrome
			// extension — see server/devicebus/connection.go).
			df := devicebus.DeviceFrame{
				Direction:       string(kadapter.DirectionToDevice),
				DeviceSessionID: string(sf.DeviceSessionID),
				ChannelID:       string(sf.ChannelID),
				RequestID:       sf.RequestID,
				CorrelationID:   sf.CorrelationID,
				Payload:         sf.Payload,
				ExpiresAt:       sf.ExpiresAt,
			}
			return a.devicebus.SendFrameToDevice(ctx, df.DeviceSessionID, df)
		},
	}
}

// Bind implements devicebus.BindNotifier (T147 §A-S2). After
// devicebus.IssueSession allocates the row in state=pending, the HTTP
// handler calls Bind so the daemon mirrors the row and acks; on a
// successful Accepted=true ack the caller advances pending → ready.
// Bind blocks until the ack arrives or the context is cancelled
// (gin's request context).
//
// Errors:
//   - daemon connection is missing → daemonbus.ErrDaemonNotRegistered
//     wrapped with the daemon_id (HTTP 502 to client).
//   - daemon ack reports Accepted=false → error string carries the
//     daemon-supplied Reason / Detail so triage points at the rejecting
//     edge.
//   - send / decode failure → wrapped error.
func (a *App) Bind(ctx context.Context, in devicebus.BindInput) error {
	conn, ok := a.daemonbus.ConnectionFor(in.Session.DaemonID)
	if !ok {
		return fmt.Errorf("gateway: bind_device_session: daemon %s not connected", in.Session.DaemonID)
	}
	body := kerneldaemonbus.BindDeviceSessionBody{
		FrameID:          uuid.NewString(),
		SessionID:        kadapter.DeviceSessionID(in.Session.ID),
		ChannelID:        in.Session.ChannelID,
		DeviceID:         in.Session.DeviceID,
		DeviceType:       in.Session.DeviceType,
		DaemonID:         in.Session.DaemonID,
		TokenFingerprint: in.TokenFingerprint,
		ExpiresAt:        in.Session.ExpiresAt,
		BoundAt:          in.Session.CreatedAt,
	}
	ackFrame, err := conn.SendAndAwait(ctx, kerneldaemonbus.FrameTypeControlBindDeviceSession, body)
	if err != nil {
		return fmt.Errorf("gateway: bind_device_session send: %w", err)
	}
	var ack kerneldaemonbus.BindDeviceSessionAckBody
	if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
		return fmt.Errorf("gateway: bind_device_session ack decode: %w", err)
	}
	if !ack.Accepted {
		if ack.Reason == "" {
			ack.Reason = "rejected"
		}
		if ack.Detail == "" {
			return fmt.Errorf("gateway: bind_device_session rejected: %s", ack.Reason)
		}
		return fmt.Errorf("gateway: bind_device_session rejected: %s — %s", ack.Reason, ack.Detail)
	}
	return nil
}

// Unbind implements devicebus.BindNotifier — sends
// control.unbind_device_session and waits for the ack. Daemon
// disconnection is NOT an error (best-effort tear-down: a daemon that
// reboots reloads its mirror from server-emitted bind frames anyway).
// Any other failure is wrapped so the HTTP caller can surface a 502.
func (a *App) Unbind(ctx context.Context, in devicebus.UnbindInput) error {
	conn, ok := a.daemonbus.ConnectionFor(in.Session.DaemonID)
	if !ok {
		// Daemon offline — server row revoke proceeds; the daemon
		// reloads from server on the next bind cycle.
		return nil
	}
	body := kerneldaemonbus.UnbindDeviceSessionBody{
		FrameID:   uuid.NewString(),
		SessionID: kadapter.DeviceSessionID(in.Session.ID),
		ChannelID: in.Session.ChannelID,
		Reason:    in.Reason,
	}
	ackFrame, err := conn.SendAndAwait(ctx, kerneldaemonbus.FrameTypeControlUnbindDeviceSession, body)
	if err != nil {
		return fmt.Errorf("gateway: unbind_device_session send: %w", err)
	}
	var ack kerneldaemonbus.UnbindDeviceSessionAckBody
	if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
		return fmt.Errorf("gateway: unbind_device_session ack decode: %w", err)
	}
	if !ack.Accepted {
		if ack.Reason == "" {
			ack.Reason = "rejected"
		}
		return fmt.Errorf("gateway: unbind_device_session rejected: %s", ack.Reason)
	}
	return nil
}

// ForwardDeviceFrame implements devicebus.TransitForwarder — converts
// a DeviceFrame received from the Chrome extension into a daemonbus
// device_transit.recv frame whose body is the canonical
// kernel/adapter.SendFrame shape. The daemon decodes the same struct on
// the receiving side (runtime/transit.DeviceTransit.DispatchIncoming),
// so the gateway translates the device-WS-flavoured DeviceFrame into
// the SendFrame here rather than shipping two different schemas across
// the daemonbus mux (T147 §A-S4).
func (a *App) ForwardDeviceFrame(ctx context.Context, frame devicebus.DeviceFrame) error {
	conn, err := a.daemonbus.ConnectionForChannel(ctx, frame.ChannelID)
	if err != nil {
		return err
	}
	sf := kadapter.SendFrame{
		ChannelID:       channel.ID(frame.ChannelID),
		DeviceSessionID: kadapter.DeviceSessionID(frame.DeviceSessionID),
		Direction:       kadapter.DirectionFromDevice,
		RequestID:       frame.RequestID,
		CorrelationID:   frame.CorrelationID,
		Payload:         frame.Payload,
		ExpiresAt:       frame.ExpiresAt,
	}
	_, err = conn.SendFrame(ctx, kerneldaemonbus.FrameTypeDeviceTransitRecv, sf)
	return err
}

func (a *App) correlationForParent(ctx context.Context, channelID, parentID string) string {
	parent, ok, err := a.viewcache.MessageByID(ctx, channel.ID(channelID), parentID)
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
	auth.POST("/workspaces/:wsID/channels/:chID/bind", a.handleBindChannel)
	auth.POST("/channels/:chID/messages", a.handleWriteMessage)

	r.GET("/ws", a.pushhub.HandleWS(a.identity))
	r.GET("/daemonbus", a.daemonbus.HandleWS(a))
	r.GET("/devicebus", a.devicebus.HandleWS(a))
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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
	Type          string          `json:"type"        binding:"required"`
	Payload       json.RawMessage `json:"payload"     binding:"required"`
	ParentID      string          `json:"parent_id"`
	CorrelationID string          `json:"correlation_id"`
	Audience      []string        `json:"audience"`
	Visibility    string          `json:"visibility"`
	// FIX-T8: caller may supply kind explicitly. When omitted the
	// gateway fills the L1 §1.1 default for core types (and leaves
	// it empty for business types — daemon harness will reject on
	// step 5 with `kind_not_allowed`).
	Kind string `json:"kind"`
}

// HumanCaller is the JSON object carried inside control.write_message.
// Daemon recomputes the HMAC + verifies actor_id_in_channel against
// its local actor_registry.
type HumanCaller struct {
	UserID           string `json:"user_id"`
	ActorIDInChannel string `json:"actor_id_in_channel"`
	TS               int64  `json:"ts"`
	Nonce            string `json:"nonce"`
	ServerToken      string `json:"server_token"`
}

// writeMessageBody is the daemonbus.control.write_message payload.
type writeMessageBody struct {
	FrameID         string           `json:"frame_id"`
	ChannelID       string           `json:"channel_id"`
	HumanCaller     HumanCaller      `json:"human_caller"`
	EnvelopePartial message.Envelope `json:"envelope_partial"`
}

func (a *App) handleWriteMessage(c *gin.Context) {
	var req writeMessageReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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
	// system.event) reject caller overrides. kind=request frames
	// must carry exactly one concrete audience (L1 §10.2 step 5).
	kind, ok := resolveKind(req.Type, message.Kind(req.Kind))
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "kind_not_allowed"})
		return
	}
	if kind == message.KindRequest {
		if len(req.Audience) != 1 || req.Audience[0] == "" || req.Audience[0] == "*" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "request_audience_invalid"})
			return
		}
	}

	conn, err := a.daemonbus.ConnectionForChannel(c.Request.Context(), channelID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}

	ts := time.Now().UnixMilli()
	nonce, err := newNonce()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "nonce: " + err.Error()})
		return
	}
	caller := HumanCaller{
		UserID: u.ID, ActorIDInChannel: member.ActorIDInChannel,
		TS: ts, Nonce: nonce,
		ServerToken: a.signHumanCaller(channelID, u.ID, member.ActorIDInChannel, ts, nonce),
	}
	vis := message.VisibilityPublic
	if req.Visibility != "" {
		vis = message.Visibility(req.Visibility)
	}
	envelope := message.Envelope{
		Type:          req.Type,
		ChannelID:     channelID,
		Sender:        message.Sender{Kind: message.SenderHuman, ID: member.ActorIDInChannel},
		Kind:          kind,
		Payload:       req.Payload,
		ParentID:      req.ParentID,
		CorrelationID: req.CorrelationID,
		Audience:      req.Audience,
		Visibility:    vis,
		TS:            ts,
	}
	if kind == message.KindResponse && envelope.ParentID != "" && envelope.CorrelationID == "" {
		envelope.CorrelationID = a.correlationForParent(c.Request.Context(), channelID, envelope.ParentID)
	}
	body := writeMessageBody{
		FrameID:         uuid.NewString(),
		ChannelID:       channelID,
		HumanCaller:     caller,
		EnvelopePartial: envelope,
	}
	ack, err := conn.SendAndAwait(c.Request.Context(), kerneldaemonbus.FrameTypeControlWriteMessage, body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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
				// Surface harness rejects as 409 so `coagent ask` can
				// map exit=3 onto stable harness reasons (L1 §10.3.1).
				c.JSON(http.StatusConflict, resp)
				return
			}
			resp["accepted"] = ackBody.Accepted
		}
	}
	c.JSON(http.StatusOK, resp)
}

// signHumanCaller produces the HMAC token consumed by daemon when it
// receives control.write_message. The daemon-side recomputation uses
// the same secret + same input concatenation order (covers codex #12).
func (a *App) signHumanCaller(channelID, userID, actorID string, ts int64, nonce string) string {
	mac := hmac.New(sha256.New, []byte(a.cfg.HumanCallerSecret))
	mac.Write([]byte(channelID))
	mac.Write([]byte("|"))
	mac.Write([]byte(userID))
	mac.Write([]byte("|"))
	mac.Write([]byte(actorID))
	mac.Write([]byte("|"))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("|"))
	mac.Write([]byte(nonce))
	return hex.EncodeToString(mac.Sum(nil))
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
