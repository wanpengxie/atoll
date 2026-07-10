package app

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// ---------------------------------------------------------------------------
// WebSocket: the subject's single gateway link (/ws)
// ---------------------------------------------------------------------------
//
// One gateway ws is the ONLY link between a channel and a subject (单链路律,
// 红线 10): it carries the read axis DOWN (tail: subscribe + since_seq) and the
// WRITE FRAMES up (message / resolve / cancel — payload carries only pointers and
// content fields, NEVER an envelope; the door welds identity). Frame decode /
// session / ACL stay in the app; each write frame drives a HumanHandle verb
// (Home.Human → Submit/Resolve/Cancel), the door welds the "user:<id>" pen inside
// the wall. The connection is also this subject's L3 device-presence producer:
// connect feeds online, the LAST disconnect feeds an explicit offline snapshot
// (门喂; presence is advisory and正交 actor 活性, 绝不互训).
//
// A SINGLE writer pump owns the ws write side: tail pushes, and every frame ack /
// error, all funnel through one outbound queue — the upstream-reader goroutine and
// the tail goroutine never write the ws concurrently (gorilla forbids it).

const (
	// wsMaxFrameBytes caps a single inbound ws frame. The write frames (message /
	// resolve / cancel) carry only pointers + content fields — bulk bytes never enter
	// the log (file/blob is the resource axis), so a few hundred KB is ample. An
	// oversized frame is a malformed / hostile client: SetReadLimit fails the read
	// (CloseMessageTooBig) instead of letting one frame allocate unboundedly.
	wsMaxFrameBytes = 512 * 1024
	// wsWriteWait bounds one ws write so a stuck / slow-reading peer cannot pin the
	// single writer pump goroutine forever; a write past this deadline tears the
	// conn down.
	wsWriteWait = 10 * time.Second
	// wsPongWait is how long the server waits for a pong before treating the peer as
	// dead (the read deadline). Each pong renews it, so a half-open TCP conn (peer
	// vanished with no FIN) is reclaimed within one window instead of leaking a
	// goroutine + subscription forever.
	wsPongWait = 60 * time.Second
	// wsPingPeriod is how often the writer pump pings; strictly less than wsPongWait
	// so a healthy peer always answers before its read deadline lapses.
	wsPingPeriod = (wsPongWait * 9) / 10
)

// wsUpgrader gates the WS handshake. CheckOrigin defends against cross-site
// WebSocket hijacking: /ws authenticates by cookie, so without an Origin check a
// malicious page in the user's browser could open a cross-origin WS riding the
// user's session. Same-origin only; an absent Origin (non-browser client, e.g.
// curl/API) is allowed — it still needs a valid session cookie + channel ACL.
var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		u, err := url.Parse(origin)
		return err == nil && u.Host == r.Host
	},
}

// inFrame is the client→server frame union. "type" is the frame discriminator;
// the remaining fields are read per frame kind (message content type is msg_type,
// distinct from the frame "type"). Identity (sender/channel_id) is NEVER carried —
// the door's pen welds it.
type inFrame struct {
	Type       string          `json:"type"`
	Ref        string          `json:"ref"` // optional client correlation id, echoed in ack/error
	ChannelID  string          `json:"channel_id"`
	SinceSeq   int64           `json:"since_seq"`
	ID         string          `json:"id"`
	MsgType    string          `json:"msg_type"`
	Kind       string          `json:"kind"`
	Payload    json.RawMessage `json:"payload"`
	Audience   []string        `json:"audience"`
	Visibility string          `json:"visibility"`
	ParentID   string          `json:"parent_id"`
	ReqID      string          `json:"req_id"`
	Decision   string          `json:"decision"`
	DurationMs int64           `json:"duration_ms"`
	TimerID    string          `json:"timer_id"`
}

func (a *App) handleWS(c *gin.Context) {
	// Auth via cookie.
	token, err := c.Cookie(middleware.SessionCookie)
	if err != nil || token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
		return
	}
	userID, ok := middleware.VerifySession(c.Request.Context(), a.db, token)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid session"})
		return
	}

	ws, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	// Harden the link BEFORE the first read: cap inbound frame size, arm the read
	// deadline, and renew it on every pong. The ping/pong keepalive is driven by the
	// single writer pump below; a peer that stops answering pongs trips this deadline
	// and its reader loop unblocks with an error (half-open reclaim).
	ws.SetReadLimit(wsMaxFrameBytes)
	_ = ws.SetReadDeadline(time.Now().Add(wsPongWait))
	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(wsPongWait))
	})

	// The opening frame MUST be a subscribe (starts the tail; carries channel + cursor).
	var sub inFrame
	if err := ws.ReadJSON(&sub); err != nil {
		return
	}
	if sub.Type != "subscribe" {
		_ = ws.WriteJSON(gin.H{"error": "expected subscribe frame"})
		return
	}

	chID := channel.ID(sub.ChannelID)

	// Channel-access ACL: a valid session is NOT enough — the user must be a
	// member of the channel's workspace, the same gate REST goes through
	// (requireChannelAccess). Without this any logged-in user could subscribe to
	// any channel and tail its whole log from seq 0.
	wsID, okWs := a.channelWorkspaceID(c.Request.Context(), string(chID))
	if !okWs || !a.isWorkspaceMember(c.Request.Context(), wsID, userID) {
		_ = ws.WriteJSON(gin.H{"error": "forbidden"})
		return
	}

	home := a.getHome(chID)
	if home == nil {
		// Access already passed (workspace member of an existing channel), so a nil
		// home is the present-but-not-open state (A-P8): honestly "unavailable"
		// (retryable), not "not found". A ws handshake has no HTTP status to carry;
		// the frame + log mirror the 503 semantics the REST handlers return.
		a.logger.Warn("channel unavailable: directory has channel but its home is not open",
			"channel", string(chID))
		_ = ws.WriteJSON(gin.H{"error": "channel unavailable"})
		return
	}

	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()

	// Door handle: workspace ACL (tail) is broader than channel membership. A
	// channel member gets a HumanHandle (write frames + L3 presence); a
	// non-member (看得见≠在里面) may only tail — its write frames are refused.
	userActorID := actor.ActorID("user:" + userID)
	handle, herr := home.Human(ctx, userActorID)
	if herr != nil && errors.Is(herr, platform.ErrClosed) {
		// The home is shutting down: same honest "unavailable" the nil-home
		// path answers — not a membership verdict.
		_ = ws.WriteJSON(gin.H{"error": "channel unavailable"})
		return
	}
	isMember := herr == nil
	var presenceToken string
	if isMember {
		// Token-form presence (期12 S4): this connection holds its OWN token;
		// a late disconnect can only remove itself, never a successor
		// session's online.
		presenceToken = handle.PresenceConnect()
		defer handle.PresenceDisconnect(presenceToken)
	}

	// The SINGLE ws writer. Everything (tail messages, frame acks, errors) is
	// serialized through outbound so no two goroutines write the ws concurrently.
	outbound := make(chan any, 64)
	go func() {
		ping := time.NewTicker(wsPingPeriod)
		defer ping.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-outbound:
				_ = ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
				if err := ws.WriteJSON(m); err != nil {
					cancel()
					return
				}
			case <-ping.C:
				// Keepalive rides the SAME single writer (gorilla forbids concurrent
				// writes), never a side goroutine. A failed ping tears the conn down;
				// a peer that stops ponging trips the reader's read deadline.
				_ = ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
				if err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteWait)); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	send := func(m any) {
		select {
		case outbound <- m:
		case <-ctx.Done():
		}
	}
	// Unblock the reader's ReadJSON on teardown (writer failure / request done).
	go func() {
		<-ctx.Done()
		_ = ws.SetReadDeadline(time.Now())
	}()

	// Tail goroutine (read axis): backfill then follow the commit Signal, pushing
	// through the shared writer. cursor is touched only here.
	notify, cancelSub := home.Subscribe()
	defer cancelSub()
	cursor := sub.SinceSeq
	go func() {
		a.wsTail(home, &cursor, string(chID), send)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-notify:
				if !ok {
					cancel()
					return
				}
				a.wsTail(home, &cursor, string(chID), send)
			}
		}
	}()

	// Reader loop: the SINGLE ws reader. Each write frame drives a door verb.
	for {
		var f inFrame
		if err := ws.ReadJSON(&f); err != nil {
			return
		}
		a.dispatchFrame(ctx, chID, handle, isMember, f, send)
	}
}

// wsTail drains every log row after *cursor into the shared writer as tail frames.
func (a *App) wsTail(home *platform.Home, cursor *int64, chID string, send func(any)) {
	for {
		rows, err := home.View().ReadAfterSeq(context.Background(), *cursor, 100)
		if err != nil || len(rows) == 0 {
			return
		}
		for _, r := range rows {
			send(gin.H{
				"type":       "message",
				"channel_id": chID,
				"seq":        r.Seq,
				"envelope":   r.Envelope,
			})
			if r.Seq > *cursor {
				*cursor = r.Seq
			}
		}
	}
}

// dispatchFrame routes one upstream write frame to its door verb.
func (a *App) dispatchFrame(ctx context.Context, chID channel.ID, handle *platform.HumanHandle, isMember bool, f inFrame, send func(any)) {
	switch f.Type {
	case "message":
		a.wsHandleMessage(ctx, chID, handle, isMember, f, send)
	case "resolve":
		if !isMember {
			send(wsErr(f, "not_member", "not a channel member"))
			return
		}
		if err := handle.Resolve(ctx, message.ID(f.ReqID), f.Decision, f.Payload); err != nil {
			send(wsErr(f, doorErrCode(err), err.Error()))
			return
		}
		send(wsAck(f, gin.H{"req_id": f.ReqID}))
	case "cancel":
		if !isMember {
			send(wsErr(f, "not_member", "not a channel member"))
			return
		}
		if err := handle.Cancel(ctx, message.ID(f.ReqID)); err != nil {
			send(wsErr(f, doorErrCode(err), err.Error()))
			return
		}
		send(wsAck(f, gin.H{"req_id": f.ReqID}))
	case "after":
		if !isMember {
			send(wsErr(f, "not_member", "not a channel member"))
			return
		}
		// Input bounds (期12 v0.4): the schedule engine treats a past FireAt
		// as "fire now", so a negative/zero or overflow duration would be a
		// legal immediate trigger — refuse it at the edge. No upper cap
		// (abuse hardening is the engine-level anti-storm axis, deferred).
		if f.DurationMs <= 0 || f.DurationMs > math.MaxInt64/int64(time.Millisecond) {
			send(wsErr(f, "invalid_argument", "duration_ms must be a positive millisecond count"))
			return
		}
		tid, err := handle.After(ctx, time.Duration(f.DurationMs)*time.Millisecond, f.MsgType, f.Payload)
		if err != nil {
			send(wsErr(f, doorErrCode(err), err.Error()))
			return
		}
		send(wsAck(f, gin.H{"timer_id": string(tid)}))
	case "cancel_timer":
		if !isMember {
			send(wsErr(f, "not_member", "not a channel member"))
			return
		}
		if err := handle.CancelTimer(ctx, schedule.TimerID(f.TimerID)); err != nil {
			send(wsErr(f, doorErrCode(err), err.Error()))
			return
		}
		send(wsAck(f, gin.H{"timer_id": f.TimerID}))
	default:
		send(wsErr(f, "unknown_frame", "unrecognised frame type: "+f.Type))
	}
}

// wsHandleMessage resolves routing policy (app domain) then commits through the
// door — the ws twin of the retired POST /messages handler (H3 已拍死: the write
// path is the frame, not HTTP).
func (a *App) wsHandleMessage(ctx context.Context, chID channel.ID, handle *platform.HumanHandle, isMember bool, f inFrame, send func(any)) {
	if !isMember {
		send(wsErr(f, "not_member", "not a channel member"))
		return
	}
	in := submitInput{
		ID: f.ID, Type: f.MsgType, Kind: f.Kind, Payload: f.Payload,
		Audience: f.Audience, Visibility: f.Visibility, ParentID: f.ParentID,
	}
	audience, kind, rErr := a.resolveRouting(ctx, chID, in)
	if rErr != nil {
		var re *routingError
		if errors.As(rErr, &re) {
			send(wsErr(f, "unavailable", re.detail))
			return
		}
		a.logger.Error("ws message: routing", "channel", string(chID), "err", rErr)
		send(wsErr(f, "internal", ""))
		return
	}
	exp := time.Now().UnixMilli() + clientRequestTTLMs
	msgID, seq, err := handle.Submit(ctx, platform.SubmitSpec{
		ID:         message.ID(f.ID),
		Type:       f.MsgType,
		Kind:       kind,
		Payload:    f.Payload,
		Audience:   audience,
		Visibility: message.Visibility(f.Visibility),
		ParentID:   message.ID(f.ParentID),
		ExpiresAt:  &exp,
	})
	if err != nil {
		var wr *platform.WriteRejectedError
		if errors.As(err, &wr) {
			send(wsErr(f, wr.Reason, wr.Detail))
			return
		}
		if errors.Is(err, platform.ErrNotMember) {
			send(wsErr(f, "not_member", "not a channel member"))
			return
		}
		// 期12 v0.4 P0-2: the message path does NOT route through doorErrCode
		// (resolve/cancel's map) — these two arms must live here or a killed
		// cell reads as internal instead of the honest retryable.
		if errors.Is(err, platform.ErrCellUnavailable) {
			send(wsErr(f, "unavailable", "subject cell unavailable — retry"))
			return
		}
		if errors.Is(err, platform.ErrClosed) {
			send(wsErr(f, "closed", "channel home is closed"))
			return
		}
		a.logger.Error("ws message: submit", "channel", string(chID), "err", err)
		send(wsErr(f, "internal", ""))
		return
	}
	send(wsAck(f, gin.H{"message_id": string(msgID), "seq": seq}))
}

// doorErrCode maps a HumanHandle verb error to a flat frame error code.
func doorErrCode(err error) string {
	switch {
	case errors.Is(err, platform.ErrRequestNotFound):
		return "request_not_found"
	case errors.Is(err, platform.ErrNotInAudience):
		return "not_in_audience"
	case errors.Is(err, platform.ErrNotRequestSender):
		return "unauthorized_sender"
	case errors.Is(err, platform.ErrRequestClosed):
		return "already_closed"
	case errors.Is(err, platform.ErrNotMember):
		return "not_member"
	case errors.Is(err, platform.ErrInvalidDecision):
		return "invalid_decision"
	case errors.Is(err, platform.ErrClosed):
		return "closed"
	case errors.Is(err, platform.ErrCellUnavailable):
		return "unavailable"
	}
	var wr *platform.WriteRejectedError
	if errors.As(err, &wr) {
		return wr.Reason
	}
	return "internal"
}

// wsErr builds an error frame, echoing the client's ref + originating frame type.
func wsErr(f inFrame, code, detail string) gin.H {
	return gin.H{"type": "error", "ref": f.Ref, "frame": f.Type, "error": code, "detail": detail}
}

// wsAck builds an ack frame carrying frame-specific receipt fields.
func wsAck(f inFrame, extra gin.H) gin.H {
	m := gin.H{"type": "ack", "ref": f.Ref, "frame": f.Type}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

// ---------------------------------------------------------------------------
// WebSocket: compute attach (/compute)
// ---------------------------------------------------------------------------

func (a *App) handleCompute(c *gin.Context) {
	// v1 approach: api-key + channel in query params. App does all auth
	// (key verification + daemon-channel binding check) before handing off
	// to the link acceptor, which receives the pre-authenticated daemonID.
	apiKey := c.Query("key")
	chIDStr := c.Query("channel")

	if apiKey == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "api key required"})
		return
	}
	if chIDStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "channel query param required"})
		return
	}

	chID := channel.ID(chIDStr)

	// Single auth path: verify key + daemon-channel binding. The specific
	// reason (bad key / no binding / db error) stays server-side — a public
	// auth endpoint must not be an oracle, so the client gets one flat 403.
	daemonID, err := a.authAndResolve(apiKey, chID)
	if err != nil {
		a.logger.Warn("compute attach: auth failed", "channel", chID, "err", err)
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}

	home := a.homeOrError(c, chID)
	if home == nil {
		return
	}

	// Delegate to the link acceptor with the pre-authenticated daemonID.
	home.ServeAttach(c.Writer, c.Request, daemonID)
}
