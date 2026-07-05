package app

import (
	"context"
	"encoding/json"
	"errors"
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
	isMember := herr == nil
	if isMember {
		handle.PresenceConnect()
		defer handle.PresenceDisconnect()
	}

	// The SINGLE ws writer. Everything (tail messages, frame acks, errors) is
	// serialized through outbound so no two goroutines write the ws concurrently.
	outbound := make(chan any, 64)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-outbound:
				if err := ws.WriteJSON(m); err != nil {
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
