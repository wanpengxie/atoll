package app

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/app/internal/middleware"
	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// statusProbeTimeout bounds how long the app waits for an actor to self-answer
// actor.status. It is SHORT by design: a status probe is a UI affordance, not a
// business call. An actor that does not answer within this window (it does not
// implement actor.status, its device round-trip is irrelevant — status is a pure
// self-answer, or its cell is gone) is reported live:false rather than erroring.
const statusProbeTimeout = 2 * time.Second

// statusPollInterval is the message-log poll cadence while waiting for the
// actor.status response.
const statusPollInterval = 25 * time.Millisecond

// handleActorStatus probes one actor's LIVE state for the frontend. The app
// sends the actor a plain actor.status request (kind=request, audience=[actorID])
// as the requesting user, then polls the channel message log for the actor's
// self-answer. The actor's opaque status snapshot is returned verbatim under
// "status", alongside live:true. If the actor never answers within the short
// probe window — it does not implement actor.status, or its cell is unreachable
// — the app reports {live:false} (treated as offline) rather than failing the
// request: an unanswered probe is a normal, expected outcome here.
//
// Why a self-answer query and not a substrate obs read: a daemon-hosted adapter's
// rich device-presence does not cross the port wire (the frame set has no obs
// frame), so the substrate cannot surface it. Asking the actor over the ordinary
// request/response path reaches it wherever it is hosted (cell or daemon) without
// any substrate change.
func (a *App) handleActorStatus(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	actorID := c.Param("actorID")
	if actorID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "actor id required"})
		return
	}

	home := a.getHome(channel.ID(chID))
	if home == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
		return
	}

	channelID := channel.ID(chID)
	senderID := actor.ActorID("user:" + middleware.UserID(c))

	// Build the probe as the requesting user (same sending identity as
	// handleSendMessage), addressed solely to the target actor.
	env := a.newClientEnvelope(channelID, senderID, "", introspect.QueryStatus,
		message.KindRequest, nil, []actor.ActorID{actor.ActorID(actorID)})
	// Mark the probe (and the response threaded to it) system-visibility so the UI
	// threading filters it out — a status probe is a UI affordance, not chat, and
	// must not grow an actor.status thread in the conversation stream. Interim
	// mitigation; the deeper "live state in truth" decision is owner's to make.
	env.Visibility = message.VisibilitySystem

	gw := homeGateway(channelID, home)
	ctx := harness.CtxWithCaller(c.Request.Context(), harness.CallerContext{
		ActorID:   senderID,
		ChannelID: channelID,
	})

	res, err := gw.SendMessage(ctx, env)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !res.Accepted() {
		// The send itself was rejected (e.g. no such actor in the channel) — that
		// is an offline outcome, not a server error.
		c.JSON(http.StatusOK, gin.H{"live": false})
		return
	}

	snapshot, answered := a.awaitStatusAnswer(ctx, gw, res.Seq, res.MessageID)
	if !answered {
		c.JSON(http.StatusOK, gin.H{"live": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{"live": true, "status": snapshot})
}

// awaitStatusAnswer polls the channel message log (after the probe's own seq) for
// the actor's kind=response to the probe, returning its opaque status snapshot.
// answered=false on timeout (the caller maps that to live:false). It returns the
// snapshot only for a COMPLETED response; a failed terminal (the actor errored
// the probe) is treated as not-live.
func (a *App) awaitStatusAnswer(
	ctx context.Context,
	gw gateway,
	afterSeq int64,
	probeID message.ID,
) (map[string]any, bool) {
	deadline := time.Now().Add(statusProbeTimeout)
	for {
		rows, err := gw.ListMessages(ctx, afterSeq, 200)
		if err == nil {
			for _, row := range rows {
				e := row.Envelope
				if e.Kind != message.KindResponse || e.ParentID != probeID {
					continue
				}
				return parseStatusResponse(e.Payload)
			}
		}
		if time.Now().After(deadline) {
			return nil, false
		}
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(statusPollInterval):
		}
	}
}

// parseStatusResponse pulls the opaque status snapshot out of an actor.status
// response payload. The payload is the merged response object: the introspect
// Status fields (actor_id, status_snapshot) plus the behavior terminal `status`.
// A non-completed terminal → not live. The snapshot is returned verbatim (the
// app does not interpret its keys — substrate守结构不守词汇).
func parseStatusResponse(payload []byte) (map[string]any, bool) {
	var body struct {
		Status   string         `json:"status"`
		Snapshot map[string]any `json:"status_snapshot"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, false
	}
	if body.Status != "completed" {
		return nil, false
	}
	if body.Snapshot == nil {
		body.Snapshot = map[string]any{}
	}
	return body.Snapshot, true
}
