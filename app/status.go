package app

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// handleActorStatus reports an actor's L3 device-presence for the UI — read
// OUT-OF-BAND from the home device-presence fold (the actor-source obs PUSH the adapter
// published, folded read-time + lease-decayed). It does NOT probe the actor and
// does NOT write the truth log: a UI status read must never pollute truth — that
// was the retired probe's sin (it sent an actor.status request and polled the log
// for the self-answer, two log writes to answer a read-only question).
// The current reserved actor.status is different: the system actor reads this
// same Snapshot for in-channel callers and never asks the target to self-probe.
//
// Three honest states (device presence is ADVISORY; authoritative reachability is
// send→terminal — try it):
//   - known:false                 → UNKNOWN. The actor never reported device presence:
//     not a device-bearing adapter, its device has
//     no liveness signal, or its link dropped and
//     the fold decayed it. NOT offline.
//   - known:true, online:true/false → the adapter's last device-presence edge.
// handleChannelPresenceDrops reports the channel presence fold's dropped-event
// counters per obs kind — the operator read of the fold's loudness ledger
// (events the fold refused must stay countable: a silently shrinking fold is
// indistinguishable from a healthy one without this account). Same OUT-OF-BAND
// discipline as handleActorStatus: a read never writes truth.
func (a *App) handleChannelPresenceDrops(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	home := a.homeOrError(c, channel.ID(chID))
	if home == nil {
		return
	}
	drops := home.View().PresenceDrops()
	out := make(map[string]uint64, len(drops))
	for kind, n := range drops {
		out[string(kind)] = n
	}
	c.JSON(http.StatusOK, gin.H{"drops": out})
}

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
	home := a.homeOrError(c, channel.ID(chID))
	if home == nil {
		return
	}

	view := home.View()
	snapshot, err := view.Snapshot(c.Request.Context(), actor.ActorID(actorID))
	testimony, known := snapshot.L3[actorrt.ObsKind(introspect.ObsDevicePresence)]
	projectActorStatus(c, err, snapshot.Member, known, testimony.Val, testimony.ReceivedAt, view.TestimonyAgeMs)
}

func projectActorStatus(c *gin.Context, err error, member, known bool, val []byte, receivedAt int64, ageMs func(int64) int64) {
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "presence unavailable"})
		return
	}
	if !member {
		c.JSON(http.StatusNotFound, gin.H{"error": "actor not found"})
		return
	}
	if !known {
		c.JSON(http.StatusOK, gin.H{"known": false})
		return
	}
	p, ok := introspect.ParseDevicePresence(val)
	if !ok {
		// A folded value we cannot decode is treated as unknown (the convention is
		// the adapter+app's; a malformed blob is honestly "we don't know").
		c.JSON(http.StatusOK, gin.H{"known": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"known": true, "online": p.Online,
		"age_ms": ageMs(receivedAt),
	})
}
