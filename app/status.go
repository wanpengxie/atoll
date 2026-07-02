package app

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// handleActorStatus reports an actor's L3 device-presence for the UI — read
// OUT-OF-BAND from the home device-presence fold (the actor-source obs PUSH the adapter
// published, folded read-time + lease-decayed). It does NOT probe the actor and
// does NOT write the truth log: a UI status read must never pollute truth — that
// was the retired probe's sin (it sent an actor.status request and polled the log
// for the self-answer, two log writes to answer a read-only question).
//
// Three honest states (device presence is ADVISORY; authoritative reachability is
// send→terminal — try it):
//   - known:false                 → UNKNOWN. The actor never reported device presence:
//     not a device-bearing adapter, its device has
//     no liveness signal, or its link dropped and
//     the fold decayed it. NOT offline.
//   - known:true, online:true/false → the adapter's last device-presence edge.
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

	snapshot, known := home.View().DevicePresence(actor.ActorID(actorID))
	if !known {
		c.JSON(http.StatusOK, gin.H{"known": false})
		return
	}
	p, ok := introspect.ParseDevicePresence(snapshot)
	if !ok {
		// A folded value we cannot decode is treated as unknown (the convention is
		// the adapter+app's; a malformed blob is honestly "we don't know").
		c.JSON(http.StatusOK, gin.H{"known": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{"known": true, "online": p.Online})
}
