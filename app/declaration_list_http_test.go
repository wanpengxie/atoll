package app_test

// Declaration list read face: /api/actor-decls answers from the realm relation
// index, so it stays available even when a channel's serving image is not —
// the projection is the read path, the membrane is not consulted per request.

import (
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestActorDeclListUsesRelationIndexWhenChannelUnavailable(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)
	env.app.DropHomeForTest(channel.ID(setup.chID))

	listed := env.do(t, http.MethodGet, "/api/actor-decls", nil, setup.cookies)
	assertStatus(t, listed, http.StatusOK)
}
