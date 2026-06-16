package app

import (
	"net/http"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// SetAgentOverride installs a test-only builder for the channel's bundled agent
// cell, bypassing the actor catalog (registry.Build). Tests inject a stub /
// custom agent without LLM credentials. Call before any channel is created.
// TEST-ONLY: this seam exists nowhere on the production Config.
func SetAgentOverride(a *App, fn func(chID channel.ID, agentID actor.ActorID, w harness.Writer) (actorrt.Actor, error)) {
	a.agentOverride = fn
}

// Handler exposes the assembled gin engine as an http.Handler so black-box
// (package app_test) tests can drive the whole server through httptest without
// binding a port. It lives in export_test.go on purpose: compiled only under
// `go test`, it is a test-only seam and never reaches the production API surface
// (production serves via Run). Routes stay owned by the app; callers get a
// read-only handler.
func (a *App) Handler() http.Handler {
	return a.engine
}
