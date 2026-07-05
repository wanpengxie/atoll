package app

import (
	"context"
	"net/http"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// OperateFaceForTest exposes the app's channel-operate executor so black-box
// tests can drive the four control verbs directly (as the sysactor gate would
// after authorising the sender), without the not-yet-built message senders
// (S5b/shims). Test-only.
func (a *App) OperateFaceForTest() platform.OperateExecutor {
	return a.operateFace()
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

// DropHomeForTest removes chID's open Home from the app's home map WITHOUT
// deleting its channels-table directory row — reproducing the "present in the
// directory but its universe is not open" state (getHome==nil) that homeOrError
// answers with 503 (A-P8). Test-only.
func (a *App) DropHomeForTest(chID channel.ID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.homes, chID)
}

// KillCellForTest kills id's live embodiment on chID's home (despawn + dereg) —
// the "brain went dead" event resolveRouting must answer with 503 when id is the
// channel's default agent. Test-only.
func (a *App) KillCellForTest(chID channel.ID, id actor.ActorID) error {
	home := a.getHome(chID)
	if home == nil {
		return errChannelNotLoaded
	}
	return home.Remove(context.Background(), id)
}
