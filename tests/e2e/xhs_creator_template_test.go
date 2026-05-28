//go:build e2e

package e2e

import (
	"os"
	"testing"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_XHSCreatorChannel_TemplateDoesNotSeedTool covers the current
// proxy-facade production model.
//
// Wiring under test: cmd/daemon.buildChannelTemplates returns an xhs-creator
// ChannelTemplate with WorkdirSubdirs = published-notes/ drafts/ assets/ and
// no actor/type seeds. When a channel is created with type="xhs-creator"
// the bootstrap saga MUST:
//
//  1. Not insert tool:xhs into actor_registry. The proxy facade installs it
//     only after the proxy daemon advertises the actor.
//  2. Mkdir each declared workdir subdir under the channel root.
func TestE2E_XHSCreatorChannel_TemplateDoesNotSeedTool(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "xhstpl+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-xhstpl-" + uniqSuffix())

	// Counterpoint: a type=group channel must not carry xhs workdir data.
	groupID := s.CreateChannel(wsID, "ch-group-"+uniqSuffix(), "group")
	s.BindChannel(wsID, groupID)

	// Main case: type=xhs-creator materialises only workdir subdirs. The
	// actor/type rows arrive later through proxy facade.
	xhsID := s.CreateChannel(wsID, "ch-xhs-"+uniqSuffix(), "xhs-creator")
	s.BindChannel(wsID, xhsID)

	if got := countActor(t, s, xhsID, "tool:xhs"); got != 0 {
		t.Errorf("xhs channel actor_registry statically contains tool:xhs (count=%d)", got)
	}
	if got := countActor(t, s, groupID, "tool:xhs"); got != 0 {
		t.Errorf("group channel actor_registry contains tool:xhs (count=%d)", got)
	}

	// Workdir subdirs — exact set declared by the xhs creator template.
	for _, sub := range []string{"published-notes", "drafts", "assets"} {
		path := s.ChannelSqlitePath(xhsID)
		dir := dirOf(path) + "/" + sub
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("xhs channel workdir missing %s: %v", sub, err)
		}
	}
}

func countActor(t *testing.T, s *harness.Stack, channelID, actorID string) int {
	t.Helper()
	db := s.OpenChannelDB(channelID)
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM actor_registry WHERE actor_id=?`, actorID).Scan(&n); err != nil {
		t.Fatalf("count actor: %v", err)
	}
	return n
}

// dirOf returns the directory of a file path without dragging in
// path/filepath at the test boundary (tests stay focused on observable
// state; harness owns path layout).
func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return p
}
