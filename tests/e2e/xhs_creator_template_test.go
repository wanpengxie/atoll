//go:build e2e

package e2e

import (
	"os"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_XHSCreatorChannel_BootstrapSeedsTool covers phase-2 case 1.
//
// Wiring under test: cmd/daemon.buildChannelTemplates(false) returns a
// production-flavoured xhs-creator ChannelTemplate (DeviceXHSActorSeed
// with binding=runtime_inbound_via_relay + WorkdirSubdirs = published-notes/
// drafts/ assets/). When a channel is created with type="xhs-creator"
// the bootstrap saga MUST:
//
//  1. Insert tool:xhs-adapter into actor_registry with kind=tool +
//     binding=runtime_inbound_via_relay.
//  2. Mkdir each declared workdir subdir under the channel root.
//
// Regression target: the owner observed type=group channels lacking the
// adapter actor (legitimate — generic group template carries no seeds)
// and concluded type=xhs-creator was required for xhs.publish to work.
// This test pins the wiring: creating a channel with the right type
// MUST seed the actor, and creating one without MUST NOT (so the gate
// at adapter Install time stays meaningful).
func TestE2E_XHSCreatorChannel_BootstrapSeedsTool(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "xhstpl+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-xhstpl-" + uniqSuffix())

	// Counterpoint: a type=group channel must NOT carry the xhs adapter
	// seed (template gating proof — see cmd/daemon.buildChannelTemplates
	// + XHSScaffoldFactory's ChannelType check).
	groupID := s.CreateChannel(wsID, "ch-group-"+uniqSuffix(), "group")
	s.BindChannel(wsID, groupID)

	// Main case: type=xhs-creator must materialise the adapter row +
	// the three template workdir subdirs.
	xhsID := s.CreateChannel(wsID, "ch-xhs-"+uniqSuffix(), "xhs-creator")
	s.BindChannel(wsID, xhsID)

	// actor_registry assertions — both channels' sqlite has the system
	// + initial member actor already, but only the xhs-creator one must
	// also have tool:xhs-adapter.
	harness.Eventually(t, "tool:xhs-adapter seeded into xhs channel", 5*time.Second, func() bool {
		return countActor(t, s, xhsID, "tool:xhs-adapter") == 1
	})

	if got := countActor(t, s, groupID, "tool:xhs-adapter"); got != 0 {
		t.Errorf("group channel actor_registry contains tool:xhs-adapter (count=%d) — template gating broken", got)
	}

	// Kind + binding round-trip — full row sanity check.
	row := lookupActor(t, s, xhsID, "tool:xhs-adapter")
	if row.Kind != "tool" {
		t.Errorf("tool:xhs-adapter kind=%q want tool", row.Kind)
	}
	if row.Binding != "runtime_inbound_via_relay" {
		t.Errorf("tool:xhs-adapter binding=%q want runtime_inbound_via_relay", row.Binding)
	}

	// Workdir subdirs — exact set declared by adapters/xhs.WorkdirSubdirs().
	for _, sub := range []string{"published-notes", "drafts", "assets"} {
		path := s.ChannelSqlitePath(xhsID)
		dir := dirOf(path) + "/" + sub
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("xhs channel workdir missing %s: %v", sub, err)
		}
	}
}

// actorRow is a stripped-down projection of the channel-local
// actor_registry row. Tests only assert on the columns covered by the
// template snapshot.
type actorRow struct {
	ID      string
	Kind    string
	Binding string
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

func lookupActor(t *testing.T, s *harness.Stack, channelID, actorID string) actorRow {
	t.Helper()
	db := s.OpenChannelDB(channelID)
	defer func() { _ = db.Close() }()
	var row actorRow
	var binding *string
	if err := db.QueryRow(`SELECT actor_id, actor_kind, actor_binding FROM actor_registry WHERE actor_id=?`,
		actorID).Scan(&row.ID, &row.Kind, &binding); err != nil {
		t.Fatalf("lookup actor %s: %v", actorID, err)
	}
	if binding != nil {
		row.Binding = *binding
	}
	return row
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
