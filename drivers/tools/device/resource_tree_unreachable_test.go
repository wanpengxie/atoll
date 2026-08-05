package device

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

// TestDeviceWorkspaceCannotReachResourceTree pins 期11 spec §4.2's sibling
// layout guarantee at the device actor's own boundary — spec §9 DoD#5's
// "device 文件词表对资源树不可达回归测试". Resource-axis object bytes live at
// <daemonRoot>/resources/<channelID>/live/<coord> (the exact layout
// drivers/devicehost/internal/storagehost's doc.go names), a SIBLING of the device
// workspace tree <daemonRoot>/<channelID>/ — not nested under it, sharing
// only the common ancestor <daemonRoot>. §4.2's own words: "分树=盘层/客体层
// 各有其树的误铲防护...不是机密性执法" — but that misc-cast protection only
// HOLDS if the device actor's own path vocabulary (resolvePath) is
// structurally incapable of ever producing a path under the resource tree,
// which is what this test proves — not merely that a couple of attack
// strings happen to get rejected today.
func TestDeviceWorkspaceCannotReachResourceTree(t *testing.T) {
	sys, root := startActor(t)

	// Materialize a resource object exactly where storagehost would land it
	// (§4.2 layout) — a secret the device actor must never be able to reach,
	// regardless of what path string a caller supplies.
	resourceDir := filepath.Join(root, "resources", string(testChannel), "live")
	if err := os.MkdirAll(resourceDir, 0o755); err != nil {
		t.Fatalf("seed resource tree: %v", err)
	}
	resourceFile := filepath.Join(resourceDir, "secret-coord")
	if err := os.WriteFile(resourceFile, []byte("resource-axis-secret"), 0o600); err != nil {
		t.Fatalf("seed resource file: %v", err)
	}

	// Structural proof, independent of any specific attack string: the
	// device workspace (<root>/<channelID>/) is not an ancestor of the
	// resource tree (<root>/resources/<channelID>/…) — they are siblings
	// under <root>. resolvePath's confinement (Clean+HasPrefix against
	// exactly one directory, the workspace) can therefore never Join its way
	// to a path under the resource tree: the resource tree isn't a
	// "blocked" destination among ones resolvePath could otherwise reach —
	// it sits wholly outside the one subtree resolvePath is capable of
	// returning a path inside of.
	ws := filepath.Join(root, string(testChannel))
	if strings.HasPrefix(resourceFile, ws+string(os.PathSeparator)) || resourceFile == ws {
		t.Fatalf("test setup invariant violated: resource tree %q is nested under the device workspace %q (sibling layout assumption broken)", resourceFile, ws)
	}

	// Primitive-level proof: resolvePath itself rejects every relative-path
	// encoding that would reach the resource file if its workspace
	// confinement were weaker than a strict HasPrefix check.
	for _, p := range []string{
		"../resources/" + string(testChannel) + "/live/secret-coord",
		"../../resources/" + string(testChannel) + "/live/secret-coord",
		"./../resources/" + string(testChannel) + "/live/secret-coord",
		"a/../../resources/" + string(testChannel) + "/live/secret-coord",
		"notes/../../resources/" + string(testChannel) + "/live/secret-coord",
	} {
		if _, err := resolvePath(ws, p); err == nil {
			t.Fatalf("resolvePath(%q, %q) succeeded — the device word list can reach the sibling resource tree", ws, p)
		}
	}

	// Actor-level proof: the same encodings, driven through the live device
	// actor exactly as a caller would (device.file.read), all terminate
	// path_invalid — never file_not_found (which would mean resolvePath let
	// the path OUT of the workspace and only the OS's own ENOENT stopped it)
	// and never completed with the secret content.
	for i, p := range []string{
		"../resources/" + string(testChannel) + "/live/secret-coord",
		"../../resources/" + string(testChannel) + "/live/secret-coord",
	} {
		msg := request(TypeFileRead, FileReadPayload{Path: p})
		msg.ID = message.ID("req-resource-unreachable-" + string(rune('a'+i)))
		sys.push(msg)
		status, code, _ := waitTerminal(t, sys, msg.ID)
		if status != "failed" || code != "path_invalid" {
			t.Fatalf("path %q: status=%s code=%s; want failed/path_invalid (device word list must never reach the resource tree)", p, status, code)
		}
	}
}
