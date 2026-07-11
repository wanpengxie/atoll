package main

// walk_workspace_test.go is 期11 spec §6.1 item①'s "父授工作区 walk", restored
// to its LITERAL single-workspace form by 丁12 (the dir write句柄): a real
// creator actor CreateFile(dir=true)'s ONE workspace (a directory-shaped file
// resource — one ResourceID / one户口行, R granularity = the whole tree),
// ShareActor-grants a second real actor {read,write} on it, that second actor
// Open(mode=write)'s the workspace and receives a REAL os.Root SUBTREE lease
// (accessdoor.LocalFile.Dir), then uses os.Create/Mkdir directly to build a
// real file tree (multiple files + a subdirectory) on the real daemon
// filesystem — NO Commit boundary, every os.* call lands immediately (the
// design's "写 workspace = 门颁 os.Root 子目录句柄...无 Commit 边界"). The
// creator then deletes the workspace (row-first, tombstone written), the
// Reclaimer rm -rf's the WHOLE subtree, and the second actor's later deref sees
// resource_not_found. Scans (per spec): file / placement / Share / local dir
// handle / tombstone — five things, one object.
//
// One grounded deviation from §6.1's literal "fork子代" text: the second actor
// is a real, Admit'd, daemon-attached SIBLING actor rather than a Fork child.
// See walk_harness_test.go's package doc for the full grounding (Fork's child
// is architecturally home-hosted-only and non-member; Open needs daemon-hosted
// + membership; taking bytes随 human 债② is deferred, not built in 丁12). The
// PREVIOUS split into a dir MARKER + a separate regular FILE (S6's stopgap,
// because no dir Open ADT existed) is now GONE — 丁12 built LocalDirHandle, so
// the walk exercises the one true directory workspace the design describes.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
)

func TestWalk1_FatherAuthoredWorkspace(t *testing.T) {
	const chID = channelpkg.ID("walk1-workspace")
	parentID := actor.ActorID("agent:walk1-parent")
	childID := actor.ActorID("agent:walk1-child")
	const wsID = resource.ResourceID("file:walk1-workspace")
	const noteContent = "hello from the child's os.Root lease\n"
	const nestedContent = "nested under a subdir the child mkdir'd\n"

	h := newWalkHome(t, chID)
	wsURL := newWalkDaemonServer(t, h, "walk1-daemon")
	d := startWalkDaemon(t, wsURL, "walk1-daemon", string(chID), walkDaemonConfig{})
	liveDir := filepath.Join(d.WSRoot, "resources", string(chID), "live")

	parentOps := map[string]walkOpFunc{
		"walk.create_ws": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			_, out, err := sys.Resource().CreateFile(wsID, true /*dir*/, false /*withContent*/)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			return walkResult{OK: out.Accepted(), Reason: string(out.RejectReason)}, "", ""
		},
		"walk.share_child_rw": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			out, err := sys.Resource().ShareActor(wsID, childID, []access.Operation{access.OpRead, access.OpWrite})
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			return walkResult{OK: out.Accepted(), Reason: string(out.RejectReason)}, "", ""
		},
		"walk.delete_ws": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			out, err := sys.Resource().Delete(wsID)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			return walkResult{OK: out.Accepted(), Reason: string(out.RejectReason)}, "", ""
		},
	}

	type openDirResult struct {
		walkResult
		Local bool `json:"local"`
	}
	childOps := map[string]walkOpFunc{
		// child has NO grant of its own at this point — proves ShareActor is
		// LOAD-BEARING (not merely performed): without it this call would be
		// access_denied, since wsID's creator is the PARENT.
		"walk.probe_before_share": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			_, out, err := sys.Resource().Open(wsID, access.OpWrite)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			return walkResult{OK: out.Accepted(), Reason: string(out.RejectReason)}, "", ""
		},
		"walk.open_dir_and_write": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			fa, out, err := sys.Resource().Open(wsID, access.OpWrite)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			if !out.Accepted() {
				return openDirResult{walkResult: walkResult{OK: false, Reason: string(out.RejectReason)}}, "", ""
			}
			if fa.Local == nil || fa.Local.Dir == nil {
				return openDirResult{walkResult: walkResult{OK: false, Reason: "no local dir handle (expected same-daemon Local os.Root lease)"}}, "", ""
			}
			dir := fa.Local.Dir
			defer dir.Close()
			// Build a real file tree through the chroot-confined lease — no
			// Commit boundary, each op lands immediately.
			f, ferr := dir.Create("note.txt")
			if ferr != nil {
				return nil, "internal_error", "create note.txt: " + ferr.Error()
			}
			if _, werr := f.Write([]byte(noteContent)); werr != nil {
				_ = f.Close()
				return nil, "internal_error", "write note.txt: " + werr.Error()
			}
			if cerr := f.Close(); cerr != nil {
				return nil, "internal_error", "close note.txt: " + cerr.Error()
			}
			if merr := dir.Mkdir("sub", 0o755); merr != nil {
				return nil, "internal_error", "mkdir sub: " + merr.Error()
			}
			f2, ferr := dir.Create("sub/other.txt")
			if ferr != nil {
				return nil, "internal_error", "create sub/other.txt: " + ferr.Error()
			}
			if _, werr := f2.Write([]byte(nestedContent)); werr != nil {
				_ = f2.Close()
				return nil, "internal_error", "write sub/other.txt: " + werr.Error()
			}
			if cerr := f2.Close(); cerr != nil {
				return nil, "internal_error", "close sub/other.txt: " + cerr.Error()
			}
			return openDirResult{walkResult: walkResult{OK: true}, Local: true}, "", ""
		},
		"walk.deref": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			st, err := sys.Resource().Stat(wsID)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			return walkResult{OK: st.Reject == "", Reason: string(st.Reject)}, "", ""
		},
	}

	parentID = d.addActor(t, h, parentID, actor.KindAgent, walkActorDef(parentOps))
	childID = d.addActor(t, h, childID, actor.KindAgent, walkActorDef(childOps))
	controller := newControllerPen(t, h, actor.ActorID("user:walk1-driver"), actor.KindHuman)

	// --- 1. father CreateFile(dir=true): a real directory workspace lands ----
	before := listEntries(t, liveDir)
	term := sendAndAwait(t, h, controller, parentID, "walk.create_ws", nil, 15*time.Second)
	requireCompleted(t, "create_ws", term)
	var createRes walkResult
	decodeWalkPayload(t, term, &createRes)
	if !createRes.OK {
		t.Fatalf("CreateFile(dir=true) rejected: %s", createRes.Reason)
	}
	coord := diffOneNewEntry(t, liveDir, before)
	coordDir := filepath.Join(liveDir, coord)
	info, err := os.Stat(coordDir)
	if err != nil {
		t.Fatalf("stat new workspace dir %s: %v", coord, err)
	}
	if !info.IsDir() {
		t.Fatalf("CreateFile(dir=true)'s landed entry %s is not a directory", coord)
	}

	// --- probe: child has no grant yet -> access_denied ---------------------
	term = sendAndAwait(t, h, controller, childID, "walk.probe_before_share", nil, 15*time.Second)
	requireCompleted(t, "probe_before_share", term)
	var probeRes walkResult
	decodeWalkPayload(t, term, &probeRes)
	if probeRes.OK {
		t.Fatalf("child Open(write) succeeded BEFORE ShareActor — grant is not load-bearing")
	}
	if probeRes.Reason != string(access.AccessDenied) {
		t.Fatalf("probe_before_share reason = %q, want %q", probeRes.Reason, access.AccessDenied)
	}

	// --- 2. ShareActor(child, {read,write}) ---------------------------------
	term = sendAndAwait(t, h, controller, parentID, "walk.share_child_rw", nil, 15*time.Second)
	requireCompleted(t, "share_child_rw", term)
	var shareRes walkResult
	decodeWalkPayload(t, term, &shareRes)
	if !shareRes.OK {
		t.Fatalf("ShareActor(child,{read,write}) rejected: %s", shareRes.Reason)
	}

	// --- 3. child Open(mode=write) -> os.Root lease -> os.* real file tree ---
	term = sendAndAwait(t, h, controller, childID, "walk.open_dir_and_write", nil, 15*time.Second)
	requireCompleted(t, "open_dir_and_write", term)
	var openRes openDirResult
	decodeWalkPayload(t, term, &openRes)
	if !openRes.OK {
		t.Fatalf("child Open(dir write) rejected (post-share): %s", openRes.Reason)
	}
	if !openRes.Local {
		t.Fatalf("child Open(dir write) did not resolve a Local os.Root lease (same-daemon expected)")
	}

	// Real os.* verification: the tree actually landed on the real daemon
	// filesystem under the workspace's own coord dir — read it back directly,
	// independent of the resource axis's own read path.
	gotNote, err := os.ReadFile(filepath.Join(coordDir, "note.txt"))
	if err != nil {
		t.Fatalf("os.ReadFile(note.txt): %v", err)
	}
	if string(gotNote) != noteContent {
		t.Fatalf("note.txt content = %q, want %q", gotNote, noteContent)
	}
	subInfo, err := os.Stat(filepath.Join(coordDir, "sub"))
	if err != nil || !subInfo.IsDir() {
		t.Fatalf("child's Mkdir(sub) did not land a real subdirectory: err=%v", err)
	}
	gotNested, err := os.ReadFile(filepath.Join(coordDir, "sub", "other.txt"))
	if err != nil {
		t.Fatalf("os.ReadFile(sub/other.txt): %v", err)
	}
	if string(gotNested) != nestedContent {
		t.Fatalf("sub/other.txt content = %q, want %q", gotNested, nestedContent)
	}

	// --- 4. father delete -> tombstone written ------------------------------
	term = sendAndAwait(t, h, controller, parentID, "walk.delete_ws", nil, 15*time.Second)
	requireCompleted(t, "delete_ws", term)
	var deleteRes walkResult
	decodeWalkPayload(t, term, &deleteRes)
	if !deleteRes.OK {
		t.Fatalf("parent Delete(workspace) rejected: %s", deleteRes.Reason)
	}

	// --- 5. child deref -> not_found -----------------------------------
	// Precondition (S6 account item④, 申报): this is the SAME child
	// incarnation used in step 3 — never despawned/recreated — so the
	// not_found verdict below is genuinely "the row died under a caller that
	// never changed identity".
	term = sendAndAwait(t, h, controller, childID, "walk.deref", nil, 15*time.Second)
	requireCompleted(t, "deref", term)
	var derefRes walkResult
	decodeWalkPayload(t, term, &derefRes)
	if derefRes.OK {
		t.Fatalf("child deref after parent delete: OK=true, want not_found")
	}
	if derefRes.Reason != string(access.ResourceNotFound) {
		t.Fatalf("child deref reason = %q, want %q", derefRes.Reason, access.ResourceNotFound)
	}

	// --- 6. tombstone收地: the Reclaimer rm -rf's the WHOLE subtree (an
	// axis-allocated dir tombstone) within the fast reconcile cadence this rig
	// configured — poll until the entire coord directory (files, subdir, and
	// all) is gone.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(coordDir); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workspace dir %s still present after 10s — Reclaimer/ReclaimAck never rm -rf'd the tombstoned subtree", coordDir)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// listEntries returns dir's current top-level entry names (empty map, not an
// error, if dir does not exist yet).
func listEntries(t *testing.T, dir string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return out
	}
	for _, e := range entries {
		out[e.Name()] = true
	}
	return out
}

// diffOneNewEntry re-lists dir and returns the single entry name absent from
// before — the walk's way of discovering an opaque, server-generated coord
// without ever having the resource axis leak it directly (§3.4/§3.6 red
// lines: coord never crosses the door to a caller).
func diffOneNewEntry(t *testing.T, dir string, before map[string]bool) string {
	t.Helper()
	after := listEntries(t, dir)
	var found string
	for name := range after {
		if !before[name] {
			if found != "" {
				t.Fatalf("more than one new entry appeared under %s: %s and %s", dir, found, name)
			}
			found = name
		}
	}
	if found == "" {
		t.Fatalf("no new entry appeared under %s", dir)
	}
	return found
}
