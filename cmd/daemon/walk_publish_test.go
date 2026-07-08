package main

// walk_publish_test.go is 期11 spec §6 item②'s "发布 D5 walk": a real
// publisher actor Create(file)'s WITH content via the real write path (the
// create-outbox: reservation -> daemon staging+rename -> Committed RPC ->
// row lands, §1.6/§1.7), ShareMembers({read}), a DIFFERENT real actor on a
// DIFFERENT real daemon sees it via List/Stat (ops echoed) and reads the
// bytes cross-daemon over the real lane (§5's对称数据路 — server-mediated,
// not zerocopy), then the decay law regresses: an actor with NO read of its
// own cannot grant read to someone else. Scans (per spec): members grant /
// 读面 / 对称数据路 / create-outbox 落行 / 衰减律 — five things.

import (
	"io"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

func TestWalk2_PublishD5(t *testing.T) {
	const chID = channelpkg.ID("walk2-publish")
	const publisherID = actor.ActorID("agent:walk2-publisher")
	const readerID = actor.ActorID("agent:walk2-reader")
	const outsiderID = actor.ActorID("agent:walk2-outsider")
	const bystanderID = actor.ActorID("agent:walk2-bystander")
	const fileID = resource.ResourceID("file:walk2-publish/report.txt")
	const content = "the published D5 report, in full\n"

	h := newWalkHome(t, chID)
	wsURLA := newWalkDaemonServer(t, h, "walk2-daemon-a")
	wsURLB := newWalkDaemonServer(t, h, "walk2-daemon-b")
	daemonA := startWalkDaemon(t, wsURLA, "walk2-daemon-a", string(chID), walkDaemonConfig{})
	daemonB := startWalkDaemon(t, wsURLB, "walk2-daemon-b", string(chID), walkDaemonConfig{})

	type createWithContentResult struct {
		walkResult
		Local bool `json:"local"`
	}
	publisherOps := map[string]walkOpFunc{
		"walk.publish": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			fa, out, err := sys.Resource().CreateFile(fileID, false /*dir*/, true /*withContent*/)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			if !out.Accepted() {
				return createWithContentResult{walkResult: walkResult{OK: false, Reason: string(out.RejectReason)}}, "", ""
			}
			if fa.Local == nil || fa.Local.Write == nil {
				return createWithContentResult{walkResult: walkResult{OK: false, Reason: "no local write handle (publisher creates on its own daemon, expected Local)"}}, "", ""
			}
			if _, werr := fa.Local.Write.Write([]byte(content)); werr != nil {
				return nil, "internal_error", "write: " + werr.Error()
			}
			if cerr := fa.Local.Write.Commit(); cerr != nil {
				return nil, "internal_error", "commit: " + cerr.Error()
			}
			return createWithContentResult{walkResult: walkResult{OK: true}, Local: true}, "", ""
		},
		"walk.share_members_read": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			out, err := sys.Resource().ShareMembers(fileID, []access.Operation{access.OpRead})
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			return walkResult{OK: out.Accepted(), Reason: string(out.RejectReason)}, "", ""
		},
	}

	type statResult struct {
		walkResult
		Ops []string `json:"ops"`
	}
	type listResult struct {
		walkResult
		Found bool     `json:"found"`
		Ops   []string `json:"ops"`
	}
	type readResult struct {
		walkResult
		Local   bool   `json:"local"`
		Content string `json:"content"`
	}
	readerOps := map[string]walkOpFunc{
		"walk.stat": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			st, err := sys.Resource().Stat(fileID)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			if st.Reject != "" {
				return statResult{walkResult: walkResult{OK: false, Reason: string(st.Reject)}}, "", ""
			}
			return statResult{walkResult: walkResult{OK: true}, Ops: opsToStrings(st.Ops)}, "", ""
		},
		"walk.list": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			page, err := sys.Resource().List(accessdoor.ListQuery{Prefix: "file:walk2-publish", Limit: 10})
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			for _, e := range page.Entries {
				if e.ID == fileID {
					return listResult{walkResult: walkResult{OK: true}, Found: true, Ops: opsToStrings(e.Ops)}, "", ""
				}
			}
			return listResult{walkResult: walkResult{OK: true}, Found: false}, "", ""
		},
		"walk.read_cross_daemon": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			fa, out, err := sys.Resource().Open(fileID, access.OpRead)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			if !out.Accepted() {
				return readResult{walkResult: walkResult{OK: false, Reason: string(out.RejectReason)}}, "", ""
			}
			if fa.Local != nil {
				return readResult{walkResult: walkResult{OK: false, Reason: "resolved Local, want cross-daemon Stream"}}, "", ""
			}
			if fa.Stream == nil {
				return readResult{walkResult: walkResult{OK: false, Reason: "no Stream route (expected cross-daemon lane)"}}, "", ""
			}
			b, rerr := io.ReadAll(fa.Stream)
			_ = fa.Stream.Close()
			if rerr != nil {
				return nil, "internal_error", "read stream: " + rerr.Error()
			}
			return readResult{walkResult: walkResult{OK: true}, Local: false, Content: string(b)}, "", ""
		},
	}

	// outsiderOps / bystanderOps drive the decay-law regression: outsider has
	// NO grant of its own on fileID, so it may not grant read to a third
	// party (§2's decay law: set(X,ops) requires ops ⊆ granter's own
	// effective ops — granting what you do not hold is denied, not merely
	// narrowed).
	outsiderOps := map[string]walkOpFunc{
		"walk.escalate_share": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			out, err := sys.Resource().ShareActor(fileID, bystanderID, []access.Operation{access.OpRead})
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			return walkResult{OK: out.Accepted(), Reason: string(out.RejectReason)}, "", ""
		},
	}

	daemonA.addActor(t, h, publisherID, actor.KindAgent, walkActorDef(publisherOps))
	daemonB.addActor(t, h, readerID, actor.KindAgent, walkActorDef(readerOps))
	daemonB.addActor(t, h, outsiderID, actor.KindAgent, walkActorDef(outsiderOps))
	// bystanderID only needs channel membership to be a legal ShareActor
	// grantee target — it never itself sends a request in this walk.
	if err := h.Admit(t.Context(), bystanderID, actor.KindAgent); err != nil {
		t.Fatalf("admit bystander: %v", err)
	}
	controller := newControllerPen(t, h, actor.ActorID("user:walk2-driver"), actor.KindHuman)

	// --- 1. publisher Create(file) WITH content, via the real write path ---
	term := sendAndAwait(t, h, controller, publisherID, "walk.publish", nil, 15*time.Second)
	requireCompleted(t, "publish", term)
	var pubRes createWithContentResult
	decodeWalkPayload(t, term, &pubRes)
	if !pubRes.OK {
		t.Fatalf("publisher CreateFile(withContent=true) rejected: %s", pubRes.Reason)
	}
	if !pubRes.Local {
		t.Fatalf("publisher's own write did not resolve Local (same-daemon expected)")
	}

	// --- reader sees nothing yet (no grant) --------------------------------
	term = sendAndAwait(t, h, controller, readerID, "walk.stat", nil, 15*time.Second)
	requireCompleted(t, "stat_before_share", term)
	var statBefore statResult
	decodeWalkPayload(t, term, &statBefore)
	if statBefore.OK {
		t.Fatalf("reader Stat before ShareMembers succeeded (ops=%v) — any-grant visibility should hide it", statBefore.Ops)
	}
	if statBefore.Reason != string(access.ResourceNotFound) {
		t.Fatalf("stat_before_share reason = %q, want %q (zero-grant indistinguishable from absent)", statBefore.Reason, access.ResourceNotFound)
	}

	// --- 2. ShareMembers({read}) --------------------------------------------
	term = sendAndAwait(t, h, controller, publisherID, "walk.share_members_read", nil, 15*time.Second)
	requireCompleted(t, "share_members_read", term)
	var shareRes walkResult
	decodeWalkPayload(t, term, &shareRes)
	if !shareRes.OK {
		t.Fatalf("ShareMembers({read}) rejected: %s", shareRes.Reason)
	}

	// --- 3. reader List/Stat now see it, ops echoed -------------------------
	term = sendAndAwait(t, h, controller, readerID, "walk.stat", nil, 15*time.Second)
	requireCompleted(t, "stat_after_share", term)
	var statAfter statResult
	decodeWalkPayload(t, term, &statAfter)
	if !statAfter.OK {
		t.Fatalf("reader Stat after ShareMembers rejected: %s", statAfter.Reason)
	}
	if !containsOp(statAfter.Ops, access.OpRead) {
		t.Fatalf("Stat ops = %v, want to contain %q", statAfter.Ops, access.OpRead)
	}

	term = sendAndAwait(t, h, controller, readerID, "walk.list", nil, 15*time.Second)
	requireCompleted(t, "list_after_share", term)
	var listRes listResult
	decodeWalkPayload(t, term, &listRes)
	if !listRes.OK || !listRes.Found {
		t.Fatalf("reader List after ShareMembers: ok=%v found=%v, want both true (reason=%s)", listRes.OK, listRes.Found, listRes.Reason)
	}
	if !containsOp(listRes.Ops, access.OpRead) {
		t.Fatalf("List entry ops = %v, want to contain %q", listRes.Ops, access.OpRead)
	}

	// --- 4. cross-daemon read over the real lane (对称数据路) ---------------
	term = sendAndAwait(t, h, controller, readerID, "walk.read_cross_daemon", nil, 15*time.Second)
	requireCompleted(t, "read_cross_daemon", term)
	var readRes readResult
	decodeWalkPayload(t, term, &readRes)
	if !readRes.OK {
		t.Fatalf("reader cross-daemon Open(read) rejected: %s", readRes.Reason)
	}
	if readRes.Local {
		t.Fatalf("cross-daemon read resolved Local — placement/consumer host detection is broken")
	}
	if readRes.Content != content {
		t.Fatalf("cross-daemon read content = %q, want %q", readRes.Content, content)
	}

	// --- 5. 衰减律回归: outsider (no grant) cannot escalate read to bystander
	term = sendAndAwait(t, h, controller, outsiderID, "walk.escalate_share", nil, 15*time.Second)
	requireCompleted(t, "escalate_share", term)
	var escRes walkResult
	decodeWalkPayload(t, term, &escRes)
	if escRes.OK {
		t.Fatalf("outsider (no grant) successfully granted read to bystander — decay law (set ops ⊆ granter's own effective ops) is broken")
	}
	if escRes.Reason != string(access.AccessDenied) {
		t.Fatalf("escalate_share reason = %q, want %q", escRes.Reason, access.AccessDenied)
	}
}

func opsToStrings(ops accessdoor.OpSet) []string {
	out := make([]string, 0, len(ops))
	for _, op := range ops {
		out = append(out, string(op))
	}
	return out
}

func containsOp(ops []string, op access.Operation) bool {
	for _, o := range ops {
		if o == string(op) {
			return true
		}
	}
	return false
}
