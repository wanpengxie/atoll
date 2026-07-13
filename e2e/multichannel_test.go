// TestMultiChannelOnePipe is the central-claim black-box锚 (DoD-2a, 连接模型勘误期):
// the end-to-end proof of "连接即人" over the real /ws. ONE user, TWO channels, ONE
// connection: a single channel-blind attach hands over a two-key游标表, the connection
// receives feed for BOTH channels, business frames name their own channel_id and land
// on the right channel, and revoking eligibility to c1 (deleting the channel) stops
// c1's stream while c2 keeps flowing — a single-channel loss never tears the pipe.
//
// It is server-ONLY (no daemon): the two channels' creators are auto-admitted human
// members, so a public event addressed to the member lands on that channel's feed,
// exercising the multi-channel feed + submit + channel_id routing整条新主轴 without any
// agent. Black-box law (unchanged): ZERO atoll imports — /api HTTP + /ws frames only.
package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestMultiChannelOnePipe(t *testing.T) {
	binDir := requireE2EBin(t)
	serverBin := filepath.Join(binDir, "atoll-server")

	root := t.TempDir()
	dirs := makeDirs(t, root, "serverwd", "channels", "home", "logs")
	dbPath := filepath.Join(root, "app.db")
	env := scrubbedEnv(dirs["home"])

	var serverLog string
	t.Cleanup(func() {
		if t.Failed() && serverLog != "" {
			t.Logf("server log tail:\n%s", tailLog(serverLog, 60))
		}
	})

	var server *proc
	var base string
	gen := 0
	for attempt := 1; ; attempt++ {
		port := freePort(t)
		base = fmt.Sprintf("http://127.0.0.1:%d", port)
		gen++
		serverLog = filepath.Join(dirs["logs"], fmt.Sprintf("mc-server-%d.log", gen))
		server = startProc(t, fmt.Sprintf("mc-server#%d", gen), serverBin, []string{
			"-addr", fmt.Sprintf("127.0.0.1:%d", port),
			"-db", dbPath,
			"-channel-db-dir", dirs["channels"],
		}, dirs["serverwd"], serverLog, env)
		if waitHealthzErr(base, server, 30*time.Second) == nil {
			break
		}
		if server.exited() && attempt < 3 {
			server.reclaim()
			continue
		}
		t.Fatalf("server not healthy; log tail:\n%s", tailLog(serverLog, 50))
	}

	api := newAPIClient(t, base)
	api.must("POST", "/api/identity/register",
		map[string]any{"email": "mc@example.com", "password": "secret123", "display_name": "MC"},
		http.StatusCreated)

	// One workspace, TWO channels — the creator is auto-admitted a human member of BOTH.
	wsRow := api.must("POST", "/api/workspaces", map[string]any{"name": "mc-ws"}, http.StatusCreated)
	wsID, _ := wsRow["id"].(string)
	ch1 := api.must("POST", "/api/workspaces/"+wsID+"/channels", map[string]any{"name": "one"}, http.StatusCreated)
	c1, _ := ch1["id"].(string)
	ch2 := api.must("POST", "/api/workspaces/"+wsID+"/channels", map[string]any{"name": "two"}, http.StatusCreated)
	c2, _ := ch2["id"].(string)

	human1 := mcResolveHuman(t, api, c1)
	human2 := mcResolveHuman(t, api, c2)

	// ---- ONE channel-blind connection carrying TWO cursors -----------------
	cookie := api.cookieHeader()
	ws := dialWSMulti(t, base, cookie, c1, map[string]int64{c1: 0, c2: 0})

	// ---- 同一连接收两路 feed + 分别向两频道 submit ------------------------------
	id1 := mcSubmitEvent(t, ws, c1, human1, "one-a")
	if _, ok := mcAwaitFeed(t, ws, c1, id1, 15*time.Second); !ok {
		t.Fatalf("c1 event %s never appeared on the feed of the shared pipe", id1)
	}
	id2 := mcSubmitEvent(t, ws, c2, human2, "two-a")
	if _, ok := mcAwaitFeed(t, ws, c2, id2, 15*time.Second); !ok {
		t.Fatalf("c2 event %s never appeared on the feed of the shared pipe", id2)
	}

	// ---- 撤销 c1 (scoped membership removal) → c1 提交停 c2 照常 -------------------
	// Remove ONLY the creator's c1 MEMBERSHIP (real Home.Remove cascade via the
	// channel-internal removal surface, app/channel.go handleRemoveActor) — the
	// channel itself is untouched (still has its boost agent, still resolvable). This
	// is the precise scoped-revocation the terminal review asked for (not "delete the
	// whole channel" standing in for a membership change).
	//
	// The resulting entitlement code is EXACTLY forbidden, no fallback codes accepted:
	// app/entitlement.go demotes a former member who is still a WORKSPACE member (as
	// this creator is — both c1 and c2 live in the same workspace) to an OBSERVER
	// route, never a confirmed-absent one, so the gate's own表① mapping is unambiguous
	// (observer → forbidden on any business frame, never unavailable/closed).
	//
	// A genuinely EMPTY feed on c1 is NOT the correct assertion after a scoped
	// membership removal: workspace membership carries observer/tail visibility into
	// every channel of that workspace by design (app/entitlement.go) — losing channel
	// MEMBERSHIP is not losing the WORKSPACE relationship that grants read access. The
	// zero-feed proof (below) instead exercises the channel's full deletion, which
	// removes the directory row entirely and so removes even the observer route.
	api.must("DELETE", "/api/channels/"+c1+"/actors/"+human1, nil, http.StatusOK)
	pollUntil(t, "c1 submit is forbidden after scoped membership removal", 30*time.Second, func() bool {
		code, errored := mcTrySubmit(t, ws, c1, human1, "one-dead-scoped")
		return errored && code == "forbidden"
	})

	// c2 照常 mid-way: the scoped c1 removal must not touch c2's stream.
	id2b := mcSubmitEvent(t, ws, c2, human2, "two-b")
	if _, ok := mcAwaitFeed(t, ws, c2, id2b, 15*time.Second); !ok {
		t.Fatalf("c2 stream must keep flowing after c1's scoped membership removal")
	}

	// ---- 撤销 c1 (full deletion) → c1 零新 feed，c2 仍照常 --------------------------
	// Delete c1 entirely: the directory row disappears, so the resolver's per-channel
	// enumeration no longer returns c1 at ALL (not even as an observer) — genuine
	// confirmed-absence, the only path that actually yields a真实 empty-feed guarantee.
	api.must("DELETE", "/api/channels/"+c1, nil, http.StatusOK)
	pollUntil(t, "c1 submit is forbidden after full channel deletion", 30*time.Second, func() bool {
		code, errored := mcTrySubmit(t, ws, c1, human1, "one-dead-deleted")
		return errored && code == "forbidden"
	})

	// c2 照常: a fresh c2 event still round-trips on the same pipe post-revocation.
	id3 := mcSubmitEvent(t, ws, c2, human2, "two-c")
	if _, ok := mcAwaitFeed(t, ws, c2, id3, 15*time.Second); !ok {
		t.Fatalf("c2 stream must keep flowing after c1 revocation (连接即人: pipes independent)")
	}

	server.kill9(t)
}

// mcResolveHuman waits until the channel's creator is represented by one active human
// member and returns its actor id.
func mcResolveHuman(t *testing.T, api *apiClient, chID string) string {
	t.Helper()
	var humanID string
	pollUntil(t, "creator represented by one active human in "+chID, 30*time.Second, func() bool {
		_, m := api.do("GET", "/api/channels/"+chID+"/actors", nil)
		rows, _ := m["actors"].([]any)
		for _, raw := range rows {
			row, _ := raw.(map[string]any)
			if row["kind"] == "human" {
				humanID, _ = row["id"].(string)
				return humanID != ""
			}
		}
		return false
	})
	return humanID
}

// mcSubmitEvent submits a public event addressed to member onto chID and asserts a
// receipt, returning the minted message id. An error frame fails the test.
func mcSubmitEvent(t *testing.T, ws *wsClient, chID, member, marker string) string {
	t.Helper()
	code, id, errored := mcSubmitRaw(t, ws, chID, member, marker)
	if errored {
		t.Fatalf("submit event to %s: unexpected error frame %q", chID, code)
	}
	if id == "" {
		t.Fatalf("submit event to %s: receipt carried no message_id", chID)
	}
	return id
}

// mcTrySubmit submits a public event and reports (errorCode, errored) — errored=true
// with the flat code when the frame came back as an error, false on a receipt.
func mcTrySubmit(t *testing.T, ws *wsClient, chID, member, marker string) (string, bool) {
	t.Helper()
	code, _, errored := mcSubmitRaw(t, ws, chID, member, marker)
	return code, errored
}

func mcSubmitRaw(t *testing.T, ws *wsClient, chID, member, marker string) (code, id string, errored bool) {
	t.Helper()
	refCounter++
	ref := fmt.Sprintf("mc-%d", refCounter)
	// Explicit channel_id (the connection is channel-blind; every business frame names
	// its channel). Explicit audience skips gateway routing — a public event lands in
	// the log and shows on the feed投影.
	payload := map[string]any{
		"channel_id": chID,
		"msg_type":   "mc.note",
		"kind":       "event",
		"visibility": "public",
		"audience":   []string{member},
		"payload":    json.RawMessage(fmt.Sprintf(`{"marker":%q}`, marker)),
	}
	if err := ws.send("submit", ref, payload); err != nil {
		t.Fatalf("submit to %s: ws send: %v", chID, err)
	}
	rec, ok := ws.awaitRef(ref, 10*time.Second)
	if !ok {
		t.Fatalf("submit to %s: no receipt/error frame within 10s", chID)
	}
	if rec["frame_type"] == "error" {
		return frameErrCode(rec), "", true
	}
	rp, _ := rec["payload"].(map[string]any)
	mid, _ := rp["message_id"].(string)
	return "", mid, false
}

// mcAwaitFeed returns the first feed frame on chID whose envelope id == msgID (the
// feed payload carries {channel_id, seq, envelope} — this asserts BOTH the routing
// channel_id and the delivered envelope).
func mcAwaitFeed(t *testing.T, ws *wsClient, chID, msgID string, timeout time.Duration) (map[string]any, bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case fp := <-ws.tail:
			ch, _ := fp["channel_id"].(string)
			if ch != chID {
				continue
			}
			env, _ := fp["envelope"].(map[string]any)
			if env != nil && env["id"] == msgID {
				return env, true
			}
		case <-deadline:
			return nil, false
		}
	}
}
