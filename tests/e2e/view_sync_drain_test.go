//go:build e2e

package e2e

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_ViewSyncGapDrain_BacklogReplay covers phase-2 case 5.
//
// Wiring under test: every POST /messages flows through daemonbus →
// daemon harness chain → channel.sqlite (messages + view_sync_outbox).
// The transit.Pusher polls the outbox at ~50ms cadence, emits
// viewsync.push frames, and the server's viewcache.Apply CAS commits
// into view_cache_messages. After a server restart the daemon must
// reconnect (supervisor) AND any still-pending outbox rows must drain
// in monotonic seq order so view_cache_messages stays gap-free.
//
// Regression target:
//   - Daemon supervisor stops dialing after a single server crash
//     (would leave view_cache_messages permanently truncated past the
//     last pre-crash seq).
//   - Pusher resumes from the persisted outbox rather than re-sending
//     already-acked rows (would surface as seq duplicates or skipped
//     gaps the viewcache.Apply gap-resync path papers over).
//
// Scope: this case asserts the survives-restart contract; it does NOT
// directly inject channel.sqlite rows (which would require either the
// daemon to be stopped — the harness has no IPC seam — or running the
// write inside a goroutine that fights the daemon's outbox pusher for
// the same sqlite file). The "gap" we exercise is the WS disconnect
// window between server SIGINT and server restart.
func TestE2E_ViewSyncGapDrain_BacklogReplay(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "viewsync+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-viewsync-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-viewsync-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	// Phase 1 — POST a batch, confirm view_cache_messages mirrors it.
	const preBatch = 5
	preIDs := make([]string, preBatch)
	for i := 0; i < preBatch; i++ {
		resp := s.PostMessage(chID, "human.text",
			fmt.Sprintf("pre-restart-%d", i), "")
		if !resp.Accepted {
			t.Fatalf("pre-batch POST %d: %+v", i, resp)
		}
		preIDs[i] = resp.MessageID
	}

	// Give the pusher one tick to drain the outbox.
	harness.Eventually(t, "view_cache_messages contains pre-batch", 5*time.Second, func() bool {
		return countViewCacheHuman(t, s, chID) >= preBatch
	})
	if got := countViewCacheHuman(t, s, chID); got != preBatch {
		t.Fatalf("pre-restart view_cache count=%d want %d", got, preBatch)
	}

	// Phase 2 — restart server. Daemonbus supervisor must dial back and
	// resume. While the server is down POST /messages returns 503;
	// during the dead window the outbox sits idle on the daemon, with
	// nothing new flowing in.
	s.RestartServer()

	// Phase 3 — once reconnected, the daemon must accept new writes
	// AND the existing view_cache rows must still be present in
	// monotonic order.
	const postBatch = 5
	postIDs := make([]string, postBatch)
	harness.Eventually(t, "daemonbus reconnect lets POST succeed", 30*time.Second, func() bool {
		// Probe; the reconnect supervisor backs off exponentially. We
		// retry the POST until it stops returning 503 / 524. Probe
		// directly so we don't trip the harness's status-200 Fatal.
		return probePost(t, s, chID, "probe-after-restart")
	})
	for i := 0; i < postBatch; i++ {
		resp := s.PostMessage(chID, "human.text",
			fmt.Sprintf("post-restart-%d", i), "")
		if !resp.Accepted {
			t.Fatalf("post-batch POST %d after restart: %+v", i, resp)
		}
		postIDs[i] = resp.MessageID
	}

	// Total expected: preBatch + 1 probe + postBatch human.text rows.
	wantHuman := preBatch + 1 + postBatch
	harness.Eventually(t, "view_cache catches up", 10*time.Second, func() bool {
		return countViewCacheHuman(t, s, chID) >= wantHuman
	})
	got := countViewCacheHuman(t, s, chID)
	if got != wantHuman {
		t.Errorf("post-restart view_cache human count=%d want %d", got, wantHuman)
	}

	// Phase 4 — assert monotonicity. Read every row, check seq is
	// strictly increasing AND every pre/post id is present.
	seqs := listViewCacheSeqs(t, s, chID)
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Errorf("view_cache seq non-monotonic at i=%d: %d <= %d (full=%v)",
				i, seqs[i], seqs[i-1], seqs)
		}
	}
	body := readViewCacheBodies(t, s, chID)
	allTexts := strings.Join(body, "|")
	for _, expect := range append(append([]string{}, prefixed("pre-restart-", preBatch)...), prefixed("post-restart-", postBatch)...) {
		if !strings.Contains(allTexts, expect) {
			t.Errorf("view_cache missing payload %q in: %s", expect, allTexts)
		}
	}
}

func countViewCacheHuman(t *testing.T, s *harness.Stack, channelID string) int {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+s.ServerDBPath()+"?mode=ro")
	if err != nil {
		t.Fatalf("open server.db: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT envelope_json FROM view_cache_messages WHERE channel_id=?`, channelID)
	if err != nil {
		t.Fatalf("count view cache: %v", err)
	}
	defer func() { _ = rows.Close() }()
	n := 0
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("scan view cache: %v", err)
		}
		var env struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			continue
		}
		if env.Type == "human.text" {
			n++
		}
	}
	return n
}

func listViewCacheSeqs(t *testing.T, s *harness.Stack, channelID string) []int64 {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+s.ServerDBPath()+"?mode=ro")
	if err != nil {
		t.Fatalf("open server.db: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT seq FROM view_cache_messages WHERE channel_id=? ORDER BY seq ASC`, channelID)
	if err != nil {
		t.Fatalf("list seqs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := []int64{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatalf("scan seq: %v", err)
		}
		out = append(out, seq)
	}
	return out
}

func readViewCacheBodies(t *testing.T, s *harness.Stack, channelID string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+s.ServerDBPath()+"?mode=ro")
	if err != nil {
		t.Fatalf("open server.db: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT envelope_json FROM view_cache_messages WHERE channel_id=?`, channelID)
	if err != nil {
		t.Fatalf("read bodies: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := []string{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("scan body: %v", err)
		}
		out = append(out, raw)
	}
	return out
}

func prefixed(prefix string, n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return out
}

// probePost issues a single POST /messages without the harness fatal
// guard. Returns true on 2xx; false otherwise (503 while daemonbus
// reconnects, etc.).
func probePost(t *testing.T, s *harness.Stack, channelID, text string) bool {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"id":       "probe-" + text,
		"type":     "human.text",
		"payload":  json.RawMessage(`{"text":"` + text + `"}`),
		"audience": []string{"agent:channel-agent"},
	})
	req, err := http.NewRequest("POST", s.ServerURLBase()+"/api/channels/"+channelID+"/messages",
		bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.Client().Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode/100 == 2
}
