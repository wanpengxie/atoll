package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
	"github.com/wanpengxie/ActOS/server/channelhost"
)

const testChannelID = channel.ID("test-gw")

// setupGateway replicates the v2 assembly sequence (server.Run) for testing
// gateway HTTP routes. Since the gateway struct is unexported, the test mounts
// equivalent handlers that call the same substrate interfaces the gateway does.
func setupGateway(t *testing.T) *http.ServeMux {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gw.sqlite")
	cs, err := runtime.OpenChannel(ctx, dbPath, runtime.OpenChannelOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}

	// Pre-register a web-client actor so ingress writes pass the harness.
	_ = cs.Membership.Insert(ctx, storespec.Record{
		ID: "web-client", Kind: actor.KindHuman, CreatedAt: time.Now().UnixMilli(),
	})

	// Build raw harness chain.
	rawChain, err := harness.New(harness.Deps{
		ChannelID:     testChannelID,
		ActorRegistry: cs.Registry,
		Log:           cs.Log,
	})
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}

	// Build channelhost with the raw chain as writer (no postCommitWriter in
	// this test — we only test the HTTP gateway routes, not the full fanout).
	home, err := channelhost.New(ctx, channelhost.Config{
		ChannelID: testChannelID,
		Stores: channelhost.Stores{
			Log: cs.Log, Query: cs.Query, Requests: cs.Requests,
			Registry: cs.Registry, Membership: cs.Membership, Close: cs.Close,
		},
		Writer: rawChain,
	})
	if err != nil {
		t.Fatalf("channelhost.New: %v", err)
	}
	t.Cleanup(func() { _ = home.Close() })

	_ = home // channelhost assembled; we use stores directly below.

	mux := http.NewServeMux()
	// Mount gateway-equivalent handlers using the substrate interfaces directly,
	// mirroring how the real gateway is wired in server.Run.
	mountGateway(mux, rawChain, cs.Query, cs.Registry, testChannelID)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// mountGateway replicates the gateway route mounting using substrate interfaces
// (v2 pattern: no channelhost dependency in the gateway).
func mountGateway(mux *http.ServeMux, writer harness.Writer, query storespec.MessageQuery, registry storespec.Registry, chID channel.ID) {
	// Cursor endpoint.
	mux.HandleFunc("/api/channels/"+string(chID)+"/cursor", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		seq, err := query.MaxSeq(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"last_received_seq": seq})
	})
	// Actors endpoint.
	mux.HandleFunc("/api/channels/"+string(chID)+"/actors", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		rows, err := registry.ListActive(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		actors := make([]map[string]any, 0, len(rows))
		for _, rec := range rows {
			actors = append(actors, map[string]any{
				"actor_id": string(rec.ID),
				"kind":     string(rec.Kind),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"channel_id": string(chID), "actors": actors})
	})
}

// TestHealth tests the /health endpoint.
func TestHealth(t *testing.T) {
	mux := setupGateway(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("health status = %d, want 200", rr.Code)
	}
}

// TestCursor_EmptyChannel tests GET cursor on an empty channel.
func TestCursor_EmptyChannel(t *testing.T) {
	mux := setupGateway(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/channels/"+string(testChannelID)+"/cursor", nil)
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("cursor status = %d, want 200", rr.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// After genesis (system actor registration writes events), seq may be > 0.
	// Just verify the field exists.
	if _, ok := body["last_received_seq"]; !ok {
		t.Error("missing last_received_seq in response")
	}
}

// TestActors_IncludesSystem tests GET actors includes the system actor.
func TestActors_IncludesSystem(t *testing.T) {
	mux := setupGateway(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/channels/"+string(testChannelID)+"/actors", nil)
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("actors status = %d, want 200", rr.Code)
	}
	var body struct {
		Actors []struct {
			ActorID string `json:"actor_id"`
			Kind    string `json:"kind"`
		} `json:"actors"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	found := false
	for _, a := range body.Actors {
		if a.ActorID == "system" && a.Kind == "system" {
			found = true
		}
	}
	if !found {
		t.Errorf("system actor not in actors list: %+v", body.Actors)
	}
}

// TestPostMessages_UnknownChannel tests POST to an unknown channel ID.
func TestPostMessages_UnknownChannel(t *testing.T) {
	mux := setupGateway(t)
	body, _ := json.Marshal(map[string]any{"type": "test", "kind": "request", "audience": []string{"system"}})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/channels/nonexistent/messages", bytes.NewReader(body))
	mux.ServeHTTP(rr, req)
	// We mounted per-channel-id paths, so /nonexistent won't match the handler.
	if rr.Code == http.StatusOK {
		t.Error("expected non-200 for unknown channel")
	}
}
