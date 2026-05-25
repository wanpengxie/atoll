package kimibridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/adapters/framework"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestHeartbeatEmitsDaemonOnlineOfflineTransitions(t *testing.T) {
	var statusOK atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		if !statusOK.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(StatusResponse{
			Running:            true,
			Version:            "test",
			ExtensionConnected: false,
		})
	}))
	t.Cleanup(srv.Close)

	var ms atomic.Int64
	ms.Store(1_700_000_000_000)
	mod, err := New(Config{
		BaseURL: srv.URL,
		Now: func() time.Time {
			return time.UnixMilli(ms.Add(1))
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	chain := &recordingChain{}
	mod.client = NewClient(framework.NewHTTPClient(framework.HTTPClientConfig{
		BaseURL: srv.URL,
		Timeout: time.Second,
		Logger:  framework.NoopLogger{},
		Metrics: framework.NoopMetrics{},
	}))
	mod.mctx = &adapter.ModuleContext{
		AdapterActorID: DefaultAdapterActorID,
		ChannelID:      channel.ID("ch-test"),
		HarnessChain:   chain,
	}

	if report, err := mod.Heartbeat(context.Background()); err != nil || report.Available {
		t.Fatalf("offline heartbeat report=%+v err=%v", report, err)
	}
	if len(chain.Written()) != 0 {
		t.Fatalf("initial offline heartbeat emitted events: %+v", chain.Written())
	}

	statusOK.Store(true)
	if report, err := mod.Heartbeat(context.Background()); err != nil || report.Reason != "extension_disconnected" {
		t.Fatalf("online heartbeat report=%+v err=%v", report, err)
	}
	statusOK.Store(false)
	if report, err := mod.Heartbeat(context.Background()); err != nil || report.Available {
		t.Fatalf("offline transition report=%+v err=%v", report, err)
	}

	written := chain.Written()
	if len(written) != 2 {
		t.Fatalf("events=%d want online+offline", len(written))
	}
	if written[0].Type != TypeDaemonOnline || written[1].Type != TypeDaemonOffline {
		t.Fatalf("event types=%s,%s", written[0].Type, written[1].Type)
	}
	var payload struct {
		Available bool   `json:"available"`
		Reason    string `json:"reason"`
	}
	if err := json.Unmarshal(written[0].Payload, &payload); err != nil {
		t.Fatalf("decode online payload: %v", err)
	}
	if !payload.Available || payload.Reason != "extension_disconnected" {
		t.Fatalf("online payload=%+v", payload)
	}
}

type recordingChain struct {
	mu      sync.Mutex
	written []*message.Envelope
}

func (c *recordingChain) Write(_ context.Context, env *message.Envelope) (khar.WriteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *env
	c.written = append(c.written, &cp)
	return khar.WriteResult{MessageID: env.ID, Seq: int64(len(c.written))}, nil
}

func (c *recordingChain) Written() []*message.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*message.Envelope, len(c.written))
	copy(out, c.written)
	return out
}
