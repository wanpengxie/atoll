package piworkspace

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workspaceproto "github.com/wanpengxie/atoll/drivers/tools/piworkspace/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

func TestDecodeArgsEnforcesAdvertisedClosedSchemaBeforePi(t *testing.T) {
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"path":"x","content":"ok","misspelled_option":true}`),
		json.RawMessage(`{"path":7,"content":"ok"}`),
	} {
		if _, _, err := decodeArgs("workspace.write", raw); err == nil {
			t.Fatalf("invalid write args accepted: %s", raw)
		}
	}
	if path, got, err := decodeArgs("workspace.write", json.RawMessage(`{"path":"x","content":"ok"}`)); err != nil || path != "x" || string(got) != `{"path":"x","content":"ok"}` {
		t.Fatalf("path=%q raw=%s err=%v", path, got, err)
	}
	for _, raw := range []json.RawMessage{json.RawMessage(`{"path":"x","offset":0}`), json.RawMessage(`{"path":"x","limit":0}`)} {
		if _, _, err := decodeArgs(workspaceproto.TypeRead, raw); err == nil {
			t.Fatalf("invalid pagination accepted: %s", raw)
		}
	}
}

type schedulerTestSys struct {
	actorbase.Sys
	jobs     chan actorbase.Msg
	mu       sync.Mutex
	failures map[message.ID]string
}

func (s *schedulerTestSys) Recv() (actorbase.Msg, error) {
	msg, ok := <-s.jobs
	if !ok {
		return actorbase.Msg{}, errors.New("stopped")
	}
	return msg, nil
}

func (s *schedulerTestSys) Fail(msg actorbase.Msg, code, _ string, _ ...map[string]any) (message.ID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures[msg.ID] = code
	return "failed", nil
}

func schedulerMsg(ctx context.Context, id message.ID, typ string) actorbase.Msg {
	return actorbase.NewBodyMsg(actorbase.OriginMailbox, ctx, message.Envelope{ID: id, Kind: message.KindRequest, Type: typ, Payload: json.RawMessage(`{}`)})
}

func TestSchedulerRunsAllWorkspaceWordsThroughOneBoundedPool(t *testing.T) {
	sys := &schedulerTestSys{jobs: make(chan actorbase.Msg, 8), failures: map[message.ID]string{}}
	release := make(chan struct{})
	started := make(chan string, 8)
	var active atomic.Int32
	execute := func(msg actorbase.Msg) {
		active.Add(1)
		started <- string(msg.ID)
		<-release
		active.Add(-1)
	}
	done := make(chan error, 1)
	go func() { done <- runScheduler(sys, Config{MaxConcurrency: 3, QueueCapacity: 8}, execute, func() {}) }()
	for _, msg := range []actorbase.Msg{
		schedulerMsg(context.Background(), "w1", workspaceproto.TypeWrite),
		schedulerMsg(context.Background(), "w2", workspaceproto.TypeEdit),
		schedulerMsg(context.Background(), "r1", workspaceproto.TypeRead),
		schedulerMsg(context.Background(), "r2", workspaceproto.TypeLS),
	} {
		sys.jobs <- msg
	}
	seen := map[string]bool{}
	deadline := time.After(time.Second)
	for len(seen) < 3 {
		select {
		case id := <-started:
			seen[id] = true
		case <-deadline:
			t.Fatalf("only started %v", seen)
		}
	}
	if len(seen) != 3 || active.Load() != 3 {
		t.Fatalf("starts=%v active=%d", seen, active.Load())
	}
	close(release)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second write did not start")
	}
	close(sys.jobs)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler leaked on close")
	}
}

func TestSchedulerDropsCancelledQueuedCallAndReportsCapacity(t *testing.T) {
	sys := &schedulerTestSys{jobs: make(chan actorbase.Msg, 4), failures: map[message.ID]string{}}
	release := make(chan struct{})
	started := make(chan string, 4)
	execute := func(msg actorbase.Msg) { started <- string(msg.ID); <-release }
	done := make(chan error, 1)
	go func() { done <- runScheduler(sys, Config{MaxConcurrency: 1, QueueCapacity: 1}, execute, func() {}) }()
	sys.jobs <- schedulerMsg(context.Background(), "w1", workspaceproto.TypeWrite)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first write did not start")
	}
	cancelCtx, cancel := context.WithCancel(context.Background())
	sys.jobs <- schedulerMsg(cancelCtx, "w2", workspaceproto.TypeWrite)
	time.Sleep(20 * time.Millisecond)
	cancel()
	sys.jobs <- schedulerMsg(context.Background(), "w3", workspaceproto.TypeWrite)
	time.Sleep(20 * time.Millisecond)
	close(release)
	time.Sleep(20 * time.Millisecond)
	close(sys.jobs)
	<-done
	sys.mu.Lock()
	defer sys.mu.Unlock()
	if sys.failures["w2"] != "cancelled" || sys.failures["w3"] != "capacity" {
		t.Fatalf("failures=%v", sys.failures)
	}
}

func TestSearchArgumentsValidateOnlyTheSearchRootAsAPath(t *testing.T) {
	for _, tc := range []struct {
		word string
		raw  json.RawMessage
		path string
	}{
		{workspaceproto.TypeGrep, json.RawMessage(`{"pattern":"../not-a-path.*","glob":"../*.go"}`), "."},
		{workspaceproto.TypeFind, json.RawMessage(`{"pattern":"../**/*.go","path":"dir with spaces"}`), "dir with spaces"},
		{workspaceproto.TypeLS, json.RawMessage(`{}`), "."},
	} {
		path, _, err := decodeArgs(tc.word, tc.raw)
		if err != nil || path != tc.path {
			t.Fatalf("%s path=%q err=%v", tc.word, path, err)
		}
	}
}

func TestManifestPublishesSearchAndOnlyRunnablePowerShell(t *testing.T) {
	words := manifest().Words
	for _, word := range []string{workspaceproto.TypeGrep, workspaceproto.TypeFind, workspaceproto.TypeLS} {
		if spec, ok := words[word]; !ok || len(spec.InputSchema) == 0 || spec.Description == "" {
			t.Fatalf("missing manifest word %s: %+v", word, spec)
		}
		var schema struct {
			Required   []string `json:"required"`
			Properties struct {
				Input json.RawMessage `json:"input"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(words[word].InputSchema, &schema); err != nil || len(schema.Properties.Input) == 0 || len(schema.Required) != 2 {
			t.Fatalf("%s does not publish its word-specific transport schema: %s", word, words[word].InputSchema)
		}
	}
	_, advertised := words[workspaceproto.TypePowerShell]
	if advertised != powerShellAvailable() || isWord(workspaceproto.TypePowerShell) != advertised {
		t.Fatalf("powershell advertised=%v available=%v", advertised, powerShellAvailable())
	}
}

func TestConfigControlsBridgeIdleLifetime(t *testing.T) {
	cfg, err := parseConfig(json.RawMessage(`{"node":"node","max_concurrency":2,"queue_capacity":3,"idle_ms":17}`))
	if err != nil || cfg.IdleMS != 17 {
		t.Fatalf("config=%+v err=%v", cfg, err)
	}
	if _, err := parseConfig(json.RawMessage(`{"idle_ms":-1}`)); err == nil {
		t.Fatal("negative idle lifetime accepted")
	}
}

func TestBridgePoolIsPerSessionAndIdleCacheOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := newBridgePool(ctx, Config{Node: "node", IdleMS: 10}, registry.Deps{WorkspaceDir: t.TempDir()})
	t.Cleanup(pool.Close)
	first, err := pool.Acquire("branch-one")
	if err != nil {
		t.Fatal(err)
	}
	again, err := pool.Acquire("branch-one")
	if err != nil || again != first {
		t.Fatalf("same session did not reuse Bridge: first=%p again=%p err=%v", first, again, err)
	}
	second, err := pool.Acquire("branch-two")
	if err != nil || second == first {
		t.Fatalf("different sessions shared Bridge: first=%p second=%p err=%v", first, second, err)
	}
	pool.Release("branch-one", first)
	pool.Release("branch-one", again)
	pool.Release("branch-two", second)
	time.Sleep(30 * time.Millisecond)
	pool.mu.Lock()
	remaining := len(pool.sessions)
	pool.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("idle Bridge cache retained %d sessions", remaining)
	}
}

func TestBridgePoolEnsureReplacesExitedProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := newBridgePool(ctx, Config{Node: "node", IdleMS: 1000}, registry.Deps{WorkspaceDir: t.TempDir()})
	t.Cleanup(pool.Close)
	first, err := pool.Acquire("branch")
	if err != nil {
		t.Fatal(err)
	}
	pool.Release("branch", first)
	first.Close()
	replacement, err := pool.Acquire("branch")
	if err != nil {
		t.Fatal(err)
	}
	if replacement == first || !replacement.Alive() {
		t.Fatalf("ensure reused an exited Bridge: first=%p replacement=%p", first, replacement)
	}
	pool.Release("branch", replacement)
}
