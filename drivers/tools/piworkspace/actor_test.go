package piworkspace

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workspaceproto "github.com/wanpengxie/atoll/drivers/tools/piworkspace/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
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
	return actorbase.NewMsg(actorbase.OriginMailbox, ctx, message.Envelope{ID: id, Kind: message.KindRequest, Type: typ, Payload: json.RawMessage(`{"body":{}}`)})
}

func TestSchedulerOverlapsReadsAndKeepsWritesFIFO(t *testing.T) {
	sys := &schedulerTestSys{jobs: make(chan actorbase.Msg, 8), failures: map[message.ID]string{}}
	release := make(chan struct{})
	started := make(chan string, 8)
	var activeReads atomic.Int32
	var activeWrites atomic.Int32
	execute := func(msg actorbase.Msg) {
		if isReadWord(msg.Type) {
			activeReads.Add(1)
		} else {
			activeWrites.Add(1)
		}
		started <- string(msg.ID)
		<-release
		if isReadWord(msg.Type) {
			activeReads.Add(-1)
		} else {
			activeWrites.Add(-1)
		}
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
	if !seen["w1"] || seen["w2"] || !seen["r1"] || !seen["r2"] || activeReads.Load() != 2 || activeWrites.Load() != 1 {
		t.Fatalf("starts=%v active reads=%d writes=%d", seen, activeReads.Load(), activeWrites.Load())
	}
	close(release)
	select {
	case id := <-started:
		if id != "w2" {
			t.Fatalf("second write start=%s", id)
		}
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

func TestSchedulerDropsCancelledQueuedWriteAndReportsCapacity(t *testing.T) {
	sys := &schedulerTestSys{jobs: make(chan actorbase.Msg, 4), failures: map[message.ID]string{}}
	release := make(chan struct{})
	started := make(chan string, 4)
	execute := func(msg actorbase.Msg) { started <- string(msg.ID); <-release }
	done := make(chan error, 1)
	go func() { done <- runScheduler(sys, Config{MaxConcurrency: 2, QueueCapacity: 1}, execute, func() {}) }()
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

func TestSafeWorkspacePathRejectsLexicalAndSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "outside-link")); err != nil {
		t.Fatal(err)
	}
	if err := safeWorkspacePath(root, "nested/new.txt"); err != nil {
		t.Fatalf("safe new path rejected: %v", err)
	}
	for _, path := range []string{"../escape", filepath.Join(root, "absolute"), "outside-link/file.txt"} {
		if err := safeWorkspacePath(root, path); err == nil {
			t.Fatalf("unsafe path %q accepted", path)
		}
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
	for _, raw := range []json.RawMessage{json.RawMessage(`{"pattern":"x","path":"../escape"}`), json.RawMessage(`{"pattern":"x","path":"/absolute"}`)} {
		path, _, err := decodeArgs(workspaceproto.TypeGrep, raw)
		if err != nil {
			t.Fatal(err)
		}
		if err := safeWorkspacePath(t.TempDir(), path); err == nil {
			t.Fatalf("unsafe search root accepted: %s", raw)
		}
	}
}

func TestManifestPublishesSearchAndOnlyRunnablePowerShell(t *testing.T) {
	words := manifest().Words
	for _, word := range []string{workspaceproto.TypeGrep, workspaceproto.TypeFind, workspaceproto.TypeLS} {
		if spec, ok := words[word]; !ok || len(spec.InputSchema) == 0 || spec.Description == "" {
			t.Fatalf("missing manifest word %s: %+v", word, spec)
		}
	}
	_, advertised := words[workspaceproto.TypePowerShell]
	if advertised != powerShellAvailable() || isWord(workspaceproto.TypePowerShell) != advertised {
		t.Fatalf("powershell advertised=%v available=%v", advertised, powerShellAvailable())
	}
}
