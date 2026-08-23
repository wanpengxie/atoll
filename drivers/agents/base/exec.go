package base

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/protocol/message"
)

func ExecFace(sys actorbase.Sys, fastPathWindow time.Duration) *metatool.Exec {
	jobs, _ := sys.(actorbase.JobTable)
	return &metatool.Exec{Jobs: jobs, Call: transientCallBridge(jobs), Clock: time.Now, FastPathWindow: fastPathWindow}
}

func transientCallBridge(jobs actorbase.JobTable) metatool.CallFunc {
	return func(ctx context.Context, spec behavior.RequestSpec, window time.Duration) (*message.Envelope, bool, error) {
		id, err := jobs.Submit(spec)
		if err != nil {
			return nil, false, err
		}
		env, ok, err := jobs.Await(ctx, id, window)
		if err != nil {
			_ = jobs.Cancel(id)
			return nil, false, err
		}
		if !ok {
			_ = jobs.Cancel(id)
			return nil, false, nil
		}
		return env, true, nil
	}
}

type terminalCandidate struct {
	fail         bool
	code, detail string
	value        any
}

type executor struct {
	sys     actorbase.Sys
	runtime runtimeproto.Runtime
	vault   *effectcap.Vault
	mu      sync.Mutex
	handles map[string]actorbase.Msg
	writers chan struct{}
}

func newExecutor(sys actorbase.Sys, vault *effectcap.Vault) *executor {
	return &executor{sys: sys, vault: vault, handles: map[string]actorbase.Msg{}, writers: make(chan struct{}, 8)}
}
func (x *executor) bindRuntime(rt runtimeproto.Runtime) { x.runtime = rt }
func (x *executor) install(msg actorbase.Msg) {
	x.mu.Lock()
	x.handles[string(msg.ID)] = msg
	x.mu.Unlock()
}
func (x *executor) release(id string) { x.mu.Lock(); delete(x.handles, id); x.mu.Unlock() }
func (x *executor) handle(id string) (actorbase.Msg, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	msg, ok := x.handles[id]
	return msg, ok
}

// terminal is the only Sys.Reply/Sys.Fail production write mouth. The
// semantic row is already gone when this is called; write completion never
// flows back into Base state.
func (x *executor) terminal(id string, c terminalCandidate) {
	msg, ok := x.handle(id)
	if !ok {
		return
	}
	go func() {
		x.writers <- struct{}{}
		defer func() { <-x.writers; x.release(id) }()
		var err error
		if c.fail {
			_, err = x.sys.Fail(msg, c.code, c.detail)
		} else {
			_, err = x.sys.Reply(msg, c.value)
		}
		if err != nil {
			slog.Warn("agent terminal write failed", "request", id, "error", err)
		}
	}()
}

func (x *executor) progress(id, status string, value any) {
	msg, ok := x.handle(id)
	if !ok {
		return
	}
	if _, err := x.sys.Progress(msg, status, value); err != nil {
		slog.Warn("agent progress write failed", "request", id, "error", err)
	}
}

func (x *executor) runtimeStart(v runtimeproto.StartCommand) error     { return x.runtime.Start(v) }
func (x *executor) runtimeControl(v runtimeproto.ControlCommand) error { return x.runtime.Control(v) }
func (x *executor) runtimeTerminate() error                            { return x.runtime.Terminate() }
func (x *executor) runtimeEnsureReady(op runtimeproto.OpID) error      { return x.runtime.EnsureReady(op) }
func (x *executor) revoke(scope effectcap.Scope)                       { x.vault.Revoke(scope) }
func (x *executor) persistSeed(value []byte)                           { persistSeed(x.sys, value) }
func (x *executor) persistSelection(value runtimeproto.TurnOptions)    { persistSelection(x.sys, value) }
