package codex

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base"
)

type connection struct {
	id      uint64
	process *childProcess
	rpc     *rpcClient
	retired atomic.Bool
	dead    atomic.Bool

	// Per-generation turn bookkeeping, guarded by engine.mu. It lives on the
	// connection — not on the engine — so that a dead process cannot leave a
	// live turn account behind: the account's reachability ends with the
	// generation that produced it (workspace-process binding law).
	startOp base.OpID
	turnOp  base.OpID
	turnID  string
	final   map[string]string
}

func (e *engine) openConnection(ctx context.Context) (*connection, error) {
	p, err := e.cfg.processFactory(ctx, e.cfg)
	if err != nil {
		return nil, err
	}
	c := &connection{id: e.nextConnection.Add(1), process: p, final: map[string]string{}}
	c.rpc = newRPC(p)
	c.rpc.onNotification = func(method string, params json.RawMessage) {
		if !c.retired.Load() {
			e.handleNotification(c, method, params)
		}
	}
	c.rpc.onRequest = handleServerRequest
	c.rpc.onClose = func(err error) {
		// Death observation, not death handling: request the single detach
		// transition; only the winner of that CAS reports the loss. Explicitly
		// retired generations lose the CAS and stay silent (Terminate 消音律).
		c.dead.Store(true)
		if e.detach(c) {
			e.events.ProviderLost(base.LostCrash, err.Error())
		}
	}
	c.rpc.start()
	params := map[string]any{"clientInfo": map[string]any{"name": "atoll", "title": "Atoll Codex Agent", "version": "1"}, "capabilities": map[string]any{"experimentalApi": true, "optOutNotificationMethods": deltaNotificationMethods()}}
	raw, err := c.rpc.call(ctx, "initialize", params, initializeTimeout)
	if err != nil {
		c.retire()
		return nil, err
	}
	var init struct {
		UserAgent string `json:"userAgent"`
	}
	_ = json.Unmarshal(raw, &init)
	if init.UserAgent == "" {
		init.UserAgent = "unknown"
	}
	e.cfg.Logger.Info("codex.app_server.initialized", "connection", c.id, "version", init.UserAgent)
	if err := c.rpc.notify("initialized", map[string]any{}); err != nil {
		c.retire()
		return nil, err
	}
	return c, nil
}

func (c *connection) retire() {
	if !c.retired.Swap(true) {
		c.dead.Store(true)
		c.rpc.retire()
	}
	c.process.stop()
}

func (c *connection) retireAsync() {
	if !c.retired.Swap(true) {
		c.dead.Store(true)
		// The whole teardown runs on its own goroutine: retireAsync is called
		// from inside rpc.onClose (the EOF observer's detach), and rpc.retire
		// re-enters closeWith — same-goroutine re-entry of its sync.Once would
		// deadlock, while a fresh goroutine merely waits it out.
		go func() {
			c.rpc.retire()
			c.process.stop()
		}()
	}
}

func handleServerRequest(method string, _ json.RawMessage) (any, *rpcError) {
	switch method {
	case "currentTime/read":
		return map[string]any{"currentTimeAt": time.Now().Unix()}, nil
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		return map[string]any{"decision": "decline"}, nil
	case "execCommandApproval", "applyPatchApproval":
		return map[string]any{"decision": "denied"}, nil
	case "item/permissions/requestApproval":
		return nil, &rpcError{Code: -32601, Message: "permission escalation unavailable"}
	default:
		return nil, &rpcError{Code: -32601, Message: "method not supported: " + method}
	}
}

func deltaNotificationMethods() []string {
	return []string{"item/agentMessage/delta", "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "item/plan/delta", "item/reasoning/summaryTextDelta", "item/reasoning/textDelta", "command/exec/outputDelta", "process/outputDelta", "thread/realtime/outputAudio/delta", "thread/realtime/transcript/delta"}
}
