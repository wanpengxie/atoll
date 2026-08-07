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
}

func (e *engine) openConnection(ctx context.Context) (*connection, error) {
	p, err := e.cfg.processFactory(ctx, e.cfg)
	if err != nil {
		return nil, err
	}
	c := &connection{id: e.nextConnection.Add(1), process: p}
	c.rpc = newRPC(p)
	c.rpc.onNotification = func(method string, params json.RawMessage) {
		if !c.retired.Load() {
			e.handleNotification(c, method, params)
		}
	}
	c.rpc.onRequest = handleServerRequest
	c.rpc.onClose = func(err error) {
		c.dead.Store(true)
		if !c.retired.Load() && e.isCurrentObject(c) {
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
	if c.retired.Swap(true) {
		return
	}
	c.dead.Store(true)
	c.rpc.retire()
	c.process.stop()
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
