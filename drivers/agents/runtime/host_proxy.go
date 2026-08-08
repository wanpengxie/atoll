package runtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type workerHost struct {
	engine     *Engine
	generation uint64
	life       context.Context
	sink       *eventSink
	logger     *slog.Logger
	mu         sync.Mutex
	active     driverproto.WorkerTurnTarget
	ready      chan struct{}
}

func (h *workerHost) activate(t driverproto.WorkerTurnTarget) {
	h.mu.Lock()
	h.active = t
	if h.ready != nil {
		close(h.ready)
		h.ready = nil
	}
	h.mu.Unlock()
}
func (h *workerHost) deactivate(t driverproto.WorkerTurnTarget) {
	h.mu.Lock()
	if h.active == t {
		h.active = driverproto.WorkerTurnTarget{}
	}
	h.mu.Unlock()
}

func (h *workerHost) GenerationLife() context.Context     { return h.life }
func (h *workerHost) Events() driverproto.EventSink       { return h.sink }
func (h *workerHost) Logger() *slog.Logger                { return h.logger }
func (h *workerHost) Tools() driverproto.ToolPort         { return toolPort{host: h} }
func (h *workerHost) Resources() driverproto.ResourcePort { return resourcePort{host: h} }

type effectRequest struct {
	generation  uint64
	target      driverproto.WorkerTurnTarget
	tool        *driverproto.ToolInvocation
	resource    *driverproto.ResourceInvocation
	fingerprint [32]byte
	reply       chan effectReply
}
type effectReply struct {
	tool     driverproto.ToolResult
	resource driverproto.ResourceResult
}

func (r *effectRequest) replyError(detail string) {
	select {
	case r.reply <- effectReply{tool: driverproto.ToolResult{Text: detail, IsError: true}, resource: driverproto.ResourceResult{Error: detail}}:
	default:
	}
}

type toolPort struct{ host *workerHost }

func (p toolPort) Catalog() []driverproto.ToolSpec {
	if p.host.engine.deps.Tools == nil {
		return nil
	}
	in := p.host.engine.deps.Tools.Catalog()
	out := make([]driverproto.ToolSpec, len(in))
	for i, v := range in {
		out[i] = driverproto.ToolSpec{Name: v.Name, Description: v.Description, Schema: append(json.RawMessage(nil), v.Schema...)}
	}
	return out
}
func (p toolPort) Invoke(ctx context.Context, target driverproto.WorkerTurnTarget, in driverproto.ToolInvocation) driverproto.ToolResult {
	r := &effectRequest{generation: p.host.generation, target: target, tool: &in, reply: make(chan effectReply, 1), fingerprint: fingerprint([]byte(in.Name), in.Params)}
	if !p.submit(ctx, r) {
		return driverproto.ToolResult{Text: "worker host closed", IsError: true}
	}
	select {
	case out := <-r.reply:
		return out.tool
	case <-ctx.Done():
		return driverproto.ToolResult{Text: ctx.Err().Error(), IsError: true}
	case <-p.host.life.Done():
		return driverproto.ToolResult{Text: "worker retired", IsError: true}
	}
}

type resourcePort struct{ host *workerHost }

func (p resourcePort) Invoke(ctx context.Context, target driverproto.WorkerTurnTarget, in driverproto.ResourceInvocation) driverproto.ResourceResult {
	r := &effectRequest{generation: p.host.generation, target: target, resource: &in, reply: make(chan effectReply, 1), fingerprint: fingerprint([]byte(in.Operation), []byte(in.ResourceID), in.Payload)}
	if !(toolPort{host: p.host}).submit(ctx, r) {
		return driverproto.ResourceResult{Error: "worker host closed"}
	}
	select {
	case out := <-r.reply:
		return out.resource
	case <-ctx.Done():
		return driverproto.ResourceResult{Error: ctx.Err().Error()}
	case <-p.host.life.Done():
		return driverproto.ResourceResult{Error: "worker retired"}
	}
}
func (p toolPort) submit(ctx context.Context, r *effectRequest) bool {
	for {
		p.host.mu.Lock()
		if p.host.active == r.target {
			p.host.mu.Unlock()
			break
		}
		ready := p.host.ready
		if ready == nil {
			ready = make(chan struct{})
			p.host.ready = ready
		}
		p.host.mu.Unlock()
		select {
		case <-ready:
			continue
		case <-ctx.Done():
			return false
		case <-p.host.life.Done():
			return false
		}
	}
	select {
	case p.host.engine.internal <- r:
		return true
	case <-ctx.Done():
		return false
	case <-p.host.life.Done():
		return false
	}
}

func (e *Engine) onEffectRequest(b *runtimeBook, r *effectRequest) {
	g := b.worker
	if g == nil || g.id != r.generation || g.turn == nil || g.turn.phase != turnActive || g.turn.target != r.target {
		r.replyError("effect target is not active")
		return
	}
	t := g.turn
	var callID driverproto.ProviderToolCallID
	resource := false
	if r.tool != nil {
		callID = r.tool.CallID
	} else {
		callID = r.resource.CallID
		resource = true
	}
	if callID == "" {
		r.replyError("effect call id required")
		return
	}
	key := effectKey{target: r.target, callID: callID, resource: resource}
	if old := t.effects[key]; old != nil {
		if old.fingerprint != r.fingerprint {
			r.replyError("effect call id reused with different parameters")
			e.protocolFault(b, g, "effect call id collision")
			return
		}
		if old.done {
			r.reply <- effectReply{tool: old.tool, resource: old.resource}
		} else {
			old.waiters = append(old.waiters, r)
		}
		return
	}
	t.effects[key] = &effectRow{fingerprint: r.fingerprint, waiters: []*effectRequest{r}}
	if t.transition != nil {
		t.transition.held = append(t.transition.held, r)
		return
	}
	e.admitEffect(b, t, r)
}

func (e *Engine) admitEffect(_ *runtimeBook, t *runtimeTurn, r *effectRequest) {
	if r.tool != nil {
		if e.deps.Tools == nil {
			e.rejectEffect(t, r, "tool bridge unavailable")
			return
		}
		lease, ok := e.deps.Tools.Acquire(t.scope)
		if !ok {
			e.rejectEffect(t, r, "effect scope revoked")
			return
		}
		row := e.effectRow(t, r)
		row.dispatched = true
		t.pendingEffects++
		e.tool(t.canonical, base.ToolEvent{CallID: string(r.tool.CallID), Phase: "started", Name: r.tool.Name})
		in := base.ToolInvocation{CallID: string(r.tool.CallID), Name: r.tool.Name, Params: append(json.RawMessage(nil), r.tool.Params...)}
		go func() {
			out := e.deps.Tools.Invoke(t.life, lease, in)
			e.sendInternal(effectDone{request: r, tool: driverproto.ToolResult{Text: out.Text, IsError: out.IsError}})
		}()
		return
	}
	if e.deps.Resources == nil {
		e.rejectEffect(t, r, "resource bridge unavailable")
		return
	}
	lease, ok := e.deps.Resources.Acquire(t.scope)
	if !ok {
		e.rejectEffect(t, r, "effect scope revoked")
		return
	}
	row := e.effectRow(t, r)
	row.dispatched = true
	t.pendingEffects++
	in := base.ResourceInvocation{CallID: string(r.resource.CallID), Operation: r.resource.Operation, ResourceID: r.resource.ResourceID, Payload: append(json.RawMessage(nil), r.resource.Payload...)}
	go func() {
		out := e.deps.Resources.Invoke(t.life, lease, in)
		e.sendInternal(effectDone{request: r, resource: driverproto.ResourceResult{Payload: append(json.RawMessage(nil), out.Payload...), Error: out.Error}})
	}()
}

func (e *Engine) onEffectDone(b *runtimeBook, d effectDone) {
	g := b.worker
	if g == nil || g.turn == nil {
		return
	}
	t := g.turn
	var callID driverproto.ProviderToolCallID
	resource := false
	if d.request.tool != nil {
		callID = d.request.tool.CallID
	} else {
		callID = d.request.resource.CallID
		resource = true
	}
	row := t.effects[effectKey{target: d.request.target, callID: callID, resource: resource}]
	if row == nil || row.done {
		return
	}
	row.done = true
	row.tool = d.tool
	row.resource = d.resource
	for _, w := range row.waiters {
		select {
		case w.reply <- effectReply{tool: d.tool, resource: d.resource}:
		default:
		}
	}
	row.waiters = nil
	if row.dispatched {
		row.dispatched = false
		if t.pendingEffects > 0 {
			t.pendingEffects--
		}
	}
	if d.request.tool != nil {
		status := "completed"
		if d.tool.IsError {
			status = "failed"
		}
		e.tool(t.canonical, base.ToolEvent{CallID: string(callID), Phase: "ended", Name: d.request.tool.Name, Status: status, Detail: d.tool.Text})
	}
	if t.phase == turnTerminal {
		e.finalizeTurnEnded(b, g, t)
	}
}

func (e *Engine) effectRow(t *runtimeTurn, r *effectRequest) *effectRow {
	var callID driverproto.ProviderToolCallID
	resource := false
	if r.tool != nil {
		callID = r.tool.CallID
	} else {
		callID = r.resource.CallID
		resource = true
	}
	return t.effects[effectKey{target: r.target, callID: callID, resource: resource}]
}

func (e *Engine) rejectEffect(t *runtimeTurn, r *effectRequest, detail string) {
	row := e.effectRow(t, r)
	if row == nil || row.done {
		r.replyError(detail)
		return
	}
	row.done = true
	row.tool = driverproto.ToolResult{Text: detail, IsError: true}
	row.resource = driverproto.ResourceResult{Error: detail}
	for _, waiter := range row.waiters {
		waiter.replyError(detail)
	}
	row.waiters = nil
}

var _ driverproto.WorkerHost = (*workerHost)(nil)
var _ driverproto.ToolPort = toolPort{}
var _ driverproto.ResourcePort = resourcePort{}
