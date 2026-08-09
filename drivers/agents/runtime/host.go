package runtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type driverFact struct {
	generation uint64
	event      driverproto.DriverEvent
}

type generationSink struct {
	mu                    sync.Mutex
	sealed                bool
	observationDropLogged bool
	generation            uint64
	queue                 *inbox
	gate                  *hostAdmission
	logger                *slog.Logger
}

type hostAdmission struct{ sealed atomic.Bool }

func (s *generationSink) Publish(event driverproto.DriverEvent) bool {
	if event == nil || s.gate.sealed.Load() {
		return false
	}
	s.mu.Lock()
	if s.sealed {
		s.mu.Unlock()
		return false
	}
	g := s.generation
	s.mu.Unlock()
	if a, ok := event.(driverproto.Activity); ok {
		if s.queue.pushActivity(g, a.Target) {
			return true
		}
		return s.dropObservation("activity")
	}
	event = cloneDriverEvent(event)
	if isObservation(event) {
		if s.queue.push(classObservation, driverFact{generation: g, event: event}) {
			return true
		}
		return s.dropObservation(observationName(event))
	}
	if s.queue.push(classGeneral, driverFact{generation: g, event: event}) {
		return true
	}
	s.mu.Lock()
	s.sealed = true
	s.gate.sealed.Store(true)
	s.mu.Unlock()
	s.queue.latchFault(protocolFault{generation: g, code: "overloaded", detail: "provider lifecycle ingress capacity exceeded"})
	return false
}

func (s *generationSink) dropObservation(kind string) bool {
	if s.queue.isSealed() {
		return false
	}
	s.mu.Lock()
	first := !s.observationDropLogged
	s.observationDropLogged = true
	s.mu.Unlock()
	if first {
		s.logger.Warn("agent provider observation dropped under ingress pressure", "generation", s.generation, "kind", kind)
	}
	return true
}

func isObservation(event driverproto.DriverEvent) bool {
	switch event.(type) {
	case driverproto.Tool, driverproto.Diagnostic:
		return true
	default:
		return false
	}
}

func observationName(event driverproto.DriverEvent) string {
	if _, ok := event.(driverproto.Tool); ok {
		return "tool"
	}
	return "diagnostic"
}

func (s *generationSink) seal() {
	s.mu.Lock()
	s.sealed = true
	s.gate.sealed.Store(true)
	s.mu.Unlock()
}

func cloneDriverEvent(v driverproto.DriverEvent) driverproto.DriverEvent {
	switch x := v.(type) {
	case driverproto.SeedUpdated:
		x.Value = append([]byte(nil), x.Value...)
		return x
	case driverproto.Tool:
		return x
	default:
		return v
	}
}

type callbackKind uint8

const (
	callbackTool callbackKind = iota
	callbackResource
)

type callbackRequest struct {
	generation uint64
	kind       callbackKind
	target     driverproto.WorkerTurnTarget
	callID     string
	tool       driverproto.ToolInvocation
	resource   driverproto.ResourceInvocation
	ctx        context.Context
	response   chan callbackResult
}
type callbackResult struct {
	tool     driverproto.ToolResult
	resource driverproto.ResourceResult
}
type callbackCompletion struct {
	generation uint64
	key        string
	result     callbackResult
}

type toolPort struct {
	generation uint64
	q          *inbox
	catalog    []driverproto.ToolSpec
	gate       *hostAdmission
}

func (p *toolPort) Catalog() []driverproto.ToolSpec {
	out := make([]driverproto.ToolSpec, len(p.catalog))
	copy(out, p.catalog)
	for i := range out {
		out[i].Schema = append(json.RawMessage(nil), out[i].Schema...)
	}
	return out
}
func (p *toolPort) Invoke(ctx context.Context, target driverproto.WorkerTurnTarget, in driverproto.ToolInvocation) driverproto.ToolResult {
	if p.gate.sealed.Load() {
		return driverproto.ToolResult{Text: "host callback unavailable", IsError: true}
	}
	r := &callbackRequest{generation: p.generation, kind: callbackTool, target: target, callID: string(in.CallID), tool: cloneToolInvocation(in), ctx: ctx, response: make(chan callbackResult, 1)}
	if !p.q.push(classCallback, r) {
		if !p.q.isSealed() {
			p.gate.sealed.Store(true)
			p.q.latchFault(protocolFault{generation: p.generation, code: "callback_overloaded", detail: "host callback ingress capacity exceeded"})
		}
		return driverproto.ToolResult{Text: "host callback unavailable", IsError: true}
	}
	select {
	case out := <-r.response:
		return out.tool
	case <-ctx.Done():
		return driverproto.ToolResult{Text: ctx.Err().Error(), IsError: true}
	}
}

type resourcePort struct {
	generation uint64
	q          *inbox
	gate       *hostAdmission
}

func (p *resourcePort) Invoke(ctx context.Context, target driverproto.WorkerTurnTarget, in driverproto.ResourceInvocation) driverproto.ResourceResult {
	if p.gate.sealed.Load() {
		return driverproto.ResourceResult{Error: "host callback unavailable"}
	}
	r := &callbackRequest{generation: p.generation, kind: callbackResource, target: target, callID: string(in.CallID), resource: cloneResourceInvocation(in), ctx: ctx, response: make(chan callbackResult, 1)}
	if !p.q.push(classCallback, r) {
		if !p.q.isSealed() {
			p.gate.sealed.Store(true)
			p.q.latchFault(protocolFault{generation: p.generation, code: "callback_overloaded", detail: "host callback ingress capacity exceeded"})
		}
		return driverproto.ResourceResult{Error: "host callback unavailable"}
	}
	select {
	case out := <-r.response:
		return out.resource
	case <-ctx.Done():
		return driverproto.ResourceResult{Error: ctx.Err().Error()}
	}
}

func cloneToolInvocation(v driverproto.ToolInvocation) driverproto.ToolInvocation {
	v.Params = append(json.RawMessage(nil), v.Params...)
	return v
}
func cloneResourceInvocation(v driverproto.ResourceInvocation) driverproto.ResourceInvocation {
	v.Payload = append(json.RawMessage(nil), v.Payload...)
	return v
}

type workerHost struct {
	life      context.Context
	sink      *generationSink
	tools     *toolPort
	resources *resourcePort
	logger    *slog.Logger
}

func (h *workerHost) GenerationLife() context.Context     { return h.life }
func (h *workerHost) Events() driverproto.EventSink       { return h.sink }
func (h *workerHost) Tools() driverproto.ToolPort         { return h.tools }
func (h *workerHost) Resources() driverproto.ResourcePort { return h.resources }
func (h *workerHost) Logger() *slog.Logger                { return h.logger }
