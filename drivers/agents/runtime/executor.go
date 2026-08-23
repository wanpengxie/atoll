package runtime

import (
	"context"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
)

// Runtime's closed cross-boundary effect vocabulary is implemented by this
// executor: SpawnWorker, WorkerOpen, WorkerStart, WorkerControl, Retire,
// PublishToBase, and InvokeBridge. Timer bookkeeping and slot clearing stay
// local to the supervisor and are intentionally not effects.

type publishFact interface{ publishFact() }
type publishTurnStarted struct {
	op   runtimeproto.OpID
	turn runtimeproto.TurnID
}
type publishTurnRejected struct {
	op           runtimeproto.OpID
	code, detail string
}
type publishTool struct {
	turn  runtimeproto.TurnID
	event runtimeproto.ToolEvent
}
type publishProgress struct {
	turn  runtimeproto.TurnID
	stage string
}
type publishTurnEnded struct {
	turn         runtimeproto.TurnID
	status       runtimeproto.TurnStatus
	text, detail string
	usage        runtimeproto.TurnUsage
}
type publishControlDone struct {
	op      runtimeproto.OpID
	turn    runtimeproto.TurnID
	verdict runtimeproto.ControlVerdict
	detail  string
}
type publishReadyDone struct {
	op     runtimeproto.OpID
	result runtimeproto.ReadyResult
}
type publishProviderLost struct {
	turn   runtimeproto.TurnID
	cause  runtimeproto.LostCause
	detail string
}
type publishSeed struct{ value []byte }
type publishFault struct{ code, detail string }

func (publishTurnStarted) publishFact()  {}
func (publishTurnRejected) publishFact() {}
func (publishTool) publishFact()         {}
func (publishProgress) publishFact()     {}
func (publishTurnEnded) publishFact()    {}
func (publishControlDone) publishFact()  {}
func (publishReadyDone) publishFact()    {}
func (publishProviderLost) publishFact() {}
func (publishSeed) publishFact()         {}
func (publishFault) publishFact()        {}

func (e *engine) publish(f publishFact) {
	switch x := f.(type) {
	case publishTurnStarted:
		e.events.TurnStarted(x.op, x.turn)
	case publishTurnRejected:
		e.events.TurnRejected(x.op, x.code, x.detail)
	case publishTool:
		e.events.Tool(x.turn, x.event)
	case publishProgress:
		e.events.Progress(x.turn, x.stage)
	case publishTurnEnded:
		e.events.TurnEnded(x.turn, x.status, x.text, x.detail, x.usage)
	case publishControlDone:
		e.events.ControlDone(x.op, x.turn, x.verdict, x.detail)
	case publishReadyDone:
		e.events.ReadyDone(x.op, x.result)
	case publishProviderLost:
		e.events.ProviderLost(x.turn, x.cause, x.detail)
	case publishSeed:
		e.events.ResumeSeedUpdated(append([]byte(nil), x.value...))
	case publishFault:
		e.events.RuntimeFault(x.code, x.detail)
	}
}

func (e *engine) spawnWorker(host driverproto.WorkerHost) (driverproto.Worker, error) {
	return e.provider.NewWorker(host)
}
func (e *engine) workerOpen(w driverproto.Worker, req driverproto.OpenRequest) {
	e.callWorker(func(ctx context.Context) { w.Open(ctx, req) })
}
func (e *engine) workerStart(w driverproto.Worker, req driverproto.StartRequest) {
	e.callWorker(func(ctx context.Context) { w.Start(ctx, req) })
}
func (e *engine) workerControl(w driverproto.Worker, req driverproto.ControlRequest) {
	e.callWorker(func(ctx context.Context) { w.Control(ctx, req) })
}
func (e *engine) retireWorker(generation uint64) { e.slot.retire(generation) }

func (e *engine) callWorker(fn func(context.Context)) {
	ctx, cancel := context.WithTimeout(e.root, e.policy.MethodCall)
	go func() { defer cancel(); fn(ctx) }()
}

func (e *engine) invokeBridge(r *callbackRequest, key string, start runtimeproto.StartCommand, steer runtimeproto.ControlCommand, steered bool) {
	scope := start.Scope
	if steered {
		scope = steer.Scope
	}
	go func() {
		out := callbackResult{}
		if r.kind == callbackTool {
			if e.deps.Tools == nil {
				out.tool = runtimeprotoToDriverTool(runtimeproto.ToolResult{Text: "tool bridge unavailable", IsError: true})
			} else {
				out.tool = runtimeprotoToDriverTool(e.deps.Tools.Invoke(r.ctx, scope, runtimeproto.ToolInvocation{CallID: r.callID, Name: r.tool.Name, Params: append([]byte(nil), r.tool.Params...)}))
			}
		} else {
			if e.deps.Resources == nil {
				out.resource = driverproto.ResourceResult{Error: "resource bridge unavailable"}
			} else {
				x := e.deps.Resources.Invoke(r.ctx, scope, runtimeproto.ResourceInvocation{CallID: r.callID, Operation: r.resource.Operation, ResourceID: r.resource.ResourceID, Payload: append([]byte(nil), r.resource.Payload...)})
				out.resource = driverproto.ResourceResult{Payload: append([]byte(nil), x.Payload...), Error: x.Error}
			}
		}
		if !e.inbox.push(classCompletion, callbackCompletion{generation: r.generation, key: key, result: out}) {
			respondCancelled(r, "runtime closed")
		}
	}()
}
