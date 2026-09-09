# Native work-view controls

The Controller accepts work in an explicitly selected `view_id`. This identity
is supplied by the caller/organization layer; the Controller does not invent a
new independent session when the field is absent. `related_work_id` continues
that visible work's existing live view. Main/fork/merge/compact organization is
outside this implementation.

```json
{"text":"Investigate the build failure","view_id":"view:build","delivery":"receipt","submission_key":"build-1"}
```

A normal ask queues in that view. Different views share bounded Looper capacity
with round-robin scheduling. Same-sender buffered inputs may be batched, within
input and byte limits. Work IDs are stable result addresses; assignment IDs
identify executions and remain unchanged when steer transfers the owner.

- `agent.steer`: `text`, waiting request `target`, or `all: true`. `view_id` or
  `work_id` locates the view; a target also resolves it. Ambiguous scope fails.
  `all` gathers only the effective caller's waiting inputs in that view.
  `expected_turn_id` checks text steering against the expected execution.
- `agent.replace`: waiting `target`, `old_text`, `new_text`. CAS preserves queue
  position; replacement input records the replacing caller's identity.
- `agent.hold`: view/work/target, optional `duration_ms` (1–1800000; default
  1800000). A current-owner target interrupts and requeues only after its
  execution stops. The resumed input identifies prior potentially unknown
  effects. Holding someone else's target fails.
- `agent.unhold`: releases a hold and preserves an earlier interrupt freeze.
  An ordinary ask explicitly releases the view's freeze.
- `agent.interrupt`: a work target stops only that work; a view target interrupts
  that view. The empty compatibility form stops visible open works across views
  and returns per-view results. Busy control slots are not forcibly reassigned.

Looper admission and model consumption are distinct. A control response is
produced only after the Looper's admission decision; a final report carries the
same decisions to reconcile responses delivered out of order. Unknown admission
is not automatically requeued. Controls and finalization share an execution
lock; accepted pending input prevents normal completion until consumed at a
legal model boundary. Tool results close their assistant batch before a steer
input is appended. Cancellation and deadlines do not restart an episode or
replay tools.

Views hold runtime control state only. On Controller restart, old open work is
closed with `execution_unknown`; no model or tool is replayed. These controls do
not implement durable View history or the main supervisor.

Validation: native/workapi/agentlooper package tests cover ownership handoff,
report-before-ack, queue restoration, CAS, hold/rebuffer, scope isolation,
operation limits and duplicate controls. Looper tests exercise concurrent
seal/admission and injection during model/tool calls, with history pairing
checked at every subsequent model request. The Handle example exports the same
schemas as `native.Manifest()`.
