package link

import (
	"errors"
	"fmt"
)

// controlExecution is the table's execution-position cell. Inline is reserved
// for zero-I/O bookkeeping; worker is for handlers that may touch storage,
// policy, or another layer.
type controlExecution uint8

const (
	controlExecutionInline controlExecution = iota + 1
	controlExecutionWorker
)

type controlGate uint8

const (
	controlGateNone controlGate = iota + 1
	controlGateSession
)

// controlStateNeed makes the three non-default state inputs visible in the
// table instead of hiding them in handler folklore.
type controlStateNeed uint8

const (
	controlStateNone controlStateNeed = iota + 1
	controlStateCandidate
	controlStateSessionGate
	controlStateProbe
)

type controlMessage struct {
	requestID string
	payload   any
}

type controlDispatchInput struct {
	// peerID is the authenticated connection identity. It is always passed to
	// handlers; no handler re-derives it from a payload.
	peerID  string
	session *sessionRecord
	link    *linkSession
}

type controlRoute struct {
	parse      func([]byte) (controlMessage, error)
	handle     func(controlDispatchInput, controlMessage)
	execution  controlExecution
	gate       controlGate
	state      controlStateNeed
	busy       func(controlDispatchInput, controlMessage)
	gateReject func(controlDispatchInput, controlMessage)
}

type controlRouter struct {
	rows        map[controlKind]controlRoute
	sessionGate func() bool
	malformed   func(controlKind, error)
	unknown     func(controlKind)
}

// newControlRouter is a completeness constructor, not a registration
// framework. Each endpoint supplies one explicit known-kind list and one plain
// map; construction fails unless their key sets are exactly equal and every
// required cell is present.
func newControlRouter(
	known []controlKind,
	rows map[controlKind]controlRoute,
	sessionGate func() bool,
	malformed func(controlKind, error),
	unknown func(controlKind),
) (*controlRouter, error) {
	expected := make(map[controlKind]struct{}, len(known))
	for _, kind := range known {
		if kind == "" {
			return nil, errors.New("link: empty known control kind")
		}
		if _, duplicate := expected[kind]; duplicate {
			return nil, fmt.Errorf("link: duplicate known control kind %q", kind)
		}
		expected[kind] = struct{}{}
	}
	if len(rows) != len(expected) {
		return nil, fmt.Errorf("link: control table has %d rows, want %d", len(rows), len(expected))
	}
	for kind := range expected {
		row, ok := rows[kind]
		if !ok {
			return nil, fmt.Errorf("link: control table missing row %q", kind)
		}
		if row.parse == nil || row.handle == nil {
			return nil, fmt.Errorf("link: control table row %q missing parse/handle", kind)
		}
		if row.execution != controlExecutionInline && row.execution != controlExecutionWorker {
			return nil, fmt.Errorf("link: control table row %q missing execution position", kind)
		}
		if row.gate != controlGateNone && row.gate != controlGateSession {
			return nil, fmt.Errorf("link: control table row %q missing gate decision", kind)
		}
		if row.state < controlStateNone || row.state > controlStateProbe {
			return nil, fmt.Errorf("link: control table row %q missing state declaration", kind)
		}
		if row.execution == controlExecutionWorker && row.busy == nil {
			return nil, fmt.Errorf("link: worker control row %q missing busy disposition", kind)
		}
		if row.gate == controlGateSession {
			if row.execution != controlExecutionWorker || row.gateReject == nil || sessionGate == nil {
				return nil, fmt.Errorf("link: gated control row %q is incomplete", kind)
			}
		}
	}
	for kind := range rows {
		if _, ok := expected[kind]; !ok {
			return nil, fmt.Errorf("link: control table has unknown extra row %q", kind)
		}
	}
	return &controlRouter{
		rows: rows, sessionGate: sessionGate, malformed: malformed, unknown: unknown,
	}, nil
}

func (r *controlRouter) dispatch(in controlDispatchInput, raw []byte) {
	kind := peekControlKind(raw)
	if kind == "" {
		if r.malformed != nil {
			r.malformed(kind, errors.New("control frame missing kind"))
		}
		return
	}
	row, known := r.rows[kind]
	if !known {
		if r.unknown != nil {
			r.unknown(kind)
		}
		return
	}
	msg, err := row.parse(raw)
	if err != nil {
		if r.malformed != nil {
			r.malformed(kind, err)
		}
		return
	}
	if row.execution == controlExecutionInline {
		row.handle(in, msg)
		return
	}
	in.link.submitControlTask(func() {
		// The only session gate is asked here, inside the worker immediately
		// before the handler. The admission predicate is never passed onward.
		if row.gate == controlGateSession && !r.sessionGate() {
			row.gateReject(in, msg)
			return
		}
		row.handle(in, msg)
	}, func() {
		row.busy(in, msg)
	})
}

func parseControlPart(
	raw []byte,
	requireRequestID bool,
	selectPart func(controlFrame) (any, bool),
) (controlMessage, error) {
	frame, err := decodeControl(raw)
	if err != nil {
		return controlMessage{}, err
	}
	part, ok := selectPart(frame)
	if !ok {
		return controlMessage{}, errors.New("link: control payload does not match kind")
	}
	if requireRequestID && frame.RequestID == "" {
		return controlMessage{}, errors.New("link: control envelope request_id is required")
	}
	return controlMessage{requestID: frame.RequestID, payload: part}, nil
}

func parseStoragePart(raw []byte, selectPart func(storageControlFrame) (any, bool)) (controlMessage, error) {
	frame, err := decodeStorageControl(raw)
	if err != nil {
		return controlMessage{}, err
	}
	part, ok := selectPart(frame)
	if !ok {
		return controlMessage{}, errors.New("link: storage control payload does not match kind")
	}
	return controlMessage{payload: part}, nil
}

func parseLanePart(raw []byte, selectPart func(laneControlFrame) (any, bool)) (controlMessage, error) {
	frame, err := decodeLaneControl(raw)
	if err != nil {
		return controlMessage{}, err
	}
	part, ok := selectPart(frame)
	if !ok {
		return controlMessage{}, errors.New("link: lane control payload does not match kind")
	}
	return controlMessage{payload: part}, nil
}

func parseAttach(raw []byte) (controlMessage, error) {
	return parseControlPart(raw, true, func(f controlFrame) (any, bool) { return f.Attach, f.Attach != nil })
}

func parseAttachReply(raw []byte) (controlMessage, error) {
	return parseControlPart(raw, true, func(f controlFrame) (any, bool) { return f.AttachReply, f.AttachReply != nil })
}

func parsePlanPull(raw []byte) (controlMessage, error) {
	return parseControlPart(raw, true, func(f controlFrame) (any, bool) { return f.PlanPull, f.PlanPull != nil })
}

func parsePlanReply(raw []byte) (controlMessage, error) {
	return parseControlPart(raw, true, func(f controlFrame) (any, bool) { return f.PlanReply, f.PlanReply != nil })
}

func parseProbe(raw []byte) (controlMessage, error) {
	return parseControlPart(raw, false, func(f controlFrame) (any, bool) { return f.Probe, f.Probe != nil })
}

func parseProbeReply(raw []byte) (controlMessage, error) {
	return parseControlPart(raw, false, func(f controlFrame) (any, bool) { return f.ProbeReply, f.ProbeReply != nil })
}

func parsePlanPoke(raw []byte) (controlMessage, error) {
	if !validPlanPoke(raw) {
		return controlMessage{}, errors.New("link: plan_poke must contain exactly the kind field")
	}
	return controlMessage{}, nil
}

func parseAllocRequest(raw []byte) (controlMessage, error) {
	return parseStoragePart(raw, func(f storageControlFrame) (any, bool) { return f.AllocRequest, f.AllocRequest != nil })
}

func parseAllocReply(raw []byte) (controlMessage, error) {
	return parseStoragePart(raw, func(f storageControlFrame) (any, bool) { return f.AllocReply, f.AllocReply != nil })
}

func parseCommitted(raw []byte) (controlMessage, error) {
	return parseStoragePart(raw, func(f storageControlFrame) (any, bool) { return f.Committed, f.Committed != nil })
}

func parseCommittedReply(raw []byte) (controlMessage, error) {
	return parseStoragePart(raw, func(f storageControlFrame) (any, bool) { return f.CommittedReply, f.CommittedReply != nil })
}

func parseReclaimAck(raw []byte) (controlMessage, error) {
	return parseStoragePart(raw, func(f storageControlFrame) (any, bool) { return f.ReclaimAck, f.ReclaimAck != nil })
}

func parseReclaimAckReply(raw []byte) (controlMessage, error) {
	return parseStoragePart(raw, func(f storageControlFrame) (any, bool) { return f.ReclaimAckReply, f.ReclaimAckReply != nil })
}

func parseReconcilePull(raw []byte) (controlMessage, error) {
	return parseStoragePart(raw, func(f storageControlFrame) (any, bool) { return f.ReconcilePull, f.ReconcilePull != nil })
}

func parseReconcilePullReply(raw []byte) (controlMessage, error) {
	return parseStoragePart(raw, func(f storageControlFrame) (any, bool) { return f.ReconcilePullReply, f.ReconcilePullReply != nil })
}

func parseReclaimRequest(raw []byte) (controlMessage, error) {
	return parseStoragePart(raw, func(f storageControlFrame) (any, bool) { return f.ReclaimRequest, f.ReclaimRequest != nil })
}

func parseReclaimReply(raw []byte) (controlMessage, error) {
	return parseStoragePart(raw, func(f storageControlFrame) (any, bool) { return f.ReclaimReply, f.ReclaimReply != nil })
}

func parseResolveCoord(raw []byte) (controlMessage, error) {
	return parseLanePart(raw, func(f laneControlFrame) (any, bool) { return f.ResolveCoord, f.ResolveCoord != nil })
}

func parseResolveCoordReply(raw []byte) (controlMessage, error) {
	return parseLanePart(raw, func(f laneControlFrame) (any, bool) { return f.ResolveCoordReply, f.ResolveCoordReply != nil })
}

var homeKnownControlKinds = []controlKind{
	ctrlAttach, ctrlPlanPull,
	ctrlAllocReply, ctrlReclaimReply,
	ctrlCommitted, ctrlReclaimAck, ctrlReconcilePull,
	ctrlResolveCoord, ctrlProbe, ctrlProbeReply,
}

var daemonKnownControlKinds = []controlKind{
	ctrlAttachReply, ctrlPlanReply, ctrlPlanPoke,
	ctrlAllocRequest, ctrlReclaimRequest,
	ctrlCommittedReply, ctrlReclaimAckReply, ctrlReconcilePullReply,
	ctrlResolveCoordReply, ctrlProbe, ctrlProbeReply,
}
