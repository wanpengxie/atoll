// Package book is the pure semantic ledger for Agent Base. It contains no
// channel handles, IO, clocks, goroutines, or lifecycle APIs.
package book

import (
	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
)

type RequestID string
type Location uint8

const (
	Buffered Location = iota
	Starting
	Workspace
	ControlPending
	ControlRunning
)

type Request struct {
	ID            RequestID
	Input         runtimeproto.Input
	Scope         effectcap.Scope
	Bytes         int
	Sender        string
	ParentID      string
	CorrelationID string
	ExplicitCAS   bool
	ExpectedTurn  runtimeproto.TurnID
	Location      Location
	TurnKind      runtimeproto.TurnKind
	Options       runtimeproto.TurnOptions
	Resumed       bool
	// LastHeartbeatMs is when (unix ms) this row last had a queued provisional
	// written for it (the admission frame counts as the first). The loop's
	// queued heartbeat throttles on it while the row waits in the buffer. Kept
	// as a plain integer: the book is transition state and carries no clock.
	LastHeartbeatMs int64
}

type TurnPhase uint8

const (
	TurnStarting TurnPhase = iota
	TurnActive
)

type Turn struct {
	Serial            uint64
	Phase             TurnPhase
	StartOp           runtimeproto.OpID
	ID                runtimeproto.TurnID
	Owner             RequestID
	Scope             effectcap.Scope
	AnchorParent      string
	AnchorCorrelation string
	// StartedAtMs is when (unix ms) the loop opened this turn (the Starting op
	// was issued); queued heartbeats report it so a waiting caller can see how
	// long the turn ahead of it has been running.
	StartedAtMs int64
}

type ActionKind uint8

const (
	ActionSteer ActionKind = iota
	ActionInterrupt
	ActionTerminate
	ActionRestart
	ActionCleanup
)

type ActionDisposition uint8

const (
	DispFailOwner ActionDisposition = iota
	DispRebufferOwner
)

type Action struct {
	Serial       uint64
	Kind         ActionKind
	Request      RequestID
	Op           runtimeproto.OpID
	Target       runtimeproto.TurnID
	ControlDone  bool
	TerminalSeen bool
	Disposition  ActionDisposition
	HolderID     RequestID
	OwnerAtAdmit RequestID
	SteerTarget  bool
	BufferIndex  int
}

type State struct {
	Requests    map[RequestID]*Request
	Buffer      []RequestID
	BufferBytes int
	Turn        *Turn
	Running     *Action
	Pending     *Action
	Faulted     bool
}

func New() State { return State{Requests: make(map[RequestID]*Request)} }

func (s *State) RemoveFromBuffer(id RequestID) bool {
	for i, candidate := range s.Buffer {
		if candidate != id {
			continue
		}
		if row := s.Requests[id]; row != nil {
			s.BufferBytes -= row.Bytes
		}
		copy(s.Buffer[i:], s.Buffer[i+1:])
		s.Buffer[len(s.Buffer)-1] = ""
		s.Buffer = s.Buffer[:len(s.Buffer)-1]
		return true
	}
	return false
}

func (s *State) InsertAt(idx int, id RequestID) {
	if idx < 0 || idx > len(s.Buffer) {
		idx = len(s.Buffer)
	}
	s.Buffer = append(s.Buffer, "")
	copy(s.Buffer[idx+1:], s.Buffer[idx:])
	s.Buffer[idx] = id
}

func (s *State) IndexInBuffer(id RequestID) int {
	for idx, candidate := range s.Buffer {
		if candidate == id {
			return idx
		}
	}
	return -1
}

func (s *State) RemoveRequest(id RequestID) *Request {
	row := s.Requests[id]
	if row == nil {
		return nil
	}
	if row.Location == Buffered {
		s.RemoveFromBuffer(id)
	}
	delete(s.Requests, id)
	return row
}
