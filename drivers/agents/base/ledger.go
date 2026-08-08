package base

import "time"

type waitSet uint8

const (
	waitRPC waitSet = 1 << iota
	waitTurnTerminal
	waitWorkerReady
)

type actionKind uint8

const (
	actionInterrupt actionKind = iota
	actionStop
	actionTerminate
	actionRestart
)

type turnOpKind uint8

const (
	turnOpSteer turnOpKind = iota
	turnOpInterrupt
)

type turnOp struct {
	kind     turnOpKind
	blocking bool
}
type commitKind uint8

const (
	commitStart commitKind = iota
	commitSteer
)

type commitOp struct {
	kind       commitKind
	items      []*requestItem
	targetTurn uint64
}
type turnTerminal struct {
	status       TurnStatus
	text, detail string
}
type baseTurn struct {
	seq           uint64
	startOp       OpID
	turnID        TurnID
	owner, anchor *requestItem
	scope         EffectScope
	ops           map[OpID]*turnOp
	terminal      *turnTerminal
}
type baseAction struct {
	id         uint64
	kind       actionKind
	item       *requestItem
	op         OpID
	targetTurn uint64
	await      waitSet
}
type baseFault struct{ source, detail string }
type baseBook struct {
	turn             *baseTurn
	running, pending *baseAction
	buffer           requestBuffer
	committing       map[OpID]*commitOp
	fault            *baseFault
}
type deadlineLease struct {
	turnSeq, owner, revision uint64
	kind                     string
	timer                    *time.Timer
}
type deadlineFire struct {
	turnSeq, owner, revision uint64
	kind                     string
}
