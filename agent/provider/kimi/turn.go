package kimi

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// turnQueueCap bounds the turn backlog. On overflow the OLDEST queued turn
// is evicted (newest input wins) and a system-visibility note records the
// drop — Receive never blocks (cell serial contract).
const turnQueueCap = 64

type terminalEmittedError struct {
	cause error
}

func (e terminalEmittedError) Error() string {
	if e.cause == nil {
		return ""
	}
	return e.cause.Error()
}

func (e terminalEmittedError) Unwrap() error { return e.cause }

func isTerminalEmittedError(err error) bool {
	var handled terminalEmittedError
	return errors.As(err, &handled)
}

// turnItem is one mailbox envelope awaiting serial execution by the
// private LLM loop.
type turnItem struct {
	env message.Envelope
}

// correlationID resolves the correlation anchor for a turn: the trigger
// envelope's correlation id, falling back to its own id.
func (t turnItem) correlationID() message.ID {
	return behavior.CorrelationID("", t.env.CorrelationID, t.env.ID)
}

// enqueueTurn pushes a turn without ever blocking: on overflow the oldest
// queued turn is evicted (newest input wins) and a system-visibility note
// records the drop.
func (b *Bridge) enqueueTurn(env message.Envelope) {
	item := turnItem{env: env}
	select {
	case b.turnQ <- item:
		return
	default:
	}
	// Queue full: evict the oldest, then push (both non-blocking — the
	// private loop is the only consumer and Receive the only producer).
	var dropped turnItem
	select {
	case dropped = <-b.turnQ:
	default:
	}
	select {
	case b.turnQ <- item:
	default:
	}
	if dropped.env.ID != "" {
		payload := map[string]any{
			"text":        fmt.Sprintf("turn queue overflow: dropped oldest trigger %s (%s)", dropped.env.ID, dropped.env.Type),
			"next_action": "dropped",
		}
		// Best-effort note; the write seam is concurrency-safe.
		_ = b.emitEnvelope(context.Background(), turnItem{env: dropped.env}, "agent.text", message.VisibilitySystem, payload)
	}
}

// runLoop is the private LLM loop — the client edge where blocking is
// legal. Turns run strictly serially (go-kimi's Agent is not safe for
// concurrent Run; one session per actor).
func (b *Bridge) runLoop(ctx context.Context) {
	defer b.loopWG.Done()
	turns := 0
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-b.turnQ:
			if !ok {
				return
			}
			turns++
			if err := b.runTurn(ctx, item, turns); err != nil {
				if isTerminalEmittedError(err) {
					continue // failure already surfaced as a terminal envelope
				}
				if errors.Is(err, context.Canceled) {
					return
				}
				// Unrecoverable plumbing failure: record + die positively on
				// the next contact.
				b.fatalMu.Lock()
				b.fatal = err
				b.fatalMu.Unlock()
				return
			}
		}
	}
}

// runTurn drives one Agent.Run call: it composes the user input from
// the trigger envelope, kicks the agent in a goroutine, and consumes
// wire events until the turn completes or the agent errors out.
//
// turnIndex is 1-based; it is stamped on every envelope this turn emits
// so the UI / observability layer can order progress vs. text events.
func (b *Bridge) runTurn(ctx context.Context, item turnItem, turnIndex int) error {
	input := composeUserInput(item.env)

	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	turnCtx = context.WithValue(turnCtx, channelToolRuntimeKey{}, item)

	// agent.Run drives wire events into wireCh; consumeWire collates
	// them into envelopes. We signal turn completion via an explicit
	// `agentDone` channel rather than closing wireCh — closing wireCh
	// would prevent the next runTurn call from reusing the same agent.
	agentDone := make(chan struct{})
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- b.kagent.Run(turnCtx, input)
		close(agentDone)
	}()

	consumeErr := b.consumeWire(turnCtx, agentDone, item, turnIndex)
	if consumeErr != nil {
		cancel()
		select {
		case runErr := <-runErrCh:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				// Agent.Run failed (e.g. provider 429 / 500 / auth) and
				// consumeWire reported a missing TurnEnd. The provider
				// error is the meaningful signal — emit a public failed
				// terminal envelope so the LLM error surfaces in the
				// channel log instead of being swallowed by the
				// no-TurnEnd consumeErr.
				return b.emitTerminalLLMError(ctx, runErr, item.env.ID, item.correlationID())
			}
			return consumeErr
		case <-ctx.Done():
			return errors.Join(consumeErr, ctx.Err())
		}
	}
	runErr := <-runErrCh
	if runErr != nil {
		return b.emitTerminalLLMError(ctx, runErr, item.env.ID, item.correlationID())
	}
	return nil
}
