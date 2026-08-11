package sysactor

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
)

var ErrCallPortClosed = errors.New("sysactor: call port closed")

type callRequest struct {
	ctx     context.Context
	target  actor.ActorID
	word    string
	payload any
	done    chan callResult
}

type callResult struct {
	msg actorbase.Msg
	err error
}

// CallPort is the in-process operation channel owned by one system actor. It
// gives assembly code no pen: requests are queued to the actor, which authors
// the real Call with its own welded system identity.
type CallPort struct {
	requests chan callRequest
	done     chan struct{}
	once     sync.Once
}

func NewCallPort() *CallPort {
	return &CallPort{requests: make(chan callRequest), done: make(chan struct{})}
}

func (p *CallPort) Call(ctx context.Context, target actor.ActorID, word string, payload any) (actorbase.Msg, error) {
	if p == nil {
		return actorbase.Msg{}, ErrCallPortClosed
	}
	req := callRequest{ctx: ctx, target: target, word: word, payload: payload, done: make(chan callResult, 1)}
	select {
	case p.requests <- req:
	case <-ctx.Done():
		return actorbase.Msg{}, ctx.Err()
	case <-p.done:
		return actorbase.Msg{}, ErrCallPortClosed
	}
	select {
	case out := <-req.done:
		return out.msg, out.err
	case <-ctx.Done():
		return actorbase.Msg{}, ctx.Err()
	case <-p.done:
		return actorbase.Msg{}, ErrCallPortClosed
	}
}

func (p *CallPort) serve(sys actorbase.Sys) {
	defer p.close()
	for {
		select {
		case <-sys.Life().Done():
			return
		case req := <-p.requests:
			pending, err := sys.Call(req.target, req.word, req.payload)
			var msg actorbase.Msg
			if err == nil {
				msg, err = pending.Wait(req.ctx, 0)
				if err == nil && msg.Kind == "" {
					err = context.DeadlineExceeded
				}
			}
			req.done <- callResult{msg: msg, err: err}
		}
	}
}

func (p *CallPort) close() { p.once.Do(func() { close(p.done) }) }
