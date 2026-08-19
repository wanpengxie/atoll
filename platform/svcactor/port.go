package svcactor

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/channel"
)

var ErrChannelClosed = errors.New("svcactor: channel closed")

type portRequest struct {
	ctx      context.Context
	caller   channel.ID
	frame    channel.Request
	progress func(channel.Progress)
	done     chan channel.Result
}

// Port belongs to one serving ChannelHost generation, not to one svcactor
// incarnation. Replacing the cell therefore leaves queued callers attached.
type Port struct {
	requests chan portRequest
	done     chan struct{}
	once     sync.Once
}

func NewPort() *Port {
	return &Port{requests: make(chan portRequest), done: make(chan struct{})}
}

func (p *Port) Call(ctx context.Context, caller channel.ID, frame channel.Request, onProgress func(channel.Progress)) (channel.Result, error) {
	if p == nil {
		return channel.Result{}, ErrChannelClosed
	}
	select {
	case <-p.done:
		return closedResult(), nil
	default:
	}
	req := portRequest{ctx: ctx, caller: caller, frame: cloneRequest(frame), progress: onProgress, done: make(chan channel.Result, 1)}
	select {
	case p.requests <- req:
	case <-ctx.Done():
		return channel.Result{}, ctx.Err()
	case <-p.done:
		return closedResult(), nil
	}
	select {
	case result := <-req.done:
		return result, nil
	case <-ctx.Done():
		return channel.Result{}, ctx.Err()
	case <-p.done:
		return closedResult(), nil
	}
}

func (p *Port) receive(ctx context.Context) (portRequest, error) {
	select {
	case <-p.done:
		return portRequest{}, ErrChannelClosed
	default:
	}
	select {
	case req := <-p.requests:
		return req, nil
	case <-ctx.Done():
		return portRequest{}, ctx.Err()
	case <-p.done:
		return portRequest{}, ErrChannelClosed
	}
}

func (p *Port) Close() { p.once.Do(func() { close(p.done) }) }

func cloneRequest(in channel.Request) channel.Request {
	in.Payload = append([]byte(nil), in.Payload...)
	return in
}

func closedResult() channel.Result {
	return channel.Result{Fail: &channel.Failure{Stage: channel.StageGate, Code: string(channel.GateChannelUnavailable), Detail: "channel generation closed"}}
}
