package svcactor

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/platform/peerproto"
	"github.com/wanpengxie/atoll/protocol/channel"
)

var ErrChannelClosed = errors.New("svcactor: channel closed")

type portRequest struct {
	ctx    context.Context
	caller channel.ID
	frame  peerproto.Request
	done   chan peerproto.Result
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

func (p *Port) Call(ctx context.Context, caller channel.ID, frame peerproto.Request) (peerproto.Result, error) {
	if p == nil {
		return peerproto.Result{}, ErrChannelClosed
	}
	req := portRequest{ctx: ctx, caller: caller, frame: cloneRequest(frame), done: make(chan peerproto.Result, 1)}
	select {
	case p.requests <- req:
	case <-ctx.Done():
		return peerproto.Result{}, ctx.Err()
	case <-p.done:
		return closedResult(), nil
	}
	select {
	case result := <-req.done:
		return result, nil
	case <-ctx.Done():
		return peerproto.Result{}, ctx.Err()
	case <-p.done:
		return closedResult(), nil
	}
}

func (p *Port) receive(ctx context.Context) (portRequest, error) {
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

func cloneRequest(in peerproto.Request) peerproto.Request {
	in.Payload = append([]byte(nil), in.Payload...)
	return in
}

func closedResult() peerproto.Result {
	return peerproto.Result{Fail: &peerproto.Failure{Code: "channel_closed", Detail: "channel generation closed"}}
}
