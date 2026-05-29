package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/adapter/futurereg"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// injectCallerContext wires the caller-side ModuleContext fields (Submit /
// Await / Watch / AwaitAll / Call / Abandon) onto the Manager-level router's
// shared FutureRegistry. The caller side is transport = in-daemon framework:
// Submit registers a future BEFORE the harness write (subscribe-before-send,
// §3.2) and the router's ObserveResponse feeds responses back into the same
// FutureRegistry.
//
// parent / correlation derivation: an embedded caller initiates a downstream
// request as a fresh root request from the adapter actor. The request id is a
// new RequestID; parent/correlation linkage to a triggering envelope is the
// concern of the leaf-migration callers (P3) — at this layer the request is
// self-rooted (correlation_id = its own id) so Await/Resolve work end to end.
func (m *Manager) injectCallerContext(mctx *adapter.ModuleContext, decl adapter.Declaration) {
	senderActor := decl.ActorID
	maxPendingMs := decl.MaxPendingMs

	submit := func(ctx context.Context, req adapter.CallRequest) (adapter.SubmitResult, error) {
		if req.TargetActor == "" {
			return adapter.SubmitResult{}, errors.New("framework: Submit TargetActor required")
		}
		if req.Type == "" {
			return adapter.SubmitResult{}, errors.New("framework: Submit Type required")
		}
		payload := req.Payload
		if len(payload) == 0 {
			payload = json.RawMessage(`{}`)
		}

		id := message.ID("req:" + uuid.NewString())
		now := m.cfg.Clock()
		estWaitMs := maxPendingMs
		var expiresAt *int64
		deadline := now.Add(time.Duration(maxPendingMs) * time.Millisecond)
		if req.Timeout > 0 {
			deadline = now.Add(req.Timeout)
		}
		exp := deadline.UnixMilli()
		expiresAt = &exp

		env := &message.Envelope{
			ID:            id,
			TS:            now.UnixMilli(),
			ChannelID:     m.cfg.ChannelID,
			Sender:        message.Sender{Kind: actor.KindTool, ID: senderActor},
			Kind:          message.KindRequest,
			Type:          req.Type,
			Payload:       payload,
			CorrelationID: id,
			Visibility:    message.VisibilityPrivate,
			Audience:      message.Audience{req.TargetActor},
			ExpiresAt:     expiresAt,
		}

		// subscribe-before-send: register the future BEFORE the write so a
		// response that races back cannot be missed (§3.2).
		handle := m.router.futures.Register(id)

		res, err := m.cfg.HarnessChain.Write(ctx, env)
		if err != nil {
			handle.Close()
			return adapter.SubmitResult{}, fmt.Errorf("framework: Submit chain write: %w", err)
		}
		if !res.Accepted() {
			handle.Close()
			return adapter.SubmitResult{}, fmt.Errorf("framework: Submit rejected: %s (%s)", res.RejectReason, res.RejectDetail)
		}

		ack := adapter.AckDescriptor{
			RequestID: id,
			Accepted:  true,
			Status:    "accepted",
			EstWaitMs: estWaitMs,
			Guidance:  fmt.Sprintf("accepted; to wait call await_result(request_id=%s); otherwise the result returns as a new message (parent_id=%s)", id, id),
			ToWait: adapter.ToWaitHint{
				Tool:   "await_result",
				Params: map[string]any{"request_id": id.String()},
			},
			IfNotWaiting: fmt.Sprintf("result returns as kind=response, parent_id=%s new turn trigger", id),
		}
		return adapter.SubmitResult{RequestID: id, Ack: ack}, nil
	}

	await := func(ctx context.Context, id adapter.RequestID, timeout time.Duration) (adapter.Terminal, error) {
		handle := m.router.futures.Register(id) // idempotent: rebinds the existing set
		if timeout <= 0 {
			timeout = time.Duration(maxPendingMs) * time.Millisecond
		}
		env, err := handle.Await(ctx, timeout)
		if err != nil {
			return adapter.Terminal{}, err
		}
		return terminalFromEnvelope(env), nil
	}

	watch := func(ctx context.Context, id adapter.RequestID) (adapter.Watcher, error) {
		handle := m.router.futures.Register(id)
		w, err := handle.Watch()
		if err != nil {
			return nil, err
		}
		return &watcherAdapter{inner: w}, nil
	}

	awaitAll := func(ctx context.Context, ids []adapter.RequestID, timeout time.Duration) ([]adapter.Outcome, error) {
		if timeout <= 0 {
			timeout = time.Duration(maxPendingMs) * time.Millisecond
		}
		out := make([]adapter.Outcome, len(ids))
		type res struct {
			i int
			o adapter.Outcome
		}
		ch := make(chan res, len(ids))
		for i, id := range ids {
			go func(i int, id adapter.RequestID) {
				t, err := await(ctx, id, timeout)
				o := adapter.Outcome{RequestID: id}
				if err != nil {
					o.Err = err
				} else {
					tt := t
					o.Terminal = &tt
				}
				ch <- res{i: i, o: o}
			}(i, id)
		}
		for range ids {
			r := <-ch
			out[r.i] = r.o
		}
		return out, nil
	}

	call := func(ctx context.Context, req adapter.CallRequest) (adapter.Terminal, error) {
		sr, err := submit(ctx, req)
		if err != nil {
			return adapter.Terminal{}, err
		}
		return await(ctx, sr.RequestID, req.Timeout)
	}

	abandon := func(id adapter.RequestID) {
		m.router.futures.Cancel(id)
	}

	mctx.Submit = submit
	mctx.Await = await
	mctx.Watch = watch
	mctx.AwaitAll = awaitAll
	mctx.Call = call
	mctx.Abandon = abandon
}

func terminalFromEnvelope(env *message.Envelope) adapter.Terminal {
	status := parseResponseStatus(env.Payload)
	return adapter.Terminal{
		Envelope: env,
		Status:   status,
		OK:       status == "completed",
	}
}

// watcherAdapter converts a futurereg.Watcher into an adapter.Watcher
// (re-emitting events with the adapter.WatchEvent shape).
type watcherAdapter struct {
	inner  futurereg.Watcher
	events chan adapter.WatchEvent
	done   chan struct{}
}

func (w *watcherAdapter) Events() <-chan adapter.WatchEvent {
	if w.events == nil {
		w.events = make(chan adapter.WatchEvent, 16)
		w.done = make(chan struct{})
		go func() {
			defer close(w.events)
			for ev := range w.inner.Events() {
				select {
				case w.events <- adapter.WatchEvent{Envelope: ev.Envelope, IsFinal: ev.IsFinal, Err: ev.Err}:
				case <-w.done:
					return
				}
			}
		}()
	}
	return w.events
}

func (w *watcherAdapter) Close() error {
	if w.done != nil {
		select {
		case <-w.done:
		default:
			close(w.done)
		}
	}
	return w.inner.Close()
}
