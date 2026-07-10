package behavior

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// recordingWriter is a concurrency-safe harness.Pen test double. It is
// relay-only: it never injects identity, so envelopes it records keep the
// builder's zero Sender/ChannelID (the real boundPen welds those).
type recordingWriter struct {
	mu        sync.Mutex
	writes    []*message.Envelope
	duplicate bool
	err       error
	signal    chan struct{} // closed-once notify on first write (nil = no notify)
	once      sync.Once
}

func (w *recordingWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.mu.Lock()
	w.writes = append(w.writes, env)
	dup := w.duplicate
	err := w.err
	w.mu.Unlock()
	if w.signal != nil {
		w.once.Do(func() { close(w.signal) })
	}
	if err != nil {
		return harness.WriteResult{}, err
	}
	r := harness.WriteResult{MessageID: env.ID}
	if dup {
		r.RejectReason = harness.HarnessTerminalDuplicate
	}
	return r, nil
}

func (w *recordingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.writes)
}

func (w *recordingWriter) last() *message.Envelope {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.writes) == 0 {
		return nil
	}
	return w.writes[len(w.writes)-1]
}

func fixedClock(ms int64) func() time.Time {
	return func() time.Time { return time.UnixMilli(ms) }
}

func testSender() message.Sender {
	return message.Sender{Kind: actor.Kind("agent"), ID: actor.ActorID("caller-1")}
}

// newRequest builds a kind=request envelope. expiresAt nil = no deadline.
func newRequest(id message.ID, expiresAt *int64) *message.Envelope {
	return &message.Envelope{
		ID:         id,
		TS:         0,
		ChannelID:  channel.ID("ch-1"),
		Sender:     testSender(),
		Kind:       message.KindRequest,
		Type:       "ask",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.Visibility("channel"),
		Audience:   message.Audience{actor.ActorID("svc")},
		ExpiresAt:  expiresAt,
	}
}

func responseFor(req *message.Envelope, status string) *message.Envelope {
	payload, _ := json.Marshal(map[string]string{"status": status})
	return &message.Envelope{
		ID:        message.ID("resp-" + string(req.ID)),
		ChannelID: req.ChannelID,
		Sender:    message.Sender{Kind: actor.Kind("agent"), ID: actor.ActorID("svc")},
		Kind:      message.KindResponse,
		Type:      req.Type,
		Payload:   payload,
		ParentID:  req.ID,
	}
}

// pendingLen reads the (cell-goroutine-owned) pending map for assertions. The
// test calls this only between synchronous Arm/Match/Stop calls — i.e. on the
// "cell goroutine" — so it observes the same single-threaded contract.
