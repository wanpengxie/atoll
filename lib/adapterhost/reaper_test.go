package adapterhost

import (
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
)

// TestReapExpired_BoundsInflight proves the reaper clears in-flight requests
// past their deadline, keeps live ones, and leaves no-deadline ones alone
// (止泄漏). The in-flight cache IS the pending tracker (env.expires_at is the
// deadline; no parallel correlation entry).
func TestReapExpired_BoundsInflight(t *testing.T) {
	ms := func(v int64) *int64 { return &v }
	a := &adapterActor{inflight: map[behavior.CorrelationKey]*message.Envelope{}}
	a.inflight["exp"] = &message.Envelope{ID: "exp", ExpiresAt: ms(100)}
	a.inflight["live"] = &message.Envelope{ID: "live", ExpiresAt: ms(10000)}
	a.inflight["nodeadline"] = &message.Envelope{ID: "nodeadline"} // ExpiresAt nil
	a.reapExpired(500)
	if _, ok := a.inflight["exp"]; ok {
		t.Error("expired in-flight request NOT reaped (leak)")
	}
	if _, ok := a.inflight["live"]; !ok {
		t.Error("live in-flight request wrongly reaped")
	}
	if _, ok := a.inflight["nodeadline"]; !ok {
		t.Error("no-deadline request wrongly reaped (must stay until terminal)")
	}
}

// TestRemember_LazyReapsOnNewRequest proves the reaper runs LAZILY: a new
// request arriving sweeps already-expired entries (Redis-style memory hygiene),
// with NO self-scheduled ticker / no self-send. The cache stays bounded by
// activity, not by a timer.
func TestRemember_LazyReapsOnNewRequest(t *testing.T) {
	now := int64(1000)
	a := &adapterActor{
		clock:    func() time.Time { return time.UnixMilli(now) },
		inflight: map[behavior.CorrelationKey]*message.Envelope{},
	}
	past := int64(100)
	a.inflight["old"] = &message.Envelope{ID: "old", ExpiresAt: &past} // already expired

	a.remember(&message.Envelope{ID: "new"}) // new request → lazy sweep

	if _, ok := a.inflight["old"]; ok {
		t.Error("expired entry not lazily reaped on new request (leak)")
	}
	if _, ok := a.inflight["new"]; !ok {
		t.Error("new request not remembered")
	}
}
