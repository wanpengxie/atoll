// Package timerfire's fire-result mapping tests.
//
// The mechanism under test is the ONE thing this package exists to do: turn a
// harness.WriteResult into the schedule.FireSink tri-state the engine's run
// loop branches on (types.go's FireSink contract). The failure mode the
// contract is spelled out against is silent: a naive `_, err := pen.Write(...)`
// swallows a DETERMINISTIC reject into a false "success", the engine deletes
// the row, and the fire is lost with no trace. Nothing in the type system
// catches that — only a test driving a REAL harness (real 9-step chain, real
// sqlite messages table, real UNIQUE index) over the real sink can.
//
// Everything below therefore runs against harness.New over a real channel
// store; the only stubs are the membership authority (whose verdict IS the
// input variable) and the presence face the harness's receiver gate consults.
package timerfire_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/internal/store"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/timerfire"
)

const (
	fireMapChannelID channel.ID    = "C-timerfire-map"
	fireMapNowMs     int64         = 1_700_000_000_000
	fireMapAuthor    actor.ActorID = "agent:fire-author"
	fireMapAuthorKnd actor.Kind    = actor.KindAgent
	fireMapStranger  actor.ActorID = "agent:not-a-member"
)

// fireMapAuthority is the CollaborationAuthority face the sink consults before
// it ever touches a pen — the §7.4 exit guard. `admitted` is the closed set of
// ids that answer ok=true; `err` (when set) makes every call a store fault, the
// transient class.
type fireMapAuthority struct {
	admitted map[actor.ActorID]storespec.IdentityAdmission
	err      error
}

func (a fireMapAuthority) AdmitIdentity(_ context.Context, id actor.ActorID) (storespec.IdentityAdmission, bool, error) {
	if a.err != nil {
		return storespec.IdentityAdmission{}, false, a.err
	}
	admission, ok := a.admitted[id]
	return admission, ok, nil
}

// fireMapPresence is the harness's receiver-gate face. The fire envelope is
// kind=event (self-targeted), which that gate does not consult at all, so a
// blanket "everyone is here" keeps the variable under test to exactly one:
// the WriteResult the chain produces.
type fireMapPresence struct{}

func (fireMapPresence) IsActive(context.Context, actor.ActorID) (bool, error) { return true, nil }

// fireMapFixture is a real harness over a real per-test sqlite channel log,
// plus the sink under test welded to the supplied authority.
type fireMapFixture struct {
	sink schedule.FireSink
	log  storespec.MessageLog
}

func newFireMapFixture(t *testing.T, authority storespec.CollaborationAuthority) fireMapFixture {
	t.Helper()
	ctx := context.Background()
	cs, err := store.OpenChannel(ctx, fireMapChannelID,
		filepath.Join(t.TempDir(), "channel.sqlite"), store.OpenOptions{}, nil)
	if err != nil {
		t.Fatalf("store.OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	_, minter, err := harness.New(harness.Deps{
		ChannelID: fireMapChannelID,
		Log:       cs.Log,
		Presence:  fireMapPresence{},
		NowMs:     func() int64 { return fireMapNowMs },
	})
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}
	sink, err := timerfire.New(authority, minter)
	if err != nil {
		t.Fatalf("timerfire.New: %v", err)
	}
	return fireMapFixture{sink: sink, log: cs.Log}
}

// liveAuthorFixture is the common case: one admitted author.
func liveAuthorFixture(t *testing.T) fireMapFixture {
	t.Helper()
	return newFireMapFixture(t, fireMapAuthority{admitted: map[actor.ActorID]storespec.IdentityAdmission{
		fireMapAuthor: {ID: fireMapAuthor, Kind: fireMapAuthorKnd},
	}})
}

// fireMapEnvelope mirrors the engine's buildFireEnvelope field table: the
// deterministic `timer:` id, engine-stamped TS, welded kind=event, self
// audience, and — critically — an EMPTY Sender.ID / ChannelID, because those
// are pen-injected and a pre-stuffed value is itself a reject. A fresh
// envelope per call is not cosmetic: Write mutates the envelope in place, so
// re-submitting the same struct would trip the not-caller-settable guard
// instead of the duplicate-id path the replay test is aiming at.
func fireMapEnvelope(timerID, typ string) *message.Envelope {
	return &message.Envelope{
		ID:       message.ID("timer:" + timerID),
		TS:       fireMapNowMs - 1_000,
		Kind:     message.KindEvent,
		Type:     typ,
		Payload:  []byte(`{}`),
		Audience: message.Audience{fireMapAuthor},
	}
}

// assertNotDuplicate / assertNotRejected are the two false-classification
// guards every mapping assertion below pairs with its positive claim: the
// engine branches on CLASS, so landing in the right class matters exactly as
// much as not landing in a neighbouring one (a duplicate misread as
// FireRejected gets the row moved to timer_dead; a reject misread as duplicate
// gets the row silently completed).
func assertNotDuplicate(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, schedule.ErrDuplicateFire) {
		t.Fatalf("err %v was classified as ErrDuplicateFire", err)
	}
}

func assertNotRejected(t *testing.T, err error) {
	t.Helper()
	var rejected schedule.FireRejected
	if errors.As(err, &rejected) {
		t.Fatalf("err %v was classified as FireRejected(%+v)", err, rejected)
	}
}

// TestFireSinkMapsDuplicateAppendToDuplicateFire — T55. The crash-replay path:
// the engine re-fires a timer whose deterministic message id ALREADY landed in
// truth (it crashed between the harness append and MarkFired). The real
// messages.id UNIQUE index produces the reject; the sink must translate it to
// ErrDuplicateFire so the run loop COMPLETES the row rather than either
// deleting it as poison or retrying it forever.
//
// The guard that matters: a nil return here would be a false success on a
// write that did not happen this pass. It is only benign because the id is
// deterministic and the row is already there — which is precisely what the
// second half of this test proves (one row, one seq, unchanged).
func TestFireSinkMapsDuplicateAppendToDuplicateFire(t *testing.T) {
	ctx := context.Background()
	f := liveAuthorFixture(t)

	if err := f.sink.Append(ctx, fireMapAuthor, fireMapEnvelope("dup-1", "timer.tick")); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	first, ok, err := f.log.FindByID(ctx, message.ID("timer:dup-1"))
	if err != nil || !ok {
		t.Fatalf("FindByID after first Append = (%v, %v, %v)", first, ok, err)
	}

	err = f.sink.Append(ctx, fireMapAuthor, fireMapEnvelope("dup-1", "timer.tick"))
	if !errors.Is(err, schedule.ErrDuplicateFire) {
		t.Fatalf("replay Append err = %v, want ErrDuplicateFire", err)
	}
	assertNotRejected(t, err)

	// Truth is untouched by the replay: same single row, same seq.
	second, ok, err := f.log.FindByID(ctx, message.ID("timer:dup-1"))
	if err != nil || !ok {
		t.Fatalf("FindByID after replay = (%v, %v, %v)", second, ok, err)
	}
	if second.Seq != first.Seq {
		t.Fatalf("replay moved the committed row: seq %d -> %d", first.Seq, second.Seq)
	}
}

// TestFireSinkMapsReservedTypeToFireRejected — T56. A reserved-namespace type
// is the canonical deterministic reject: retrying it is a hot loop, never
// at-least-once delivery, so the sink must surface FireRejected (the engine's
// poison-row disposal class) and NOT a bare nil (row silently completed, fire
// lost) nor ErrDuplicateFire (same silent completion by another door).
//
// Both reserved sub-cases are covered because they leave the chain at
// different steps with different reasons, and the sink must carry the reason
// through verbatim — the reason string is what lands in timer_dead and in the
// loud disposal log, so it is the operator's only evidence.
func TestFireSinkMapsReservedTypeToFireRejected(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name    string
		timerID string
		typ     string
		reason  harness.HarnessRejectReason
	}{
		{
			// A reserved bootstrap type only the channel system actor may
			// emit — a timer author is never that actor.
			name:    "reserved bootstrap type from a non-system author",
			timerID: "reserved-bootstrap",
			typ:     actor.ReservedSystemActorRegistered,
			reason:  harness.HarnessReservedTypeUnauthorizedSender,
		},
		{
			// Inside the reserved namespace but not an installed member of it.
			name:    "uninstallable reserved-namespace type",
			timerID: "reserved-unknown",
			typ:     message.ReservedTypePrefix + "not_a_real_event",
			reason:  harness.HarnessTypeUnknown,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := liveAuthorFixture(t)
			err := f.sink.Append(ctx, fireMapAuthor, fireMapEnvelope(test.timerID, test.typ))
			if err == nil {
				t.Fatal("deterministic reject was swallowed into a false success")
			}
			assertNotDuplicate(t, err)
			var rejected schedule.FireRejected
			if !errors.As(err, &rejected) {
				t.Fatalf("Append err = %v (%T), want schedule.FireRejected", err, err)
			}
			if rejected.Reason != string(test.reason) {
				t.Fatalf("FireRejected.Reason = %q, want %q", rejected.Reason, test.reason)
			}
			if _, ok, ferr := f.log.FindByID(ctx, message.ID("timer:"+test.timerID)); ferr != nil || ok {
				t.Fatalf("rejected fire reached truth: ok=%v err=%v", ok, ferr)
			}
		})
	}
}

// TestFireSinkRefusesDeadAuthorBeforeTouchingThePen is the §7.4 exit guard —
// the reason the schedule arm needs no entry-side classification gate. A
// durable row outliving its author must be refused as a deterministic reject
// (so the engine annihilates it), never as a transient error (retried
// forever) and never as a success.
func TestFireSinkRefusesDeadAuthorBeforeTouchingThePen(t *testing.T) {
	ctx := context.Background()
	f := liveAuthorFixture(t)

	err := f.sink.Append(ctx, fireMapStranger, fireMapEnvelope("dead-author", "timer.tick"))
	var rejected schedule.FireRejected
	if !errors.As(err, &rejected) {
		t.Fatalf("Append err = %v (%T), want schedule.FireRejected", err, err)
	}
	if rejected.Reason != "author_not_member" {
		t.Fatalf("FireRejected.Reason = %q, want author_not_member", rejected.Reason)
	}
	if rejected.Detail != string(fireMapStranger) {
		t.Fatalf("FireRejected.Detail = %q, want the refused author id", rejected.Detail)
	}
	if _, ok, ferr := f.log.FindByID(ctx, message.ID("timer:dead-author")); ferr != nil || ok {
		t.Fatalf("refused fire reached truth: ok=%v err=%v", ok, ferr)
	}
}

// TestFireSinkPropagatesAuthorityFaultAsTransient pins the OTHER half of the
// classification: a store/link fault while ASKING the membership question says
// nothing about the author, so it must come back as a plain Go error — the
// engine's transient class, which leaves the row in place for the next tick.
// Mapping it to FireRejected would destroy live timers during any authority
// outage.
func TestFireSinkPropagatesAuthorityFaultAsTransient(t *testing.T) {
	ctx := context.Background()
	authorityDown := errors.New("timerfire-test: authority unavailable")
	f := newFireMapFixture(t, fireMapAuthority{err: authorityDown})

	err := f.sink.Append(ctx, fireMapAuthor, fireMapEnvelope("authority-down", "timer.tick"))
	if !errors.Is(err, authorityDown) {
		t.Fatalf("Append err = %v, want the authority fault verbatim", err)
	}
	assertNotRejected(t, err)
	assertNotDuplicate(t, err)
}

// TestNewFailsFastOnMissingOrgan: a sink missing an organ would fail at the
// first fire (hours later, in the run loop) instead of at construction.
func TestNewFailsFastOnMissingOrgan(t *testing.T) {
	authority := fireMapAuthority{}
	_, minter, err := harness.New(harness.Deps{
		ChannelID: fireMapChannelID,
		Log:       nopMessageLog{},
		Presence:  fireMapPresence{},
	})
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}
	if _, err := timerfire.New(nil, minter); !errors.Is(err, timerfire.ErrInvalidInput) {
		t.Fatalf("New(nil authority) err = %v, want ErrInvalidInput", err)
	}
	if _, err := timerfire.New(authority, nil); !errors.Is(err, timerfire.ErrInvalidInput) {
		t.Fatalf("New(nil minter) err = %v, want ErrInvalidInput", err)
	}
}

// nopMessageLog satisfies harness.Deps.Validate for the construction-only test
// above; no Append ever reaches it.
type nopMessageLog struct{}

func (nopMessageLog) Append(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
	return storespec.AppendResult{}, errors.New("timerfire-test: log not wired")
}

func (nopMessageLog) FindByID(context.Context, message.ID) (*storespec.StoredRow, bool, error) {
	return nil, false, nil
}

func (nopMessageLog) HasFinalResponse(context.Context, message.ID) (bool, error) {
	return false, nil
}
