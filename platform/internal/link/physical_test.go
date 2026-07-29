package link

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

type physicalPen struct{}

func (physicalPen) Write(context.Context, *message.Envelope) (harness.WriteResult, error) {
	return harness.WriteResult{}, nil
}

type physicalState struct{}

func (physicalState) Invoke(context.Context, access.Operation, resource.ResourceID, []byte, *access.Grant) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}

type physicalAccess struct{ physicalState }

func (physicalAccess) Create(context.Context, resource.ResourceID, accessdoor.CreateSpec, []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (physicalAccess) Stat(context.Context, resource.ResourceID) (accessdoor.StatResult, error) {
	return accessdoor.StatResult{}, nil
}
func (physicalAccess) List(context.Context, accessdoor.ListQuery) (accessdoor.ListPage, error) {
	return accessdoor.ListPage{}, nil
}
func (physicalAccess) Open(context.Context, resource.ResourceID, access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	return accessdoor.FileAccess{}, accessdoor.Outcome{}, nil
}
func (physicalAccess) Redeem(context.Context, accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	return accessdoor.FileAccess{}, nil
}

type physicalSchedule struct{}

func (physicalSchedule) Schedule(context.Context, schedule.ScheduleReq) (schedule.TimerID, error) {
	return "timer", nil
}
func (physicalSchedule) Cancel(context.Context, schedule.TimerID) error { return nil }
func (physicalSchedule) Ack(context.Context, schedule.TimerID) error    { return nil }

type physicalLifecycle struct{}

func (physicalLifecycle) Fork(context.Context, message.ID, actorcaps.ForkSpec) (actor.ActorID, error) {
	return "agent:child", nil
}
func (physicalLifecycle) EndSelf(context.Context, actorcaps.EndSelfRequest) error {
	return nil
}

func physicalArms() RawActorArms {
	return RawActorArms{
		Pen:       physicalPen{},
		Access:    physicalAccess{},
		State:     physicalState{},
		Schedule:  physicalSchedule{},
		Lifecycle: physicalLifecycle{},
	}
}

type physicalEndpoint struct {
	delivered chan message.ID
	canceled  chan message.ID
}

func newPhysicalEndpoint() *physicalEndpoint {
	return &physicalEndpoint{
		delivered: make(chan message.ID, 4),
		canceled:  make(chan message.ID, 4),
	}
}

func (e *physicalEndpoint) Deliver(env *message.Envelope) error {
	e.delivered <- env.ID
	return nil
}
func (e *physicalEndpoint) CancelRequest(id message.ID) { e.canceled <- id }

func physicalKey(t *testing.T) actorhost.AttemptKey {
	t.Helper()
	key, err := actorhost.NewAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func physicalSession(t *testing.T, opener ActorStreamOpener) *AuthenticatedLinkSession {
	t.Helper()
	if opener == nil {
		opener = func(context.Context, actor.ActorID, actorhost.AttemptKey) (ActorStreamResource, error) {
			done := make(chan struct{})
			var once sync.Once
			return ActorStreamResource{
				Arms: physicalArms(),
				Close: func() error {
					once.Do(func() { close(done) })
					return nil
				},
				Done: done,
			}, nil
		}
	}
	registry := newSessionRegistry(nil)
	record, err := registry.mint("peer")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.activate(record); err != nil {
		t.Fatal(err)
	}
	session, err := NewAuthenticatedLinkSession(AuthenticatedLinkSessionConfig{
		Peer:            "peer",
		Authority:       authorityPair(registry, record),
		OpenActorStream: opener,
	})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func waitPhysical(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not converge")
}

func TestBindingReaderSelfDownNeverJoinsItself(t *testing.T) {
	t.Parallel()
	session := physicalSession(t, nil)
	endpoint := newPhysicalEndpoint()
	eof := make(chan struct{})
	callback := make(chan *Binding, 1)
	binding, err := session.NewBinding(BindingConfig{
		Endpoint: endpoint,
		Run: func(context.Context) error {
			<-eof
			return io.EOF
		},
		Close: func() error { return nil },
		OnDown: func(exact *Binding, cause error) {
			if !errors.Is(cause, io.EOF) {
				t.Errorf("cause = %v", cause)
			}
			select {
			case <-exact.Done():
				t.Error("Done closed before route worker returned")
			default:
			}
			_ = exact.Close()
			callback <- exact
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := binding.Deliver(&message.Envelope{ID: "m1"}); err != nil {
		t.Fatal(err)
	}
	if got := <-endpoint.delivered; got != "m1" {
		t.Fatalf("delivered %q", got)
	}
	close(eof)
	if exact := <-callback; exact != binding {
		t.Fatal("BindingDown callback lost exact pointer")
	}
	select {
	case <-binding.Done():
	case <-time.After(time.Second):
		t.Fatal("Binding route worker self-deadlocked")
	}
	waitPhysical(t, func() bool {
		bindings, _ := session.ChildCounts()
		return bindings == 0
	})
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	<-session.Done()
}

func TestLedgerSealThenPhysicalCloseJoinsChildrenOutsideRegistryLock(t *testing.T) {
	t.Parallel()
	var session *AuthenticatedLinkSession
	streamClosed := make(chan struct{})
	var streamOnce sync.Once
	opener := func(context.Context, actor.ActorID, actorhost.AttemptKey) (ActorStreamResource, error) {
		return ActorStreamResource{
			Arms: physicalArms(),
			Close: func() error {
				// This would deadlock if the session held its registry lock while
				// signaling a child.
				session.ChildCounts()
				streamOnce.Do(func() { close(streamClosed) })
				return nil
			},
			Done: streamClosed,
		}, nil
	}
	registry := newSessionRegistry(nil)
	record, err := registry.mint("peer")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.activate(record); err != nil {
		t.Fatal(err)
	}
	session, err = NewAuthenticatedLinkSession(AuthenticatedLinkSessionConfig{
		Peer: "peer", Authority: authorityPair(registry, record), OpenActorStream: opener,
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := session.OpenActorStream(t.Context(), "agent:stream", physicalKey(t))
	if err != nil {
		t.Fatal(err)
	}
	binding, err := session.NewBinding(BindingConfig{
		Endpoint: newPhysicalEndpoint(),
		Close: func() error {
			session.ChildCounts()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if registry.beginSeal(record, sessionEvidence{reason: SessionRevoked}) != sealCommitted {
		t.Fatal("ledger seal did not commit")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.Done():
	case <-time.After(time.Second):
		t.Fatal("session did not join exact children")
	}
	select {
	case <-stream.Done():
	default:
		t.Fatal("stream was not joined")
	}
	select {
	case <-binding.Done():
	default:
		t.Fatal("binding was not joined")
	}
	if _, err := session.OpenActorStream(t.Context(), "agent:late", physicalKey(t)); !errors.Is(err, ErrPhysicalSessionClosed) {
		t.Fatalf("late open error = %v", err)
	}
	if _, err := session.NewBinding(BindingConfig{
		Endpoint: newPhysicalEndpoint(),
		Close:    func() error { return nil },
	}); !errors.Is(err, ErrPhysicalSessionClosed) {
		t.Fatalf("late binding error = %v", err)
	}
}

func TestActorStreamNaturalDoneExactUnregister(t *testing.T) {
	t.Parallel()
	rawDone := make(chan struct{})
	var closeOnce sync.Once
	session := physicalSession(t, func(context.Context, actor.ActorID, actorhost.AttemptKey) (ActorStreamResource, error) {
		return ActorStreamResource{
			Arms: physicalArms(),
			Close: func() error {
				closeOnce.Do(func() { close(rawDone) })
				return nil
			},
			Done: rawDone,
		}, nil
	})
	stream, err := session.OpenActorStream(t.Context(), "agent:one", physicalKey(t))
	if err != nil {
		t.Fatal(err)
	}
	closeOnce.Do(func() { close(rawDone) })
	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatal("natural stream completion did not close Done")
	}
	waitPhysical(t, func() bool {
		_, streams := session.ChildCounts()
		return streams == 0
	})
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	<-session.Done()
}

func TestSamePeerSessionsAndBindingsRemainExactObjects(t *testing.T) {
	t.Parallel()
	s1 := physicalSession(t, nil)
	s2 := physicalSession(t, nil)
	b1, err := s1.NewBinding(BindingConfig{
		Endpoint: newPhysicalEndpoint(),
		Close:    func() error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	b2, err := s2.NewBinding(BindingConfig{
		Endpoint: newPhysicalEndpoint(),
		Close:    func() error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if s1 == s2 || b1 == b2 {
		t.Fatal("same authenticated peer collapsed exact physical identity")
	}
	_ = s1.Close()
	<-s1.Done()
	select {
	case <-b2.Done():
		t.Fatal("closing predecessor session closed successor binding")
	default:
	}
	_ = s2.Close()
	<-s2.Done()
}

// Physical close gates late child registration in the same critical section
// as the shutdown snapshot: a binding arriving after Close is rejected even
// while the ledger still admits.
func TestPhysicalCloseGatesLateChildRegistration(t *testing.T) {
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	session, err := NewAuthenticatedLinkSession(AuthenticatedLinkSessionConfig{
		Peer:      "daemon-a",
		Authority: authorityPair(registry, record),
		OpenActorStream: func(context.Context, actor.ActorID, actorhost.AttemptKey) (ActorStreamResource, error) {
			return ActorStreamResource{Arms: physicalArms(), Close: func() error { return nil }}, nil
		},
		CloseTransport: func() error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = session.Close()

	if _, err := session.NewBinding(BindingConfig{
		Endpoint: newPhysicalEndpoint(),
		Close:    func() error { return nil },
	}); !errors.Is(err, ErrPhysicalSessionClosed) {
		t.Fatalf("late binding registration err=%v want closed", err)
	}
	if _, err := session.OpenActorStream(
		context.Background(), "agent:late", physicalKey(t),
	); !errors.Is(err, ErrPhysicalSessionClosed) {
		t.Fatalf("late stream open err=%v want closed", err)
	}
	if !registry.admit(record).allows() {
		t.Fatal("ledger admission changed; the local closed gate was not what rejected")
	}
	select {
	case <-session.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session did not collect after close")
	}
}

// Verdict-once: an open that passed the admission gate is in-flight work and
// is never revoked by a second reading when a seal lands mid-open; the
// closed-gate handoff and shutdown own its collection instead.
func TestInFlightOpenSurvivesSealVerdict(t *testing.T) {
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	key := physicalKey(t)
	gate := make(chan struct{})
	entered := make(chan struct{})
	session, err := NewAuthenticatedLinkSession(AuthenticatedLinkSessionConfig{
		Peer:      "daemon-a",
		Authority: authorityPair(registry, record),
		OpenActorStream: func(context.Context, actor.ActorID, actorhost.AttemptKey) (ActorStreamResource, error) {
			close(entered)
			<-gate
			done := make(chan struct{})
			var once sync.Once
			return ActorStreamResource{
				Arms: physicalArms(),
				Close: func() error {
					once.Do(func() { close(done) })
					return nil
				},
				Done: done,
			}, nil
		},
		CloseTransport: func() error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	type opened struct {
		stream *ActorStream
		err    error
	}
	result := make(chan opened, 1)
	go func() {
		stream, openErr := session.OpenActorStream(context.Background(), "agent:a", key)
		result <- opened{stream: stream, err: openErr}
	}()
	<-entered
	if registry.beginSeal(record, sessionEvidence{reason: SessionRevoked}) != sealCommitted {
		t.Fatal("seal did not commit")
	}
	close(gate)
	got := <-result
	if got.err != nil {
		t.Fatalf("in-flight open was revoked by a second verdict: %v", got.err)
	}
	_ = session.Close()
	select {
	case <-got.stream.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not collect the in-flight stream")
	}
}
