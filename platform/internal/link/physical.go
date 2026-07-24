package link

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

var (
	ErrPhysicalSessionClosed = errors.New("link: authenticated session closed")
	ErrInvalidPhysicalChild  = errors.New("link: invalid physical child")
)

// RawActorArms is the immutable five-capability result of opening one exact
// daemon actor stream.
type RawActorArms struct {
	Pen       harness.Pen
	Access    accessdoor.ResourceAccessHandle
	State     accessdoor.AccessHandle
	Schedule  schedule.ScheduleHandle
	Lifecycle actorcaps.LifecycleHandle
}

func (a RawActorArms) valid() bool {
	return !nilInterface(a.Pen) &&
		!nilInterface(a.Access) &&
		!nilInterface(a.State) &&
		!nilInterface(a.Schedule) &&
		!nilInterface(a.Lifecycle)
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

// ActorStreamResource is the raw transport product adapted by
// AuthenticatedLinkSession. Close must be non-blocking; Done is the physical
// join handle. When Done is nil, Close itself is the complete teardown.
type ActorStreamResource struct {
	Arms          RawActorArms
	Close         func() error
	Done          <-chan struct{}
	CancelRequest func(message.ID) error
	PublishObs    func(string, []byte) error
}

// ActorStreamOpener opens one fresh physical actor stream. It performs no
// semantic ACK and never reuses an existing stream object.
type ActorStreamOpener func(context.Context, actor.ActorID, actorhost.AttemptKey) (ActorStreamResource, error)

// AuthenticatedLinkSessionConfig supplies the already-authenticated transport
// owner. Peer is diagnostic/authorization input at construction time; it is
// not part of Binding identity.
type AuthenticatedLinkSessionConfig struct {
	Peer            actorhost.ExecutionDomain
	OpenActorStream ActorStreamOpener
	CloseTransport  func() error
	TransportDone   <-chan struct{}
}

// AuthenticatedLinkSession owns one physical connection and every exact child
// Binding/ActorStream it creates.
type AuthenticatedLinkSession struct {
	peer   actorhost.ExecutionDomain
	opener ActorStreamOpener

	closeTransport func() error
	transportDone  <-chan struct{}

	mu       sync.Mutex
	sealed   bool
	bindings map[*Binding]struct{}
	streams  map[*ActorStream]struct{}
	openWG   sync.WaitGroup

	closeOnce sync.Once
	done      chan struct{}

	errMu    sync.Mutex
	closeErr error
}

// NewAuthenticatedLinkSession constructs a physical owner. It does not imply
// that any actor route is present.
func NewAuthenticatedLinkSession(cfg AuthenticatedLinkSessionConfig) (*AuthenticatedLinkSession, error) {
	if cfg.Peer == "" {
		return nil, ErrInvalidPhysicalChild
	}
	session := &AuthenticatedLinkSession{
		peer:           cfg.Peer,
		opener:         cfg.OpenActorStream,
		closeTransport: cfg.CloseTransport,
		transportDone:  cfg.TransportDone,
		bindings:       make(map[*Binding]struct{}),
		streams:        make(map[*ActorStream]struct{}),
		done:           make(chan struct{}),
	}
	if cfg.TransportDone != nil {
		go func() {
			select {
			case <-cfg.TransportDone:
				_ = session.Close()
			case <-session.done:
			}
		}()
	}
	return session, nil
}

// Peer reports the authenticated physical peer.
func (s *AuthenticatedLinkSession) Peer() actorhost.ExecutionDomain {
	if s == nil {
		return ""
	}
	return s.peer
}

// Done closes after admission is sealed, all in-flight opens have resolved,
// and all exact children have finished.
func (s *AuthenticatedLinkSession) Done() <-chan struct{} {
	if s == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return s.done
}

// Err reports an asynchronous transport-close fault after Done.
func (s *AuthenticatedLinkSession) Err() error {
	if s == nil {
		return nil
	}
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.closeErr
}

// OpenActorStream opens and registers a fresh exact child. An open racing
// session seal is closed as a loser and never published.
func (s *AuthenticatedLinkSession) OpenActorStream(
	ctx context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
) (*ActorStream, error) {
	if s == nil || id == "" {
		return nil, ErrPhysicalSessionClosed
	}
	if s.opener == nil {
		return nil, ErrInvalidPhysicalChild
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := actorhost.ParseAttemptKey(string(key)); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.sealed {
		s.mu.Unlock()
		return nil, ErrPhysicalSessionClosed
	}
	s.openWG.Add(1)
	s.mu.Unlock()
	defer s.openWG.Done()

	resource, err := s.opener(ctx, id, key)
	if err != nil {
		return nil, err
	}
	if !resource.Arms.valid() || resource.Close == nil {
		if resource.Close != nil {
			_ = resource.Close()
		}
		if resource.Done != nil {
			select {
			case <-resource.Done:
			case <-ctx.Done():
			}
		}
		return nil, ErrInvalidPhysicalChild
	}
	stream := newActorStream(s, resource)
	s.mu.Lock()
	if s.sealed {
		s.mu.Unlock()
		stream.start()
		_ = stream.Close()
		<-stream.Done()
		return nil, ErrPhysicalSessionClosed
	}
	s.streams[stream] = struct{}{}
	s.mu.Unlock()
	stream.start()
	return stream, nil
}

// BindingConfig adapts one exact server-side actor route.
type BindingConfig struct {
	Endpoint actorhost.ActorEndpoint
	Run      func(context.Context) error
	Close    func() error
	OnDown   func(*Binding, error)
}

// NewBinding registers an exact route before its reader starts.
func (s *AuthenticatedLinkSession) NewBinding(cfg BindingConfig) (*Binding, error) {
	if s == nil || nilInterface(cfg.Endpoint) || cfg.Close == nil {
		return nil, ErrInvalidPhysicalChild
	}
	binding := newBinding(s, cfg)
	s.mu.Lock()
	if s.sealed {
		s.mu.Unlock()
		_ = binding.Close()
		return nil, ErrPhysicalSessionClosed
	}
	s.bindings[binding] = struct{}{}
	s.mu.Unlock()
	binding.start()
	return binding, nil
}

// Close seals child admission, signals the transport, then joins exact
// children on a session-owned goroutine. It never waits inline.
func (s *AuthenticatedLinkSession) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.sealed = true
		s.mu.Unlock()
		if s.closeTransport != nil {
			closeErr := s.closeTransport()
			s.errMu.Lock()
			s.closeErr = closeErr
			s.errMu.Unlock()
		}
		go s.shutdown()
	})
	return s.Err()
}

func (s *AuthenticatedLinkSession) shutdown() {
	s.openWG.Wait()
	s.mu.Lock()
	bindings := make([]*Binding, 0, len(s.bindings))
	for binding := range s.bindings {
		bindings = append(bindings, binding)
	}
	streams := make([]*ActorStream, 0, len(s.streams))
	for stream := range s.streams {
		streams = append(streams, stream)
	}
	s.mu.Unlock()

	for _, binding := range bindings {
		_ = binding.Close()
	}
	for _, stream := range streams {
		_ = stream.Close()
	}
	for _, binding := range bindings {
		<-binding.Done()
	}
	for _, stream := range streams {
		<-stream.Done()
	}
	if s.transportDone != nil {
		<-s.transportDone
	}
	close(s.done)
}

func (s *AuthenticatedLinkSession) unregisterBinding(binding *Binding) {
	s.mu.Lock()
	delete(s.bindings, binding)
	s.mu.Unlock()
}

func (s *AuthenticatedLinkSession) unregisterStream(stream *ActorStream) {
	s.mu.Lock()
	delete(s.streams, stream)
	s.mu.Unlock()
}

// ChildCounts is a diagnostic snapshot.
func (s *AuthenticatedLinkSession) ChildCounts() (bindings, streams int) {
	if s == nil {
		return 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.bindings), len(s.streams)
}

// Binding is a pointer-backed exact remote ActorEndpoint.
type Binding struct {
	session  *AuthenticatedLinkSession
	endpoint actorhost.ActorEndpoint
	run      func(context.Context) error
	closeFn  func() error
	onDown   func(*Binding, error)

	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	closeOnce sync.Once
	done      chan struct{}

	errMu    sync.Mutex
	closeErr error
}

func newBinding(session *AuthenticatedLinkSession, cfg BindingConfig) *Binding {
	ctx, cancel := context.WithCancel(context.Background())
	return &Binding{
		session:  session,
		endpoint: cfg.Endpoint,
		run:      cfg.Run,
		closeFn:  cfg.Close,
		onDown:   cfg.OnDown,
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
}

func (b *Binding) start() {
	b.startOnce.Do(func() { go b.runLoop() })
}

func (b *Binding) runLoop() {
	var cause error
	if b.run == nil {
		<-b.ctx.Done()
		cause = b.ctx.Err()
	} else {
		cause = b.run(b.ctx)
	}
	_ = b.Close()
	if b.onDown != nil {
		b.onDown(b, cause)
	}
	close(b.done)
	b.session.unregisterBinding(b)
}

func (b *Binding) Deliver(env *message.Envelope) error {
	if b == nil {
		return ErrPhysicalSessionClosed
	}
	return b.endpoint.Deliver(env)
}

func (b *Binding) CancelRequest(id message.ID) {
	if b != nil {
		b.endpoint.CancelRequest(id)
	}
}

// Close only signals route teardown; it never waits for Done.
func (b *Binding) Close() error {
	if b == nil {
		return nil
	}
	var closeErr error
	b.closeOnce.Do(func() {
		b.cancel()
		closeErr = b.closeFn()
		b.errMu.Lock()
		b.closeErr = closeErr
		b.errMu.Unlock()
	})
	return closeErr
}

func (b *Binding) Done() <-chan struct{} {
	if b == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return b.done
}

func (b *Binding) Err() error {
	if b == nil {
		return nil
	}
	b.errMu.Lock()
	defer b.errMu.Unlock()
	return b.closeErr
}

// ActorStream is one fresh exact daemon-side stream and its immutable raw arms.
type ActorStream struct {
	session  *AuthenticatedLinkSession
	resource ActorStreamResource

	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	closeOnce sync.Once
	done      chan struct{}

	errMu    sync.Mutex
	closeErr error
}

func newActorStream(session *AuthenticatedLinkSession, resource ActorStreamResource) *ActorStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &ActorStream{
		session:  session,
		resource: resource,
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
}

func (s *ActorStream) start() {
	s.startOnce.Do(func() { go s.runLoop() })
}

func (s *ActorStream) runLoop() {
	if s.resource.Done == nil {
		<-s.ctx.Done()
	} else {
		select {
		case <-s.ctx.Done():
			<-s.resource.Done
		case <-s.resource.Done:
			_ = s.Close()
		}
	}
	close(s.done)
	s.session.unregisterStream(s)
}

// Arms returns the immutable physical capability bundle.
func (s *ActorStream) Arms() RawActorArms {
	if s == nil {
		return RawActorArms{}
	}
	return s.resource.Arms
}

func (s *ActorStream) SendCancelRequest(id message.ID) error {
	if s == nil || s.resource.CancelRequest == nil {
		return ErrPhysicalSessionClosed
	}
	return s.resource.CancelRequest(id)
}

func (s *ActorStream) PublishObs(kind string, value []byte) error {
	if s == nil || s.resource.PublishObs == nil {
		return ErrPhysicalSessionClosed
	}
	return s.resource.PublishObs(kind, value)
}

// Close only signals teardown; the session remains the physical join owner.
func (s *ActorStream) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	s.closeOnce.Do(func() {
		s.cancel()
		closeErr = s.resource.Close()
		s.errMu.Lock()
		s.closeErr = closeErr
		s.errMu.Unlock()
	})
	return closeErr
}

func (s *ActorStream) Done() <-chan struct{} {
	if s == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return s.done
}

func (s *ActorStream) Err() error {
	if s == nil {
		return nil
	}
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.closeErr
}

var (
	_ actorhost.Binding = (*Binding)(nil)
)
