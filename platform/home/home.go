package home

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/dataplane"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/platform/internal/tap"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/platform/svcactor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/managedcaps"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/systemkernel"
)

var (
	ErrClosed         = errors.New("platform: channel home is closed")
	ErrOwnerInvariant = errors.New("platform: channel owner invariant")
	ErrSchemaMismatch = errors.New("platform: channel schema mismatch")
)

type Config struct {
	ChannelID   channelpkg.ID
	ChannelName string
	DBPath      string

	Genesis         *storespec.ChannelGenesis
	ExpectedGenesis *storespec.ChannelGenesis
	MustExistDB     bool
	Bootstrap       bool
	// Bootstrap values are committed before Controller.Start so the
	// Controller publishes one complete durable image.
	BootstrapHumanPrincipals []string
	BootstrapDeclarations    []DeclareRequest
	BootstrapService         BootstrapService

	Logger               *slog.Logger
	ReconcileInterval    time.Duration
	CompositionResolver  CompositionResolver
	IntroductionResolver IntroductionResolver
	Clock                schedule.Clock
	DaemonRoutes         platform.DaemonRoutes
	DataPlaneIssuer      dataplane.Issuer
	DataPlaneRedeemer    dataplane.Redeemer
	DeviceDirectory      DeviceDirectory
	RegistryBindings     BindingReader
	ServicePort          *svcactor.Port
	HumanSessions        platform.HumanSessionLister
}

type BootstrapService struct {
	SvcAgent  *string
	Endpoints map[string]string
}

type DeviceDirectory interface {
	// ResolveDeviceName exists only for reading legacy daemon://name/... rows.
	// Every new address uses the stable registry DeviceID.
	ResolveDeviceName(context.Context, string) (id string, present bool, found bool, err error)
	// ResolveDeviceID decorates the canonical id with its display label. Present
	// and found remain separate so a retired canonical id can never fall through
	// and be reinterpreted as some other device's legacy display name.
	ResolveDeviceID(context.Context, string) (name string, present bool, found bool, err error)
}

type BindingReader interface {
	IsBound(context.Context, channelpkg.ID, string) (bool, error)
	ListBoundDeviceIDs(context.Context, channelpkg.ID) ([]string, error)
	ChannelDesired(context.Context, channelpkg.ID) (channelspec.ChannelDesiredFacts, bool, error)
}

type unavailableBindingReader struct{}

func (unavailableBindingReader) IsBound(context.Context, channelpkg.ID, string) (bool, error) {
	return false, errors.New("platform: registry binding reader unavailable")
}
func (unavailableBindingReader) ListBoundDeviceIDs(context.Context, channelpkg.ID) ([]string, error) {
	return nil, errors.New("platform: registry binding reader unavailable")
}
func (unavailableBindingReader) ChannelDesired(context.Context, channelpkg.ID) (channelspec.ChannelDesiredFacts, bool, error) {
	return channelspec.ChannelDesiredFacts{}, false, errors.New("platform: registry binding reader unavailable")
}

// Home is the channel composition root. Runtime organs are held as peers;
// actorSystem is only the Platform workflow facade over them.
type Home struct {
	channelID   channelpkg.ID
	channelName string
	actors      *actorSystem
	resolver    IntroductionResolver
	// ownerPrincipal is the channel's one owner pointer, read once from the
	// immutable genesis. It is the sole source of every owner judgement.
	ownerPrincipal string

	controller   *actorctl.Controller
	serverHost   *actorhost.HostSupervisor
	systemKernel *systemkernel.Kernel
	// managedCaps IS kept: the Host's BodyBuilder mints one bundle per body, so
	// it is a running-period capability. Its system twin is not — that one mints
	// once, inside Open, and what survives is the pen it produced (systemPen
	// below), not the mint that produced it.
	managedCaps *managedcaps.Minter

	// The store faces the channel keeps AFTER assembly: reads, plus the
	// daemon-binding management write the spec
	// assigns to their own domains. The assembly surface is absent — raw Log,
	// the actor record store, genesis and the leaf ports live only inside Open,
	// where they are handed to the organs that own them and then go out of
	// scope. Home cannot reach a write path around the harness pen, the
	// Controller ledger or an organ door because it does not hold one.
	query            storespec.MessageQuery
	visible          storespec.VisibleMessageQuery
	expiry           storespec.ExpiryQuery
	requests         storespec.RequestLookup
	registryBindings BindingReader
	closeStore       func() error

	// The two harness capabilities are held apart, exactly as they are handed
	// out: minter goes to the three components that mint pens, admittedWriter
	// goes to timer fire and nowhere else. Home writes through neither.
	minter         harness.Minter
	admittedWriter harness.AdmittedWriter
	stateHandles   accessdoor.StateHandleResolver
	engine         *schedule.Engine
	timers         timerPort

	signal         *tap.Signal
	delivery       *tap.Pump
	daemonRoutes   platform.DaemonRoutes
	daemonMembrane platform.DaemonMembrane
	presenceFold   *presence.Fold
	subjectgate    *subjectgate.Registry
	factories      ActorFactoryResolver
	opEntry        *opEntry
	servicePort    *svcactor.Port
	humanSessions  platform.HumanSessionLister

	systemPen    harness.Pen
	expiryCursor storespec.ExpiryCursor

	logger        *slog.Logger
	nowMs         func() int64
	reconcileStop func()
	reconcileDone chan struct{}
	pokeCh        chan struct{}

	closed    atomic.Bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error

	storeCloseDone atomic.Bool
	storeCloseMu   sync.Mutex
}
