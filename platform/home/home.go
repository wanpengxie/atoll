package home

import (
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/platform/internal/tap"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/managedcaps"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/systemkernel"
)

var ErrClosed = errors.New("platform: channel home is closed")

type Config struct {
	ChannelID channelpkg.ID
	DBPath    string

	Genesis         *storespec.ChannelGenesis
	ExpectedGenesis *storespec.ChannelGenesis
	MustExistDB     bool
	Bootstrap       bool
	// Bootstrap values are committed before Controller.Start so the
	// Controller publishes one complete durable image.
	BootstrapOwnerPrincipal string
	BootstrapDeclarations   []DeclareRequest

	Logger               *slog.Logger
	ReconcileInterval    time.Duration
	CompositionResolver  CompositionResolver
	IntroductionResolver IntroductionResolver
	Clock                schedule.Clock
	ReservationTimeout   time.Duration
	OnMembershipChange   func(principal string)
}

// Home is the channel composition root. Runtime organs are held as peers;
// actorSystem is only the Platform workflow facade over them.
type Home struct {
	channelID channelpkg.ID
	actors    *actorSystem
	resolver  IntroductionResolver
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
	query        storespec.MessageQuery
	visible      storespec.VisibleMessageQuery
	expiry       storespec.ExpiryQuery
	requests     storespec.RequestLookup
	bindings     storespec.DaemonBindingStore
	defaultAgent *defaultAgentFold
	resourceRead storespec.ResourceReadStore
	closeStore   func() error

	// The two harness capabilities are held apart, exactly as they are handed
	// out: minter goes to the three components that mint pens, admittedWriter
	// goes to timer fire and nowhere else. Home writes through neither.
	minter         harness.Minter
	admittedWriter harness.AdmittedWriter
	outbox         resourcespec.ResourceOutbox
	stateHandles   accessdoor.StateHandleResolver
	engine         *schedule.Engine

	signal       *tap.Signal
	delivery     *tap.Pump
	links        *link.Acceptor
	presenceFold *presence.Fold
	subjectgate  *subjectgate.Registry
	factories    ActorFactoryResolver
	opEntry      *opEntry

	systemPen    harness.Pen
	expiryCursor storespec.ExpiryCursor

	logger             *slog.Logger
	nowMs              func() int64
	onMembershipChange func(string)

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
