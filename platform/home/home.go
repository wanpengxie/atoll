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
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/managedcaps"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/systemcaps"
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
	channelID  channelpkg.ID
	actors     *actorSystem
	actorStore *homeActorStore

	controller   *actorctl.Controller
	serverHost   *actorhost.HostSupervisor
	systemKernel *systemkernel.Kernel
	managedCaps  *managedcaps.Minter
	systemCaps   *systemcaps.Minter

	cs           *runtime.ChannelStores
	minter       harness.Minter
	access       accessdoor.AccessMinter
	outbox       resourcespec.ResourceOutbox
	stateHandles accessdoor.StateHandleResolver
	schedMinter  schedule.Minter
	engine       *schedule.Engine

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

	reconcileStop   func()
	reconcileDone   chan struct{}
	reconcileLeaked atomic.Int64
	pokeCh          chan struct{}

	closed    atomic.Bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error

	storeCloseDone atomic.Bool
	storeCloseMu   sync.Mutex
}
