// Package devicebus owns the server-side proxy-daemon registry used by
// local device proxy daemons to attach channel-local adapter actors.
package devicebus

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/channelaccess"
)

var (
	ErrRegistrationNotFound = errors.New("devicebus: actor route not found")
)

type Config struct {
	AllowedOrigins     []string
	AllowMissingOrigin bool

	PingCadence      time.Duration
	IdleReadTimeout  time.Duration
	PingWriteTimeout time.Duration

	Logger *slog.Logger
}

type ProxyDaemonNotifier interface {
	NotifyProxyDaemonReady(ctx context.Context, daemon Daemon, ready DaemonReadyInput) error
	NotifyProxyDaemonOffline(ctx context.Context, daemon Daemon, actors []actor.ActorID) error
}

type Service struct {
	db  *sql.DB
	cfg Config
	now func() time.Time
	rng io.Reader

	mu            sync.Mutex
	daemonConns   map[placement.DaemonID]*DaemonConnection
	actorToDaemon map[string]placement.DaemonID
	connGen       atomic.Uint64

	accessMu sync.RWMutex
	access   channelaccess.Authorizer

	proxyDaemonMu sync.RWMutex
	proxyDaemon   ProxyDaemonNotifier

	allowedOrigins map[string]struct{}
	log            *slog.Logger
}

func (s *Service) SetProxyDaemonNotifier(n ProxyDaemonNotifier) {
	s.proxyDaemonMu.Lock()
	s.proxyDaemon = n
	s.proxyDaemonMu.Unlock()
}

func (s *Service) proxyDaemonNotifier() ProxyDaemonNotifier {
	s.proxyDaemonMu.RLock()
	defer s.proxyDaemonMu.RUnlock()
	return s.proxyDaemon
}

type AccessAuthorizer = channelaccess.Authorizer

func (s *Service) SetAccessAuthorizer(a channelaccess.Authorizer) {
	s.accessMu.Lock()
	s.access = a
	s.accessMu.Unlock()
}

func (s *Service) accessAuthorizer() channelaccess.Authorizer {
	s.accessMu.RLock()
	defer s.accessMu.RUnlock()
	return s.access
}

func NewService(db *sql.DB, cfg Config) *Service {
	allowed := make(map[string]struct{}, len(cfg.AllowedOrigins))
	for _, origin := range cfg.AllowedOrigins {
		origin = strings.TrimSpace(origin)
		if origin != "" {
			allowed[origin] = struct{}{}
		}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	svc := &Service{
		db:             db,
		cfg:            cfg,
		now:            time.Now,
		rng:            rand.Reader,
		daemonConns:    map[placement.DaemonID]*DaemonConnection{},
		actorToDaemon:  map[string]placement.DaemonID{},
		allowedOrigins: allowed,
		log:            log.With("subsystem", "devicebus"),
	}
	if err := svc.InvalidateRuntimeProjections(context.Background(), "server devicebus service started"); err != nil {
		svc.log.Warn("devicebus.runtime_projection_invalidate_failed",
			"err", err.Error(),
		)
	}
	return svc
}

func (s *Service) WithClock(now func() time.Time) *Service {
	s.now = now
	return s
}

func (s *Service) nowMs() int64 { return s.now().UnixMilli() }

func routeKey(channelID channel.ID, actorID actor.ActorID) string {
	return string(channelID) + "\x00" + string(actorID)
}
