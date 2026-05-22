// Package pushhub owns the front-end WebSocket fan-out — when
// server/viewcache applies a new message, the hub broadcasts it to
// every WS-subscribed front-end user that is currently a member of
// the channel.
//
// Authoritative contract: pushhub fan-out is ordered per subscriber,
// suppresses duplicate channel seq values, and closes slow subscribers
// so clients reconnect and resync instead of silently missing frames.
//
// Demo-period: single-instance; no Redis pub/sub. Production would
// replace Service.broadcast with a pub/sub backend so multiple server
// processes can fan out.
package pushhub

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/pkg/metrics"
	"github.com/wanpengxie/ActOS/server/channelaccess"
	"github.com/wanpengxie/ActOS/server/identity"
)

// pushhubWSWriteTimeout caps a single fan-out WS write. Without this
// cap, a slow / stuck front-end TCP send buffer would block pumpWrite
// indefinitely while holding writeMu, preventing the subscriber's
// other broadcasts from making progress (broadcast goroutine fills up
// the send chan).
const pushhubWSWriteTimeout = 10 * time.Second

// pushhubPingCadence is how often the server sends a WS PingMessage
// to each UI subscriber. 30s gives ~3× margin against the cloudflare
// tunnel ~100s idle reaper. Idle channels (no pushes for minutes) are
// the exact failure mode this catches: without ping/pong the conn
// silently dies but lingers in h.subs, and the user sees stale data
// until they refresh.
const pushhubPingCadence = 30 * time.Second

// pushhubIdleReadTimeout is the maximum gap between any frame from
// the UI (pong, subscribe, unsubscribe) before the server tears down
// the subscriber. ~2.3× pushhubPingCadence absorbs one missed pong.
const pushhubIdleReadTimeout = 70 * time.Second

// pushhubWSReadLimit caps a single inbound UI control frame at 4 MiB.
// Larger frames are closed before JSON allocation/validation.
const pushhubWSReadLimit int64 = 4 << 20

// pushhubPingWriteTimeout caps a single ping write; on failure the
// subscriber is closed and pumpRead exits.
const pushhubPingWriteTimeout = 5 * time.Second

// Config tunes Service.
type Config struct {
	// AllowedOrigins is the exact Origin allowlist for browser WebSocket
	// handshakes. Empty means deny browser-origin WS handshakes. Requests
	// with no Origin header are allowed for non-browser clients.
	AllowedOrigins []string

	// Logger receives structured pushhub events. nil means slog.Default.
	Logger *slog.Logger

	// Metrics receives pushhub fan-out counters. nil means metrics.Default.
	Metrics *metrics.Registry
}

// Service holds the live subscriber registry.
type Service struct {
	mu sync.RWMutex
	// channel_id → user_id → set of subscribers
	subs map[channel.ID]map[string]map[*subscriber]struct{}

	// observersMu guards observers slice.
	observersMu sync.RWMutex
	observers   []*pushObserver

	accessMu sync.RWMutex
	access   channelaccess.Authorizer

	// keepalive parameters. Mutated only by SetKeepaliveForTest; read
	// without lock at subscribe-time (zero = production default).
	pingCadence      time.Duration
	idleReadTimeout  time.Duration
	pingWriteTimeout time.Duration

	allowedOrigins map[string]struct{}
	log            *slog.Logger
	metrics        *metrics.Registry
}

// PushedFrame is the in-memory shape delivered to test observers.
// It mirrors what the WS subscriber receives without the JSON wrap.
type PushedFrame struct {
	ChannelID channel.ID
	Seq       viewsync.Seq
	Envelope  message.Envelope
}

// pushObserver is a registered test-only push capture function.
type pushObserver struct {
	channelID channel.ID // empty = all channels
	fn        func(PushedFrame)
}

// NewService builds a Service.
func NewService(opts ...Config) *Service {
	var cfg Config
	if len(opts) > 0 {
		cfg = opts[0]
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("subsystem", "pushhub")
	reg := cfg.Metrics
	if reg == nil {
		reg = metrics.Default()
	}
	return &Service{
		subs:           map[channel.ID]map[string]map[*subscriber]struct{}{},
		allowedOrigins: normalizeAllowedOrigins(cfg.AllowedOrigins),
		log:            log,
		metrics:        reg,
	}
}

// SetAccessAuthorizer wires the subscribe-time channel access check.
func (h *Service) SetAccessAuthorizer(a channelaccess.Authorizer) {
	h.accessMu.Lock()
	h.access = a
	h.accessMu.Unlock()
}

// SetKeepaliveForTest overrides the ping cadence + idle read timeout
// for the next-accepted subscriber. NOT for production callers — used
// by service_test.go to keep assertion windows tight without sleeping for
// the full 70s default.
func (h *Service) SetKeepaliveForTest(pingCadence, idleReadTimeout, pingWriteTimeout time.Duration) {
	h.pingCadence = pingCadence
	h.idleReadTimeout = idleReadTimeout
	h.pingWriteTimeout = pingWriteTimeout
}

func (h *Service) keepaliveCfg() (time.Duration, time.Duration, time.Duration) {
	cadence := h.pingCadence
	if cadence <= 0 {
		cadence = pushhubPingCadence
	}
	idle := h.idleReadTimeout
	if idle <= 0 {
		idle = pushhubIdleReadTimeout
	}
	pingWrite := h.pingWriteTimeout
	if pingWrite <= 0 {
		pingWrite = pushhubPingWriteTimeout
	}
	return cadence, idle, pingWrite
}

func normalizeAllowedOrigins(origins []string) map[string]struct{} {
	allowed := make(map[string]struct{}, len(origins))
	for _, origin := range origins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		allowed[origin] = struct{}{}
	}
	return allowed
}

func (h *Service) upgrader() websocket.Upgrader {
	return websocket.Upgrader{CheckOrigin: h.checkOrigin}
}

func (h *Service) checkOrigin(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	_, ok := h.allowedOrigins[origin]
	return ok
}

func (h *Service) accessAuthorizer() channelaccess.Authorizer {
	h.accessMu.RLock()
	defer h.accessMu.RUnlock()
	return h.access
}

// subscriber is one open WS connection.
type subscriber struct {
	ws      *websocket.Conn
	userID  string
	chans   map[channel.ID]struct{}
	send    chan []byte
	once    sync.Once
	done    chan struct{}
	writeMu sync.Mutex
	mu      sync.Mutex

	// lastEnqueued is the per-channel duplicate detector for server-to-UI
	// delivery. It records the highest seq accepted into send, not merely
	// observed by PushMessage.
	lastEnqueued map[channel.ID]viewsync.Seq

	// keepalive parameters captured at construction (so test overrides
	// don't race with production defaults).
	pingCadence      time.Duration
	idleReadTimeout  time.Duration
	pingWriteTimeout time.Duration
}

func newSubscriber(ws *websocket.Conn, userID string, pingCadence, idleReadTimeout, pingWriteTimeout time.Duration) *subscriber {
	return &subscriber{
		ws:               ws,
		userID:           userID,
		chans:            map[channel.ID]struct{}{},
		send:             make(chan []byte, 64),
		done:             make(chan struct{}),
		lastEnqueued:     map[channel.ID]viewsync.Seq{},
		pingCadence:      pingCadence,
		idleReadTimeout:  idleReadTimeout,
		pingWriteTimeout: pingWriteTimeout,
	}
}

// Close cleans up.
func (s *subscriber) Close() {
	s.once.Do(func() {
		close(s.done)
		if s.ws != nil {
			_ = s.ws.Close()
		}
	})
}

type subscriberEnqueueResult string

const (
	subscriberEnqueueDelivered  subscriberEnqueueResult = "delivered"
	subscriberEnqueueDuplicate  subscriberEnqueueResult = "duplicate"
	subscriberEnqueueClosed     subscriberEnqueueResult = "closed"
	subscriberEnqueueSlowClosed subscriberEnqueueResult = "slow_closed"
)

func (s *subscriber) enqueueMessage(channelID channel.ID, seq viewsync.Seq, raw []byte) subscriberEnqueueResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if last, ok := s.lastEnqueued[channelID]; ok && seq <= last {
		return subscriberEnqueueDuplicate
	}

	select {
	case s.send <- raw:
		s.lastEnqueued[channelID] = seq
		return subscriberEnqueueDelivered
	case <-s.done:
		return subscriberEnqueueClosed
	default:
		s.Close()
		return subscriberEnqueueSlowClosed
	}
}

// pumpWrite drains the send channel onto the WS and emits periodic
// PingMessage control frames. Pings travel via gorilla WriteControl,
// which uses an internal control-frame lock — it does NOT contend
// against writeMu (the application-frame lock), so a slow business
// fan-out cannot starve pings.
func (s *subscriber) pumpWrite() {
	pingTicker := time.NewTicker(s.pingCadence)
	defer pingTicker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-pingTicker.C:
			err := s.ws.WriteControl(
				websocket.PingMessage,
				nil,
				time.Now().Add(s.pingWriteTimeout),
			)
			if err != nil {
				// Conn is dead — close so pumpRead unwedges and the
				// hub.unregister cleanup fires.
				s.Close()
				return
			}
		case msg, ok := <-s.send:
			if !ok {
				return
			}
			s.writeMu.Lock()
			_ = s.ws.SetWriteDeadline(time.Now().Add(pushhubWSWriteTimeout))
			err := s.ws.WriteMessage(websocket.TextMessage, msg)
			s.writeMu.Unlock()
			if err != nil {
				s.Close()
				return
			}
		}
	}
}

// pumpRead reads control + subscribe frames from the client.
//
// Read deadline policy: every successful read refreshes the deadline
// to now+pushhubIdleReadTimeout. gorilla's installed PongHandler
// (registered before this loop starts) does the same on every pong
// control frame. If the UI tab is closed without an explicit close
// frame the conn would otherwise linger in h.subs until the OS TCP
// stack notices — ping/pong + idle deadline forces a bounded reap.
func (s *subscriber) pumpRead(ctx context.Context, hub *Service) {
	defer hub.unregister(s)
	// Initial deadline + ping/pong handlers. We install here (not in
	// HandleWS) because pumpRead is the goroutine that owns reads;
	// gorilla calls the handlers from inside ReadMessage. The
	// PongHandler is the dominant refresh path on idle channels
	// (server pumpWrite pings every cadence → UI auto-pongs → server
	// PongHandler fires here).
	_ = s.ws.SetReadDeadline(time.Now().Add(s.idleReadTimeout))
	defaultPingHandler := s.ws.PingHandler()
	s.ws.SetPingHandler(func(appData string) error {
		_ = s.ws.SetReadDeadline(time.Now().Add(s.idleReadTimeout))
		return defaultPingHandler(appData)
	})
	s.ws.SetPongHandler(func(string) error {
		_ = s.ws.SetReadDeadline(time.Now().Add(s.idleReadTimeout))
		return nil
	})
	for {
		_, raw, err := s.ws.ReadMessage()
		if err != nil {
			return
		}
		// Successful business frame = liveness signal.
		_ = s.ws.SetReadDeadline(time.Now().Add(s.idleReadTimeout))
		var ctrl struct {
			Type      string     `json:"type"`
			ChannelID channel.ID `json:"channel_id"`
		}
		if err := json.Unmarshal(raw, &ctrl); err != nil {
			continue
		}
		switch ctrl.Type {
		case "subscribe":
			if err := hub.subscribe(ctx, s, ctrl.ChannelID); err != nil {
				s.sendSubscribeRejected(ctrl.ChannelID)
			}
		case "unsubscribe":
			hub.unsubscribe(s, ctrl.ChannelID)
		}
	}
}

func (s *subscriber) sendSubscribeRejected(channelID channel.ID) {
	payload := map[string]any{
		"type":       "subscribe_rejected",
		"channel_id": string(channelID),
		"error":      channelaccess.ErrDenied.Error(),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	select {
	case s.send <- raw:
	case <-s.done:
	default:
	}
}

func (s *subscriber) sendSubscribeRevoked(channelID channel.ID) {
	payload := map[string]any{
		"type":       "subscribe_revoked",
		"channel_id": string(channelID),
		"error":      "membership_revoked",
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	select {
	case s.send <- raw:
	case <-s.done:
	default:
		s.Close()
	}
}

// HandleWS upgrades a front-end WS. The identity middleware MUST
// have authenticated the request upstream (via cookie or Bearer).
//
// Wire protocol (client → server JSON frames):
//
//	{"type":"subscribe",   "channel_id":"…"}
//	{"type":"unsubscribe", "channel_id":"…"}
//
// Server-pushed frames carry the kernel/message.Envelope shape
// inside `{"type":"message","channel_id":"…","seq":N,"envelope":…}`.
func (h *Service) HandleWS(ident *identity.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		// HTTP→WS upgrade can't use middleware-set context (the
		// request lifecycle is hijacked); re-authenticate by reading
		// the cookie / Bearer directly.
		raw := identity.ExtractTokenFromRequest(c.Request)
		user, err := ident.Authenticate(c.Request.Context(), raw)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		upgrader := h.upgrader()
		ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		ws.SetReadLimit(pushhubWSReadLimit)
		cadence, idle, pingWrite := h.keepaliveCfg()
		sub := newSubscriber(ws, user.ID, cadence, idle, pingWrite)
		go sub.pumpWrite()
		sub.pumpRead(c.Request.Context(), h)
	}
}

// PushMessage fan-outs a stored message to every subscriber of the channel.
// Called by gateway after viewcache.Apply commits. The server-to-subscriber
// contract is at-least-once by channel seq until the subscriber becomes too
// slow; duplicate seq values are suppressed per subscriber, and a full queue
// closes the subscriber so the client reconnects and resyncs explicitly.
func (h *Service) PushMessage(channelID channel.ID, seq viewsync.Seq, env message.Envelope) {
	h.mu.RLock()
	bucket := h.subs[channelID]
	type userSubscriber struct {
		userID string
		sub    *subscriber
	}
	snap := make([]userSubscriber, 0, len(bucket))
	for userID, perUser := range bucket {
		for s := range perUser {
			snap = append(snap, userSubscriber{userID: userID, sub: s})
		}
	}
	h.mu.RUnlock()

	auth := h.accessAuthorizer()
	for _, target := range snap {
		memberActorID, err := channelaccess.RequireMemberActor(context.Background(), auth, string(channelID), target.userID)
		if err != nil || !channelaccess.VisibleToActor(env, memberActorID) {
			continue
		}
		payload := map[string]any{
			"type":       "message",
			"channel_id": string(channelID),
			"seq":        int64(seq),
			"envelope":   env,
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			continue
		}
		result := target.sub.enqueueMessage(channelID, seq, raw)
		h.recordFanoutResult(result)
		switch result {
		case subscriberEnqueueDuplicate:
			h.log.Debug("pushhub.fanout_duplicate",
				"channel_id", string(channelID),
				"seq", int64(seq),
				"user_id", target.userID,
			)
		case subscriberEnqueueSlowClosed:
			h.log.Warn("pushhub.fanout_slow_subscriber_closed",
				"channel_id", string(channelID),
				"seq", int64(seq),
				"user_id", target.userID,
			)
		}
	}

	// Notify in-process observers (test-only). Snapshot under RLock,
	// invoke outside so a slow observer can't stall fan-out.
	h.observersMu.RLock()
	obs := append([]*pushObserver(nil), h.observers...)
	h.observersMu.RUnlock()
	if len(obs) == 0 {
		return
	}
	pf := PushedFrame{ChannelID: channelID, Seq: seq, Envelope: env}
	for _, o := range obs {
		if o.channelID != "" && o.channelID != channelID {
			continue
		}
		o.fn(pf)
	}
}

func (h *Service) recordFanoutResult(result subscriberEnqueueResult) {
	if h.metrics != nil {
		h.metrics.IncCounter("pushhub.fanout", "result", string(result))
	}
}

// RegisterPushObserverForTest installs an in-process push capture
// function. Pass channelID="" to observe every channel. Returns a
// cancel func that unregisters the observer. NOT for production
// callers — exists so handlers tests can assert fan-out ordering
// without spinning up a WS subscriber.
func (h *Service) RegisterPushObserverForTest(channelID channel.ID, fn func(PushedFrame)) func() {
	obs := &pushObserver{channelID: channelID, fn: fn}
	h.observersMu.Lock()
	h.observers = append(h.observers, obs)
	h.observersMu.Unlock()
	return func() {
		h.observersMu.Lock()
		defer h.observersMu.Unlock()
		for i, o := range h.observers {
			if o == obs {
				h.observers = append(h.observers[:i], h.observers[i+1:]...)
				return
			}
		}
	}
}

func (h *Service) subscribe(ctx context.Context, sub *subscriber, channelID channel.ID) error {
	if err := channelaccess.Require(ctx, h.accessAuthorizer(), string(channelID), sub.userID); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[channelID]; !ok {
		h.subs[channelID] = map[string]map[*subscriber]struct{}{}
	}
	if _, ok := h.subs[channelID][sub.userID]; !ok {
		h.subs[channelID][sub.userID] = map[*subscriber]struct{}{}
	}
	h.subs[channelID][sub.userID][sub] = struct{}{}
	sub.chans[channelID] = struct{}{}
	return nil
}

func (h *Service) unsubscribe(sub *subscriber, channelID channel.ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if perUser, ok := h.subs[channelID]; ok {
		if set, ok := perUser[sub.userID]; ok {
			delete(set, sub)
			if len(set) == 0 {
				delete(perUser, sub.userID)
			}
		}
		if len(perUser) == 0 {
			delete(h.subs, channelID)
		}
	}
	delete(sub.chans, channelID)
}

// RevokeChannelUser removes every live subscription held by userID for
// channelID. The websocket connection stays open so the client may remain
// subscribed to other channels.
func (h *Service) RevokeChannelUser(channelID channel.ID, userID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	perUser, ok := h.subs[channelID]
	if !ok {
		return
	}
	set, ok := perUser[userID]
	if !ok {
		return
	}
	for sub := range set {
		sub.sendSubscribeRevoked(channelID)
		delete(sub.chans, channelID)
	}
	delete(perUser, userID)
	if len(perUser) == 0 {
		delete(h.subs, channelID)
	}
}

func (h *Service) unregister(sub *subscriber) {
	for ch := range sub.chans {
		h.unsubscribe(sub, ch)
	}
	sub.Close()
}

// SubscriberCount returns the number of active subscribers for a
// channel (test helper).
func (h *Service) SubscriberCount(channelID channel.ID) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	bucket := h.subs[channelID]
	total := 0
	for _, set := range bucket {
		total += len(set)
	}
	return total
}
