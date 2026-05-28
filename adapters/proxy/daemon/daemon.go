package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

const (
	DefaultHeartbeatInterval = 25 * time.Second
	DefaultReadinessInterval = 30 * time.Second
	DefaultReconnectInitial  = time.Second
	DefaultReconnectMax      = 10 * time.Second
)

var ErrShutdown = errors.New("proxy daemon shutdown requested")

type Logger interface {
	Printf(format string, v ...any)
}

type Options struct {
	Registry           *Registry
	Logger             Logger
	Version            string
	Dialer             *websocket.Dialer
	Hostname           string
	DisableLocalListen bool
	HeartbeatInterval  time.Duration
	ReadinessInterval  time.Duration
	ReconnectInitial   time.Duration
	ReconnectMax       time.Duration
	Clock              func() time.Time
}

type Daemon struct {
	cfg                Config
	registry           *Registry
	log                Logger
	version            string
	dialer             *websocket.Dialer
	hostname           string
	disableLocalListen bool

	heartbeatInterval time.Duration
	readinessInterval time.Duration
	reconnectInitial  time.Duration
	reconnectMax      time.Duration
	clock             func() time.Time

	initOnce sync.Once
	initErr  error

	readinessMu sync.Mutex
	readiness   map[actor.ActorID]readinessState
}

type readinessState struct {
	Known             bool
	Ready             bool
	Reason            string
	Detail            json.RawMessage
	LastReadyAt       int64
	LastStateChangeAt int64
}

type upstreamAckHandler interface {
	OnUpstreamAck(context.Context, DeviceFrame) error
}

func New(cfg Config, opts Options) (*Daemon, error) {
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if opts.Registry == nil {
		return nil, errors.New("proxy daemon: registry required")
	}
	hostname := opts.Hostname
	if hostname == "" {
		if got, err := os.Hostname(); err == nil {
			hostname = got
		}
	}
	if opts.Logger == nil {
		opts.Logger = noopLogger{}
	}
	if opts.Version == "" {
		opts.Version = "dev"
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if opts.ReadinessInterval <= 0 {
		opts.ReadinessInterval = DefaultReadinessInterval
	}
	if opts.ReconnectInitial <= 0 {
		opts.ReconnectInitial = DefaultReconnectInitial
	}
	if opts.ReconnectMax <= 0 {
		opts.ReconnectMax = DefaultReconnectMax
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &Daemon{
		cfg:                cfg,
		registry:           opts.Registry,
		log:                opts.Logger,
		version:            opts.Version,
		dialer:             opts.Dialer,
		hostname:           hostname,
		disableLocalListen: opts.DisableLocalListen,
		heartbeatInterval:  opts.HeartbeatInterval,
		readinessInterval:  opts.ReadinessInterval,
		reconnectInitial:   opts.ReconnectInitial,
		reconnectMax:       opts.ReconnectMax,
		clock:              opts.Clock,
		readiness:          map[actor.ActorID]readinessState{},
	}, nil
}

func (d *Daemon) Run(ctx context.Context) error {
	d.initOnce.Do(func() {
		results, err := d.registry.InitEnabled(ctx, d.cfg)
		for _, res := range results {
			if res.Err != nil {
				d.log.Printf("proxy module %s init skipped: %v", res.ActorID, res.Err)
				continue
			}
			d.log.Printf("proxy module %s initialized", res.ActorID)
		}
		d.initErr = err
	})
	if d.initErr != nil {
		return d.initErr
	}
	if !d.disableLocalListen {
		local, err := StartLocalListener(ctx, d.cfg.Port, d.registry, d.log)
		if err != nil {
			return err
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := local.Shutdown(shutdownCtx); err != nil {
				d.log.Printf("proxy local listener shutdown: %v", err)
			}
		}()
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := d.registry.Shutdown(shutdownCtx); err != nil {
			d.log.Printf("proxy registry shutdown: %v", err)
		}
	}()

	backoff := d.reconnectInitial
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		conn, endpoint, err := Dial(ctx, d.cfg.ServerWS, d.cfg.APIKey, d.dialer)
		if err != nil {
			d.log.Printf("proxy transport dial failed: %v", err)
			if !sleepContext(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff, d.reconnectMax)
			continue
		}
		d.log.Printf("proxy transport connected: %s", redactedEndpoint(endpoint))
		backoff = d.reconnectInitial
		err = d.runConnection(ctx, conn)
		_ = conn.Close()
		switch {
		case errors.Is(err, ErrShutdown):
			d.log.Printf("proxy transport shutdown requested")
			return nil
		case ctx.Err() != nil:
			return nil
		case err != nil:
			d.log.Printf("proxy transport disconnected: %v", err)
		default:
			d.log.Printf("proxy transport disconnected")
		}
		if !sleepContext(ctx, backoff) {
			return nil
		}
		backoff = nextBackoff(backoff, d.reconnectMax)
	}
}

func (d *Daemon) runConnection(ctx context.Context, conn *WSConnection) error {
	if err := d.sendReady(ctx, conn); err != nil {
		return err
	}
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-connCtx.Done()
		_ = conn.Close()
	}()
	errCh := make(chan error, 2)
	reportErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
		_ = conn.Close()
	}
	go d.heartbeatLoop(connCtx, conn, reportErr)
	go d.readinessLoop(connCtx, conn, reportErr)

	for {
		select {
		case err := <-errCh:
			return err
		default:
		}
		frame, err := conn.ReadFrame()
		if err != nil {
			select {
			case loopErr := <-errCh:
				return loopErr
			default:
				return err
			}
		}
		switch frame.FrameType {
		case FrameTypeShutdown:
			return ErrShutdown
		case FrameTypeAck:
			d.handleAckFrame(connCtx, frame)
		case FrameTypeReady, FrameTypeHeartbeat:
			return fmt.Errorf("proxy daemon: unexpected server frame_type %q", frame.FrameType)
		case "":
			d.handleBusinessFrame(connCtx, conn, frame)
		default:
			return fmt.Errorf("proxy daemon: unknown server frame_type %q", frame.FrameType)
		}
	}
}

func (d *Daemon) handleAckFrame(ctx context.Context, frame DeviceFrame) {
	actorID := actor.ActorID(frame.ActorID)
	mod, ok := d.registry.Get(actorID)
	if !ok {
		return
	}
	ackHandler, ok := mod.(upstreamAckHandler)
	if !ok {
		return
	}
	if err := ackHandler.OnUpstreamAck(ctx, frame); err != nil {
		d.log.Printf("proxy ack handler failed actor=%s request=%s: %v", actorID, frame.RequestID, err)
	}
}

func (d *Daemon) sendReady(ctx context.Context, conn *WSConnection) error {
	modules := d.registry.Modules()
	actors := make([]ReadyActor, 0, len(modules))
	for _, mod := range modules {
		capability, err := capabilityFromDeclaration(mod.Declaration())
		if err != nil {
			return fmt.Errorf("proxy daemon ready capability %s: %w", mod.ActorID(), err)
		}
		actors = append(actors, ReadyActor{
			ActorID:       string(mod.ActorID()),
			CapabilitySet: capability,
		})
	}
	if len(actors) == 0 {
		return errors.New("proxy daemon ready: no initialized modules")
	}
	return conn.WriteFrame(ctx, DeviceFrame{
		Direction:    "from_device",
		FrameType:    FrameTypeReady,
		Hostname:     d.hostname,
		HostLabel:    d.hostLabel(),
		Actors:       actors,
		ProxyVersion: d.version,
	})
}

func (d *Daemon) heartbeatLoop(ctx context.Context, conn *WSConnection, reportErr func(error)) {
	ticker := time.NewTicker(d.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := conn.WriteFrame(ctx, DeviceFrame{
				Direction: "from_device",
				FrameType: FrameTypeHeartbeat,
			}); err != nil {
				reportErr(fmt.Errorf("proxy daemon heartbeat: %w", err))
				return
			}
		}
	}
}

func (d *Daemon) readinessLoop(ctx context.Context, conn *WSConnection, reportErr func(error)) {
	if err := d.probeAndEmitReadiness(ctx, conn, true); err != nil {
		reportErr(err)
		return
	}
	ticker := time.NewTicker(d.readinessInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.probeAndEmitReadiness(ctx, conn, false); err != nil {
				reportErr(err)
				return
			}
		}
	}
}

func (d *Daemon) probeAndEmitReadiness(ctx context.Context, conn *WSConnection, force bool) error {
	for _, mod := range d.registry.Modules() {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		ready, reason, err := mod.Readiness(probeCtx)
		cancel()
		detail := json.RawMessage(`{}`)
		if reason == "" {
			if ready {
				reason = "ok"
			} else {
				reason = "not_ready"
			}
		}
		if err != nil {
			ready = false
			if reason == "ok" || reason == "" {
				reason = "probe_error"
			}
			raw, _ := json.Marshal(map[string]any{"error": err.Error()})
			detail = raw
		}
		state, changed := d.recordReadiness(mod.ActorID(), ready, reason, detail)
		if !force && !changed {
			continue
		}
		if err := d.sendEnvelope(ctx, conn, mod.ActorID(), d.readinessEnvelope(mod.ActorID(), state)); err != nil {
			return fmt.Errorf("proxy daemon readiness send %s: %w", mod.ActorID(), err)
		}
	}
	return nil
}

func (d *Daemon) recordReadiness(id actor.ActorID, ready bool, reason string, detail json.RawMessage) (readinessState, bool) {
	now := d.clock().UnixMilli()
	d.readinessMu.Lock()
	defer d.readinessMu.Unlock()
	prev := d.readiness[id]
	changed := !prev.Known || prev.Ready != ready || prev.Reason != reason || string(prev.Detail) != string(detail)
	next := prev
	next.Known = true
	next.Ready = ready
	next.Reason = reason
	next.Detail = append(json.RawMessage(nil), detail...)
	if changed || next.LastStateChangeAt == 0 {
		next.LastStateChangeAt = now
	}
	if ready && (changed || next.LastReadyAt == 0) {
		next.LastReadyAt = now
	}
	d.readiness[id] = next
	return next, changed
}

func (d *Daemon) readinessEnvelope(id actor.ActorID, state readinessState) message.Envelope {
	now := d.clock().UnixMilli()
	payload, _ := json.Marshal(map[string]any{
		"actor_id": string(id),
		"current": map[string]any{
			"ready":                state.Ready,
			"state":                readinessStateName(state.Ready),
			"reason":               state.Reason,
			"detail":               rawObject(state.Detail),
			"last_ready_at":        state.LastReadyAt,
			"last_state_change_at": state.LastStateChangeAt,
		},
		"checked_at": now,
		"changed_at": state.LastStateChangeAt,
	})
	return message.Envelope{
		ID:         message.ID(fmt.Sprintf("event:%s:actor.readiness.changed:%d", id, now)),
		TS:         now,
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "actor.readiness.changed",
		Payload:    payload,
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{actor.SystemActorID},
	}
}

func (d *Daemon) handleBusinessFrame(ctx context.Context, conn *WSConnection, frame DeviceFrame) {
	go func() {
		actorID := actor.ActorID(frame.ActorID)
		mod, ok := d.registry.Get(actorID)
		if !ok {
			return
		}
		var env message.Envelope
		if err := json.Unmarshal(frame.Payload, &env); err != nil {
			d.log.Printf("proxy frame decode failed actor=%s: %v", actorID, err)
			return
		}
		start := d.clock()
		if env.Type == "actor.status" {
			d.log.Printf("proxy actor.status received actor=%s request=%s", actorID, env.ID)
		}
		var reqCtx context.Context
		var cancel context.CancelFunc
		if env.ExpiresAt != nil && *env.ExpiresAt > 0 {
			reqCtx, cancel = context.WithDeadline(ctx, time.UnixMilli(*env.ExpiresAt))
		} else {
			reqCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		}
		defer cancel()

		resp, err := mod.Handle(reqCtx, env)
		if err != nil {
			resp = failedResponse(d.clock, env, actorID, message.TerminalReceiverInternalError, "module_error", err.Error())
		}
		if err := d.sendEnvelope(ctx, conn, actorID, resp); err != nil {
			d.log.Printf("proxy response send failed actor=%s request=%s: %v", actorID, env.ID, err)
			return
		}
		if env.Type == "actor.status" {
			d.log.Printf("proxy actor.status responded actor=%s request=%s duration_ms=%d", actorID, env.ID, d.clock().Sub(start).Milliseconds())
		}
	}()
}

func (d *Daemon) sendEnvelope(ctx context.Context, conn *WSConnection, actorID actor.ActorID, env message.Envelope) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	frame := DeviceFrame{
		Direction: "from_device",
		ActorID:   string(actorID),
		ChannelID: string(env.ChannelID),
		Payload:   raw,
	}
	if env.Kind == message.KindResponse {
		frame.RequestID = env.ParentID.String()
		frame.CorrelationID = env.CorrelationID.String()
		if frame.CorrelationID == "" {
			frame.CorrelationID = frame.RequestID
		}
	}
	if env.ExpiresAt != nil {
		frame.ExpiresAt = *env.ExpiresAt
	}
	return conn.WriteFrame(ctx, frame)
}

func failedResponse(clock func() time.Time, req message.Envelope, sender actor.ActorID, reason message.TerminalFailureReason, code, detail string) message.Envelope {
	payload, _ := json.Marshal(map[string]any{
		"status":     "failed",
		"reason":     string(reason),
		"error_code": code,
		"detail":     detail,
	})
	hash, err := message.CanonicalHashPayload(payload)
	if err != nil {
		hash = fmt.Sprintf("%d", clock().UnixNano())
	}
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = req.ID
	}
	now := clock().UnixMilli()
	resp := message.Envelope{
		ID:            message.ID("response:" + req.ID.String() + ":" + hash),
		TS:            now,
		ChannelID:     req.ChannelID,
		Sender:        message.Sender{Kind: actor.KindTool, ID: sender},
		Kind:          message.KindResponse,
		Type:          req.Type,
		Payload:       payload,
		ParentID:      req.ID,
		CorrelationID: correlationID,
		Visibility:    req.Visibility,
		Audience:      message.Audience{req.Sender.ID},
	}
	if req.ExpiresAt != nil {
		exp := *req.ExpiresAt
		resp.ExpiresAt = &exp
	}
	return resp
}

type capabilitySet struct {
	Name             string                             `json:"name,omitempty"`
	Description      string                             `json:"description,omitempty"`
	SkillDoc         string                             `json:"skill_doc,omitempty"`
	Types            []string                           `json:"types,omitempty"`
	TypeDeclarations map[string]adapter.TypeDeclaration `json:"type_declarations,omitempty"`
	MaxPendingMs     int64                              `json:"max_pending_ms,omitempty"`
}

func capabilityFromDeclaration(decl adapter.Declaration) (json.RawMessage, error) {
	raw, err := json.Marshal(capabilitySet{
		Name:             decl.Name,
		Description:      decl.Description,
		SkillDoc:         decl.SkillDoc,
		Types:            append([]string(nil), decl.Types...),
		TypeDeclarations: decl.TypeDeclarations,
		MaxPendingMs:     decl.MaxPendingMs,
	})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func rawObject(raw json.RawMessage) map[string]any {
	out := map[string]any{}
	if len(raw) == 0 {
		return out
	}
	_ = json.Unmarshal(raw, &out)
	if out == nil {
		return map[string]any{}
	}
	return out
}

func readinessStateName(ready bool) string {
	if ready {
		return "ready"
	}
	return "not_ready"
}

func (d *Daemon) hostLabel() string {
	if d.cfg.HostLabel != "" {
		return d.cfg.HostLabel
	}
	if d.hostname != "" {
		return d.hostname
	}
	return "coagent-proxy"
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}

func redactedEndpoint(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "<invalid>"
	}
	q := u.Query()
	if q.Has(QueryParamAPIKey) {
		q.Set(QueryParamAPIKey, "redacted")
		u.RawQuery = q.Encode()
	}
	return u.String()
}

type noopLogger struct{}

func (noopLogger) Printf(string, ...any) {}
