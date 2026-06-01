package typeinstall

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/store"
)

// Config wires the runtime-owned type install path. The service is shared
// by adapter install today and by worker type_install IPC in the future.
type Config struct {
	ChannelID     channel.ID
	ActorRegistry actorreg.Registry
	TypeRegistry  message.TypeRegistry
	HarnessChain  khar.Chain
	NowFn         func() int64
}

// Service validates type_registry rows, writes them, and mirrors the
// mutation as system.type.installed.
type Service struct {
	cfg      Config
	registry installRegistry
}

type installRegistry interface {
	message.TypeRegistry
	BeginInstall(ctx context.Context, row message.TypeRow) (store.TypeInstallAttempt, error)
	MarkInstalled(ctx context.Context, typeName, attemptID string) error
	MarkInstallFailed(ctx context.Context, typeName, attemptID, reason string) error
	RecoverInstalling(ctx context.Context, reason string) (int, error)
}

// New constructs a Service.
func New(cfg Config) (*Service, error) {
	if cfg.ChannelID == "" {
		return nil, fmt.Errorf("typeinstall: ChannelID required")
	}
	if cfg.ActorRegistry == nil {
		return nil, fmt.Errorf("typeinstall: ActorRegistry required")
	}
	if cfg.TypeRegistry == nil {
		return nil, fmt.Errorf("typeinstall: TypeRegistry required")
	}
	registry, ok := cfg.TypeRegistry.(installRegistry)
	if !ok {
		return nil, fmt.Errorf("typeinstall: TypeRegistry must support atomic install lifecycle")
	}
	if cfg.HarnessChain == nil {
		return nil, fmt.Errorf("typeinstall: HarnessChain required")
	}
	if cfg.NowFn == nil {
		cfg.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	return &Service{cfg: cfg, registry: registry}, nil
}

// InstallType validates row, upserts type_registry, and emits the
// system.type.installed mirror event through the channel harness.
func (s *Service) InstallType(ctx context.Context, row message.TypeRow) (message.TypeRow, error) {
	if err := s.validate(ctx, row); err != nil {
		return message.TypeRow{}, err
	}
	if existing, ok, err := s.registry.Lookup(ctx, row.Type); err != nil {
		return message.TypeRow{}, fmt.Errorf("typeinstall: registry lookup %s: %w", row.Type, err)
	} else if ok && sameTypeRow(existing, row) {
		return existing, nil
	}

	attempt, err := s.registry.BeginInstall(ctx, row)
	if err != nil {
		return message.TypeRow{}, &Error{
			Reason: message.InstallTypeRegistryInvalid,
			Err:    fmt.Errorf("typeinstall: registry begin install %s: %w", row.Type, err),
		}
	}
	mutationKind := "create"
	if attempt.Existed {
		mutationKind = "compatible_update"
	}

	if err := s.emitInstalled(ctx, attempt.Row, attempt.AttemptID, mutationKind); err != nil {
		_ = s.registry.MarkInstallFailed(context.WithoutCancel(ctx), attempt.Row.Type, attempt.AttemptID, err.Error())
		return message.TypeRow{}, err
	}
	if err := s.registry.MarkInstalled(ctx, attempt.Row.Type, attempt.AttemptID); err != nil {
		return message.TypeRow{}, &Error{
			Reason: message.InstallTypeRegistryInvalid,
			Err:    fmt.Errorf("typeinstall: registry mark installed %s: %w", attempt.Row.Type, err),
		}
	}
	return attempt.Row, nil
}

func sameTypeRow(a, b message.TypeRow) bool {
	if a.Type != b.Type ||
		a.HandlerActorID != b.HandlerActorID ||
		a.HandlerBinding != b.HandlerBinding ||
		a.MaxPendingMs != b.MaxPendingMs ||
		len(a.AllowedKinds) != len(b.AllowedKinds) {
		return false
	}
	for i := range a.AllowedKinds {
		if a.AllowedKinds[i] != b.AllowedKinds[i] {
			return false
		}
	}
	return true
}

func (s *Service) RecoverInstalling(ctx context.Context, reason string) (int, error) {
	if reason == "" {
		reason = "crash recovered before system.type.installed"
	}
	return s.registry.RecoverInstalling(ctx, reason)
}

func (s *Service) validate(ctx context.Context, row message.TypeRow) error {
	if strings.HasPrefix(row.Type, "system.") || strings.HasPrefix(row.Type, "actor.") {
		return &Error{Reason: message.InstallTypeRegistryReservedNamespace, Err: fmt.Errorf("typeinstall: reserved namespace type %q", row.Type)}
	}
	if err := row.Validate(); err != nil {
		return &Error{Reason: message.InstallTypeRegistryInvalid, Err: err}
	}
	if err := validateAllowedKinds(row); err != nil {
		return err
	}

	rec, ok, err := s.cfg.ActorRegistry.Lookup(ctx, row.HandlerActorID)
	if err != nil {
		return fmt.Errorf("typeinstall: actor lookup %s: %w", row.HandlerActorID, err)
	}
	if !ok || !rec.IsActive() {
		return &Error{Reason: message.InstallHandlerActorNotRegistered, Err: fmt.Errorf("typeinstall: handler actor %q not active", row.HandlerActorID)}
	}
	if rec.Binding != row.HandlerBinding {
		return &Error{
			Reason: message.InstallHandlerActorBindingMismatch,
			Err: fmt.Errorf("typeinstall: handler actor %q binding=%s row_binding=%s",
				row.HandlerActorID, rec.Binding, row.HandlerBinding),
		}
	}
	if rec.Kind == actor.KindTool && row.MaxPendingMs <= 0 {
		return &Error{Reason: message.InstallAdapterTimeoutMissing, Err: fmt.Errorf("typeinstall: max_pending_ms required for tool handler %q", row.HandlerActorID)}
	}
	return nil
}

func validateAllowedKinds(row message.TypeRow) error {
	if len(row.AllowedKinds) == 0 {
		return &Error{Reason: message.InstallTypeRegistryInvalid, Err: fmt.Errorf("typeinstall: type %q allowed_kinds empty", row.Type)}
	}
	seen := map[message.Kind]struct{}{}
	for _, k := range row.AllowedKinds {
		switch k {
		case message.KindEvent, message.KindRequest, message.KindResponse:
		default:
			return &Error{Reason: message.InstallTypeRegistryInvalid, Err: fmt.Errorf("typeinstall: type %q invalid kind %q", row.Type, k)}
		}
		if _, dup := seen[k]; dup {
			return &Error{Reason: message.InstallTypeRegistryInvalid, Err: fmt.Errorf("typeinstall: type %q duplicate kind %q", row.Type, k)}
		}
		seen[k] = struct{}{}
	}
	return nil
}

func (s *Service) emitInstalled(ctx context.Context, row message.TypeRow, attemptID, mutationKind string) error {
	now := s.cfg.NowFn()
	allowed := make([]string, len(row.AllowedKinds))
	for i, k := range row.AllowedKinds {
		allowed[i] = string(k)
	}
	payload, err := json.Marshal(map[string]any{
		"type":               row.Type,
		"allowed_kinds":      allowed,
		"handler_actor_id":   string(row.HandlerActorID),
		"handler_binding":    string(row.HandlerBinding),
		"install_attempt_id": attemptID,
		"max_pending_ms":     row.MaxPendingMs,
		"installed_at":       now,
		"mutation_kind":      mutationKind,
	})
	if err != nil {
		return fmt.Errorf("typeinstall: marshal system.type.installed payload: %w", err)
	}
	env := &message.Envelope{
		ID:         message.ID(fmt.Sprintf("system.type.installed:%s:%d", row.Type, now)),
		TS:         now,
		ChannelID:  s.cfg.ChannelID,
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "system.type.installed",
		Payload:    payload,
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{actor.SystemActorID},
	}
	chainCtx := rtharness.CtxWithCaller(ctx, rtharness.CallerContext{
		ActorID:                 actor.SystemActorID,
		ChannelID:               s.cfg.ChannelID,
		AllowProvidedSenderKind: true,
	})
	res, err := s.cfg.HarnessChain.Write(chainCtx, env)
	if err != nil {
		return fmt.Errorf("typeinstall: write system.type.installed: %w", err)
	}
	if !res.Accepted() {
		return fmt.Errorf("typeinstall: system.type.installed rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}
	return nil
}

// Error carries the install_reason closed-set value across package seams.
type Error struct {
	Reason message.InstallReason
	Err    error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return string(e.Reason)
	}
	return e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) InstallReason() message.InstallReason { return e.Reason }
