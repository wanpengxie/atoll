package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	sysOpStarted   = "sysop_started"
	sysOpCompleted = "sysop_completed"
)

type sysOpStore struct {
	db        *sql.DB
	channelID channel.ID
	onCommit  func()
}

type completedPayload struct {
	Operation     string                     `json:"operation"`
	RequestDigest string                     `json:"request_digest"`
	Result        json.RawMessage            `json:"result,omitempty"`
	ErrorCode     channel.OperationErrorCode `json:"error_code,omitempty"`
	ErrorDetail   string                     `json:"error_detail,omitempty"`
}

type sysOpOutcome struct {
	result  any
	effects storespec.PostCommitEffects
	opErr   *channel.OperationError
}

func newSysOpStore(db *sql.DB, channelID channel.ID, onCommit func()) *sysOpStore {
	return &sysOpStore{db: db, channelID: channelID, onCommit: onCommit}
}

func (s *sysOpStore) IsBound(ctx context.Context, id storespec.DaemonID) (bool, error) {
	var bound bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channel_daemon_bindings WHERE daemon_id=?)`, string(id)).Scan(&bound)
	return bound, err
}

func (s *sysOpStore) ListBound(ctx context.Context) ([]storespec.DaemonID, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT daemon_id FROM channel_daemon_bindings ORDER BY daemon_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storespec.DaemonID
	for rows.Next() {
		var id storespec.DaemonID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *sysOpStore) LookupCompleted(ctx context.Context, anchor, digest string) (storespec.CompletedView, bool, error) {
	return lookupCompleted(ctx, s.db, anchor, digest)
}

type sysOpRowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func lookupCompleted(ctx context.Context, q sysOpRowQuerier, anchor, digest string) (storespec.CompletedView, bool, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT payload FROM messages WHERE correlation_id=? AND kind='event' AND type=?`, anchor, sysOpCompleted).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.CompletedView{}, false, nil
	}
	if err != nil {
		return storespec.CompletedView{}, false, err
	}
	var payload completedPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return storespec.CompletedView{}, false, fmt.Errorf("store: decode completed sysop: %w", err)
	}
	if digest != "" && payload.RequestDigest != digest {
		return storespec.CompletedView{}, true, &channel.OperationError{Code: channel.ErrCodeRefConflict, Detail: "anchor reused with a different request digest"}
	}
	return storespec.CompletedView{
		Operation: payload.Operation, RequestDigest: payload.RequestDigest,
		Result: payload.Result, ErrorCode: payload.ErrorCode, ErrorDetail: payload.ErrorDetail,
	}, true, nil
}

func (s *sysOpStore) run(ctx context.Context, meta storespec.SysOpMeta, operation string, execute func(*sql.Tx, int64) (sysOpOutcome, error)) (json.RawMessage, storespec.PostCommitEffects, error) {
	if meta.Anchor == "" || meta.RequestDigest == "" {
		return nil, storespec.PostCommitEffects{}, errors.New("store: sysop anchor and digest required")
	}
	if meta.Source != storespec.SysOpSourceSystem && meta.Source != storespec.SysOpSourceMember {
		return nil, storespec.PostCommitEffects{}, errors.New("store: invalid sysop source")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, storespec.PostCommitEffects{}, err
	}
	defer tx.Rollback()
	completed, found, err := lookupCompleted(ctx, tx, meta.Anchor, meta.RequestDigest)
	if err != nil {
		return nil, storespec.PostCommitEffects{}, err
	}
	if found {
		if completed.Operation != operation {
			return nil, storespec.PostCommitEffects{}, &channel.OperationError{Code: channel.ErrCodeRefConflict, Detail: "anchor reused for another operation"}
		}
		if completed.ErrorCode != "" {
			return nil, storespec.PostCommitEffects{}, &channel.OperationError{Code: completed.ErrorCode, Detail: completed.ErrorDetail}
		}
		return append(json.RawMessage(nil), completed.Result...), storespec.PostCommitEffects{}, nil
	}
	now := time.Now().UnixMilli()
	started, _ := json.Marshal(map[string]any{
		"operation": operation, "source": meta.Source, "sender": meta.Sender,
		"request_digest": meta.RequestDigest,
	})
	if err := s.appendEvent(ctx, tx, sysOpStarted, meta.Anchor, started, now); err != nil {
		return nil, storespec.PostCommitEffects{}, err
	}
	var outcome sysOpOutcome
	if meta.DecisiveError != nil {
		if meta.DecisiveError.Retryable {
			return nil, storespec.PostCommitEffects{}, meta.DecisiveError
		}
		outcome = decisive(meta.DecisiveError.Code, meta.DecisiveError.Detail)
	} else {
		outcome, err = execute(tx, now)
	}
	if err != nil {
		return nil, storespec.PostCommitEffects{}, err
	}
	if outcome.opErr != nil && outcome.opErr.Retryable {
		return nil, storespec.PostCommitEffects{}, outcome.opErr
	}
	// Member-source rejections are noise, not truth: the whole transaction
	// (started event included) rolls back and only the typed reply leaves the
	// component. Anything else would let a rejected sender grow the channel
	// ledger by repeating garbage — a rejection-DDOS through the kernel's
	// serial section. Redelivery re-judges against current state, which is
	// freshness, not a defect: a rejection never published anything to replay.
	// System-source decisive rejections still commit their pair — the realm's
	// same-ref retry machinery needs a terminal to stop on, and ref_conflict
	// protection needs the digest row.
	if outcome.opErr != nil && meta.Source == storespec.SysOpSourceMember {
		return nil, storespec.PostCommitEffects{}, outcome.opErr
	}
	var raw json.RawMessage
	if outcome.result != nil {
		raw, err = json.Marshal(outcome.result)
		if err != nil {
			return nil, storespec.PostCommitEffects{}, err
		}
	}
	payload := completedPayload{Operation: operation, RequestDigest: meta.RequestDigest, Result: raw}
	if outcome.opErr != nil {
		payload.ErrorCode = outcome.opErr.Code
		payload.ErrorDetail = outcome.opErr.Detail
	}
	completedRaw, _ := json.Marshal(payload)
	if err := s.appendEvent(ctx, tx, sysOpCompleted, meta.Anchor, completedRaw, now); err != nil {
		return nil, storespec.PostCommitEffects{}, err
	}
	if err := tx.Commit(); err != nil {
		return nil, storespec.PostCommitEffects{}, err
	}
	if s.onCommit != nil {
		s.onCommit()
	}
	if outcome.opErr != nil {
		return nil, storespec.PostCommitEffects{}, outcome.opErr
	}
	return raw, outcome.effects, nil
}

func (s *sysOpStore) appendEvent(ctx context.Context, tx *sql.Tx, typ, anchor string, payload json.RawMessage, at int64) error {
	_, err := appendTx(ctx, tx, &message.Envelope{
		ID: message.ID(uuid.NewString()), TS: at, TSReceived: at, ChannelID: s.channelID,
		Sender: message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:   message.KindEvent, Type: typ, Payload: payload,
		CorrelationID: message.ID(anchor), Visibility: message.VisibilitySystem,
		Audience: message.Audience{actor.SystemActorID},
	}, false)
	return err
}

func decisive(code channel.OperationErrorCode, detail string) sysOpOutcome {
	return sysOpOutcome{opErr: &channel.OperationError{Code: code, Detail: detail}}
}

func decodeResult[T any](raw json.RawMessage, effects storespec.PostCommitEffects, err error) (T, error) {
	var result T
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return result, fmt.Errorf("store: decode sysop result: %w", err)
	}
	switch value := any(&result).(type) {
	case *storespec.AdmitResult:
		value.Effects = effects
	case *storespec.IntroduceResult:
		value.Effects = effects
	case *storespec.BindingResult:
		value.Effects = effects
	case *storespec.RestartResult:
		value.Effects = effects
	case *storespec.SetDefaultResult:
		value.Effects = effects
	}
	return result, nil
}

func (s *sysOpStore) Admit(ctx context.Context, in storespec.AdmitTx) (storespec.AdmitResult, error) {
	raw, effects, err := s.run(ctx, in.SysOpMeta, "admit", func(tx *sql.Tx, now int64) (sysOpOutcome, error) {
		if in.Source != storespec.SysOpSourceSystem {
			return decisive(channel.ErrCodeNotAcceptedSource, "admit accepts only the system source"), nil
		}
		if in.Principal == "" {
			return decisive(channel.ErrCodeBadPayload, "principal required"), nil
		}
		var existing actor.ActorID
		err := tx.QueryRowContext(ctx, `SELECT actor_id FROM actor_registry WHERE actor_kind='human' AND principal=? AND deregistered_at IS NULL`, in.Principal).Scan(&existing)
		if err == nil {
			return sysOpOutcome{result: storespec.AdmitResult{ActorID: existing}}, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return sysOpOutcome{}, err
		}
		id, err := mintActorIDTx(ctx, tx, actor.KindHuman, in.Principal, now)
		if err != nil {
			return sysOpOutcome{}, err
		}
		if err := insertDeclaredTx(ctx, tx, id, actor.KindHuman, in.Principal, "", nil, channel.RenderedSnapshot{
			Class: "human", Placement: channel.Placement{Kind: channel.PlacementServer},
		}, storespec.RoleNone, now); err != nil {
			return sysOpOutcome{}, err
		}
		if _, err := appendTx(ctx, tx, actorRegisteredEnvelope(s.channelID, id, actor.KindHuman, "", now), false); err != nil {
			return sysOpOutcome{}, err
		}
		return sysOpOutcome{
			result:  storespec.AdmitResult{ActorID: id, Created: true},
			effects: storespec.PostCommitEffects{Poke: true, Principals: []string{in.Principal}},
		}, nil
	})
	return decodeResult[storespec.AdmitResult](raw, effects, err)
}

func (s *sysOpStore) Introduce(ctx context.Context, in storespec.IntroduceTx) (storespec.IntroduceResult, error) {
	raw, effects, err := s.run(ctx, in.SysOpMeta, "introduce", func(tx *sql.Tx, now int64) (sysOpOutcome, error) {
		// Initiator qualification lives in Home's unified authority gate. The
		// durable store cannot re-judge run-world fork actors, so it receives the
		// actor coordinate plus the login principal (when the active actor has
		// one) solely for private-declaration ownership comparison.
		if in.DeclID == "" || in.InitiatorActorID == "" {
			return decisive(channel.ErrCodeBadPayload, "decl_id and initiator_actor_id required"), nil
		}
		if _, ok := actor.ParseKind(string(in.Kind)); !ok || in.Kind == actor.KindHuman || in.Kind == actor.KindSystem {
			return decisive(channel.ErrCodeUnknownClass, in.Rendered.Class), nil
		}
		if err := in.Rendered.Validate(); err != nil {
			return decisive(channel.ErrCodeBadPayload, err.Error()), nil
		}
		if in.Source == storespec.SysOpSourceMember && in.Visibility != "public" {
			return decisive(channel.ErrCodeForbidden, "member introduction is limited to public declarations"), nil
		}
		if in.Visibility != "public" && in.InitiatorPrincipal != in.OwnerPrincipal {
			return decisive(channel.ErrCodeForbidden, "declaration is private"), nil
		}
		var existing actor.ActorID
		err := tx.QueryRowContext(ctx, `SELECT actor_id FROM actor_registry WHERE source_decl_id=? AND deregistered_at IS NULL`, in.DeclID).Scan(&existing)
		if err == nil {
			return sysOpOutcome{result: storespec.IntroduceResult{ActorID: existing}}, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return sysOpOutcome{}, err
		}
		rendered := in.Rendered
		if rendered.Placement.Kind == channel.PlacementDaemon {
			host := rendered.Placement.DesiredHost
			if host == "" {
				err = tx.QueryRowContext(ctx, `SELECT daemon_id FROM channel_daemon_bindings ORDER BY daemon_id LIMIT 1`).Scan(&host)
			} else {
				var bound bool
				err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channel_daemon_bindings WHERE daemon_id=?)`, host).Scan(&bound)
				if err == nil && !bound {
					err = sql.ErrNoRows
				}
			}
			if errors.Is(err, sql.ErrNoRows) {
				return decisive(channel.ErrCodeInvalidDesiredHost, "daemon is not bound to this channel"), nil
			}
			if err != nil {
				return sysOpOutcome{}, err
			}
			rendered.Placement.DesiredHost = host
			rendered, err = rendered.Seal()
			if err != nil {
				return sysOpOutcome{}, err
			}
		}
		id, err := mintActorIDTx(ctx, tx, in.Kind, in.DeclID, now)
		if err != nil {
			return sysOpOutcome{}, err
		}
		if err := insertDeclaredTx(ctx, tx, id, in.Kind, "", in.DeclID, rendered.Config, rendered, storespec.RoleNone, now); err != nil {
			return sysOpOutcome{}, err
		}
		binding := actor.Binding("")
		if rendered.Placement.Kind == channel.PlacementDaemon {
			binding = actor.BindingRuntimeInboundViaRelay
		}
		if _, err := appendTx(ctx, tx, actorRegisteredEnvelope(s.channelID, id, in.Kind, binding, now), false); err != nil {
			return sysOpOutcome{}, err
		}
		return sysOpOutcome{result: storespec.IntroduceResult{ActorID: id, Created: true}, effects: storespec.PostCommitEffects{Poke: true}}, nil
	})
	return decodeResult[storespec.IntroduceResult](raw, effects, err)
}

func (s *sysOpStore) AttachDaemon(ctx context.Context, in storespec.AttachTx) (storespec.AttachResult, error) {
	raw, effects, err := s.run(ctx, in.SysOpMeta, "attach_daemon", func(tx *sql.Tx, now int64) (sysOpOutcome, error) {
		if in.Source != storespec.SysOpSourceSystem {
			return decisive(channel.ErrCodeNotAcceptedSource, "attach_daemon accepts only the system source"), nil
		}
		if in.DaemonID == "" {
			return decisive(channel.ErrCodeBadPayload, "daemon_id required"), nil
		}
		res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO channel_daemon_bindings(daemon_id,attached_at) VALUES (?,?)`, string(in.DaemonID), now)
		if err != nil {
			return sysOpOutcome{}, err
		}
		created, _ := res.RowsAffected()
		return sysOpOutcome{result: storespec.BindingResult{Bound: true}, effects: storespec.PostCommitEffects{Poke: created == 1}}, nil
	})
	return decodeResult[storespec.AttachResult](raw, effects, err)
}

func (s *sysOpStore) DetachDaemon(ctx context.Context, in storespec.DetachTx) (storespec.DetachResult, error) {
	raw, effects, err := s.run(ctx, in.SysOpMeta, "detach_daemon", func(tx *sql.Tx, now int64) (sysOpOutcome, error) {
		if in.Source != storespec.SysOpSourceSystem {
			return decisive(channel.ErrCodeNotAcceptedSource, "detach_daemon accepts only the system source"), nil
		}
		if in.DaemonID == "" {
			return decisive(channel.ErrCodeBadPayload, "daemon_id required"), nil
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM channel_daemon_bindings WHERE daemon_id=?`, string(in.DaemonID)); err != nil {
			return sysOpOutcome{}, err
		}
		_, _, err := cascadeWriteTx(ctx, tx, s.channelID, in.DurableIDs, in.Envelopes, now)
		if err != nil {
			return sysOpOutcome{}, err
		}
		daemon := in.DaemonID
		return sysOpOutcome{
			result:  storespec.BindingResult{Bound: false, ClearedInstances: in.AllIDs},
			effects: storespec.PostCommitEffects{Poke: true, KickDaemon: &daemon},
		}, nil
	})
	return decodeResult[storespec.DetachResult](raw, effects, err)
}

// ApplyResolvedDeclaration is deliberately separate from SysOpAdmission: it
// is Home's private level-reconcile store port, not a realm-facing word. Equal
// content returns before either sysop event is appended, making repeated pokes
// genuine zero-write observations.
func (s *sysOpStore) ApplyResolvedDeclaration(ctx context.Context, in storespec.DeclarationSyncTx) (storespec.DeclarationSyncResult, error) {
	if in.Anchor == "" || in.RequestDigest == "" || in.Source != storespec.SysOpSourceSystem {
		return storespec.DeclarationSyncResult{}, errors.New("store: declaration sync requires a system anchor and digest")
	}
	if in.ActorID == "" || in.DeclID == "" || in.Class == "" {
		return storespec.DeclarationSyncResult{}, errors.New("store: declaration sync requires actor, declaration, and class")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	defer tx.Rollback()

	completed, found, err := lookupCompleted(ctx, tx, in.Anchor, in.RequestDigest)
	if err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	if found {
		if completed.Operation != "sync_declaration" {
			return storespec.DeclarationSyncResult{}, &channel.OperationError{Code: channel.ErrCodeRefConflict, Detail: "anchor reused for another operation"}
		}
		var replay storespec.DeclarationSyncResult
		if err := json.Unmarshal(completed.Result, &replay); err != nil {
			return storespec.DeclarationSyncResult{}, err
		}
		return replay, nil
	}

	var version int64
	var class, placement, host string
	var config []byte
	var idleMS int64
	err = tx.QueryRowContext(ctx, `SELECT r.current_decl_version,d.class,d.config_json,d.placement,d.desired_host,d.t_idle_ms
		FROM actor_registry r JOIN actor_decl_versions d
		  ON d.actor_id=r.actor_id AND d.version=r.current_decl_version
		WHERE r.actor_id=? AND r.source_decl_id=? AND r.deregistered_at IS NULL`, string(in.ActorID), in.DeclID).
		Scan(&version, &class, &config, &placement, &host, &idleMS)
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.DeclarationSyncResult{Status: storespec.DeclarationAbsent}, nil
	}
	if err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	current, err := (channel.RenderedSnapshot{
		Class: class, Config: append(json.RawMessage(nil), config...),
		Placement: channel.Placement{Kind: channel.PlacementKind(placement), DesiredHost: host}, TIdleMS: idleMS,
	}).Seal()
	if err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	candidate, err := (channel.RenderedSnapshot{
		Class: in.Class, Config: append(json.RawMessage(nil), in.Config...),
		Placement: current.Placement, TIdleMS: current.TIdleMS,
	}).Seal()
	if err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	if current.Digest == candidate.Digest {
		return storespec.DeclarationSyncResult{Status: storespec.DeclarationEqual, Version: version}, nil
	}

	now := time.Now().UnixMilli()
	started, _ := json.Marshal(map[string]any{
		"operation": "sync_declaration", "source": in.Source, "sender": in.Sender,
		"request_digest": in.RequestDigest,
	})
	if err := s.appendEvent(ctx, tx, sysOpStarted, in.Anchor, started, now); err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM actor_decl_versions WHERE actor_id=?`, string(in.ActorID)).Scan(&version); err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	if err := insertDeclVersionTx(ctx, tx, in.ActorID, version, candidate, now); err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE actor_registry SET current_decl_version=? WHERE actor_id=? AND source_decl_id=? AND deregistered_at IS NULL`, version, string(in.ActorID), in.DeclID)
	if err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		if err != nil {
			return storespec.DeclarationSyncResult{}, err
		}
		return storespec.DeclarationSyncResult{}, errors.New("store: declaration sync actor disappeared inside transaction")
	}
	result := storespec.DeclarationSyncResult{
		Status: storespec.DeclarationApplied, Version: version,
		Effects: storespec.PostCommitEffects{Poke: true, Despawn: []actor.ActorID{in.ActorID}},
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	completedRaw, _ := json.Marshal(completedPayload{Operation: "sync_declaration", RequestDigest: in.RequestDigest, Result: raw})
	if err := s.appendEvent(ctx, tx, sysOpCompleted, in.Anchor, completedRaw, now); err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	if s.onCommit != nil {
		s.onCommit()
	}
	return result, nil
}

func (s *sysOpStore) RemoveActor(ctx context.Context, in storespec.RemoveTx) (storespec.RemoveResult, error) {
	raw, effects, err := s.run(ctx, in.SysOpMeta, "remove_actor", func(tx *sql.Tx, now int64) (sysOpOutcome, error) {
		if in.Source != storespec.SysOpSourceMember && in.Source != storespec.SysOpSourceSystem {
			return decisive(channel.ErrCodeNotAcceptedSource, "remove_actor accepts member or system source"), nil
		}
		if in.Target == "" || in.InitiatorActorID == "" {
			return decisive(channel.ErrCodeBadPayload, "target and initiator_actor_id required"), nil
		}
		if in.Target == actor.SystemActorID {
			return decisive(channel.ErrCodeProtectedActor, "the system anchor actor cannot be removed"), nil
		}
		// The channel owner root is protected. It is checked here (durable role
		// truth) as a decisive verdict rather than the EndCascade sentinel — the
		// member word must leave a replayable protected_actor terminal.
		var role string
		err := tx.QueryRowContext(ctx, `SELECT role FROM actor_registry WHERE actor_id=? AND deregistered_at IS NULL`, string(in.Target)).Scan(&role)
		if err == nil && role == string(storespec.RoleOwner) {
			return decisive(channel.ErrCodeProtectedActor, "channel owner is protected"), nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return sysOpOutcome{}, err
		}
		// Idempotent no-op: an already-gone target yields an empty envelope set
		// (Home's closure found nothing to end). The event pair still commits, so
		// the completed terminal replays success with an empty removed set.
		_, newlyEnded, err := cascadeWriteTx(ctx, tx, s.channelID, in.DurableIDs, in.Envelopes, now)
		if err != nil {
			return sysOpOutcome{}, err
		}
		// No PostCommitEffects: the caller drives the whole session teardown
		// (durable + run-world) from its own plan under the Fork-race locks.
		return sysOpOutcome{result: storespec.RemoveResult{Removed: newlyEnded}}, nil
	})
	return decodeResult[storespec.RemoveResult](raw, effects, err)
}

func (s *sysOpStore) RestartActor(ctx context.Context, in storespec.RestartTx) (storespec.RestartResult, error) {
	raw, effects, err := s.run(ctx, in.SysOpMeta, "restart_actor", func(tx *sql.Tx, now int64) (sysOpOutcome, error) {
		if in.Source != storespec.SysOpSourceMember {
			return decisive(channel.ErrCodeNotAcceptedSource, "restart_actor accepts only the member source"), nil
		}
		if in.Target == "" {
			return decisive(channel.ErrCodeBadPayload, "instance_id required"), nil
		}
		if in.Target == actor.SystemActorID {
			return decisive(channel.ErrCodeProtectedActor, "the system anchor actor cannot be restarted"), nil
		}
		// Restart is an INCARNATION-axis operation: identity truth (who is a
		// member, which version they run) does not change, so no actor_registry
		// row is touched. The durable trace is the event pair alone; the
		// post-commit effect retires the current body with restart intent in
		// the in-memory liveness ledger — the designated home of incarnation
		// coordination (two-ledger law) — and reconcile mints the next body.
		// A crash that loses the effect kills every body anyway; reboot
		// re-embodies from identity truth, which IS the requested restart.
		var active int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_registry WHERE actor_id=? AND deregistered_at IS NULL`, string(in.Target)).Scan(&active); err != nil {
			return sysOpOutcome{}, err
		}
		if active == 0 {
			return decisive(channel.ErrCodeNotInComposition, "target is not an active composition member"), nil
		}
		return sysOpOutcome{
			result:  storespec.RestartResult{},
			effects: storespec.PostCommitEffects{Poke: true, Despawn: []actor.ActorID{in.Target}},
		}, nil
	})
	return decodeResult[storespec.RestartResult](raw, effects, err)
}

func (s *sysOpStore) SetDefaultAgent(ctx context.Context, in storespec.SetDefaultTx) (storespec.SetDefaultResult, error) {
	raw, effects, err := s.run(ctx, in.SysOpMeta, "set_default_agent", func(tx *sql.Tx, now int64) (sysOpOutcome, error) {
		if in.Source != storespec.SysOpSourceMember {
			return decisive(channel.ErrCodeNotAcceptedSource, "set_default_agent accepts only the member source"), nil
		}
		if in.Target != "" {
			var active int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_registry WHERE actor_id=? AND deregistered_at IS NULL`, string(in.Target)).Scan(&active); err != nil {
				return sysOpOutcome{}, err
			}
			if active != 1 {
				return decisive(channel.ErrCodeMemberInactive, "target is not an active member"), nil
			}
		}
		var value any
		if in.Target != "" {
			value = string(in.Target)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO channel_routing(id,default_agent) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET default_agent=excluded.default_agent`, value); err != nil {
			return sysOpOutcome{}, err
		}
		return sysOpOutcome{result: storespec.SetDefaultResult{}}, nil
	})
	return decodeResult[storespec.SetDefaultResult](raw, effects, err)
}

func principalActiveTx(ctx context.Context, tx *sql.Tx, principal string) (bool, error) {
	var active bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM actor_registry WHERE principal=? AND deregistered_at IS NULL)`, principal).Scan(&active)
	return active, err
}

func mintActorIDTx(ctx context.Context, tx *sql.Tx, kind actor.Kind, principal string, at int64) (actor.ActorID, error) {
	for attempt := int64(0); attempt < 1000; attempt++ {
		candidate := actor.ActorID(fmt.Sprintf("%s:%s:%d", kind, principal, at+attempt))
		var used bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM actor_registry WHERE actor_id=?)`, string(candidate)).Scan(&used); err != nil {
			return "", err
		}
		if !used {
			return candidate, nil
		}
	}
	return "", errors.New("store: cannot mint actor id")
}

func insertDeclaredTx(ctx context.Context, tx *sql.Tx, id actor.ActorID, kind actor.Kind, principal, source string, config json.RawMessage, rendered channel.RenderedSnapshot, role storespec.ActorRole, at int64) error {
	binding := actor.Binding("")
	if rendered.Placement.Kind == channel.PlacementDaemon {
		binding = actor.BindingRuntimeInboundViaRelay
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO actor_registry(actor_id,actor_kind,principal,source_decl_id,role,actor_binding,current_decl_version,created_at,deregistered_at) VALUES (?,?,?,?,?,?,1,?,NULL)`, string(id), string(kind), principal, source, string(role), nullableBinding(binding), at); err != nil {
		return err
	}
	return insertDeclVersionTx(ctx, tx, id, 1, rendered, at)
}

func insertDeclVersionTx(ctx context.Context, tx *sql.Tx, id actor.ActorID, version int64, rendered channel.RenderedSnapshot, at int64) error {
	var config any
	if rendered.Config != nil {
		config = string(rendered.Config)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO actor_decl_versions(actor_id,version,class,config_json,placement,desired_host,t_idle_ms,created_at) VALUES (?,?,?,?,?,?,?,?)`, string(id), version, rendered.Class, config, string(rendered.Placement.Kind), rendered.Placement.DesiredHost, rendered.TIdleMS, at)
	return err
}

var _ storespec.SysOpAdmission = (*sysOpStore)(nil)
var _ storespec.DaemonBindingReader = (*sysOpStore)(nil)
