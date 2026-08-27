package home

import (
	"encoding/json"
	"time"

	"context"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// opEntry is a transport adapter. Actor lifecycle and identity mutations all
// enter Controller through actorSystem; this type owns no actor map, gate, or
// execution state. Business policy (owner protection, declaration visibility,
// placement host) is resolved HERE, before the typed command is issued.
type opEntry struct {
	home *Home
}

// removeRequest is membrane-private. Removal is only accepted as an operate
// frame; it is deliberately not part of the cross-package channel contract.
type removeRequest struct {
	Target           actor.ActorID
	InitiatorActorID actor.ActorID
	Cause            message.Cause
}

var (
	_ sysactor.OperateExecutor = (*opEntry)(nil)
)

func (e *opEntry) available() error {
	if e == nil || e.home == nil || e.home.closed.Load() || e.home.actors == nil {
		return &channelspec.OperationError{
			Code: channelspec.ErrCodeChannelUnavailable, Detail: "channel is not serving", Retryable: true,
		}
	}
	return nil
}

func (e *opEntry) admit(ctx context.Context, principal string) (actorctl.AdmitResult, error) {
	if err := e.available(); err != nil {
		return actorctl.AdmitResult{}, err
	}
	if principal == "" {
		return actorctl.AdmitResult{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeBadPayload, Detail: "principal required",
		}
	}
	catalog, ok := e.home.resolver.(PrincipalCatalog)
	if !ok {
		return actorctl.AdmitResult{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeAuthorityUnavailable, Detail: "principal registry unavailable", Retryable: true,
		}
	}
	kind, found, err := catalog.PrincipalKind(ctx, principal)
	if err != nil {
		return actorctl.AdmitResult{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeAuthorityUnavailable, Detail: err.Error(), Retryable: true,
		}
	}
	if !found {
		return actorctl.AdmitResult{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeBadPayload, Detail: "principal " + principal + " is not registered",
		}
	}
	if kind != actor.KindHuman {
		return actorctl.AdmitResult{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeBadPayload, Detail: "principal " + principal + " is " + string(kind) + "; system.member.admit accepts human principals only",
		}
	}
	result, err := e.home.actors.Admit(ctx, actorctl.AdmitRequest{Principal: principal})
	if err == nil && result.ActorID != "" {
		e.home.ensureSubjectSlot(result.ActorID)
	}
	return result, err
}

// introduce is the one introduction path. Both doors — the out-of-band
// admission call above and the in-gate operate frame below — reach the same
// verdict, because the verdict reads the initiator's own facts instead of
// taking the door's word for what kind of initiator it is.
func (e *opEntry) introduce(
	ctx context.Context,
	declID string,
	desiredHost string,
	initiator actor.ActorID,
) (actorctl.IntroduceResult, error) {
	command, err := e.home.resolveIntroduction(ctx, declID, desiredHost, initiator)
	if err != nil {
		return actorctl.IntroduceResult{}, err
	}
	result, err := e.home.actors.Introduce(ctx, command)
	return result, err
}

func (e *opEntry) remove(
	ctx context.Context,
	req removeRequest,
) (actorctl.RemoveResult, error) {
	if err := e.available(); err != nil {
		return actorctl.RemoveResult{}, err
	}
	if err := e.home.guardOwnerTerminal(ctx, req.Target); err != nil {
		return actorctl.RemoveResult{}, err
	}
	result, err := e.home.actors.Remove(ctx, actorctl.RemoveRequest{
		Target: req.Target, InitiatorActorID: req.InitiatorActorID, Cause: req.Cause,
	})
	return result, err
}

// Execute adapts the collaboration-plane system actor verbs.
func (e *opEntry) Execute(
	ctx context.Context,
	operation string,
	req sysactor.OperateRequest,
) (any, error) {
	if err := e.available(); err != nil {
		return nil, asOperateError(err)
	}
	switch operation {
	case sysactor.TypeMemberCreate:
		var payload struct {
			DeclID string `json:"decl_id"`
			// Optional: which of this channel's bound devices runs it. Omitted
			// means no preference. Only meaningful for a daemon-placed class.
			DesiredHost string `json:"desired_host,omitempty"`
		}
		if err := actorbase.DecodeStrict(req.Payload, &payload); err != nil || payload.DeclID == "" {
			return nil, &sysactor.OperateError{
				Code: string(channelspec.ErrCodeBadPayload), Detail: "decl_id required",
			}
		}
		result, err := e.introduce(ctx, payload.DeclID, payload.DesiredHost, req.Initiator)
		if err != nil {
			return nil, asOperateError(err)
		}
		by := map[string]any{"caller": req.Caller}
		if facts, active, factsErr := e.home.actors.ActorFacts(ctx, req.Initiator); factsErr == nil && active && facts.SourceDeclID == payload.DeclID {
			by = map[string]any{"fork_of": req.Initiator}
		}
		e.home.narrateBirth(ctx, req.Cause, result.ActorID, result.Created, map[string]any{
			"decl_id": payload.DeclID, "by": by,
		})
		return map[string]any{"member": result.ActorID}, nil

	case sysactor.TypeMemberAdmit:
		var payload struct {
			Principal string `json:"principal"`
		}
		if err := actorbase.DecodeStrict(req.Payload, &payload); err != nil || payload.Principal == "" {
			return nil, &sysactor.OperateError{
				Code: string(channelspec.ErrCodeBadPayload), Detail: "principal required",
			}
		}
		result, err := e.admit(ctx, payload.Principal)
		if err != nil {
			return nil, asOperateError(err)
		}
		e.home.narrateBirth(ctx, req.Cause, result.ActorID, result.Created, map[string]any{
			"principal": payload.Principal, "by": map[string]any{"caller": req.Caller},
		})
		return map[string]any{"member": result.ActorID}, nil

	case sysactor.TypeMemberDelete:
		// Two ways to say which seat, because there are two kinds of caller.
		// A member points at another member and says its id: {member}, resolved
		// by the ordinary segment rules. A caller that HOLDS the declaration —
		// the registry unseating the handle it seated, a tool tearing down what
		// it built — holds the key, not a spelling, and says {decl_id}. Making
		// it spell an id out of the key is how a contract turns into a guess:
		// the key is exact by construction and the spelling is only exact for
		// as long as nothing about the naming changes.
		var payload struct {
			Member actor.ActorID `json:"member"`
			DeclID string        `json:"decl_id"`
		}
		if err := actorbase.DecodeStrict(req.Payload, &payload); err != nil {
			return nil, &sysactor.OperateError{Code: string(channelspec.ErrCodeBadPayload), Detail: "system.member.delete takes exactly one of {member} or {decl_id}"}
		}
		if (payload.Member == "") == (payload.DeclID == "") {
			return nil, &sysactor.OperateError{Code: string(channelspec.ErrCodeBadPayload), Detail: "system.member.delete takes exactly one of {member} (an actor id or unambiguous segment) or {decl_id} (the declaration the member was seated from)"}
		}
		var resolved actor.ActorID
		var err error
		if payload.DeclID != "" {
			resolved, err = e.home.actors.MemberOfDeclaration(payload.DeclID)
		} else {
			resolved, err = e.home.actors.ResolveTarget(string(payload.Member))
		}
		if err != nil {
			return nil, asOperateError(err)
		}
		result, err := e.remove(ctx, removeRequest{
			Target: resolved, InitiatorActorID: req.Initiator, Cause: req.Cause,
		})
		if err != nil {
			return nil, asOperateError(err)
		}
		return map[string]any{"removed": result.Removed}, nil

	case sysactor.TypeMemberRestart:
		var payload struct {
			Member actor.ActorID `json:"member"`
		}
		if err := actorbase.DecodeStrict(req.Payload, &payload); err != nil || payload.Member == "" {
			return nil, &sysactor.OperateError{
				Code: string(channelspec.ErrCodeBadPayload), Detail: "member required",
			}
		}
		resolved, err := e.home.actors.ResolveTarget(string(payload.Member))
		if err != nil {
			return nil, asOperateError(err)
		}
		payload.Member = resolved
		if err := e.home.guardOwnerTerminal(ctx, payload.Member); err != nil {
			return nil, asOperateError(err)
		}
		if err := e.home.actors.Restart(ctx, actorctl.RestartRequest{
			ActorID: payload.Member,
		}); err != nil {
			return nil, asOperateError(err)
		}
		return map[string]any{"member": payload.Member}, nil

	case sysactor.TypeRequestCancel:
		var payload struct {
			RequestID message.ID `json:"request_id"`
		}
		if err := actorbase.DecodeStrict(req.Payload, &payload); err != nil || payload.RequestID == "" {
			return nil, &sysactor.OperateError{
				Code: string(channelspec.ErrCodeBadPayload), Detail: "request_id required",
			}
		}
		return e.cancelRequest(ctx, payload.RequestID, req.Caller.Actor)

	case sysactor.TypeMemberRestartAll:
		var payload struct{}
		if err := actorbase.DecodeStrictEmpty(req.Payload, &payload); err != nil {
			return nil, &sysactor.OperateError{
				Code: string(channelspec.ErrCodeBadPayload), Detail: err.Error(),
			}
		}
		return e.restartChannel(ctx)

	default:
		return nil, &sysactor.OperateError{
			Code: string(channelspec.ErrCodeNotAcceptedSource), Detail: "operation is not accepted",
		}
	}
}

// cancelRequest closes an open request on a member's say-so, authored by the
// substrate.
//
// The detour is forced by the closure model, not chosen: the harness accepts a
// terminal from exactly three kinds of author — the receiver (its own exit),
// the caller (closing the account it opened), and the substrate. A member that
// is neither the caller nor the receiver has no arm at all, so "let me stop
// that" cannot be a message it writes itself; it has to be a thing it ASKS the
// substrate to observe. That is this word.
//
// The terminal is byte-identical to the expiry reaper's, and deliberately so:
// the fact recorded is "this request was closed unanswered", which is the same
// fact whether a deadline or a person established it. Who established it rides
// where provenance always rides — env.Sender (system) plus closed_by — never a
// new vocabulary word, so nothing downstream has to learn a fourth way for a
// request to end.
func (e *opEntry) cancelRequest(ctx context.Context, requestID message.ID, asker actor.ActorID) (any, error) {
	if e.home.systemPen == nil || e.home.requests == nil {
		return nil, &sysactor.OperateError{
			Code: string(channelspec.ErrCodeAuthorityUnavailable), Detail: "closure authority unavailable",
		}
	}
	env, found, err := e.home.requests.FindByID(ctx, requestID)
	if err != nil {
		return nil, asOperateError(err)
	}
	if !found || env == nil || env.Kind != message.KindRequest {
		return nil, &sysactor.OperateError{
			Code: string(channelspec.ErrCodeBadPayload), Detail: "no such request in this channel",
		}
	}
	closedBy, err := json.Marshal(map[string]any{"closed_by": "system", "cancelled": true, "requested_by": string(asker)})
	if err != nil {
		return nil, asOperateError(err)
	}
	clock := func() time.Time { return time.UnixMilli(e.home.nowMs()) }
	// A request that answered a moment ago loses benignly: Respond maps the
	// terminal-uniqueness conflict to success, so asking twice — or racing the
	// real answer — is not an error the caller has to reason about.
	if _, rerr := behavior.Respond(ctx, e.home.systemPen, clock, env, behavior.ResponseSpec{
		Status:  message.StatusFailed,
		Reason:  string(message.TerminalUnansweredTimeout),
		Payload: closedBy,
	}); rerr != nil {
		return nil, asOperateError(rerr)
	}
	e.home.logger.Info("platform.request_cancel",
		"channel", e.home.channelID, "request", string(requestID), "requested_by", string(asker))
	return map[string]any{"request_id": string(requestID)}, nil
}

// restartChannel is the break-glass recovery the channel restart button sends:
// give every WORKING member a fresh term, in place. It is deliberately not a
// new capability — it walks the roster and drives the same per-member restart
// the management word already exposes, so "restart the channel" can never come
// to mean anything the channel could not already do one member at a time.
//
// Only agent and tool are restarted, and that single rule is the whole
// exclusion policy: a human cell has no running work to recover; system and the
// registrar are the kernel's own residents (restarting the actor that is
// executing the restart is a self-reference with no payoff); svcactor and the
// channel peers are platform plumbing, and a peer in particular is another
// channel's door — recycling it would interrupt calls that have nothing to do
// with the channel being rescued. Every one of those falls outside agent|tool
// by kind, so the policy needs no special cases to enumerate.
//
// Partial failure does NOT abort the walk. This runs when a channel is already
// wedged, so one member that refuses to come back must not keep the others
// stuck; the caller is told exactly who restarted, who failed and why.
//
// KNOWN GAP (deliberate, see .dalek/pm — Dev backlog D4): a restart does not
// close the requests the restarted members were serving. Controller.Restart
// mints a new AttemptKey and leaves the actor ACTIVE, so the receiver-
// unavailable reconciler (which only fires for deregistered actors) does not
// see it, and the old body's in-station ledger dies with it unanswered. Those
// requests stay open until their own deadline reaps them. Making restart close
// them the way death does is a separate decision, because it would terminate
// every in-flight request of every restarted member, not only the wedged ones.
func (e *opEntry) restartChannel(ctx context.Context) (any, error) {
	identities, err := e.home.actors.ActiveIdentities()
	if err != nil {
		return nil, asOperateError(err)
	}
	restarted := make([]actor.ActorID, 0, len(identities))
	failed := make([]map[string]any, 0)
	skipped := make([]map[string]any, 0)
	for _, identity := range identities {
		if identity.Kind != actor.KindAgent && identity.Kind != actor.KindTool {
			skipped = append(skipped, map[string]any{
				"member": identity.ID, "kind": string(identity.Kind),
				"reason": "only agent and tool members run work that a restart recovers",
			})
			continue
		}
		if err := e.home.actors.Restart(ctx, actorctl.RestartRequest{ActorID: identity.ID}); err != nil {
			// Keep the member's own verdict when it has one; a bare Go error
			// from the controller is an availability fact, not a caller mistake.
			var opErr *channelspec.OperationError
			code := string(channelspec.ErrCodeAuthorityUnavailable)
			if errors.As(err, &opErr) {
				code = string(opErr.Code)
			}
			e.home.logger.Warn("platform.channel_restart.member_failed",
				"channel", e.home.channelID, "member", identity.ID, "error", err)
			failed = append(failed, map[string]any{
				"member": identity.ID, "error_code": code, "detail": err.Error(),
			})
			continue
		}
		restarted = append(restarted, identity.ID)
	}
	e.home.logger.Info("platform.channel_restart.done",
		"channel", e.home.channelID,
		"restarted", len(restarted), "failed", len(failed), "skipped", len(skipped))
	return map[string]any{
		"restarted": restarted, "failed": failed, "skipped": skipped,
		"counts": map[string]any{
			"restarted": len(restarted), "failed": len(failed), "skipped": len(skipped),
		},
	}, nil
}

// narrateBirth writes the "joined the channel" narration for a freshly created
// record. A replayed birth (created=false) narrates nothing. The narration is
// composed from the command's own inputs — the tail never reads truth back.
func (h *Home) narrateBirth(ctx context.Context, cause message.Cause, id actor.ActorID, created bool, fields map[string]any) {
	if !created {
		return
	}
	h.announceRegistered(ctx, cause, id, fields)
}

func asOperateError(err error) error {
	var operationErr *channelspec.OperationError
	if errors.As(err, &operationErr) {
		return &sysactor.OperateError{
			Code: string(operationErr.Code), Detail: operationErr.Detail,
		}
	}
	var targetErr *actorbase.TargetResolveError
	if errors.As(err, &targetErr) {
		return &sysactor.OperateError{Code: targetErr.Code, Detail: targetErr.Error()}
	}
	switch {
	case errors.Is(err, storespec.ErrConflictExists):
		return &sysactor.OperateError{
			Code: string(channelspec.ErrCodeConflictExists), Detail: err.Error(),
		}
	case errors.Is(err, actorctl.ErrInvalidMutation):
		return &sysactor.OperateError{
			Code: string(channelspec.ErrCodeBadPayload), Detail: err.Error(),
		}
	case errors.Is(err, actorctl.ErrInactive), errors.Is(err, actorctl.ErrStaleAttempt):
		return &sysactor.OperateError{
			Code: string(channelspec.ErrCodeMemberInactive), Detail: err.Error(),
		}
	case errors.Is(err, actorctl.ErrClosed), errors.Is(err, actorctl.ErrChannelClosing):
		return &sysactor.OperateError{
			Code: string(channelspec.ErrCodeChannelUnavailable), Detail: err.Error(),
		}
	default:
		return err
	}
}
