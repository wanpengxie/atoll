package app

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func (a *App) handleJoinChannel(c *gin.Context) {
	chID := channel.ID(c.Param("chID"))
	principal := middleware.UserID(c)
	outcome, err := forwardSysop(c.Request.Context(), a, chID, sysopForward[channel.AdmitResult]{
		Predicate: func(bundle channelhost.Bundle) (channel.AdmitResult, bool, error) {
			id, found, err := bundle.View().ResolvePrincipal(c.Request.Context(), principal)
			if err == nil && found {
				// Repair is a side effect over a non-authoritative index; its
				// failure never changes an answer the membrane already confirmed.
				if rerr := a.relations.ReconcilePrincipal(c.Request.Context(), chID, principal, id, true); rerr != nil {
					a.logger.Warn("membership relation repair failed", "channel", chID, "principal", principal, "err", rerr)
				}
			}
			return channel.AdmitResult{ActorID: id, Created: false}, found, err
		},
		Invoke: func(sys channelhost.SysOp, ref string) (channel.AdmitResult, error) {
			return sys.Admit(c.Request.Context(), channelspec.AdmitRequest{Ref: ref, Principal: principal})
		},
		Changed: func(result channel.AdmitResult) bool { return result.Created },
	})
	if err != nil {
		writeSysopError(c, err)
		return
	}
	status := http.StatusOK
	if outcome.Changed {
		status = http.StatusCreated
	}
	c.JSON(status, gin.H{"actor_id": outcome.Value.ActorID, "changed": outcome.Changed})
}

type introduceForwardValue struct {
	Result    channel.IntroduceResult
	Instances []actor.ActorID
}

func introduceCall(
	ctx context.Context,
	principal string,
	declID string,
	qualifyExtra func(channelhost.Bundle) error,
) sysopForward[introduceForwardValue] {
	var initiator actor.ActorID
	return sysopForward[introduceForwardValue]{
		Predicate: func(bundle channelhost.Bundle) (introduceForwardValue, bool, error) {
			instances, err := bundle.View().DeclaredInstances(ctx, declID)
			value := introduceForwardValue{Instances: instances}
			if len(instances) != 0 {
				value.Result = channel.IntroduceResult{ActorID: instances[0], Created: false}
			}
			return value, len(instances) != 0, err
		},
		Qualify: func(bundle channelhost.Bundle) error {
			id, found, err := bundle.View().ResolvePrincipal(ctx, principal)
			if err != nil {
				return &sysopUnknownError{cause: err}
			}
			if !found {
				return &sysopGateError{Status: http.StatusForbidden, Code: "forbidden"}
			}
			initiator = id
			if qualifyExtra != nil {
				return qualifyExtra(bundle)
			}
			return nil
		},
		Invoke: func(sys channelhost.SysOp, ref string) (introduceForwardValue, error) {
			result, err := sys.Introduce(ctx, channelspec.IntroduceRequest{
				Ref: ref, DeclID: declID, InitiatorActorID: initiator,
			})
			return introduceForwardValue{Result: result, Instances: []actor.ActorID{result.ActorID}}, err
		},
		Changed: func(value introduceForwardValue) bool { return value.Result.Created },
	}
}

func (a *App) handleIntroduceActor(c *gin.Context) {
	var input struct {
		DeclID string `json:"decl_id"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || strings.TrimSpace(input.DeclID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "decl_id required"})
		return
	}
	input.DeclID = strings.TrimSpace(input.DeclID)
	outcome, err := forwardSysop(
		c.Request.Context(), a, channel.ID(c.Param("chID")),
		introduceCall(c.Request.Context(), middleware.UserID(c), input.DeclID, nil),
	)
	if err != nil {
		writeSysopError(c, err)
		return
	}
	if !outcome.Changed {
		c.JSON(http.StatusOK, gin.H{"changed": false, "instances": outcome.Value.Instances})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"changed": true, "actor_id": outcome.Value.Result.ActorID,
		"instances": outcome.Value.Instances,
	})
}

func removeCall(ctx context.Context, principal string, target actor.ActorID) sysopForward[channel.RemoveResult] {
	var initiator actor.ActorID
	return sysopForward[channel.RemoveResult]{
		Predicate: func(bundle channelhost.Bundle) (channel.RemoveResult, bool, error) {
			facts, found, err := bundle.View().ActorFacts(ctx, target)
			return channel.RemoveResult{}, !found || !facts.Active, err
		},
		Qualify: func(bundle channelhost.Bundle) error {
			id, found, err := bundle.View().ResolvePrincipal(ctx, principal)
			if err != nil {
				return &sysopUnknownError{cause: err}
			}
			if !found {
				return &sysopGateError{Status: http.StatusForbidden, Code: "forbidden"}
			}
			initiator = id
			return nil
		},
		Invoke: func(sys channelhost.SysOp, ref string) (channel.RemoveResult, error) {
			return sys.Remove(ctx, channelspec.RemoveRequest{
				Ref: ref, Target: target, InitiatorActorID: initiator,
			})
		},
		Changed: func(result channel.RemoveResult) bool { return len(result.Removed) != 0 },
	}
}

func (a *App) handleRemoveChannelActor(c *gin.Context) {
	target := actor.ActorID(strings.TrimSpace(c.Param("actorID")))
	if target == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "actor_id required"})
		return
	}
	outcome, err := forwardSysop(
		c.Request.Context(), a, channel.ID(c.Param("chID")),
		removeCall(c.Request.Context(), middleware.UserID(c), target),
	)
	if err != nil {
		writeSysopError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"changed": outcome.Changed, "removed": outcome.Value.Removed})
}
