package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const bootstrapCodexName = "home-codex"

func stableBootstrapCodexDeclID(owner string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("atoll:bootstrap:"+owner+":home-codex")).String()
}

func (a *App) convergeBootstrapCodex(ctx context.Context, owner string, homeID channel.ID) error {
	declID := stableBootstrapCodexDeclID(owner)
	if _, err := a.createDeclarationCore(ctx, declID, bootstrapCodexName, owner, "codex", nil, "private"); err != nil {
		return fmt.Errorf("provision: codex declaration: %w", err)
	}
	if _, err := forwardSysop(ctx, a, homeID, introduceCall(ctx, owner, declID, nil)); err != nil {
		return fmt.Errorf("provision: introduce codex: %w", err)
	}
	return a.convergeDefaultAgent(ctx, owner, homeID, declID)
}

func (a *App) convergeDefaultAgent(ctx context.Context, owner string, homeID channel.ID, declID string) error {
	submitted := false
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		bundle, err := a.acquireBundle(ctx, homeID)
		if err != nil {
			lastErr = err
		} else {
			instances, readErr := bundle.View().DeclaredInstances(ctx, declID)
			if readErr != nil {
				lastErr = readErr
			} else if len(instances) == 1 {
				current, found, defaultErr := bundle.View().DefaultAgent(ctx)
				if defaultErr == nil && found && current == instances[0] {
					return nil
				}
				if defaultErr != nil {
					lastErr = defaultErr
				}
				if !submitted {
					if err := submitDefaultAgent(ctx, bundle, owner, homeID, declID); err != nil {
						lastErr = err
					} else {
						submitted = true
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	if lastErr != nil {
		return fmt.Errorf("provision: codex default agent did not converge: %w", lastErr)
	}
	return errors.New("provision: codex default agent did not converge")
}

func submitDefaultAgent(ctx context.Context, bundle channelhost.Bundle, owner string, homeID channel.ID, declID string) error {
	ownerActor, ok, err := bundle.View().ResolvePrincipal(ctx, owner)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("provision: home owner is not a channel member")
	}
	slot, live := bundle.Gateway().SubjectSlotFor(ownerActor)
	if !live {
		return errors.New("provision: home owner subject is not live")
	}
	payload, err := json.Marshal(map[string]any{"source_decl_id": declID})
	if err != nil {
		return err
	}
	frame, err := subjectgate.NewFrame(subjectgate.FrameSubmit, uuid.NewString(), subjectgate.SubmitPayload{
		ChannelID: string(homeID), ID: uuid.NewString(), MsgType: platform.TypeSetDefaultAgent,
		Kind: "request", Audience: []string{string(actor.SystemActorID)}, Visibility: "public", Payload: payload,
	})
	if err != nil {
		return err
	}
	_, err = slot.Deliver(ctx, frame)
	return err
}
