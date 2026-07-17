package home

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const defaultForkIdle = 5 * time.Minute

var (
	ErrForkNonceConflict = errors.New("fork_nonce_conflict")
	ErrForkSpecInvalid   = errors.New("fork_spec_invalid")
	ErrForkParentGone    = errors.New("fork_parent_not_member")
)

type forkReceiptKey struct {
	parent actor.ActorID
	nonce  string
}

type forkReceipt struct {
	digest    string
	childID   actor.ActorID
	committed bool
}

func digestForkSpec(spec actorrt.ForkSpec, placement storespec.Placement, name string) string {
	raw, _ := json.Marshal(struct {
		Kind      actor.Kind
		Class     string
		Name      string
		Config    json.RawMessage
		Placement storespec.Placement
	}{spec.Kind, spec.Class, name, spec.Config, placement})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func normalizeForkSpec(spec actorrt.ForkSpec, parent storespec.ActorControlRow) (actorrt.ForkSpec, string, storespec.Placement, error) {
	if _, ok := actor.ParseKind(string(spec.Kind)); !ok || spec.Class == "" {
		return actorrt.ForkSpec{}, "", storespec.Placement{}, ErrForkSpecInvalid
	}
	name := spec.NameHint
	if name == "" {
		name = "child"
	}
	if len(name) > 64 {
		return actorrt.ForkSpec{}, "", storespec.Placement{}, ErrForkSpecInvalid
	}
	for _, r := range name {
		if r == '/' {
			return actorrt.ForkSpec{}, "", storespec.Placement{}, ErrForkSpecInvalid
		}
	}
	placement := parent.Placement
	if spec.Placement != nil {
		placement = *spec.Placement
	}
	if err := placement.Validate(); err != nil {
		return actorrt.ForkSpec{}, "", storespec.Placement{}, fmt.Errorf("%w: %v", ErrForkSpecInvalid, err)
	}
	spec.NameHint = name
	spec.Placement = &placement
	return spec, name, placement, nil
}

// forkAdmission publishes the fork birth event, completed run-State handle,
// active authority row, and nonce receipt while the parent lifecycle gate is
// held. It never creates a durable identity or declaration row.
func (h *Home) forkAdmission(ctx context.Context, parent actor.ActorID, birthVersion int64, spec actorrt.ForkSpec, nonce string) (actor.ActorID, error) {
	if h.closed.Load() {
		return "", ErrClosed
	}
	if nonce == "" {
		return "", ErrForkSpecInvalid
	}
	unlockParent := h.actorGates.lock(parent)
	defer unlockParent()

	parentRow, ok, err := h.cs.Authority.LookupActive(ctx, parent)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", ErrForkParentGone
	}
	if verdict, err := h.controlIndex.CheckAuthor(ctx, storespec.AuthorStamp{ID: parent, BirthVersion: birthVersion}); err != nil {
		return "", err
	} else if verdict == storespec.AuthorVersionStale {
		return "", ErrEndVersionStale
	} else if verdict != storespec.AuthorOK {
		return "", ErrForkParentGone
	}
	spec, name, placement, err := normalizeForkSpec(spec, parentRow)
	if err != nil {
		return "", err
	}
	digest := digestForkSpec(spec, placement, name)
	key := forkReceiptKey{parent: parent, nonce: nonce}

	h.forkMu.Lock()
	receipt, exists := h.forkReceipts[key]
	if exists && receipt.digest != digest {
		h.forkMu.Unlock()
		return "", ErrForkNonceConflict
	}
	if exists && receipt.committed {
		h.forkMu.Unlock()
		return receipt.childID, nil
	}
	childID := receipt.childID
	for childID == "" {
		candidate := actor.ActorID(string(parent) + "/" + name + "-" + uuid.NewString())
		if _, used := h.usedForkIDs[candidate]; used {
			continue
		}
		if _, active, lookupErr := h.controlIndex.LookupActive(ctx, candidate); lookupErr != nil {
			h.forkMu.Unlock()
			return "", lookupErr
		} else if active {
			continue
		}
		childID = candidate
		h.usedForkIDs[childID] = struct{}{}
		h.forkReceipts[key] = forkReceipt{digest: digest, childID: childID}
	}
	h.forkMu.Unlock()

	// parentID + "/..." is lexically after parentID, preserving the global
	// lifecycle gate order.
	unlockChild := h.actorGates.lock(childID)
	defer unlockChild()
	now := h.nowMs()
	payload, _ := json.Marshal(map[string]any{
		"parent_id": parent, "child_id": childID, "nonce": nonce,
		"kind": spec.Kind, "class": spec.Class, "name_hint": name,
		"config": spec.Config, "placement": placement,
	})
	result, err := h.systemPen.Write(ctx, &message.Envelope{
		ID: message.ID(uuid.NewString()), TS: now, TSReceived: now,
		Kind: message.KindEvent, Type: actor.ReservedSystemActorForked,
		Payload: payload, Visibility: message.VisibilitySystem,
		Audience: message.Audience{actor.SystemActorID},
	})
	if err != nil {
		return "", err
	}
	if !result.Accepted() {
		return "", fmt.Errorf("platform: fork birth rejected: %s", result.RejectReason)
	}
	row := storespec.ActorControlRow{
		ID: childID, Kind: spec.Kind, CreatedAt: now, CurrentDeclVersion: 1,
		Sponsor: parent, Class: spec.Class, Config: append([]byte(nil), spec.Config...),
		TIdle: defaultForkIdle, Placement: placement,
	}
	if err := h.stateHandles.AdmitRun(childID); err != nil {
		return "", err
	}
	if !h.controlIndex.UpsertBatch([]controlEntry{{Row: row, World: storespec.WorldRun}}) {
		return "", errors.New("platform: invalid fork control row")
	}
	if h.liveness.AdmitIdentity(childID) != transitionApplied {
		return "", errors.New("platform: invalid fork liveness row")
	}
	h.forkMu.Lock()
	h.forkReceipts[key] = forkReceipt{digest: digest, childID: childID, committed: true}
	h.forkMu.Unlock()
	return childID, nil
}
