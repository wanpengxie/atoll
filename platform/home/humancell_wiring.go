package home

import (
	"context"
	"sync"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/humancell"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// humancell_wiring.go is the platform assembly root's wiring shell for the
// home-side human body: it holds the *Home-bound seam (factory + Proc
// shell + slot-access methods) that stitch platform/internal/humancell's
// body onto a live admitted member. The interpreter/serve-loop body itself
// lives in platform/internal/humancell (platform 拓扑批 T2 — 受养驱动 domain
// 件,住膜内因受养特权).
//
// The subjectgate seam's ensure/lookup/remove methods also live in this file.
// drives a subject's per-identity slot through these Home methods. The slot
// itself is *subjectgate.Slot — the在场与递交接头盒; its exported methods
// (PublishLevel/PublishCurrent/ForgetEpoch/Forget/Deliver) are the gateway's
// controlled面. Home hands out the slot handle, never the internal Registry
// object — the organ-bag red line (surface_test) stays intact: these three are
// capability methods, not bare-accessor leaks.

// humanCellFactory is the platform's built-in home-side human body. user域
// supply is platform internal政 — a per-channel human member's authority lives
// only in this channel's registry (the app cannot enumerate it), so the reconcile
// ring keeps a live human cell up whenever the member is admitted, without any
// app-injected factory.
//
// Proc shape (through the actorbase engine, NOT a raw actorrt.Actor implementer —
// archtest wall): TWO input faces run concurrently (标准型, design §5.2) —
//
//   - the MAILBOX serve loop (humancell.HumanServe): answers each delivered
//     request per the three-choice type table (immediate human.message /
//     deferred human.approve / describe self-answer);
//   - the FRAME interpreter (gateway 期 S2): the person's OWN actions arrive as
//     wire frames through the per-identity slot's帧递交端 and are driven onto this
//     cell's own caps via the identity-dimension Sys verbs (SubmitEnvelope/
//     RespondEnvelope/AfterIdentity/…). No slot (no gateway attach yet) → this
//     face is dormant and the cell is mailbox-only.
//
// The cell holds ZERO caller obligations (期12): a subject's own requests are
// closed by the substrate expiry reaper (义务归位 D3) — no per-user Caller, no
// Match plumbing.
func humanCellFactory(h *Home, id actor.ActorID) platform.ActorFactory {
	return platform.ActorFactory{Proc: actorbase.Def{
		Doc: "home-side human actor (subjectgate): callable; three-choice per-type closure (immediate human.message / deferred human.approve) + describe; the person drives own actions via wire frames through the slot",
		New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error { return h.runHumanCell(id, sys) }, nil
		},
	}}
}

// subjectgateSlot LOOKS UP the per-identity slot — it never creates one (装配链
// step③, v0.4.1 勘误: 槽随户籍准入 ensure, factory/cell 只 lookup). The slot's
// 生死随户籍级联 (ensured at membership准入 = factoryFor/Admit, dropped at Remove) —
// having the cell construction path self-ensure would be装配授权走私 (the cell
// minting its own binding-slot existence). A missing slot for a real human member
// is an assembly bug (factoryFor ensures it BEFORE the cell is built); this
// degrades to mailbox-only rather than fabricating a failure that kills the cell
// (defensive, same posture as the nil-registry case). nil registry → mailbox-only.
func (h *Home) subjectgateSlot(id actor.ActorID) (*subjectgate.Slot, bool) {
	if h.subjectgate == nil {
		return nil, false
	}
	return h.subjectgate.Slot(id)
}

// runHumanCell is the human cell's Proc body: it wires the frame interpreter +
// presence self-report against the slot (装配链 step③④) if one exists, then runs
// the mailbox serve loop. On serve-loop exit (Recv error = cooperative
// termination) it stops the interpreter goroutine and joins it (S1 纪律照 kimi:
// wg join +解阻 — closing stop detaches the slot so any blocked gateway Deliver
// unblocks with ErrNoOccupant).
func (h *Home) runHumanCell(id actor.ActorID, sys actorbase.Sys) error {
	var wg sync.WaitGroup
	stop := make(chan struct{})

	if slot, ok := h.subjectgateSlot(id); ok {
		deps := humancell.Deps{
			Self:       id,
			Requests:   h.requests,
			OpenCheck:  h.isRequestOpen,
			CancelHint: h.cancelRequest,
			Routing:    h.defaultAgent.snapshot,
			IsActive:   h.controller.IsActive,
			Present: func(target actor.ActorID) bool {
				_, live := h.View().Stat(target)
				return live
			},
		}
		token := humancell.WirePresenceSelfReport(sys, slot)
		frames, incarnation, release := slot.AttachInterpreter()
		h.logger.Debug("platform.subjectgate.interpreter_attached", "channel", string(h.channelID),
			"actor", string(id), "incarnation", incarnation, "reason", "cell activate")
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				reason := "normal exit"
				select {
				case <-stop:
					reason = "stop"
				default:
				}
				h.logger.Debug("platform.subjectgate.interpreter_released", "channel", string(h.channelID),
					"actor", string(id), "incarnation", incarnation, "reason", reason)
				release()
			}()
			defer slot.RemoveObserver(token)
			humancell.InterpretFrames(sys, slot, deps, frames, stop)
		}()
	}

	err := humancell.HumanServe(sys)
	close(stop)
	wg.Wait()
	return err
}

// isRequestOpen reports whether reqID is still an open request addressed to
// receiver — the truth-derived open-status check the frame interpreter's
// from-log five steps use ("仍 open"). A closed (terminal-answered) or unknown
// request is not open. (Relocated here from the removed HumanHandle door — the
// cell's own driver deps are its only consumer now.)
func (h *Home) isRequestOpen(ctx context.Context, receiver actor.ActorID, reqID message.ID) (bool, error) {
	if receiver == "" {
		return false, nil
	}
	rows, err := h.query.OpenRequestsForActor(ctx, receiver)
	if err != nil {
		return false, err
	}
	for _, r := range rows {
		if r.Envelope.ID == reqID {
			return true, nil
		}
	}
	return false, nil
}

// ensureSubjectSlot returns id's slot (在场与递交接头盒), creating it on first call
// (idempotent). The gateway calls this at attach (装配链 step②) BEFORE the human
// cell's factory looks the slot up (step③), so the factory never races an absent
// slot. A nil registry (never assembled) is defensive — every Open builds one.
func (h *Home) ensureSubjectSlot(id actor.ActorID) *subjectgate.Slot {
	if h.subjectgate == nil {
		return nil
	}
	return h.subjectgate.EnsureSlot(id)
}

// SubjectSlotFor returns id's slot IFF one exists (no create) — the gateway's
// lookup for an already-attached subject.
func (h *Home) subjectSlotFor(id actor.ActorID) (*subjectgate.Slot, bool) {
	if h.subjectgate == nil {
		return nil, false
	}
	return h.subjectgate.Slot(id)
}
