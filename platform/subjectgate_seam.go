package platform

import "github.com/wanpengxie/atoll/protocol/actor"

// subjectgate_seam.go is the gateway 期 S3 public seam: the drivers/gateway伞包
// (outside platform/, so it cannot reach platform/internal/subjectgate) drives a
// subject's per-identity slot through these Home methods. The slot itself is the
// platform-exported alias *SubjectSlot (frame.go); its exported methods
// (SetBinding/BindingGen/PublishLevel/Forget/Deliver) are the gateway's controlled
//面. Home hands out the slot handle, never the internal Registry object — the
// organ-bag red line (surface_test) stays intact: these three are capability
// methods, not bare-accessor leaks.

// EnsureSubjectSlot returns id's binding slot, creating it on first call
// (idempotent). The gateway calls this at attach (装配链 step②) BEFORE the human
// cell's factory looks the slot up (step③), so the factory never races an absent
// slot. A nil registry (never assembled) is defensive — every Open builds one.
func (h *Home) EnsureSubjectSlot(id actor.ActorID) *SubjectSlot {
	if h.subjectgate == nil {
		return nil
	}
	return h.subjectgate.EnsureSlot(id)
}

// SubjectSlotFor returns id's slot IFF one exists (no create) — the gateway's
// lookup for an already-attached subject.
func (h *Home) SubjectSlotFor(id actor.ActorID) (*SubjectSlot, bool) {
	if h.subjectgate == nil {
		return nil, false
	}
	return h.subjectgate.Slot(id)
}

// RemoveSubjectSlot drops id's slot and Forgets its testimony (户籍级联). Called
// by the revocation/teardown path when a subject is removed.
func (h *Home) RemoveSubjectSlot(id actor.ActorID) {
	if h.subjectgate == nil {
		return
	}
	h.subjectgate.Remove(id)
}
