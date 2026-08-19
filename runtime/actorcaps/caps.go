package actorcaps

import (
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// Caps is the five-capability bundle welded to one actor incarnation at birth.
// Every field is a substrate-minted, caller-welded capability: the actor never
// self-reports which id/author it is — that coordinate is welded into each
// handle at construction (non-ambient), exactly as harness.Pen welds the writer
// identity. The platform assembly root mints all five in ONE step — each handle
// is minted with the incarnation's run authority welded inside (every operation
// gates on Admit()) — so an actor never receives a bare, ungated handle.
//
// Caps is CAPABILITIES ONLY. Per-instance CONFIG (registry.InstanceSpec.Config,
// the composition spec's "ctx.Config") is deliberately NOT a member here: config
// is an independent parameter the constructor closure captures, not a
// substrate-minted authority (S-P16 红线). Adding a Config field to this struct
// would collapse that distinction.
type Caps struct {
	// Pen is the plane-1 truth-write capability (append messages AS this actor).
	// The pen holds the run authority and gates every Write with Admit().
	Pen harness.Pen
	// Access is the plane-2 off-log capability over the channel-scoped resource
	// tree (the access-plane dual of Pen) — the WIDE resource face
	// (Invoke+Create+Stat+List, 期11 spec §3.1's "Caps.Access 声明类型加宽").
	// Minted with the run authority welded inside; every verb gates on Admit().
	Access accessdoor.ResourceAccessHandle
	// State is the actor-scoped (collapsed) branch of the same plane-2 door —
	// this actor's own private durable state, minted narrow (Invoke-only) via
	// MintStateAuthority with the authority welded inside. The scope law
	// itself keeps this field's type NARROW: there is no kind/R/membership at
	// this locus for Create/Stat/List to mean anything.
	State accessdoor.AccessHandle
	// Schedule is the time-axis capability (self-targeted timers). Minted with
	// the authority welded inside; every operation gates on Admit().
	Schedule schedule.ScheduleHandle
	// Lifecycle is the closed end-self capability welded to this incarnation.
	Lifecycle LifecycleHandle
}
