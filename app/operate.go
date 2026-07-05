package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

// operate.go fills the sysactor operate-face injection point (NP-1=c): the app
// assembly's world-layer/intent-write half of the four channel-control verbs. The
// gate (platform/internal/sysactor) already authorised the sender (active member,
// NP-2=a) and supplies channel scope + operator id; this executor does the intent
// write + Home-face call and returns a reply value (or a *platform.OperateError
// picking the fail code). One instance serves every channel — it resolves the
// home per req.ChannelID.
//
// This is the CANONICAL control path. The legacy HTTP handlers (handleIntroduceAgent
// / handleSetDefaultAgent / handleRestartAgent / handleDeleteAgent) still coexist
// on their direct-write paths this slice; their retirement into gateway shims is
// CORE2. No message sender drives these verbs yet (S5b/shims deferred), so the
// live face is exercised by DoD tests until then.
type operateExecutor struct {
	a *App
}

// operateExecutor implements the injected contract.
var _ platform.OperateExecutor = (*operateExecutor)(nil)

// principalFromSender extracts the operating user's id from a "user:<id>" sender.
// A non-user sender (an agent member) has NO principal — it may only introduce a
// public agent (world-layer delegation to owned agents is deferred, §9 ⑦).
func principalFromSender(sender actor.ActorID) (string, bool) {
	s := string(sender)
	if strings.HasPrefix(s, "user:") {
		return strings.TrimPrefix(s, "user:"), true
	}
	return "", false
}

type introducePayload struct {
	AgentID     string `json:"agent_id"`
	Target      string `json:"target"` // user form: an explicit id (user:X)
	Placement   string `json:"placement"`
	DesiredHost string `json:"desired_host"`
	MakeDefault bool   `json:"make_default"`
	Engine      string `json:"engine"`
}

type instancePayload struct {
	InstanceID string `json:"instance_id"`
}

func badPayload(err error) error {
	return &platform.OperateError{Code: "bad_payload", Detail: err.Error()}
}

func channelUnavailable() error {
	return &platform.OperateError{Code: "channel_unavailable", Detail: "channel home not open"}
}

// Introduce is the add half of composition CRUD. Two forms (kind-blind同一动词):
//   - user form: introduce_actor(user:X) = pure membership (膜律 唯一动词) — no
//     class, no intent row, just Admit + poke.
//   - agent form: world-layer ref-eligibility (二型律: visibility=='public' ∨
//     principal==owner) → ClassKind precheck (unknown class当场拒) → ensure intent
//     (SW-8: pre-existing row如实 unchanged) → ensure Admit → poke. intent and Admit
//     are BOTH ensured even on a pre-existing row (幂等: a crash between the two
//     writes is self-healed on retry — else the half-written row is滤成 a never-
//     embodied dead row under desired=intent∩membership).
func (x *operateExecutor) Introduce(ctx context.Context, req platform.OperateRequest) (any, error) {
	var p introducePayload
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return nil, badPayload(err)
	}
	home := x.a.getHome(req.ChannelID)
	if home == nil {
		return nil, channelUnavailable()
	}
	if strings.HasPrefix(p.Target, "user:") {
		if err := home.Admit(ctx, actor.ActorID(p.Target), actor.KindHuman); err != nil {
			return nil, err
		}
		return map[string]any{"admitted": p.Target}, nil
	}

	agentID := strings.TrimSpace(p.AgentID)
	if agentID == "" {
		return nil, &platform.OperateError{Code: "bad_payload", Detail: "agent_id or user target required"}
	}
	var owner, visibility, defLooper string
	err := x.a.db.QueryRowContext(ctx,
		`SELECT owner, visibility, default_looper FROM agents WHERE id = ? AND deleted_at IS NULL`,
		agentID).Scan(&owner, &visibility, &defLooper)
	if err == sql.ErrNoRows {
		return nil, &platform.OperateError{Code: "agent_not_found", Detail: agentID}
	}
	if err != nil {
		return nil, err
	}
	// World-layer ref-eligibility (introduce 权是唯一复合世界层判定的动词).
	principal, hasPrincipal := principalFromSender(req.Sender)
	if visibility != "public" && !(hasPrincipal && principal == owner) {
		return nil, &platform.OperateError{Code: "forbidden", Detail: "agent is not public and sender is not its owner"}
	}
	engine := strings.TrimSpace(p.Engine)
	if engine == "" {
		engine = defLooper
	}
	kind, ok := registry.ClassKind(engine)
	if !ok {
		return nil, &platform.OperateError{Code: "unknown_class", Detail: engine}
	}
	// Default placement policy (product policy, v1.7): agents default to daemon
	// (除 human/sysactor/boost 外). Explicit placement参数 kept for API/agent paths.
	placement := strings.TrimSpace(p.Placement)
	if placement == "" {
		placement = placementDaemon
	}
	desiredHost := strings.TrimSpace(p.DesiredHost)
	if placement == placementServer && desiredHost != "" {
		return nil, &platform.OperateError{Code: "invalid_placement", Detail: "server placement cannot carry desired_host"}
	}
	instanceID := "agent:" + agentID

	var exClass, exPlacement, exDesiredHost string
	qerr := x.a.db.QueryRowContext(ctx,
		`SELECT class, placement, desired_host FROM channel_actors WHERE channel_id = ? AND instance_id = ?`,
		string(req.ChannelID), instanceID).Scan(&exClass, &exPlacement, &exDesiredHost)
	created := false
	switch qerr {
	case nil:
		// SW-8: pre-existing row如实 unchanged (rehoming = remove + re-introduce —
		// placement/class恒不变更). Report the persisted values, ensure Admit below.
		placement = exPlacement
		engine = exClass
		_ = exDesiredHost
	case sql.ErrNoRows:
		if _, err := x.a.db.ExecContext(ctx,
			`INSERT INTO channel_actors (channel_id, instance_id, class, placement, desired_host) VALUES (?,?,?,?,?)`,
			string(req.ChannelID), instanceID, engine, placement, desiredHost); err != nil {
			return nil, err
		}
		created = true
	default:
		return nil, qerr
	}
	if p.MakeDefault {
		_, _ = x.a.db.ExecContext(ctx,
			`UPDATE channels SET default_agent = ? WHERE id = ?`, instanceID, string(req.ChannelID))
	}
	// ensure Admit (idempotent) — pokes the ring; embodiment lands on the next sweep.
	if err := home.Admit(ctx, actor.ActorID(instanceID), kind); err != nil {
		return nil, err
	}
	return map[string]any{
		"instance_id": instanceID, "class": engine, "placement": placement, "created": created,
	}, nil
}

// Remove is the delete half of composition CRUD (G17): intent FIRST (desired
// authority — the ring won't re-mint) then Home.Remove (despawn→dereg级联). A
// crash between the two leaves an orphan户籍 row, reaped by主体域/反熵 backstop
// (non-crash atomicity not苛求).
func (x *operateExecutor) Remove(ctx context.Context, req platform.OperateRequest) (any, error) {
	var p instancePayload
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return nil, badPayload(err)
	}
	inst := strings.TrimSpace(p.InstanceID)
	if inst == "" {
		return nil, &platform.OperateError{Code: "bad_payload", Detail: "instance_id required"}
	}
	home := x.a.getHome(req.ChannelID)
	if home == nil {
		return nil, channelUnavailable()
	}
	if _, err := x.a.db.ExecContext(ctx,
		`DELETE FROM channel_actors WHERE channel_id = ? AND instance_id = ?`,
		string(req.ChannelID), inst); err != nil {
		return nil, err
	}
	_, _ = x.a.db.ExecContext(ctx,
		`UPDATE channels SET default_agent = NULL WHERE id = ? AND default_agent = ?`,
		string(req.ChannelID), inst)
	if err := home.Remove(ctx, actor.ActorID(inst)); err != nil {
		return nil, err
	}
	return map[string]any{"removed": inst}, nil
}

// Restart is原地换脑 (A-P14 = Spawn-replace): verify the intent row, rebuild from
// latest config, Spawn-replace. It does NOT touch intent (红线 3). Server-placed
// only (a daemon-placed instance restarts on its own host via the plan).
func (x *operateExecutor) Restart(ctx context.Context, req platform.OperateRequest) (any, error) {
	var p instancePayload
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return nil, badPayload(err)
	}
	inst := strings.TrimSpace(p.InstanceID)
	if inst == "" {
		return nil, &platform.OperateError{Code: "bad_payload", Detail: "instance_id required"}
	}
	home := x.a.getHome(req.ChannelID)
	if home == nil {
		return nil, channelUnavailable()
	}
	var class, cfg string
	err := x.a.db.QueryRowContext(ctx,
		`SELECT class, COALESCE(config_json,'') FROM channel_actors WHERE channel_id = ? AND instance_id = ? AND placement = ?`,
		string(req.ChannelID), inst, placementServer).Scan(&class, &cfg)
	if err == sql.ErrNoRows {
		return nil, &platform.OperateError{Code: "not_in_composition", Detail: inst}
	}
	if err != nil {
		return nil, err
	}
	var gcfg string
	if strings.HasPrefix(inst, "agent:") {
		_ = x.a.db.QueryRowContext(ctx,
			`SELECT COALESCE(config_json,'') FROM agents WHERE id = ? AND deleted_at IS NULL`,
			strings.TrimPrefix(inst, "agent:")).Scan(&gcfg)
	}
	if serr := x.a.spawnAgentInstance(req.ChannelID, home, inst, class, cfg, gcfg); serr != nil {
		return nil, &platform.OperateError{Code: "rebuild_failed", Detail: serr.Error()}
	}
	return map[string]any{"restarted": inst}, nil
}

// SetDefaultAgent updates the channel's default_agent pointer (the update half of
// composition CRUD). The target must已在 composition; empty clears the pointer.
func (x *operateExecutor) SetDefaultAgent(ctx context.Context, req platform.OperateRequest) (any, error) {
	var p instancePayload
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return nil, badPayload(err)
	}
	inst := strings.TrimSpace(p.InstanceID)
	if inst != "" {
		has, err := x.a.channelHasInstance(ctx, string(req.ChannelID), inst)
		if err != nil {
			return nil, err
		}
		if !has {
			return nil, &platform.OperateError{Code: "not_in_composition", Detail: inst}
		}
	}
	var val any
	if inst != "" {
		val = inst
	}
	if _, err := x.a.db.ExecContext(ctx,
		`UPDATE channels SET default_agent = ? WHERE id = ?`, val, string(req.ChannelID)); err != nil {
		return nil, err
	}
	return map[string]any{"default_agent": inst}, nil
}

// operateFace returns the executor injected into every channel home's operate
// gate (one instance, channel-resolved per request).
func (a *App) operateFace() platform.OperateExecutor {
	return &operateExecutor{a: a}
}
