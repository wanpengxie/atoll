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
// This is the CANONICAL control path, and now the ONLY one: the channel-control
// HTTP endpoints are shims (operate_shim.go) that replay the session user through
// the door (Home.Human(u).Submit, audience=[system]) into this executor — no
// handler writes the composition tables directly (红线11). handleDeleteDecl stays
// a world-layer soft-delete (its per-channel cascade is a system-authored mirror,
// not a member action), outside this face.
type operateExecutor struct {
	a *App
}

// operateExecutor implements the injected contract.
var _ platform.OperateExecutor = (*operateExecutor)(nil)

type introducePayload struct {
	DeclID      string `json:"decl_id"`
	Principal   string `json:"principal"` // human form: an opaque application principal
	Placement   string `json:"placement"`
	DesiredHost string `json:"desired_host"`
	MakeDefault bool   `json:"make_default"`
	Class       string `json:"class"`
	// Config is the per-channel config overlay (channel_actors.config_json) — the
	// tunable field of the composition-row noun. Introduce is the composition
	// row's UPSERT verb (add-or-update, 防 ioctl 名词-CRUD): absent Config leaves
	// the field untouched (pure add / ensure); present Config writes it. This is
	// K2=(a)'s 改配置门 (S8): config is an INSTANCE PARAMETER carried by
	// registry.InstanceSpec.Config into the constructor closure, NEVER a
	// capability — it does not ride actorcaps.Caps (see buildInstance /
	// archtest.TestConfigNotInCaps). A config change on a live server-placed row
	// takes effect via Restart (下方; A-P14 原语), the same 生效 path restart
	// uses — placement/class stay frozen (rehoming/reclassing = remove + re-add,
	// SW-8), only the tunable field moves.
	Config json.RawMessage `json:"config,omitempty"`
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

// Introduce is the UPSERT half of composition CRUD (add-or-update the composition
// row; remove is its delete counterpart). Two forms (kind-blind同一动词):
//   - user form: introduce_actor(user:X) = pure membership (膜律 唯一动词) — no
//     class, no intent row, just Admit + poke.
//   - agent form: world-layer ref-eligibility (二型律: visibility=='public' ∨
//     principal==owner) → ClassKind precheck (unknown class当场拒) → ensure intent
//     (SW-8: pre-existing row's placement/class如实 unchanged) → optional config
//     UPDATE (改配置门, S8: config is the row's tunable field) → ensure Admit → poke,
//     and Restart when a config change must take on an already-live
//     server-placed row (生效). intent and Admit are BOTH ensured even on a
//     pre-existing row (幂等: a crash between the two writes is self-healed on
//     retry — else the half-written row is滤成 a never-embodied dead row under
//     desired=intent∩membership).
func (x *operateExecutor) Introduce(ctx context.Context, req platform.OperateRequest) (any, error) {
	var p introducePayload
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return nil, badPayload(err)
	}
	home := x.a.getHome(req.ChannelID)
	if home == nil {
		return nil, channelUnavailable()
	}
	if p.Principal != "" {
		id, err := home.Admit(ctx, actor.KindHuman, p.Principal)
		if err != nil {
			return nil, err
		}
		return map[string]any{"admitted": id}, nil
	}

	declID := strings.TrimSpace(p.DeclID)
	if declID == "" {
		return nil, &platform.OperateError{Code: "bad_payload", Detail: "decl_id or principal required"}
	}
	var owner, visibility, defClass string
	err := x.a.db.QueryRowContext(ctx,
		`SELECT owner, visibility, default_class FROM actor_decls WHERE id = ? AND deleted_at IS NULL`,
		declID).Scan(&owner, &visibility, &defClass)
	if err == sql.ErrNoRows {
		return nil, &platform.OperateError{Code: "decl_not_found", Detail: declID}
	}
	if err != nil {
		return nil, err
	}
	// World-layer ref-eligibility (introduce 权是唯一复合世界层判定的动词).
	principal, hasPrincipal := "", false
	if _, herr := home.Human(ctx, req.Sender); herr == nil {
		if p, ok, perr := home.PrincipalOf(ctx, req.Sender); perr != nil {
			return nil, perr
		} else if ok {
			principal, hasPrincipal = p, true
		}
	}
	if visibility != "public" && !(hasPrincipal && principal == owner) {
		return nil, &platform.OperateError{Code: "forbidden", Detail: "declaration is not public and sender is not its owner"}
	}
	engine := strings.TrimSpace(p.Class)
	if engine == "" {
		engine = defClass
	}
	// Default placement policy (product policy, v1.7): agents default to daemon
	// (除 human/sysactor/boost 外). Explicit placement参数 kept for API/agent paths.
	placement := strings.TrimSpace(p.Placement)
	if placement == "" {
		placement = placementDaemon
	}
	desiredHost := strings.TrimSpace(p.DesiredHost)
	instanceID := ""
	// NB: the request engine's ClassKind + placement are validated ONLY on the create
	// branch below (after the row query), NOT here. An existing row's class is frozen
	// (SW-8) — a config/retry introduce against it must走 the frozen effective class,
	// so validating the (possibly stale/garbage) request engine up-front would wrongly
	// reject a legitimate update of an already-composed row.

	// A present config must be a JSON object (persona/knobs) — same guard the
	// agent-config API applies. Absent (missing / null) leaves the field untouched;
	// json.RawMessage round-trips a JSON null as the literal "null", so treat it as
	// absent rather than a malformed object.
	rawCfg := strings.TrimSpace(string(p.Config))
	hasConfig := rawCfg != "" && rawCfg != "null"
	if hasConfig && !isJSONObject(p.Config) {
		return nil, &platform.OperateError{Code: "bad_payload", Detail: "config must be a JSON object"}
	}

	var exID, exClass, exPlacement, exDesiredHost string
	qerr := x.a.db.QueryRowContext(ctx,
		`SELECT instance_id, class, placement, desired_host FROM channel_actors WHERE channel_id = ? AND principal = ?`,
		string(req.ChannelID), declID).Scan(&exID, &exClass, &exPlacement, &exDesiredHost)
	created := false
	configChanged := false
	switch qerr {
	case nil:
		instanceID = exID
		// SW-8: placement/class of a pre-existing row恒不变更 (rehoming/reclassing =
		// remove + re-introduce). Only config — the tunable field — is mutable here
		// (改配置门, S8): a present config UPDATEs channel_actors.config_json.
		placement = exPlacement
		engine = exClass
		_ = exDesiredHost
		if hasConfig {
			if _, err := x.a.db.ExecContext(ctx,
				`UPDATE channel_actors SET config_json = ? WHERE channel_id = ? AND principal = ?`,
				string(p.Config), string(req.ChannelID), declID); err != nil {
				return nil, err
			}
			configChanged = true
		}
	case sql.ErrNoRows:
		// Create branch only: NOW validate the request engine's ClassKind (unknown
		// class当场拒 — before we persist an unbuildable row) and the placement shape.
		if _, ok := registry.ClassKind(engine); !ok {
			return nil, &platform.OperateError{Code: "unknown_class", Detail: engine}
		}
		// placement闭集 {server, daemon}: an explicit garbage value is fail-closed
		// (same posture as unknown_class) — a row with an unknown placement builds on
		// neither host (the ring only embodies server, the plan only pulls daemon), so
		// it would persist as a silently-dead row. Empty already defaulted to daemon
		// above, so by here placement is non-empty.
		if placement != placementServer && placement != placementDaemon {
			return nil, &platform.OperateError{Code: "invalid_placement", Detail: placement}
		}
		if placement == placementServer && desiredHost != "" {
			return nil, &platform.OperateError{Code: "invalid_placement", Detail: "server placement cannot carry desired_host"}
		}
		created = true
	default:
		return nil, qerr
	}
	// The effective class is now final: a pre-existing row froze engine to exClass
	// (SW-8). Re-derive kind from THAT class, not the request's engine — a半失败
	// retry (intent landed, Admit didn't) whose request carries a different class
	// must Admit under the row's ACTUAL class-kind, never the request's stale one
	// (the kind precheck above only guards the create path's unknown-class reject).
	kind, ok := registry.ClassKind(engine)
	if !ok {
		return nil, &platform.OperateError{Code: "unknown_class", Detail: engine}
	}
	// ensure Admit (idempotent) — pokes the ring; embodiment lands on the next sweep.
	mintedID, err := home.Admit(ctx, kind, declID)
	if err != nil {
		return nil, err
	}
	if created {
		var cfg any
		if hasConfig {
			cfg = string(p.Config)
		}
		if _, err := x.a.db.ExecContext(ctx,
			`INSERT INTO channel_actors (channel_id, instance_id, principal, class, placement, desired_host, config_json) VALUES (?,?,?,?,?,?,?)`,
			string(req.ChannelID), string(mintedID), declID, engine, placement, desiredHost, cfg); err != nil {
			return nil, err
		}
		instanceID = string(mintedID)
		_ = home.Restart(ctx, mintedID) // desired landed after Admit's poke; issue a post-write poke.
	}
	if string(mintedID) != instanceID {
		if _, err := x.a.db.ExecContext(ctx, `UPDATE channel_actors SET instance_id=? WHERE channel_id=? AND instance_id=?`, string(mintedID), string(req.ChannelID), instanceID); err != nil {
			return nil, err
		}
		_, _ = x.a.db.ExecContext(ctx, `UPDATE channels SET default_agent=? WHERE id=? AND default_agent=?`, string(mintedID), string(req.ChannelID), instanceID)
		instanceID = string(mintedID)
		_ = home.Restart(ctx, mintedID)
	}
	if p.MakeDefault {
		_, _ = x.a.db.ExecContext(ctx,
			`UPDATE channels SET default_agent = ? WHERE id = ?`, instanceID, string(req.ChannelID))
	}
	// Config takes effect through the same placement-neutral Restart face for
	// both home cells and daemon-hosted ports. A new row has no predecessor.
	if configChanged {
		if serr := home.Restart(ctx, mintedID); serr != nil {
			return nil, &platform.OperateError{Code: "rebuild_failed", Detail: serr.Error()}
		}
	}
	return map[string]any{
		"instance_id": instanceID, "class": engine, "placement": placement,
		"created": created, "config_updated": configChanged,
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
	if principal, ok, err := home.PrincipalOf(ctx, actor.ActorID(inst)); err != nil {
		return nil, err
	} else if ok && principal == "boost" {
		return nil, &platform.OperateError{Code: "protected_actor", Detail: "boost cannot be removed"}
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

// Restart kills actual state while leaving desired intact; reconcile rebuilds
// it on whichever placement owns the instance.
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
	var present int
	err := x.a.db.QueryRowContext(ctx,
		`SELECT 1 FROM channel_actors WHERE channel_id = ? AND instance_id = ?`,
		string(req.ChannelID), inst).Scan(&present)
	if err == sql.ErrNoRows {
		return nil, &platform.OperateError{Code: "not_in_composition", Detail: inst}
	}
	if err != nil {
		return nil, err
	}
	if serr := home.Restart(ctx, actor.ActorID(inst)); serr != nil {
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
