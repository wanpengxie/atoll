package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	platformhome "github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// operate.go fills the sysactor operate-face injection point (NP-1=c): the app
// assembly's world-layer/intent-write half of the four channel-control verbs. The
// gate (platform/internal/sysactor) already authorised the sender (active member,
// NP-2=a) and supplies channel scope + operator id; this executor does the intent
// write + Home-face call and returns a reply value (or a *platformhome.OperateError
// picking the fail code). One instance serves every channel — it resolves the
// home per req.ChannelID.
//
// This is the CANONICAL control path, and now the ONLY one: the channel-control
// HTTP endpoints are adapters (operate_http.go) that replay the session user through
// the subjectgate frame path (a submit frame, audience=[system]) into this
// executor — no handler writes the composition tables directly (红线11). handleDeleteDecl stays
// a world-layer soft-delete (its per-channel cascade is a system-authored mirror,
// not a member action), outside this face.
type operateExecutor struct {
	a *App
}

// operateExecutor implements the injected contract.
var _ platformhome.OperateExecutor = (*operateExecutor)(nil)

type introducePayload struct {
	DeclID      string `json:"decl_id"`
	Principal   string `json:"principal"` // human form: an opaque application principal
	Placement   string `json:"placement"`
	DesiredHost string `json:"desired_host"`
	MakeDefault bool   `json:"make_default"`
	Class       string `json:"class"`
	// Config is the per-channel composition config overlay — the
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
	return &platformhome.OperateError{Code: "bad_payload", Detail: err.Error()}
}

func channelUnavailable() error {
	return &platformhome.OperateError{Code: "channel_unavailable", Detail: "channel home not open"}
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
func (x *operateExecutor) Introduce(ctx context.Context, req platformhome.OperateRequest) (any, error) {
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
		return nil, &platformhome.OperateError{Code: "bad_payload", Detail: "decl_id or principal required"}
	}
	releaseDecl := x.a.declLocks.lock(declID)
	defer releaseDecl()
	var owner, visibility, defClass string
	err := x.a.db.QueryRowContext(ctx,
		`SELECT owner, visibility, default_class FROM actor_decls WHERE id = ? AND deleted_at IS NULL`,
		declID).Scan(&owner, &visibility, &defClass)
	if err == sql.ErrNoRows {
		return nil, &platformhome.OperateError{Code: "decl_not_found", Detail: declID}
	}
	if err != nil {
		return nil, err
	}
	// World-layer ref-eligibility (introduce 权是唯一复合世界层判定的动词). The
	// authority test is the template two-axis law — owner 恒是 principal 匹配, with
	// NO species restriction: any sender whose principal == the decl's owner may
	// introduce it. (纠错归位, gateway 期 S5: the former home.Human() guard smuggled
	// a Kind==human limit into this authorization path — an off-species owner would
	// have been wrongly refused. The registry principal query (PrincipalOf) is the
	// sole owner-match source.)
	principal, hasPrincipal := "", false
	if p, ok, perr := home.PrincipalOf(ctx, req.Sender); perr != nil {
		return nil, perr
	} else if ok {
		principal, hasPrincipal = p, true
	}
	if visibility != "public" && !(hasPrincipal && principal == owner) {
		return nil, &platformhome.OperateError{Code: "forbidden", Detail: "declaration is not public and sender is not its owner"}
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
		return nil, &platformhome.OperateError{Code: "bad_payload", Detail: "config must be a JSON object"}
	}

	existing, exists, err := home.CompositionByPrincipal(ctx, declID)
	if err != nil {
		return nil, err
	}
	if exists {
		engine = existing.Class
		placement = string(existing.Placement)
		desiredHost = existing.DesiredHost
	} else {
		if placement != placementServer && placement != placementDaemon {
			return nil, &platformhome.OperateError{Code: "invalid_placement", Detail: placement}
		}
		if placement == placementServer && desiredHost != "" {
			return nil, &platformhome.OperateError{Code: "invalid_placement", Detail: "server placement cannot carry desired_host"}
		}
	}
	kind, ok := registry.ClassKind(engine)
	if !ok {
		return nil, &platformhome.OperateError{Code: "unknown_class", Detail: engine}
	}
	var cfg *string
	if hasConfig {
		value := string(p.Config)
		cfg = &value
	}
	var releaseDaemon func()
	if placement == placementDaemon && desiredHost != "" {
		var lockErr error
		releaseDaemon, lockErr = (appDaemonAuthority{app: x.a}).LockAndValidate(ctx, desiredHost, req.ChannelID)
		if lockErr != nil {
			return nil, &platformhome.OperateError{Code: "invalid_desired_host", Detail: lockErr.Error()}
		}
		defer releaseDaemon()
	}
	record, created, configChanged, err := home.IntroduceComposition(ctx, storespec.CompositionIntroduce{
		DeclID: declID, Principal: declID, Class: engine, ConfigJSON: cfg,
		Placement: storespec.Placement(placement), DesiredHost: desiredHost,
		MakeDefault: p.MakeDefault, Kind: kind, At: x.a.now().UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	if configChanged {
		if _, serr := home.RestartInstanceDirect(ctx, record.InstanceID); serr != nil {
			return nil, &platformhome.OperateError{Code: "rebuild_failed", Detail: serr.Error()}
		}
	}
	return map[string]any{
		"instance_id": string(record.InstanceID), "class": record.Class, "placement": string(record.Placement),
		"created": created, "config_updated": configChanged,
	}, nil
}

// Remove handles the channel-scoped actor surface's two legitimate domains:
// composition instances use the atomic composition+deregister verb; structural
// members such as humans use membership removal and never fabricate an intent
// row merely to be removable.
func (x *operateExecutor) Remove(ctx context.Context, req platformhome.OperateRequest) (any, error) {
	var p instancePayload
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return nil, badPayload(err)
	}
	inst := strings.TrimSpace(p.InstanceID)
	if inst == "" {
		return nil, &platformhome.OperateError{Code: "bad_payload", Detail: "instance_id required"}
	}
	home := x.a.getHome(req.ChannelID)
	if home == nil {
		return nil, channelUnavailable()
	}
	if principal, ok, err := home.PrincipalOf(ctx, actor.ActorID(inst)); err != nil {
		return nil, err
	} else if ok && principal == "boost" {
		return nil, &platformhome.OperateError{Code: "protected_actor", Detail: "boost cannot be removed"}
	}
	id := actor.ActorID(inst)
	composed, err := home.HasComposition(ctx, id)
	if err != nil {
		return nil, err
	}
	if composed {
		err = home.RemoveInstance(ctx, id)
	} else {
		err = home.Remove(ctx, id)
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"removed": inst}, nil
}

// Restart kills actual state while leaving desired intact; reconcile rebuilds
// it on whichever placement owns the instance.
func (x *operateExecutor) Restart(ctx context.Context, req platformhome.OperateRequest) (any, error) {
	var p instancePayload
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return nil, badPayload(err)
	}
	inst := strings.TrimSpace(p.InstanceID)
	if inst == "" {
		return nil, &platformhome.OperateError{Code: "bad_payload", Detail: "instance_id required"}
	}
	home := x.a.getHome(req.ChannelID)
	if home == nil {
		return nil, channelUnavailable()
	}
	has, err := home.HasComposition(ctx, actor.ActorID(inst))
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, &platformhome.OperateError{Code: "not_in_composition", Detail: inst}
	}
	if _, serr := home.RestartInstanceDirect(ctx, actor.ActorID(inst)); serr != nil {
		return nil, &platformhome.OperateError{Code: "rebuild_failed", Detail: serr.Error()}
	}
	return map[string]any{"restarted": inst}, nil
}

// SetDefaultAgent updates the channel's default_agent pointer (the update half of
// composition CRUD). The target must已在 composition; empty clears the pointer.
func (x *operateExecutor) SetDefaultAgent(ctx context.Context, req platformhome.OperateRequest) (any, error) {
	var p instancePayload
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return nil, badPayload(err)
	}
	inst := strings.TrimSpace(p.InstanceID)
	home := x.a.getHome(req.ChannelID)
	if home == nil {
		return nil, channelUnavailable()
	}
	if err := home.SetDefaultAgent(ctx, actor.ActorID(inst)); err != nil {
		if errors.Is(err, storespec.ErrCompositionNotFound) {
			return nil, &platformhome.OperateError{Code: "not_in_composition", Detail: inst}
		}
		return nil, err
	}
	return map[string]any{"default_agent": inst}, nil
}

// operateFace returns the executor injected into every channel home's operate
// gate (one instance, channel-resolved per request).
func (a *App) operateFace() platformhome.OperateExecutor {
	return &operateExecutor{a: a}
}
