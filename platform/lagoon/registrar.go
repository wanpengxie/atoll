package lagoon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type Registrar struct {
	registry *Registry
	facts    ActorFactsResolver
	classes  ClassCatalog
	now      Clock
}

func NewRegistrar(registry *Registry, facts ActorFactsResolver, classes ClassCatalog) *Registrar {
	return &Registrar{registry: registry, facts: facts, classes: classes, now: time.Now}
}

// ReconcileSystem verifies and repairs the registry's fixed system rows. It
// deliberately never touches credentials or user/device identities.
func (r *Registrar) ReconcileSystem(ctx context.Context) error {
	if r == nil || r.registry == nil {
		return errors.New("lagoon: registrar registry required")
	}
	tx, err := r.registry.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var rootExists, localExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM principals WHERE id=? AND kind='human')`, protocol.RootPrincipalID).Scan(&rootExists); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM devices WHERE id=?)`, protocol.LocalDeviceID).Scan(&localExists); err != nil {
		return err
	}
	if !rootExists || !localExists {
		return errors.New("lagoon: installation identity incomplete")
	}
	now := r.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `INSERT INTO principals(id,kind,email,display_name,status,created_at) VALUES(?,'agent',NULL,'Steward','present',?) ON CONFLICT(id) DO UPDATE SET kind='agent',email=NULL,display_name='Steward',status='present'`, protocol.StewardPrincipalID, now); err != nil {
		return err
	}
	spec := GenesisSpec{ChannelID: protocol.C0ChannelID, Type: "group", OwnerPrincipal: protocol.RootPrincipalID, CreatedAt: now}
	raw, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO channels(id,parent_id,name,type,status,owner_principal,spec_json,created_at) VALUES(?,NULL,?,'group','present',?,?,?) ON CONFLICT(id) DO UPDATE SET parent_id=NULL,name=excluded.name,type='group',status='present',owner_principal=excluded.owner_principal`, protocol.C0ChannelID, protocol.C0ChannelID, protocol.RootPrincipalID, string(raw), now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO decls(id,name,owner,default_class,config_json,status,visibility,created_at,updated_at) VALUES(?,?,?,?,'{}','present','public',?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,owner=excluded.owner,default_class=excluded.default_class,config_json='{}',status='present',visibility='public',updated_at=excluded.updated_at`, SpaceToolDeclID, "Space Tool", protocol.RootPrincipalID, SpaceToolClass, now, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{AllChannels: true})
	}
	return nil
}

type forwardedRequest struct {
	Source      SourceRef       `json:"source"`
	Initiator   string          `json:"initiator,omitempty"`
	Application bool            `json:"application,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

func Def(registrar *Registrar) actorbase.Def {
	return actorbase.Def{Doc: "channel-zero registrar", New: func() (actorbase.Proc, error) {
		if registrar == nil || registrar.registry == nil {
			return nil, errors.New("lagoon: registrar registry required")
		}
		return registrar.serve, nil
	}}
}

func (r *Registrar) serve(sys actorbase.Sys) error {
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		if msg.Kind != message.KindRequest {
			continue
		}
		r.handle(sys, msg)
	}
}

func (r *Registrar) handle(sys actorbase.Sys, msg actorbase.Msg) {
	word := Word(msg.Type)
	if !knownWord(word) {
		_, _ = sys.Fail(msg, string(CodeInvalidArgs), "unknown registrar word")
		return
	}
	var principal string
	source := SourceRef{ChannelID: msg.ChannelID, RequestID: string(msg.ID)}
	payload := json.RawMessage(append([]byte(nil), msg.Payload...))
	application := false
	if msg.Sender.ID == actor.SystemActorID {
		var f forwardedRequest
		if err := json.Unmarshal(msg.Payload, &f); err != nil || f.Source.ChannelID == "" || f.Source.RequestID == "" {
			_, _ = sys.Fail(msg, string(CodeInvalidArgs), "invalid forwarded request")
			return
		}
		principal, source, payload, application = f.Initiator, f.Source, f.Payload, f.Application
	} else {
		if r.facts == nil {
			_, _ = sys.Fail(msg, string(CodePermissionDenied), "initiator unavailable")
			return
		}
		facts, found, err := r.facts.ActorFacts(msg.Ctx(), msg.Sender.ID)
		if err != nil {
			_, _ = sys.Fail(msg, "internal_error", err.Error())
			return
		}
		if !found || !facts.Active || facts.Principal == "" {
			_, _ = sys.Fail(msg, string(CodePermissionDenied), "active attributable principal required")
			return
		}
		principal = facts.Principal
	}
	if word == WordPrincipalRegister {
		if !application || principal != "" {
			_, _ = sys.Fail(msg, string(CodePermissionDenied), "registration requires the anonymous application entrance")
			return
		}
	} else if application || principal == "" {
		_, _ = sys.Fail(msg, string(CodePermissionDenied), "authenticated principal required")
		return
	}
	value, err := r.execute(msg.Ctx(), principal, source.ChannelID, word, payload)
	if err != nil {
		var le *Error
		if errors.As(err, &le) {
			_, _ = sys.Fail(msg, string(le.Code), le.Detail)
		} else {
			_, _ = sys.Fail(msg, "internal_error", err.Error())
		}
		return
	}
	_, _ = sys.Reply(msg, Reply{Word: word, Value: value, Source: source})
}

func knownWord(word Word) bool {
	for _, candidate := range WriteWords {
		if word == candidate {
			return true
		}
	}
	for _, candidate := range ReadWords {
		if word == candidate {
			return true
		}
	}
	return false
}

func decodePayload(raw json.RawMessage, out any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return &Error{Code: CodeInvalidArgs, Detail: "invalid JSON payload"}
	}
	return nil
}

func (r *Registrar) execute(ctx context.Context, principal string, source channel.ID, word Word, raw json.RawMessage) (any, error) {
	switch word {
	case WordChannelCreate:
		var p ChannelCreate
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.createChannel(ctx, principal, source, p)
	case WordChannelRetire:
		var p ChannelRetire
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.retireChannel(ctx, principal, p)
	case WordPrincipalRegister:
		var p PrincipalRegister
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.registerPrincipal(ctx, p)
	case WordPrincipalRetire:
		var p PrincipalRetire
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.retirePrincipal(ctx, principal, p)
	case WordCredentialSet:
		var p CredentialSet
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.setCredential(ctx, principal, p)
	case WordDeclRegister:
		var p DeclRegister
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.registerDecl(ctx, principal, p)
	case WordDeclEdit:
		var p DeclEdit
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.editDecl(ctx, principal, p)
	case WordDeclRevoke:
		var p DeclRevoke
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.revokeDecl(ctx, principal, p)
	case WordOverlaySet:
		var p OverlaySet
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.setOverlay(ctx, principal, source, p)
	case WordOverlayClear:
		var p OverlayClear
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.clearOverlay(ctx, source, p)
	case WordDeviceMint:
		var p DeviceMint
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.mintDevice(ctx, principal, p.Name)
	case WordDeviceClaim:
		var p DeviceClaim
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.claimDevice(ctx, principal, p)
	case WordDeviceRetire:
		var p DeviceRetire
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.retireDevice(ctx, principal, p)
	case WordDeviceAttach:
		var p DeviceBinding
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.attachDevice(ctx, principal, source, p)
	case WordDeviceDetach:
		var p DeviceBinding
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.detachDevice(ctx, principal, source, p)
	case WordChannelList:
		var p ChannelList
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.readChannels(ctx, p)
	case WordChannelGet:
		var p ChannelGet
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		row, ok, err := r.registry.GetChannelDesired(ctx, p.ChannelID)
		if err != nil {
			return nil, err
		}
		if !ok || row.Status != ChannelPresent {
			return nil, notFound("channel")
		}
		return row, nil
	case WordChannelCandidates:
		var p ChannelCandidates
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		if p.ChannelID == "" {
			return nil, invalid("channel_id required")
		}
		return r.registry.listPrincipals(ctx)
	case WordDeclList:
		return r.registry.ListDecls(ctx)
	case WordDeviceList:
		return r.readDevices(ctx)
	case WordPrincipalMe:
		return r.readPrincipal(ctx, principal)
	default:
		return nil, invalid("unknown registrar word")
	}
}

func invalid(detail string) error  { return &Error{Code: CodeInvalidArgs, Detail: detail} }
func notFound(noun string) error   { return &Error{Code: CodeNotFound, Detail: noun + " not found"} }
func conflict(detail string) error { return &Error{Code: CodeConflictExists, Detail: detail} }
func denied(detail string) error   { return &Error{Code: CodePermissionDenied, Detail: detail} }
func reserved(detail string) error { return &Error{Code: CodeReserved, Detail: detail} }

func (r *Registrar) createChannel(ctx context.Context, owner string, source channel.ID, p ChannelCreate) (ChannelRow, error) {
	tx, err := r.registry.db.BeginTx(ctx, nil)
	if err != nil {
		return ChannelRow{}, err
	}
	defer tx.Rollback()
	row, _, err := r.createChannelTx(ctx, tx, owner, source, p)
	if err != nil {
		return ChannelRow{}, err
	}
	if err := tx.Commit(); err != nil {
		return ChannelRow{}, err
	}
	// Replays also emit the id-only edge: a previous post-commit physical open
	// may have failed even though the desired row is already durable.
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: row.ID})
	}
	return row, nil
}

func (r *Registrar) createChannelTx(ctx context.Context, tx *sql.Tx, owner string, source channel.ID, p ChannelCreate) (ChannelRow, bool, error) {
	p.Name = strings.TrimSpace(p.Name)
	if p.Name == "" {
		return ChannelRow{}, false, invalid("name required")
	}
	parent := p.Parent
	if parent == "" {
		parent = source
	}
	if parent == "" {
		return ChannelRow{}, false, invalid("parent required")
	}
	var parentPresent bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channels WHERE id=? AND status='present')`, parent).Scan(&parentPresent); err != nil {
		return ChannelRow{}, false, err
	}
	if !parentPresent {
		return ChannelRow{}, false, notFound("parent channel")
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,parent_id,name,type,status,owner_principal,spec_json,created_at FROM channels WHERE parent_id=? AND name=? AND status='present' ORDER BY id`, parent, p.Name)
	if err != nil {
		return ChannelRow{}, false, err
	}
	var matches []ChannelRow
	for rows.Next() {
		v, e := scanChannel(rows)
		if e != nil {
			rows.Close()
			return ChannelRow{}, false, e
		}
		matches = append(matches, v)
	}
	if err := rows.Close(); err != nil {
		return ChannelRow{}, false, err
	}
	if err := rows.Err(); err != nil {
		return ChannelRow{}, false, err
	}
	if len(matches) > 0 {
		if len(matches) == 1 && matches[0].OwnerPrincipal == owner {
			return matches[0], false, nil
		}
		return ChannelRow{}, false, conflict("sibling channel name already exists")
	}
	now := r.now().UnixMilli()
	id := channel.ID(uuid.NewString())
	snapshot, err := (channelspec.RenderedSnapshot{Class: SpaceToolClass, Config: json.RawMessage(`{}`), Placement: channel.Placement{Kind: channel.PlacementServer}}).Seal()
	if err != nil {
		return ChannelRow{}, false, err
	}
	spec := GenesisSpec{ChannelID: id, Type: "group", OwnerPrincipal: owner, CreatedAt: now, ParentID: parent, InitiatorPrincipal: owner, Declarations: []GenesisDeclaration{{DeclID: SpaceToolDeclID, Kind: actor.KindTool, Rendered: snapshot}}}
	raw, err := json.Marshal(spec)
	if err != nil {
		return ChannelRow{}, false, err
	}
	row := ChannelRow{ID: id, ParentID: parent, Name: p.Name, Type: "group", Status: ChannelPresent, OwnerPrincipal: owner, Spec: raw, CreatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO channels(id,parent_id,name,type,status,owner_principal,spec_json,created_at) VALUES(?,?,?,?,?,?,?,?)`, row.ID, row.ParentID, row.Name, row.Type, row.Status, row.OwnerPrincipal, string(row.Spec), row.CreatedAt); err != nil {
		return ChannelRow{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO bindings(channel_id,device_id,attached_at) VALUES(?,?,?)`, row.ID, protocol.LocalDeviceID, now); err != nil {
		return ChannelRow{}, false, err
	}
	return row, true, nil
}

func (r *Registrar) retireChannel(ctx context.Context, principal string, p ChannelRetire) (ChannelRow, error) {
	if p.ChannelID == "" {
		return ChannelRow{}, invalid("channel_id required")
	}
	if p.ChannelID == protocol.C0ChannelID {
		return ChannelRow{}, reserved("c0 cannot be retired")
	}
	tx, err := r.registry.db.BeginTx(ctx, nil)
	if err != nil {
		return ChannelRow{}, err
	}
	defer tx.Rollback()
	row, err := scanChannel(tx.QueryRowContext(ctx, `SELECT id,parent_id,name,type,status,owner_principal,spec_json,created_at FROM channels WHERE id=?`, p.ChannelID))
	if errors.Is(err, sql.ErrNoRows) {
		return ChannelRow{}, notFound("channel")
	}
	if err != nil {
		return ChannelRow{}, err
	}
	if row.OwnerPrincipal != principal && principal != protocol.RootPrincipalID {
		return ChannelRow{}, denied("channel owner required")
	}
	if row.Status == ChannelRetired {
		return row, nil
	}
	var parent any
	if row.ParentID != "" {
		parent = row.ParentID
	}
	if _, err := tx.ExecContext(ctx, `UPDATE channels SET parent_id=? WHERE parent_id=? AND status='present'`, parent, p.ChannelID); err != nil {
		return ChannelRow{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE channels SET status='retired' WHERE id=?`, p.ChannelID); err != nil {
		return ChannelRow{}, err
	}
	row.Status = ChannelRetired
	if err := tx.Commit(); err != nil {
		return ChannelRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: p.ChannelID})
		r.registry.onCommit(Change{AllChannels: true})
	}
	return row, nil
}

func (r *Registrar) registerPrincipal(ctx context.Context, p PrincipalRegister) (PrincipalRow, error) {
	p.Email = strings.TrimSpace(p.Email)
	if p.Email == "" || p.SecretHash == "" {
		return PrincipalRow{}, invalid("email and secret_hash required")
	}
	id := strings.TrimSpace(p.ID)
	if id == "" {
		id = uuid.NewString()
	}
	if id == protocol.RootPrincipalID {
		return PrincipalRow{}, reserved("root principal id is reserved")
	}
	tx, err := r.registry.db.BeginTx(ctx, nil)
	if err != nil {
		return PrincipalRow{}, err
	}
	defer tx.Rollback()
	now := r.now().UnixMilli()
	row := PrincipalRow{ID: id, Kind: actor.KindHuman, Email: p.Email, DisplayName: p.DisplayName, Status: PrincipalPresent, CreatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO principals(id,kind,email,display_name,status,created_at) VALUES(?,?,?,?,?,?)`, row.ID, row.Kind, row.Email, nullableText(row.DisplayName), row.Status, row.CreatedAt); err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return PrincipalRow{}, conflict("email or principal already exists")
		}
		return PrincipalRow{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO credentials(principal_id,kind,secret_hash,status,rotated_at) VALUES(?,'password',?,'active',?)`, id, p.SecretHash, now); err != nil {
		return PrincipalRow{}, err
	}
	home, created, err := r.createChannelTx(ctx, tx, id, protocol.C0ChannelID, ChannelCreate{Name: id, Parent: protocol.C0ChannelID})
	if err != nil {
		return PrincipalRow{}, err
	}
	if !created {
		return PrincipalRow{}, conflict("home channel already exists")
	}
	if err := tx.Commit(); err != nil {
		return PrincipalRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: home.ID})
	}
	return row, nil
}

func (r *Registrar) retirePrincipal(ctx context.Context, caller string, p PrincipalRetire) (PrincipalRow, error) {
	if p.PrincipalID == "" {
		return PrincipalRow{}, invalid("principal_id required")
	}
	if p.PrincipalID == protocol.RootPrincipalID {
		return PrincipalRow{}, reserved("root cannot be retired")
	}
	if caller != p.PrincipalID && caller != protocol.RootPrincipalID {
		return PrincipalRow{}, denied("principal or root required")
	}
	row, err := r.updatePrincipalStatus(ctx, p.PrincipalID, PrincipalRetired)
	return row, err
}

func (r *Registrar) updatePrincipalStatus(ctx context.Context, id string, status PrincipalStatus) (PrincipalRow, error) {
	tx, err := r.registry.db.BeginTx(ctx, nil)
	if err != nil {
		return PrincipalRow{}, err
	}
	defer tx.Rollback()
	row, err := scanPrincipal(tx.QueryRowContext(ctx, `SELECT id,kind,email,display_name,status,created_at FROM principals WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return PrincipalRow{}, notFound("principal")
	}
	if err != nil {
		return PrincipalRow{}, err
	}
	if row.Status == status {
		return row, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE principals SET status=? WHERE id=?`, status, id); err != nil {
		return PrincipalRow{}, err
	}
	row.Status = status
	if err := tx.Commit(); err != nil {
		return PrincipalRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{Principal: id})
	}
	return row, nil
}

func (r *Registrar) setCredential(ctx context.Context, caller string, p CredentialSet) (CredentialReply, error) {
	if p.PrincipalID == "" || p.SecretHash == "" {
		return CredentialReply{}, invalid("principal_id and secret_hash required")
	}
	if caller != p.PrincipalID && caller != protocol.RootPrincipalID {
		return CredentialReply{}, denied("principal or root required")
	}
	tx, err := r.registry.db.BeginTx(ctx, nil)
	if err != nil {
		return CredentialReply{}, err
	}
	defer tx.Rollback()
	var principalKind actor.Kind
	err = tx.QueryRowContext(ctx, `SELECT kind FROM principals WHERE id=?`, p.PrincipalID).Scan(&principalKind)
	if errors.Is(err, sql.ErrNoRows) {
		return CredentialReply{}, notFound("principal")
	}
	if err != nil {
		return CredentialReply{}, err
	}
	if principalKind != actor.KindHuman {
		return CredentialReply{}, denied("credentials require a human principal")
	}
	var storedHash string
	var status CredentialStatus
	var rotatedAt int64
	err = tx.QueryRowContext(ctx, `SELECT secret_hash,status,rotated_at FROM credentials WHERE principal_id=? AND kind='password'`, p.PrincipalID).Scan(&storedHash, &status, &rotatedAt)
	if err == nil && storedHash == p.SecretHash && status == CredentialActive {
		return CredentialReply{PrincipalID: p.PrincipalID, Kind: "password", Status: status, RotatedAt: rotatedAt}, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return CredentialReply{}, err
	}
	now := r.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `INSERT INTO credentials(principal_id,kind,secret_hash,status,rotated_at) VALUES(?,'password',?,'active',?) ON CONFLICT(principal_id,kind) DO UPDATE SET secret_hash=excluded.secret_hash,status='active',rotated_at=excluded.rotated_at`, p.PrincipalID, p.SecretHash, now); err != nil {
		return CredentialReply{}, err
	}
	if err := tx.Commit(); err != nil {
		return CredentialReply{}, err
	}
	return CredentialReply{PrincipalID: p.PrincipalID, Kind: "password", Status: CredentialActive, RotatedAt: now}, nil
}

func (r *Registrar) registerDecl(ctx context.Context, owner string, p DeclRegister) (DeclRow, error) {
	p.ID = strings.TrimSpace(p.ID)
	p.Name = strings.TrimSpace(p.Name)
	p.Class = strings.TrimSpace(p.Class)
	if p.ID == "" || p.Name == "" || p.Class == "" {
		return DeclRow{}, invalid("id, name and class required")
	}
	if p.Class == SpaceToolClass {
		return DeclRow{}, reserved("space-tool class is reserved")
	}
	if p.Visibility == "" {
		p.Visibility = "private"
	}
	if p.Visibility != "private" && p.Visibility != "public" {
		return DeclRow{}, invalid("invalid visibility")
	}
	if r.classes == nil {
		return DeclRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
	}
	if err := r.classes.ValidateConfig(p.Class, p.Config); err != nil {
		return DeclRow{}, invalid(err.Error())
	}
	now := r.now().UnixMilli()
	tx, err := r.registry.db.BeginTx(ctx, nil)
	if err != nil {
		return DeclRow{}, err
	}
	defer tx.Rollback()
	existing, err := scanDecl(tx.QueryRowContext(ctx, `SELECT id,name,owner,default_class,config_json,status,visibility,created_at,updated_at FROM decls WHERE id=?`, p.ID))
	if err == nil {
		if existing.Status == DeclPresent && existing.Name == p.Name && existing.Owner == owner && existing.DefaultClass == p.Class && existing.Visibility == p.Visibility && jsonEqual(existing.Config, p.Config) {
			return existing, nil
		}
		return DeclRow{}, conflict("declaration id already exists")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return DeclRow{}, err
	}
	row := DeclRow{ID: p.ID, Name: p.Name, Owner: owner, DefaultClass: p.Class, Config: cloneJSON(p.Config), Status: DeclPresent, Visibility: p.Visibility, CreatedAt: now, UpdatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO decls(id,name,owner,default_class,config_json,status,visibility,created_at,updated_at) VALUES(?,?,?,?,?,'present',?,?,?)`, row.ID, row.Name, row.Owner, row.DefaultClass, nullableJSON(row.Config), row.Visibility, row.CreatedAt, row.UpdatedAt); err != nil {
		return DeclRow{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeclRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{AllChannels: true})
	}
	return row, nil
}

func (r *Registrar) editDecl(ctx context.Context, caller string, p DeclEdit) (DeclRow, error) {
	if p.ID == "" {
		return DeclRow{}, invalid("id required")
	}
	tx, err := r.registry.db.BeginTx(ctx, nil)
	if err != nil {
		return DeclRow{}, err
	}
	defer tx.Rollback()
	row, err := scanDecl(tx.QueryRowContext(ctx, `SELECT id,name,owner,default_class,config_json,status,visibility,created_at,updated_at FROM decls WHERE id=?`, p.ID))
	if errors.Is(err, sql.ErrNoRows) || row.Status != DeclPresent {
		return DeclRow{}, notFound("declaration")
	}
	if err != nil {
		return DeclRow{}, err
	}
	if row.DefaultClass == SpaceToolClass || row.ID == SpaceToolDeclID {
		return DeclRow{}, reserved("space-tool declaration is reserved")
	}
	if row.Owner != caller && caller != protocol.RootPrincipalID {
		return DeclRow{}, denied("declaration owner required")
	}
	before := row
	before.Config = cloneJSON(row.Config)
	if p.Name != nil {
		row.Name = strings.TrimSpace(*p.Name)
	}
	if p.Class != nil {
		next := strings.TrimSpace(*p.Class)
		if next == SpaceToolClass {
			return DeclRow{}, reserved("space-tool class is reserved")
		}
		if r.classes == nil {
			return DeclRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
		}
		oldKind, oldOK := r.classes.LookupClassKind(row.DefaultClass)
		newKind, newOK := r.classes.LookupClassKind(next)
		if !oldOK || !newOK || oldKind != newKind {
			return DeclRow{}, invalid("class transition changes actor kind")
		}
		row.DefaultClass = next
	}
	if p.Config != nil {
		row.Config = cloneJSON(p.Config)
	}
	if p.Visibility != nil {
		row.Visibility = *p.Visibility
	}
	if row.Name == "" || (row.Visibility != "private" && row.Visibility != "public") {
		return DeclRow{}, invalid("invalid declaration")
	}
	if r.classes == nil {
		return DeclRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
	}
	if err := r.classes.ValidateConfig(row.DefaultClass, row.Config); err != nil {
		return DeclRow{}, invalid(err.Error())
	}
	if row.Name == before.Name && row.DefaultClass == before.DefaultClass && row.Visibility == before.Visibility && jsonEqual(row.Config, before.Config) {
		return before, nil
	}
	row.UpdatedAt = r.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `UPDATE decls SET name=?,default_class=?,config_json=?,visibility=?,updated_at=? WHERE id=?`, row.Name, row.DefaultClass, nullableJSON(row.Config), row.Visibility, row.UpdatedAt, row.ID); err != nil {
		return DeclRow{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeclRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{AllChannels: true})
	}
	return row, nil
}

func (r *Registrar) revokeDecl(ctx context.Context, caller string, p DeclRevoke) (DeclRow, error) {
	if p.ID == "" {
		return DeclRow{}, invalid("id required")
	}
	tx, err := r.registry.db.BeginTx(ctx, nil)
	if err != nil {
		return DeclRow{}, err
	}
	defer tx.Rollback()
	row, err := scanDecl(tx.QueryRowContext(ctx, `SELECT id,name,owner,default_class,config_json,status,visibility,created_at,updated_at FROM decls WHERE id=?`, p.ID))
	if errors.Is(err, sql.ErrNoRows) {
		return DeclRow{}, notFound("declaration")
	}
	if err != nil {
		return DeclRow{}, err
	}
	if row.DefaultClass == SpaceToolClass || row.ID == SpaceToolDeclID {
		return DeclRow{}, reserved("space-tool declaration is reserved")
	}
	if row.Owner != caller && caller != protocol.RootPrincipalID {
		return DeclRow{}, denied("declaration owner required")
	}
	if row.Status == DeclRevoked {
		return row, nil
	}
	row.Status = DeclRevoked
	row.UpdatedAt = r.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `UPDATE decls SET status='revoked',updated_at=? WHERE id=?`, row.UpdatedAt, row.ID); err != nil {
		return DeclRow{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeclRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{AllChannels: true})
	}
	return row, nil
}

func (r *Registrar) setOverlay(ctx context.Context, _ string, source channel.ID, p OverlaySet) (OverlayRow, error) {
	if p.DeclID == "" || p.ChannelID == "" {
		return OverlayRow{}, invalid("decl_id and channel_id required")
	}
	if p.ChannelID != source {
		return OverlayRow{}, denied("overlay target must equal source channel")
	}
	decl, ok, err := r.registry.GetDecl(ctx, p.DeclID)
	if err != nil {
		return OverlayRow{}, err
	}
	if !ok || decl.Status != DeclPresent {
		return OverlayRow{}, notFound("declaration")
	}
	if r.classes == nil {
		return OverlayRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
	}
	if err := r.classes.ValidateConfig(decl.DefaultClass, p.Config); err != nil {
		return OverlayRow{}, invalid(err.Error())
	}
	var existing OverlayRow
	var raw sql.NullString
	err = r.registry.db.QueryRowContext(ctx, `SELECT decl_id,channel_id,config_json,updated_at FROM decl_overlays WHERE decl_id=? AND channel_id=?`, p.DeclID, p.ChannelID).Scan(&existing.DeclID, &existing.ChannelID, &raw, &existing.UpdatedAt)
	if err == nil {
		if raw.Valid {
			existing.Config = json.RawMessage(raw.String)
		}
		if jsonEqual(existing.Config, p.Config) {
			return existing, nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return OverlayRow{}, err
	}
	now := r.now().UnixMilli()
	_, err = r.registry.db.ExecContext(ctx, `INSERT INTO decl_overlays(decl_id,channel_id,config_json,updated_at) VALUES(?,?,?,?) ON CONFLICT(decl_id,channel_id) DO UPDATE SET config_json=excluded.config_json,updated_at=excluded.updated_at`, p.DeclID, p.ChannelID, nullableJSON(p.Config), now)
	if err != nil {
		return OverlayRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: p.ChannelID})
	}
	return OverlayRow{DeclID: p.DeclID, ChannelID: p.ChannelID, Config: cloneJSON(p.Config), UpdatedAt: now}, nil
}

func (r *Registrar) clearOverlay(ctx context.Context, source channel.ID, p OverlayClear) (Confirmation, error) {
	if p.DeclID == "" || p.ChannelID == "" {
		return Confirmation{}, invalid("decl_id and channel_id required")
	}
	if p.ChannelID != source {
		return Confirmation{}, denied("overlay target must equal source channel")
	}
	if _, err := r.registry.db.ExecContext(ctx, `DELETE FROM decl_overlays WHERE decl_id=? AND channel_id=?`, p.DeclID, p.ChannelID); err != nil {
		return Confirmation{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: p.ChannelID})
	}
	return Confirmation{Word: WordOverlayClear, TargetID: p.DeclID, Status: "cleared"}, nil
}

func (r *Registrar) mintDevice(ctx context.Context, owner, name string) (DeviceRow, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return DeviceRow{}, invalid("name required")
	}
	row := DeviceRow{ID: uuid.NewString(), OwnerPrincipal: owner, Name: name, Key: uuid.NewString(), Status: DevicePresent, CreatedAt: r.now().UnixMilli()}
	_, err := r.registry.db.ExecContext(ctx, `INSERT INTO devices(id,owner_principal,name,key,status,created_at) VALUES(?,?,?,?,?,?)`, row.ID, row.OwnerPrincipal, row.Name, row.Key, row.Status, row.CreatedAt)
	return row, err
}

func (r *Registrar) claimDevice(ctx context.Context, owner string, p DeviceClaim) (DeviceRow, error) {
	if p.DeviceID != "" {
		row, ok, err := r.registry.GetDevice(ctx, p.DeviceID)
		if err != nil {
			return DeviceRow{}, err
		}
		if ok && row.Status == DevicePresent && row.OwnerPrincipal == owner {
			return row, nil
		}
	}
	return r.mintDevice(ctx, owner, "claimed-device")
}

func (r *Registrar) retireDevice(ctx context.Context, owner string, p DeviceRetire) (DeviceRow, error) {
	if p.DeviceID == "" {
		return DeviceRow{}, invalid("device_id required")
	}
	if p.DeviceID == protocol.LocalDeviceID {
		return DeviceRow{}, reserved("local device cannot be retired")
	}
	row, ok, err := r.registry.GetDevice(ctx, p.DeviceID)
	if err != nil {
		return DeviceRow{}, err
	}
	if !ok {
		return DeviceRow{}, notFound("device")
	}
	if row.OwnerPrincipal != owner && owner != protocol.RootPrincipalID {
		return DeviceRow{}, denied("device owner required")
	}
	if row.Status == DeviceRetired {
		return row, nil
	}
	if _, err := r.registry.db.ExecContext(ctx, `UPDATE devices SET status='retired' WHERE id=?`, p.DeviceID); err != nil {
		return DeviceRow{}, err
	}
	row.Status = DeviceRetired
	if r.registry.onCommit != nil {
		// Effective bindings join device status, so this one row can change the
		// placement view of multiple channels.
		r.registry.onCommit(Change{AllChannels: true})
	}
	return row, nil
}

func (r *Registrar) attachDevice(ctx context.Context, owner string, source channel.ID, p DeviceBinding) (BindingRow, error) {
	if err := r.authorizeBinding(ctx, owner, source, p); err != nil {
		return BindingRow{}, err
	}
	now := r.now().UnixMilli()
	_, err := r.registry.db.ExecContext(ctx, `INSERT INTO bindings(channel_id,device_id,attached_at) VALUES(?,?,?) ON CONFLICT(channel_id,device_id) DO NOTHING`, p.ChannelID, p.DeviceID, now)
	if err != nil {
		return BindingRow{}, err
	}
	var attached int64
	if err := r.registry.db.QueryRowContext(ctx, `SELECT attached_at FROM bindings WHERE channel_id=? AND device_id=?`, p.ChannelID, p.DeviceID).Scan(&attached); err != nil {
		return BindingRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: p.ChannelID})
	}
	return BindingRow{ChannelID: p.ChannelID, DeviceID: p.DeviceID, AttachedAt: attached}, nil
}

func (r *Registrar) detachDevice(ctx context.Context, owner string, source channel.ID, p DeviceBinding) (Confirmation, error) {
	if p.DeviceID == protocol.LocalDeviceID {
		return Confirmation{}, reserved("local device cannot be detached")
	}
	if err := r.authorizeBinding(ctx, owner, source, p); err != nil {
		return Confirmation{}, err
	}
	if _, err := r.registry.db.ExecContext(ctx, `DELETE FROM bindings WHERE channel_id=? AND device_id=?`, p.ChannelID, p.DeviceID); err != nil {
		return Confirmation{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: p.ChannelID})
	}
	return Confirmation{Word: WordDeviceDetach, TargetID: p.DeviceID, Status: "detached"}, nil
}

func (r *Registrar) authorizeBinding(ctx context.Context, owner string, source channel.ID, p DeviceBinding) error {
	if p.ChannelID == "" || p.DeviceID == "" {
		return invalid("channel_id and device_id required")
	}
	if p.ChannelID != source {
		return denied("binding target must equal source channel")
	}
	ch, ok, err := r.registry.GetChannelDesired(ctx, p.ChannelID)
	if err != nil {
		return err
	}
	if !ok || ch.Status != ChannelPresent {
		return notFound("channel")
	}
	device, ok, err := r.registry.GetDevice(ctx, p.DeviceID)
	if err != nil {
		return err
	}
	if !ok || device.Status != DevicePresent {
		return notFound("device")
	}
	if p.DeviceID != protocol.LocalDeviceID && device.OwnerPrincipal != owner {
		return denied("device owner required")
	}
	return nil
}

func (r *Registrar) readChannels(ctx context.Context, p ChannelList) ([]ChannelRow, error) {
	rows, err := r.registry.ListPresentChannels(ctx)
	if err != nil {
		return nil, err
	}
	if p.ParentID == nil {
		return rows, nil
	}
	out := rows[:0]
	for _, row := range rows {
		if row.ParentID == *p.ParentID {
			out = append(out, row)
		}
	}
	return out, nil
}

func (r *Registrar) readDevices(ctx context.Context) ([]DeviceRow, error) {
	rows, err := r.registry.db.QueryContext(ctx, `SELECT id,owner_principal,name,key,status,created_at FROM devices WHERE status='present' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeviceRow
	for rows.Next() {
		v, e := scanDevice(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (r *Registrar) readPrincipal(ctx context.Context, id string) (PrincipalRow, error) {
	row, err := scanPrincipal(r.registry.db.QueryRowContext(ctx, `SELECT id,kind,email,display_name,status,created_at FROM principals WHERE id=? AND status='present'`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return PrincipalRow{}, notFound("principal")
	}
	return row, err
}

func nullableText(v string) any {
	if v == "" {
		return nil
	}
	return v
}
func nullableJSON(v json.RawMessage) any {
	if len(v) == 0 {
		return nil
	}
	return string(v)
}
func cloneJSON(v json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), v...) }
func jsonEqual(a, b json.RawMessage) bool {
	var av, bv any
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return false
	}
	aa, _ := json.Marshal(av)
	bb, _ := json.Marshal(bv)
	return string(aa) == string(bb)
}

type C0Caller interface {
	CallRegistrar(context.Context, Word, any) (json.RawMessage, error)
}

type submitter struct {
	caller   C0Caller
	facts    SourceActorFactsResolver
	registry *Registry
}

func NewSubmitter(caller C0Caller, facts SourceActorFactsResolver, registry *Registry) Submitter {
	return &submitter{caller: caller, facts: facts, registry: registry}
}

func (s *submitter) Submit(ctx context.Context, in SubmitIn) (Reply, error) {
	if s.caller == nil || s.facts == nil {
		return Reply{}, errors.New("lagoon: submitter is not wired")
	}
	if in.Source == "" || in.Sender == "" || in.RequestID == "" {
		return Reply{}, invalid("source frame required")
	}
	facts, ok, err := s.facts.ActorFacts(ctx, in.Source, in.Sender)
	if err != nil {
		return Reply{}, err
	}
	if !ok || !facts.Active {
		return Reply{}, denied("active source member required")
	}
	principal := facts.Principal
	if principal == "" && facts.SourceDeclID != "" {
		decl, found, err := s.registryDecl(ctx, facts.SourceDeclID)
		if err != nil {
			return Reply{}, err
		}
		if found {
			principal = decl.Owner
		}
	}
	if principal == "" {
		return Reply{}, denied("attributable principal required")
	}
	payload, err := json.Marshal(in.Payload)
	if err != nil {
		return Reply{}, invalid("payload is not encodable")
	}
	raw, err := s.caller.CallRegistrar(ctx, in.Word, forwardedRequest{Source: SourceRef{ChannelID: in.Source, RequestID: in.RequestID}, Initiator: principal, Payload: payload})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return Reply{}, &Error{Code: CodeResultUnknown, Detail: SourceRef{ChannelID: in.Source, RequestID: in.RequestID}.String()}
		}
		return Reply{}, err
	}
	var reply Reply
	if err := json.Unmarshal(raw, &reply); err != nil {
		return Reply{}, err
	}
	return reply, nil
}

func (s *submitter) registryDecl(ctx context.Context, id string) (DeclRow, bool, error) {
	if s.registry == nil {
		return DeclRow{}, false, errors.New("lagoon: registry read face required")
	}
	return s.registry.GetDecl(ctx, id)
}

func (s *submitter) SubmitApplication(ctx context.Context, word Word, payload any) (Reply, error) {
	if word != WordPrincipalRegister {
		return Reply{}, denied("application entrance only accepts principal.register")
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return Reply{}, invalid("payload is not encodable")
	}
	ref := SourceRef{ChannelID: protocol.C0ChannelID, RequestID: uuid.NewString()}
	raw, err := s.caller.CallRegistrar(ctx, word, forwardedRequest{Source: ref, Application: true, Payload: rawPayload})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return Reply{}, &Error{Code: CodeResultUnknown, Detail: ref.String()}
		}
		return Reply{}, err
	}
	var reply Reply
	if err := json.Unmarshal(raw, &reply); err != nil {
		return Reply{}, err
	}
	return reply, nil
}
