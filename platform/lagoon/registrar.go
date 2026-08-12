package lagoon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon/internal/store"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type Registrar struct {
	registry *Registry
	facts    SourceActorFactsResolver
	classes  ClassCatalog
	now      Clock
}

func NewRegistrar(registry *Registry, facts SourceActorFactsResolver, classes ClassCatalog) *Registrar {
	return &Registrar{registry: registry, facts: facts, classes: classes, now: time.Now}
}

// ReconcileSystem verifies and repairs the registry's fixed system rows. It
// deliberately never touches credentials or user/device identities.
func (r *Registrar) ReconcileSystem(ctx context.Context) error {
	if r == nil || r.registry == nil {
		return errors.New("lagoon: registrar registry required")
	}
	rootExists, err := r.registry.store.PrincipalExistsWithKind(ctx, protocol.RootPrincipalID, actor.KindHuman)
	if err != nil {
		return err
	}
	localExists, err := r.registry.store.DeviceExists(ctx, protocol.LocalDeviceID)
	if err != nil {
		return err
	}
	if !rootExists || !localExists {
		return errors.New("lagoon: installation identity incomplete")
	}
	if err := ValidateName(string(protocol.C0ChannelID)); err != nil {
		return fmt.Errorf("lagoon: invalid c0 channel name: %w", err)
	}
	now := r.now().UnixMilli()
	if err := r.registry.store.UpsertSteward(ctx, protocol.StewardPrincipalID, now); err != nil {
		return err
	}
	c0Exists, err := r.registry.store.ChannelExists(ctx, protocol.C0ChannelID)
	if err != nil {
		return err
	}
	spec := GenesisSpec{ChannelID: protocol.C0ChannelID, Type: "group", OwnerPrincipal: protocol.RootPrincipalID, CreatedAt: now}
	if !c0Exists {
		genesis, ok := r.facts.(SystemGenesisResolver)
		if !ok {
			return errors.New("lagoon: c0 physical genesis unavailable")
		}
		var found bool
		spec, found, err = genesis.SystemGenesis(ctx)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("lagoon: c0 physical genesis missing")
		}
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	if err := r.registry.store.UpsertSystemChannel(ctx, regspec.ChannelRow{
		ID: protocol.C0ChannelID, Name: string(protocol.C0ChannelID), Type: "group",
		Status: regspec.ChannelPresent, OwnerPrincipal: protocol.RootPrincipalID, Spec: raw, CreatedAt: spec.CreatedAt,
	}); err != nil {
		return err
	}
	if err := r.registry.store.UpsertSystemDecl(ctx, regspec.DeclRow{
		ID: SpaceToolDeclID, Name: "Space Tool", Owner: protocol.RootPrincipalID, DefaultClass: SpaceToolClass,
		Config: json.RawMessage(`{}`), Status: regspec.DeclPresent, Visibility: "public", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{AllChannels: true})
	}
	return nil
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
	if msg.Sender.ID == actor.SystemActorID {
		var forwarded message.Envelope
		if err := json.Unmarshal(msg.Payload, &forwarded); err != nil || forwarded.ChannelID == "" || forwarded.ID == "" || forwarded.Type != msg.Type {
			_, _ = sys.Fail(msg, string(CodeInvalidArgs), "invalid forwarded request")
			return
		}
		source = SourceRef{ChannelID: forwarded.ChannelID, RequestID: string(forwarded.ID)}
		payload = json.RawMessage(append([]byte(nil), forwarded.Payload...))
		if forwarded.Sender.ID != "" {
			principal = r.resolvePrincipal(msg.Ctx(), forwarded.ChannelID, forwarded.Sender.ID, sys, msg)
			if principal == "" {
				return
			}
		}
	} else {
		principal = r.resolvePrincipal(msg.Ctx(), msg.ChannelID, msg.Sender.ID, sys, msg)
		if principal == "" {
			return
		}
	}
	if word == WordPrincipalRegister {
		if principal != "" {
			_, _ = sys.Fail(msg, string(CodePermissionDenied), "registration requires the anonymous application entrance")
			return
		}
	} else if principal == "" {
		_, _ = sys.Fail(msg, string(CodePermissionDenied), "authenticated principal required")
		return
	}
	value, err := r.execute(msg.Ctx(), principal, source.ChannelID, word, payload)
	if err != nil {
		var le *Error
		if errors.As(err, &le) {
			_, _ = sys.Fail(msg, string(le.Code), le.Detail)
		} else {
			_, _ = sys.Fail(msg, string(CodeResultUnknown), err.Error())
		}
		return
	}
	rawValue, err := json.Marshal(value)
	if err != nil {
		_, _ = sys.Fail(msg, string(CodeResultUnknown), err.Error())
		return
	}
	_, _ = sys.Reply(msg, Reply{Word: word, Value: rawValue, Source: source})
}

func (r *Registrar) resolvePrincipal(ctx context.Context, source channel.ID, sender actor.ActorID, sys actorbase.Sys, msg actorbase.Msg) string {
	if r.facts == nil {
		_, _ = sys.Fail(msg, string(CodePermissionDenied), "initiator unavailable")
		return ""
	}
	facts, found, err := r.facts.ActorFacts(ctx, source, sender)
	if err != nil {
		_, _ = sys.Fail(msg, string(CodeResultUnknown), err.Error())
		return ""
	}
	if !found || !facts.Active {
		_, _ = sys.Fail(msg, string(CodePermissionDenied), "active attributable principal required")
		return ""
	}
	if facts.Principal != "" {
		return facts.Principal
	}
	if facts.SourceDeclID != "" {
		decl, found, err := r.registry.GetDecl(ctx, facts.SourceDeclID)
		if err != nil {
			_, _ = sys.Fail(msg, string(CodeResultUnknown), err.Error())
			return ""
		}
		if found {
			return decl.Owner
		}
	}
	_, _ = sys.Fail(msg, string(CodePermissionDenied), "active attributable principal required")
	return ""
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
		if !ok || row.Status != regspec.ChannelPresent {
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
		return r.registry.store.ListPrincipals(ctx)
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

func (r *Registrar) createChannel(ctx context.Context, owner string, source channel.ID, p ChannelCreate) (regspec.ChannelRow, error) {
	row, created, err := r.createChannelRows(ctx, owner, source, p)
	if err != nil {
		return regspec.ChannelRow{}, err
	}
	if !created {
		if err := r.registry.store.InsertBindingIfAbsent(ctx, regspec.BindingRow{
			ChannelID: row.ID, DeviceID: protocol.LocalDeviceID, AttachedAt: r.now().UnixMilli(),
		}); err != nil {
			return regspec.ChannelRow{}, err
		}
	}
	// Replays also emit the id-only edge: a previous physical open
	// may have failed even though the desired row is already durable.
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: row.ID})
	}
	return row, nil
}

func (r *Registrar) createChannelRows(ctx context.Context, owner string, source channel.ID, p ChannelCreate) (regspec.ChannelRow, bool, error) {
	if p.Name == "" {
		return regspec.ChannelRow{}, false, invalid("name required")
	}
	if err := ValidateName(p.Name); err != nil {
		return regspec.ChannelRow{}, false, invalid(err.Error())
	}
	parent := p.Parent
	if parent == "" {
		parent = source
	}
	if parent == "" {
		return regspec.ChannelRow{}, false, invalid("parent required")
	}
	// A retired parent still has a row, and its name stays reserved — but it can
	// no longer gain children. Otherwise retirement's one rule (a parent with
	// live children cannot retire) could be walked around by retiring first and
	// hanging the child afterwards.
	parentRow, parentFound, err := r.registry.GetChannelDesired(ctx, parent)
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	if !parentFound || parentRow.Status != regspec.ChannelPresent {
		return regspec.ChannelRow{}, false, notFound("parent channel")
	}
	qualified, err := JoinName(parentRow.QualifiedName, p.Name)
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	matches, err := r.registry.store.FindChannels(ctx, parent, p.Name)
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	if len(matches) > 0 {
		if len(matches) == 1 && matches[0].Status == regspec.ChannelPresent && matches[0].OwnerPrincipal == owner {
			matches[0].QualifiedName = qualified
			return matches[0], false, nil
		}
		return regspec.ChannelRow{}, false, conflict("sibling channel name already exists")
	}
	now := r.now().UnixMilli()
	id := channel.ID(uuid.NewString())
	snapshot, err := (channelspec.RenderedSnapshot{Class: SpaceToolClass, Config: json.RawMessage(`{}`), Placement: channel.Placement{Kind: channel.PlacementServer}}).Seal()
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	spec := GenesisSpec{ChannelID: id, Type: "group", OwnerPrincipal: owner, CreatedAt: now, ParentID: parent, InitiatorPrincipal: owner, Declarations: []GenesisDeclaration{{DeclID: SpaceToolDeclID, Kind: actor.KindTool, Rendered: snapshot}}}
	raw, err := json.Marshal(spec)
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	row := regspec.ChannelRow{ID: id, ParentID: parent, Name: p.Name, QualifiedName: qualified, Type: "group", Status: regspec.ChannelPresent, OwnerPrincipal: owner, Spec: raw, CreatedAt: now}
	if err := r.registry.store.InsertChannel(ctx, row); err != nil {
		return regspec.ChannelRow{}, false, err
	}
	if err := r.registry.store.InsertBinding(ctx, regspec.BindingRow{ChannelID: row.ID, DeviceID: protocol.LocalDeviceID, AttachedAt: now}); err != nil {
		return regspec.ChannelRow{}, false, err
	}
	return row, true, nil
}

func (r *Registrar) retireChannel(ctx context.Context, principal string, p ChannelRetire) (regspec.ChannelRow, error) {
	if p.ChannelID == "" {
		return regspec.ChannelRow{}, invalid("channel_id required")
	}
	if p.ChannelID == protocol.C0ChannelID {
		return regspec.ChannelRow{}, reserved("c0 cannot be retired")
	}
	row, found, err := r.registry.GetChannelDesired(ctx, p.ChannelID)
	if !found && err == nil {
		return regspec.ChannelRow{}, notFound("channel")
	}
	if err != nil {
		return regspec.ChannelRow{}, err
	}
	if row.OwnerPrincipal != principal && principal != protocol.RootPrincipalID {
		return regspec.ChannelRow{}, denied("channel owner required")
	}
	if row.Status == regspec.ChannelRetired {
		return row, nil
	}
	hasPresentChild, err := r.registry.store.PresentChildExists(ctx, p.ChannelID)
	if err != nil {
		return regspec.ChannelRow{}, err
	}
	if hasPresentChild {
		return regspec.ChannelRow{}, conflict("channel has active child channels")
	}
	if err := r.registry.store.UpdateChannelStatus(ctx, p.ChannelID, regspec.ChannelRetired); err != nil {
		return regspec.ChannelRow{}, err
	}
	row.Status = regspec.ChannelRetired
	if r.registry.onCommit != nil {
		// This is an in-process entitlement edge only; it never enters the
		// ChannelHost/daemon reconciliation path.
		r.registry.onCommit(Change{AllPrincipals: true})
	}
	return row, nil
}

func (r *Registrar) registerPrincipal(ctx context.Context, p PrincipalRegister) (regspec.PrincipalRow, error) {
	p.Email = strings.TrimSpace(p.Email)
	if p.Email == "" || p.SecretHash == "" {
		return regspec.PrincipalRow{}, invalid("email and secret_hash required")
	}
	id := strings.TrimSpace(p.ID)
	if id == protocol.RootPrincipalID {
		return regspec.PrincipalRow{}, reserved("root principal id is reserved")
	}
	if id != "" {
		if err := ValidateName(id); err != nil {
			return regspec.PrincipalRow{}, invalid(err.Error())
		}
	}
	now := r.now().UnixMilli()
	var row regspec.PrincipalRow
	var found bool
	var err error
	if id != "" {
		row, found, err = r.registry.store.GetPrincipal(ctx, id)
	} else {
		row, found, err = r.registry.store.GetPrincipalByEmail(ctx, p.Email)
	}
	if err != nil {
		return regspec.PrincipalRow{}, err
	}
	if found {
		if row.Kind != actor.KindHuman || row.Email != p.Email || row.DisplayName != p.DisplayName || row.Status != regspec.PrincipalPresent {
			return regspec.PrincipalRow{}, conflict("email or principal already exists")
		}
		id = row.ID
	} else {
		if id == "" {
			id = uuid.NewString()
		}
		row = regspec.PrincipalRow{ID: id, Kind: actor.KindHuman, Email: p.Email, DisplayName: p.DisplayName, Status: regspec.PrincipalPresent, CreatedAt: now}
	}
	if !found {
		err = r.registry.store.InsertPrincipal(ctx, row)
	}
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			return regspec.PrincipalRow{}, conflict("email or principal already exists")
		}
		return regspec.PrincipalRow{}, err
	}
	hash, status, _, credentialFound, err := r.registry.store.PasswordCredential(ctx, id)
	if err != nil {
		return regspec.PrincipalRow{}, err
	}
	if credentialFound {
		if hash != p.SecretHash || status != regspec.CredentialActive {
			return regspec.PrincipalRow{}, conflict("principal credential already exists")
		}
	} else if err := r.registry.store.InsertPasswordCredential(ctx, id, p.SecretHash, now); err != nil {
		return regspec.PrincipalRow{}, err
	}
	home, created, err := r.createChannelRows(ctx, id, protocol.C0ChannelID, ChannelCreate{Name: id, Parent: protocol.C0ChannelID})
	if err != nil {
		return regspec.PrincipalRow{}, err
	}
	if !created {
		if err := r.registry.store.InsertBindingIfAbsent(ctx, regspec.BindingRow{ChannelID: home.ID, DeviceID: protocol.LocalDeviceID, AttachedAt: now}); err != nil {
			return regspec.PrincipalRow{}, err
		}
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: home.ID})
	}
	return row, nil
}

func (r *Registrar) retirePrincipal(ctx context.Context, caller string, p PrincipalRetire) (regspec.PrincipalRow, error) {
	if p.PrincipalID == "" {
		return regspec.PrincipalRow{}, invalid("principal_id required")
	}
	if p.PrincipalID == protocol.RootPrincipalID {
		return regspec.PrincipalRow{}, reserved("root cannot be retired")
	}
	if caller != p.PrincipalID && caller != protocol.RootPrincipalID {
		return regspec.PrincipalRow{}, denied("principal or root required")
	}
	row, err := r.updatePrincipalStatus(ctx, p.PrincipalID, regspec.PrincipalRetired)
	return row, err
}

func (r *Registrar) updatePrincipalStatus(ctx context.Context, id string, status regspec.PrincipalStatus) (regspec.PrincipalRow, error) {
	row, found, err := r.registry.store.GetPrincipal(ctx, id)
	if !found && err == nil {
		return regspec.PrincipalRow{}, notFound("principal")
	}
	if err != nil {
		return regspec.PrincipalRow{}, err
	}
	if row.Status == status {
		return row, nil
	}
	if err := r.registry.store.UpdatePrincipalStatus(ctx, id, status); err != nil {
		return regspec.PrincipalRow{}, err
	}
	row.Status = status
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
	principalKind, found, err := r.registry.store.PrincipalKind(ctx, p.PrincipalID)
	if !found && err == nil {
		return CredentialReply{}, notFound("principal")
	}
	if err != nil {
		return CredentialReply{}, err
	}
	if principalKind != actor.KindHuman {
		return CredentialReply{}, denied("credentials require a human principal")
	}
	storedHash, status, rotatedAt, found, err := r.registry.store.PasswordCredential(ctx, p.PrincipalID)
	if err == nil && found && storedHash == p.SecretHash && status == regspec.CredentialActive {
		return CredentialReply{PrincipalID: p.PrincipalID, Kind: "password", Status: status, RotatedAt: rotatedAt}, nil
	}
	if err != nil {
		return CredentialReply{}, err
	}
	now := r.now().UnixMilli()
	if err := r.registry.store.UpsertPasswordCredential(ctx, p.PrincipalID, p.SecretHash, now); err != nil {
		return CredentialReply{}, err
	}
	return CredentialReply{PrincipalID: p.PrincipalID, Kind: "password", Status: regspec.CredentialActive, RotatedAt: now}, nil
}

func (r *Registrar) registerDecl(ctx context.Context, owner string, p DeclRegister) (regspec.DeclRow, error) {
	p.ID = strings.TrimSpace(p.ID)
	p.Name = strings.TrimSpace(p.Name)
	p.Class = strings.TrimSpace(p.Class)
	if p.ID == "" || p.Name == "" || p.Class == "" {
		return regspec.DeclRow{}, invalid("id, name and class required")
	}
	if p.Class == SpaceToolClass {
		return regspec.DeclRow{}, reserved("space-tool class is reserved")
	}
	if p.Visibility == "" {
		p.Visibility = "private"
	}
	if p.Visibility != "private" && p.Visibility != "public" {
		return regspec.DeclRow{}, invalid("invalid visibility")
	}
	if r.classes == nil {
		return regspec.DeclRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
	}
	if err := r.classes.ValidateConfig(p.Class, p.Config); err != nil {
		return regspec.DeclRow{}, invalid(err.Error())
	}
	now := r.now().UnixMilli()
	existing, found, err := r.registry.store.GetDecl(ctx, p.ID)
	if err == nil && found {
		if existing.Status == regspec.DeclPresent && existing.Name == p.Name && existing.Owner == owner && existing.DefaultClass == p.Class && existing.Visibility == p.Visibility && jsonEqual(existing.Config, p.Config) {
			return existing, nil
		}
		return regspec.DeclRow{}, conflict("declaration id already exists")
	}
	if err != nil {
		return regspec.DeclRow{}, err
	}
	row := regspec.DeclRow{ID: p.ID, Name: p.Name, Owner: owner, DefaultClass: p.Class, Config: cloneJSON(p.Config), Status: regspec.DeclPresent, Visibility: p.Visibility, CreatedAt: now, UpdatedAt: now}
	if err := r.registry.store.InsertDecl(ctx, row); err != nil {
		return regspec.DeclRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{AllChannels: true})
	}
	return row, nil
}

func (r *Registrar) editDecl(ctx context.Context, caller string, p DeclEdit) (regspec.DeclRow, error) {
	if p.ID == "" {
		return regspec.DeclRow{}, invalid("id required")
	}
	row, found, err := r.registry.store.GetDecl(ctx, p.ID)
	if !found && err == nil {
		return regspec.DeclRow{}, notFound("declaration")
	}
	if err != nil {
		return regspec.DeclRow{}, err
	}
	if row.Status != regspec.DeclPresent {
		return regspec.DeclRow{}, notFound("declaration")
	}
	if row.DefaultClass == SpaceToolClass || row.ID == SpaceToolDeclID {
		return regspec.DeclRow{}, reserved("space-tool declaration is reserved")
	}
	if row.Owner != caller && caller != protocol.RootPrincipalID {
		return regspec.DeclRow{}, denied("declaration owner required")
	}
	before := row
	before.Config = cloneJSON(row.Config)
	if p.Name != nil {
		row.Name = strings.TrimSpace(*p.Name)
	}
	if p.Class != nil {
		next := strings.TrimSpace(*p.Class)
		if next == SpaceToolClass {
			return regspec.DeclRow{}, reserved("space-tool class is reserved")
		}
		if r.classes == nil {
			return regspec.DeclRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
		}
		oldKind, oldOK := r.classes.LookupClassKind(row.DefaultClass)
		newKind, newOK := r.classes.LookupClassKind(next)
		if !oldOK || !newOK || oldKind != newKind {
			return regspec.DeclRow{}, invalid("class transition changes actor kind")
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
		return regspec.DeclRow{}, invalid("invalid declaration")
	}
	if r.classes == nil {
		return regspec.DeclRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
	}
	if err := r.classes.ValidateConfig(row.DefaultClass, row.Config); err != nil {
		return regspec.DeclRow{}, invalid(err.Error())
	}
	if row.Name == before.Name && row.DefaultClass == before.DefaultClass && row.Visibility == before.Visibility && jsonEqual(row.Config, before.Config) {
		return before, nil
	}
	row.UpdatedAt = r.now().UnixMilli()
	if err := r.registry.store.UpdateDecl(ctx, row); err != nil {
		return regspec.DeclRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{AllChannels: true})
	}
	return row, nil
}

func (r *Registrar) revokeDecl(ctx context.Context, caller string, p DeclRevoke) (regspec.DeclRow, error) {
	if p.ID == "" {
		return regspec.DeclRow{}, invalid("id required")
	}
	row, found, err := r.registry.store.GetDecl(ctx, p.ID)
	if !found && err == nil {
		return regspec.DeclRow{}, notFound("declaration")
	}
	if err != nil {
		return regspec.DeclRow{}, err
	}
	if row.DefaultClass == SpaceToolClass || row.ID == SpaceToolDeclID {
		return regspec.DeclRow{}, reserved("space-tool declaration is reserved")
	}
	if row.Owner != caller && caller != protocol.RootPrincipalID {
		return regspec.DeclRow{}, denied("declaration owner required")
	}
	if row.Status == regspec.DeclRevoked {
		return row, nil
	}
	row.Status = regspec.DeclRevoked
	row.UpdatedAt = r.now().UnixMilli()
	if err := r.registry.store.RevokeDecl(ctx, row.ID, row.UpdatedAt); err != nil {
		return regspec.DeclRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{AllChannels: true})
	}
	return row, nil
}

func (r *Registrar) setOverlay(ctx context.Context, _ string, source channel.ID, p OverlaySet) (regspec.OverlayRow, error) {
	if p.DeclID == "" || p.ChannelID == "" {
		return regspec.OverlayRow{}, invalid("decl_id and channel_id required")
	}
	if p.ChannelID != source {
		return regspec.OverlayRow{}, denied("overlay target must equal source channel")
	}
	decl, ok, err := r.registry.GetDecl(ctx, p.DeclID)
	if err != nil {
		return regspec.OverlayRow{}, err
	}
	if !ok || decl.Status != regspec.DeclPresent {
		return regspec.OverlayRow{}, notFound("declaration")
	}
	if r.classes == nil {
		return regspec.OverlayRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
	}
	if err := r.classes.ValidateConfig(decl.DefaultClass, p.Config); err != nil {
		return regspec.OverlayRow{}, invalid(err.Error())
	}
	existing, found, err := r.registry.store.GetOverlay(ctx, p.DeclID, p.ChannelID)
	if err == nil && found {
		if jsonEqual(existing.Config, p.Config) {
			return existing, nil
		}
	} else if err != nil {
		return regspec.OverlayRow{}, err
	}
	now := r.now().UnixMilli()
	row := regspec.OverlayRow{DeclID: p.DeclID, ChannelID: p.ChannelID, Config: cloneJSON(p.Config), UpdatedAt: now}
	if err := r.registry.store.UpsertOverlay(ctx, row); err != nil {
		return regspec.OverlayRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: p.ChannelID})
	}
	return row, nil
}

func (r *Registrar) clearOverlay(ctx context.Context, source channel.ID, p OverlayClear) (Confirmation, error) {
	if p.DeclID == "" || p.ChannelID == "" {
		return Confirmation{}, invalid("decl_id and channel_id required")
	}
	if p.ChannelID != source {
		return Confirmation{}, denied("overlay target must equal source channel")
	}
	if err := r.registry.store.DeleteOverlay(ctx, p.DeclID, p.ChannelID); err != nil {
		return Confirmation{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: p.ChannelID})
	}
	return Confirmation{Word: WordOverlayClear, TargetID: p.DeclID, Status: "cleared"}, nil
}

func (r *Registrar) mintDevice(ctx context.Context, owner, name string) (regspec.DeviceRow, error) {
	if err := ValidateName(name); err != nil {
		return regspec.DeviceRow{}, invalid(err.Error())
	}
	if _, found, err := r.registry.GetDeviceByName(ctx, name); err != nil {
		return regspec.DeviceRow{}, err
	} else if found {
		return regspec.DeviceRow{}, conflict("device name already exists")
	}
	row := regspec.DeviceRow{ID: uuid.NewString(), OwnerPrincipal: owner, Name: name, Key: uuid.NewString(), Status: regspec.DevicePresent, CreatedAt: r.now().UnixMilli()}
	err := r.registry.store.InsertDevice(ctx, row)
	return row, err
}

func (r *Registrar) claimDevice(ctx context.Context, owner string, p DeviceClaim) (regspec.DeviceRow, error) {
	if err := ValidateName(p.Name); err != nil {
		return regspec.DeviceRow{}, invalid(err.Error())
	}
	if p.DeviceID != "" {
		row, ok, err := r.registry.GetDevice(ctx, p.DeviceID)
		if err != nil {
			return regspec.DeviceRow{}, err
		}
		if ok {
			if row.Name != p.Name {
				return regspec.DeviceRow{}, conflict("device name does not match claimed device")
			}
			if row.Status == regspec.DevicePresent && row.OwnerPrincipal == owner {
				return row, nil
			}
		}
	}
	return r.mintDevice(ctx, owner, p.Name)
}

func (r *Registrar) retireDevice(ctx context.Context, owner string, p DeviceRetire) (regspec.DeviceRow, error) {
	if p.DeviceID == "" {
		return regspec.DeviceRow{}, invalid("device_id required")
	}
	if p.DeviceID == protocol.LocalDeviceID {
		return regspec.DeviceRow{}, reserved("local device cannot be retired")
	}
	row, ok, err := r.registry.GetDevice(ctx, p.DeviceID)
	if err != nil {
		return regspec.DeviceRow{}, err
	}
	if !ok {
		return regspec.DeviceRow{}, notFound("device")
	}
	if row.OwnerPrincipal != owner && owner != protocol.RootPrincipalID {
		return regspec.DeviceRow{}, denied("device owner required")
	}
	if row.Status == regspec.DeviceRetired {
		return row, nil
	}
	if err := r.registry.store.UpdateDeviceStatus(ctx, p.DeviceID, regspec.DeviceRetired); err != nil {
		return regspec.DeviceRow{}, err
	}
	row.Status = regspec.DeviceRetired
	if r.registry.onCommit != nil {
		// Effective bindings join device status, so this one row can change the
		// placement view of multiple channels.
		r.registry.onCommit(Change{AllChannels: true})
	}
	return row, nil
}

func (r *Registrar) attachDevice(ctx context.Context, owner string, source channel.ID, p DeviceBinding) (regspec.BindingRow, error) {
	if err := r.authorizeBinding(ctx, owner, source, p); err != nil {
		return regspec.BindingRow{}, err
	}
	now := r.now().UnixMilli()
	if err := r.registry.store.InsertBindingIfAbsent(ctx, regspec.BindingRow{ChannelID: p.ChannelID, DeviceID: p.DeviceID, AttachedAt: now}); err != nil {
		return regspec.BindingRow{}, err
	}
	row, found, err := r.registry.store.Binding(ctx, p.ChannelID, p.DeviceID)
	if err != nil {
		return regspec.BindingRow{}, err
	}
	if !found {
		return regspec.BindingRow{}, errors.New("lagoon: attached binding missing")
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: p.ChannelID})
	}
	return row, nil
}

func (r *Registrar) detachDevice(ctx context.Context, owner string, source channel.ID, p DeviceBinding) (Confirmation, error) {
	if err := r.authorizeBinding(ctx, owner, source, p); err != nil {
		return Confirmation{}, err
	}
	if p.DeviceID == protocol.LocalDeviceID {
		return Confirmation{}, reserved("local device cannot be detached")
	}
	if err := r.registry.store.DeleteBinding(ctx, p.ChannelID, p.DeviceID); err != nil {
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
	if !ok || ch.Status != regspec.ChannelPresent {
		return notFound("channel")
	}
	device, ok, err := r.registry.GetDevice(ctx, p.DeviceID)
	if err != nil {
		return err
	}
	if !ok || device.Status != regspec.DevicePresent {
		return notFound("device")
	}
	if p.DeviceID != protocol.LocalDeviceID && device.OwnerPrincipal != owner {
		return denied("device owner required")
	}
	return nil
}

func (r *Registrar) readChannels(ctx context.Context, p ChannelList) ([]regspec.ChannelRow, error) {
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

func (r *Registrar) readDevices(ctx context.Context) ([]regspec.DeviceRow, error) {
	return r.registry.store.ListDevices(ctx)
}

func (r *Registrar) readPrincipal(ctx context.Context, id string) (regspec.PrincipalRow, error) {
	row, found, err := r.registry.store.GetPresentPrincipal(ctx, id)
	if !found && err == nil {
		return regspec.PrincipalRow{}, notFound("principal")
	}
	return row, err
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
	caller C0Caller
}

func NewSubmitter(caller C0Caller) Submitter {
	return &submitter{caller: caller}
}

func (s *submitter) Submit(ctx context.Context, in SubmitIn) (Reply, error) {
	if s.caller == nil {
		return Reply{}, errors.New("lagoon: submitter is not wired")
	}
	if in.Source == "" || in.Sender == "" || in.RequestID == "" {
		return Reply{}, invalid("source frame required")
	}
	payload, err := json.Marshal(in.Payload)
	if err != nil {
		return Reply{}, invalid("payload is not encodable")
	}
	raw, err := s.caller.CallRegistrar(ctx, in.Word, message.Envelope{
		ID: message.ID(in.RequestID), ChannelID: in.Source, Sender: message.Sender{ID: in.Sender},
		Kind: message.KindRequest, Type: string(in.Word), Payload: payload,
	})
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

func (s *submitter) SubmitApplication(ctx context.Context, word Word, payload any) (Reply, error) {
	if word != WordPrincipalRegister {
		return Reply{}, denied("application entrance only accepts principal.register")
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return Reply{}, invalid("payload is not encodable")
	}
	ref := SourceRef{ChannelID: protocol.C0ChannelID, RequestID: uuid.NewString()}
	raw, err := s.caller.CallRegistrar(ctx, word, message.Envelope{
		ID: message.ID(ref.RequestID), ChannelID: ref.ChannelID, Kind: message.KindRequest,
		Type: string(word), Payload: rawPayload,
	})
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
