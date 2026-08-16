package lagoon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon/internal/store"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/platform/peerproto"
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

// ReconcileSystem carves the registry's fixed system rows on first start and
// leaves them alone afterwards. An existing c0 row is never rewritten: its
// profile (description, serving, endpoints) belongs to its owner and survives
// restarts. If the stored genesis cannot be read the start fails and the
// operator wipes the installation — there is no automatic upgrade or
// recalibration. It deliberately never touches credentials or user/device
// identities.
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
	if c0Exists {
		// c0 exists: its row is the owner's to change and is left untouched.
		// Only refuse to start on a genesis this build cannot read.
		row, found, err := r.registry.GetChannelDesired(ctx, protocol.C0ChannelID)
		if err != nil {
			return err
		}
		var stored GenesisSpec
		if !found || json.Unmarshal(row.Spec, &stored) != nil {
			return errors.New("lagoon: c0 registry genesis invalid — wipe the installation and start again")
		}
	} else {
		genesis, ok := r.facts.(SystemGenesisResolver)
		if !ok {
			return errors.New("lagoon: c0 physical genesis unavailable")
		}
		spec, found, err := genesis.SystemGenesis(ctx)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("lagoon: c0 physical genesis missing")
		}
		description := "Atoll core registry and administration channel."
		serving := 1
		endpoints := make(map[string]regspec.EndpointSpec, len(WriteWords)+len(ReadWords))
		for _, word := range append(append([]Word{}, WriteWords[:]...), ReadWords[:]...) {
			endpoints[string(word)] = regspec.EndpointSpec{Description: "Registrar endpoint " + string(word) + ".", Receiver: RegistrarSeatDeclID}
		}
		spec.Profile = regspec.ChannelProfile{Description: &description, Serving: &serving, Endpoints: endpoints}
		raw, err := json.Marshal(spec)
		if err != nil {
			return err
		}
		if err := r.registry.store.UpsertSystemChannel(ctx, regspec.ChannelRow{
			ID: protocol.C0ChannelID, Name: string(protocol.C0ChannelID), Type: "group",
			Status: regspec.ChannelPresent, OwnerPrincipal: protocol.RootPrincipalID, Description: description, Serving: serving, Spec: raw, CreatedAt: spec.CreatedAt,
		}); err != nil {
			return err
		}
	}
	decls := []regspec.DeclRow{
		{ID: SvcActorDeclID, Name: "Service Actor", Owner: protocol.RootPrincipalID, DefaultClass: SvcActorClass, Config: json.RawMessage(`{}`), Status: regspec.DeclPresent, Visibility: "private", CreatedAt: now, UpdatedAt: now},
		{ID: RegistrarSeatDeclID, Name: "Registrar Seat", Owner: protocol.RootPrincipalID, DefaultClass: RegistrarClass, Config: json.RawMessage(`{}`), Status: regspec.DeclPresent, Visibility: "private", CreatedAt: now, UpdatedAt: now},
		{ID: CoreActorDeclID, Name: "Core Actor", Owner: protocol.RootPrincipalID, DefaultClass: PeerActorClass, Config: targetConfig(protocol.C0ChannelID), Status: regspec.DeclPresent, Visibility: "private", CreatedAt: now, UpdatedAt: now},
	}
	for _, decl := range decls {
		if err := r.registry.store.UpsertSystemDecl(ctx, decl); err != nil {
			return err
		}
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
	if r.isServiceActor(msg.Ctx(), msg.ChannelID, msg.Sender.ID) {
		var wrapped struct {
			Origin peerproto.Origin `json:"origin"`
			Args   json.RawMessage  `json:"args"`
		}
		if err := decodeClosed(msg.Payload, &wrapped); err != nil || wrapped.Origin.Channel == "" || wrapped.Origin.Actor == "" || wrapped.Origin.RequestID == "" {
			_, _ = sys.Fail(msg, string(CodeInvalidArgs), "invalid svcactor request")
			return
		}
		source = SourceRef{ChannelID: wrapped.Origin.Channel, RequestID: string(wrapped.Origin.RequestID)}
		payload = append(json.RawMessage(nil), wrapped.Args...)
		principal = r.resolvePrincipal(msg.Ctx(), wrapped.Origin.Channel, wrapped.Origin.Actor, sys, msg)
		if principal == "" {
			return
		}
	} else {
		principal = r.resolvePrincipal(msg.Ctx(), msg.ChannelID, msg.Sender.ID, sys, msg)
		if principal == "" {
			return
		}
	}
	if word == WordPrincipalRegister {
		if principal != protocol.GuestPrincipalID {
			_, _ = sys.Fail(msg, string(CodePermissionDenied), "registration requires guest")
			return
		}
	} else if principal == protocol.GuestPrincipalID {
		_, _ = sys.Fail(msg, string(CodePermissionDenied), "guest may only register a principal")
		return
	}
	value, err := r.execute(sys, msg.Ctx(), principal, source.ChannelID, word, payload)
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

func (r *Registrar) isServiceActor(ctx context.Context, ch channel.ID, id actor.ActorID) bool {
	if r.facts == nil || id == "" {
		return false
	}
	facts, found, err := r.facts.ActorFacts(ctx, ch, id)
	return err == nil && found && facts.Active && facts.SourceDeclID == SvcActorDeclID
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
	principal, err := channelspec.ResolveActorPrincipal(ctx, facts, func(ctx context.Context, declID string) (string, bool, error) {
		decl, found, err := r.registry.GetDecl(ctx, declID)
		return decl.Owner, found, err
	})
	if err != nil {
		_, _ = sys.Fail(msg, string(CodeResultUnknown), err.Error())
		return ""
	}
	if principal != "" {
		return principal
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

func decodeClosed(raw json.RawMessage, out any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func (r *Registrar) execute(sys actorbase.Sys, ctx context.Context, principal string, source channel.ID, word Word, raw json.RawMessage) (any, error) {
	switch word {
	case WordChannelCreate:
		var p ChannelCreate
		if err := decodeClosed(raw, &p); err != nil {
			return nil, invalid("invalid JSON payload")
		}
		return r.createChannel(sys, ctx, principal, source, p)
	case WordChannelTemplateRegister:
		var p ChannelTemplateRegister
		if err := decodeClosed(raw, &p); err != nil {
			return nil, invalid("invalid JSON payload")
		}
		return r.registerChannelTemplate(ctx, principal, p)
	case WordChannelTemplateEdit:
		var p ChannelTemplateEdit
		if err := decodeClosed(raw, &p); err != nil {
			return nil, invalid("invalid JSON payload")
		}
		return r.editChannelTemplate(ctx, principal, p)
	case WordChannelTemplateRevoke:
		var p ChannelTemplateRevoke
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.revokeChannelTemplate(ctx, principal, p)
	case WordChannelProfileSet:
		var p ChannelProfileSet
		if err := decodeClosed(raw, &p); err != nil {
			return nil, invalid("invalid JSON payload")
		}
		return r.setChannelProfile(ctx, source, p)
	case WordChannelRetire:
		var p ChannelRetire
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.retireChannel(sys, ctx, principal, source, p)
	case WordPrincipalRegister:
		var p PrincipalRegister
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.registerPrincipal(sys, ctx, p)
	case WordPrincipalRetire:
		var p PrincipalRetire
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.retirePrincipal(ctx, principal, source, p)
	case WordCredentialSet:
		var p CredentialSet
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.setCredential(ctx, principal, source, p)
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
		return r.editDecl(ctx, principal, source, p)
	case WordDeclRevoke:
		var p DeclRevoke
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.revokeDecl(ctx, principal, source, p)
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
		return r.retireDevice(ctx, principal, source, p)
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
		return r.channelView(ctx, row)
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
	case WordChannelTemplateList:
		return r.registry.ListChannelTemplates(ctx)
	case WordChannelTemplateGet:
		var p ChannelTemplateGet
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		row, ok, err := r.registry.GetChannelTemplate(ctx, p.ID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, notFound("channel template")
		}
		return row, nil
	case WordChannelDescribe:
		var p ChannelDescribe
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		if p.ChannelID == "" && p.Channel == "" {
			return nil, invalid("channel or channel_id required")
		}
		row, ok, err := r.registry.ResolveChannel(ctx, p.ChannelID, p.Channel)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, notFound("channel")
		}
		return r.registry.Describe(ctx, row.ID, source)
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

type postActionResults struct {
	Core    any                        `json:"core"`
	Parent  any                        `json:"parent"`
	Members []memberIntroductionResult `json:"members"`
}
type memberIntroductionResult struct {
	DeclID string `json:"decl_id"`
	Result any    `json:"result"`
}
type ChannelCreateReply struct {
	regspec.ChannelRow
	Introduced postActionResults `json:"introduced"`
}

func (r *Registrar) createChannel(sys actorbase.Sys, ctx context.Context, owner string, source channel.ID, p ChannelCreate) (ChannelCreateReply, error) {
	var row regspec.ChannelRow
	var created bool
	recipeConfigs := map[string]json.RawMessage{}
	err := r.registry.store.InTx(ctx, func(tx *store.Tx) error {
		var err error
		row, created, err = r.provisionChannel(ctx, tx, owner, source, p.Name, p.Template, p.Overrides, recipeConfigs)
		return err
	})
	if err != nil {
		return ChannelCreateReply{}, err
	}
	if !created {
		return ChannelCreateReply{ChannelRow: row, Introduced: postActionResults{Core: "n/a", Parent: "n/a", Members: []memberIntroductionResult{}}}, nil
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: row.ID})
	}
	results := r.introduceChannel(sys, ctx, row, recipeConfigs)
	return ChannelCreateReply{ChannelRow: row, Introduced: results}, nil
}

func (r *Registrar) provisionChannel(ctx context.Context, tx *store.Tx, owner string, parent channel.ID, name, template string, overrides *regspec.TemplateBody, recipeConfigs map[string]json.RawMessage) (regspec.ChannelRow, bool, error) {
	if name == "" {
		return regspec.ChannelRow{}, false, invalid("name required")
	}
	if err := ValidateName(name); err != nil {
		return regspec.ChannelRow{}, false, invalid(err.Error())
	}
	if parent == "" {
		return regspec.ChannelRow{}, false, invalid("parent required")
	}
	rows, err := tx.ListChannels(ctx)
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	rows, err = qualifyChannelRows(rows)
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	var parentRow regspec.ChannelRow
	var parentFound bool
	for _, candidate := range rows {
		if candidate.ID == parent {
			parentRow, parentFound = candidate, true
			break
		}
	}
	if !parentFound || parentRow.Status != regspec.ChannelPresent {
		return regspec.ChannelRow{}, false, notFound("parent channel")
	}
	qualified, err := JoinName(parentRow.QualifiedName, name)
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	matches, err := tx.FindChannels(ctx, parent, name)
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
	body, err := r.mergedTemplate(ctx, tx, template, overrides)
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	profile := regspec.ChannelProfile{Serving: intPtr(1), Endpoints: map[string]regspec.EndpointSpec{}}
	if body.Profile != nil {
		profile = *body.Profile
		if profile.Serving == nil {
			profile.Serving = intPtr(1)
		}
		if profile.Endpoints == nil {
			profile.Endpoints = map[string]regspec.EndpointSpec{}
		}
	}
	if *profile.Serving != 0 && *profile.Serving != 1 {
		return regspec.ChannelRow{}, false, invalid("serving must be 0 or 1")
	}
	if err := r.validateEndpoints(ctx, tx, profile.Endpoints); err != nil {
		return regspec.ChannelRow{}, false, err
	}
	declarations := make([]GenesisDeclaration, 0, len(body.Declarations)+2)
	svc, err := r.renderSystem(SvcActorClass, json.RawMessage(`{}`))
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	core, err := r.renderSystem(PeerActorClass, targetConfig(protocol.C0ChannelID))
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	declarations = append(declarations, GenesisDeclaration{DeclID: SvcActorDeclID, Kind: actor.KindTool, Rendered: svc}, GenesisDeclaration{DeclID: CoreActorDeclID, Kind: actor.KindTool, Rendered: core})
	for _, item := range body.Declarations {
		decl, ok, err := tx.GetDecl(ctx, item.DeclID)
		if err != nil {
			return regspec.ChannelRow{}, false, err
		}
		if !ok || decl.Status != regspec.DeclPresent {
			return regspec.ChannelRow{}, false, notFound("declaration")
		}
		if decl.Visibility != "public" && decl.Owner != owner {
			return regspec.ChannelRow{}, false, denied("declaration is private")
		}
		config := decl.Config
		if item.Config != nil {
			config = item.Config
			if recipeConfigs != nil {
				recipeConfigs[item.DeclID] = cloneJSON(item.Config)
			}
		}
		if r.classes == nil {
			return regspec.ChannelRow{}, false, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
		}
		if err := r.classes.ValidateConfig(decl.DefaultClass, config); err != nil {
			return regspec.ChannelRow{}, false, invalid(err.Error())
		}
		kind, ok := r.classes.LookupClassKind(decl.DefaultClass)
		if !ok {
			return regspec.ChannelRow{}, false, invalid("unknown class")
		}
		placement, ok := r.classes.LookupClassPlacement(decl.DefaultClass)
		if !ok {
			return regspec.ChannelRow{}, false, invalid("unknown class placement")
		}
		rendered := channelspec.RenderedSnapshot{Class: decl.DefaultClass, Config: cloneJSON(config), Placement: channel.Placement{Kind: placement}}
		if placement == channel.PlacementDaemon {
			rendered.Placement.DesiredHost = protocol.LocalDeviceID
		}
		rendered, err = rendered.Seal()
		if err != nil {
			return regspec.ChannelRow{}, false, err
		}
		declarations = append(declarations, GenesisDeclaration{DeclID: item.DeclID, Kind: kind, Rendered: rendered})
	}
	spec := GenesisSpec{ChannelID: id, Type: "group", OwnerPrincipal: owner, CreatedAt: now, ParentID: parent, InitiatorPrincipal: owner, Declarations: declarations, Profile: profile}
	raw, err := json.Marshal(spec)
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	description := ""
	if profile.Description != nil {
		description = *profile.Description
	}
	row := regspec.ChannelRow{ID: id, ParentID: parent, Name: name, QualifiedName: qualified, Type: "group", Status: regspec.ChannelPresent, OwnerPrincipal: owner, Description: description, Serving: *profile.Serving, Spec: raw, CreatedAt: now}
	if err := tx.InsertChannel(ctx, row); err != nil {
		return regspec.ChannelRow{}, false, err
	}
	if err := tx.InsertBinding(ctx, regspec.BindingRow{ChannelID: row.ID, DeviceID: protocol.LocalDeviceID, AttachedAt: now}); err != nil {
		return regspec.ChannelRow{}, false, err
	}
	for endpointName, endpoint := range profile.Endpoints {
		meta, _ := json.Marshal(map[string]any{"examples": endpoint.Examples, "schema": endpoint.Schema})
		if err := tx.InsertEndpoint(ctx, regspec.EndpointRow{ChannelID: id, Name: endpointName, Description: endpoint.Description, Receiver: endpoint.Receiver, Meta: meta, UpdatedAt: now}); err != nil {
			return regspec.ChannelRow{}, false, err
		}
	}
	peerID := peerDeclID(id)
	if err := tx.InsertDecl(ctx, regspec.DeclRow{ID: peerID, Name: qualified, Owner: owner, DefaultClass: PeerActorClass, Config: targetConfig(id), Status: regspec.DeclPresent, Visibility: "public", CreatedAt: now, UpdatedAt: now}); err != nil {
		return regspec.ChannelRow{}, false, err
	}
	return row, true, nil
}

func (r *Registrar) mergedTemplate(ctx context.Context, tx *store.Tx, id string, overrides *regspec.TemplateBody) (regspec.TemplateBody, error) {
	var body regspec.TemplateBody
	if id != "" {
		row, ok, err := tx.GetChannelTemplate(ctx, id)
		if err != nil {
			return body, err
		}
		if !ok || row.Status != regspec.DeclPresent {
			return body, notFound("channel template")
		}
		if err := json.Unmarshal(row.Body, &body); err != nil {
			return body, err
		}
		if err := r.validateTemplateDeclarations(ctx, tx, body.Declarations); err != nil {
			return body, err
		}
	}
	if overrides == nil {
		return body, nil
	}
	if err := r.validateTemplateDeclarations(ctx, tx, overrides.Declarations); err != nil {
		return body, err
	}
	index := map[string]int{}
	for i, d := range body.Declarations {
		index[d.DeclID] = i
	}
	for _, d := range overrides.Declarations {
		if i, ok := index[d.DeclID]; ok {
			body.Declarations[i] = d
		} else {
			index[d.DeclID] = len(body.Declarations)
			body.Declarations = append(body.Declarations, d)
		}
	}
	if overrides.Profile != nil {
		if body.Profile == nil {
			p := *overrides.Profile
			body.Profile = &p
		} else {
			if overrides.Profile.Description != nil {
				body.Profile.Description = overrides.Profile.Description
			}
			if overrides.Profile.Serving != nil {
				body.Profile.Serving = overrides.Profile.Serving
			}
			if overrides.Profile.Endpoints != nil {
				body.Profile.Endpoints = overrides.Profile.Endpoints
			}
		}
	}
	return body, nil
}
func (r *Registrar) renderSystem(class string, config json.RawMessage) (channelspec.RenderedSnapshot, error) {
	return (channelspec.RenderedSnapshot{Class: class, Config: config, Placement: channel.Placement{Kind: channel.PlacementServer}}).Seal()
}
func intPtr(v int) *int { return &v }
func targetConfig(id channel.ID) json.RawMessage {
	raw, _ := json.Marshal(map[string]channel.ID{"channel": id})
	return raw
}
func peerDeclID(id channel.ID) string { return PeerActorDeclPrefix + string(id) }

func (r *Registrar) validateEndpoints(ctx context.Context, tx *store.Tx, endpoints map[string]regspec.EndpointSpec) error {
	for name, endpoint := range endpoints {
		if strings.TrimSpace(name) == "" {
			return invalid("endpoint name required")
		}
		switch name {
		case introspect.QueryDescribe, "channel.introduce_actor", "channel.remove_actor", "channel.restart_actor":
			return invalid("reserved endpoint name")
		}
		if endpoint.Receiver == SvcActorDeclID || endpoint.Receiver == CoreActorDeclID {
			return invalid("system actor cannot receive endpoints")
		}
		decl, ok, err := tx.GetDecl(ctx, endpoint.Receiver)
		if err != nil {
			return err
		}
		if !ok || decl.Status != regspec.DeclPresent {
			return invalid("endpoint receiver declaration not found")
		}
	}
	return nil
}

func (r *Registrar) introduceChannel(sys actorbase.Sys, ctx context.Context, row regspec.ChannelRow, recipeConfigs map[string]json.RawMessage) postActionResults {
	core := r.callActor(sys, ctx, actor.SystemActorID, "channel.introduce_actor", map[string]any{"kind": "tool", "decl_id": peerDeclID(row.ID)})
	parent := any("n/a")
	if row.ParentID != protocol.C0ChannelID {
		if instances, ok := r.facts.(ChannelInstancesResolver); ok {
			ids, err := instances.DeclaredInstances(ctx, protocol.C0ChannelID, peerDeclID(row.ParentID))
			if err != nil {
				parent = errorValue(err)
			} else if len(ids) != 1 {
				parent = map[string]string{"error_code": "receiver_unavailable", "detail": "parent peeractor unavailable"}
			} else {
				parent = r.callActor(sys, ctx, ids[0], "channel.introduce_actor", map[string]any{"kind": "tool", "decl_id": peerDeclID(row.ID)})
			}
		} else {
			parent = map[string]string{"error_code": "receiver_unavailable", "detail": "channel instance resolver unavailable"}
		}
	}
	results := postActionResults{Core: core, Parent: parent, Members: []memberIntroductionResult{}}
	var spec GenesisSpec
	if err := json.Unmarshal(row.Spec, &spec); err != nil {
		return results
	}
	userDeclarations := make([]GenesisDeclaration, 0, len(spec.Declarations))
	for _, declaration := range spec.Declarations {
		if declaration.DeclID == SvcActorDeclID || declaration.DeclID == CoreActorDeclID || declaration.DeclID == RegistrarSeatDeclID {
			continue
		}
		userDeclarations = append(userDeclarations, declaration)
	}
	if len(userDeclarations) == 0 {
		return results
	}
	service, ok := r.facts.(ChannelServiceResolver)
	if !ok {
		failure := map[string]string{"error_code": "receiver_unavailable", "detail": "channel service resolver unavailable"}
		for _, declaration := range userDeclarations {
			results.Members = append(results.Members, memberIntroductionResult{DeclID: declaration.DeclID, Result: failure})
		}
		return results
	}
	if err := service.WaitChannelService(ctx, row.ID); err != nil {
		failure := errorValue(err)
		for _, declaration := range userDeclarations {
			results.Members = append(results.Members, memberIntroductionResult{DeclID: declaration.DeclID, Result: failure})
		}
		return results
	}
	instances, ok := r.facts.(ChannelInstancesResolver)
	if !ok {
		failure := map[string]string{"error_code": "receiver_unavailable", "detail": "channel instance resolver unavailable"}
		for _, declaration := range userDeclarations {
			results.Members = append(results.Members, memberIntroductionResult{DeclID: declaration.DeclID, Result: failure})
		}
		return results
	}
	peers, err := instances.DeclaredInstances(ctx, protocol.C0ChannelID, peerDeclID(row.ID))
	if err != nil || len(peers) != 1 {
		failure := any(map[string]string{"error_code": "receiver_unavailable", "detail": "target peeractor unavailable"})
		if err != nil {
			failure = errorValue(err)
		}
		for _, declaration := range userDeclarations {
			results.Members = append(results.Members, memberIntroductionResult{DeclID: declaration.DeclID, Result: failure})
		}
		return results
	}
	for _, declaration := range userDeclarations {
		if err := ctx.Err(); err != nil {
			results.Members = append(results.Members, memberIntroductionResult{DeclID: declaration.DeclID, Result: errorValue(err)})
			continue
		}
		if config, ok := recipeConfigs[declaration.DeclID]; ok {
			if _, err := r.writeOverlay(ctx, OverlaySet{DeclID: declaration.DeclID, ChannelID: row.ID, Config: config}); err != nil {
				results.Members = append(results.Members, memberIntroductionResult{DeclID: declaration.DeclID, Result: errorValue(err)})
				continue
			}
		}
		result := r.callActor(sys, ctx, peers[0], "channel.introduce_actor", map[string]any{"kind": declaration.Kind, "decl_id": declaration.DeclID})
		results.Members = append(results.Members, memberIntroductionResult{DeclID: declaration.DeclID, Result: result})
	}
	return results
}
func (r *Registrar) callActor(sys actorbase.Sys, ctx context.Context, target actor.ActorID, word string, payload any) any {
	pending, err := sys.Call(target, word, payload)
	if err != nil {
		return errorValue(err)
	}
	terminal, err := pending.Wait(ctx, 0)
	if err != nil {
		_ = pending.Cancel()
		return errorValue(err)
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(terminal.Payload, &fields) != nil {
		return map[string]string{"error_code": "invalid_terminal", "detail": "invalid terminal"}
	}
	var status, code, detail string
	_ = json.Unmarshal(fields["status"], &status)
	if status == message.StatusCompleted {
		return "ok"
	}
	_ = json.Unmarshal(fields["error_code"], &code)
	_ = json.Unmarshal(fields["detail"], &detail)
	return map[string]string{"error_code": code, "detail": detail}
}
func errorValue(err error) any {
	return map[string]string{"error_code": "unavailable", "detail": err.Error()}
}

type ChannelRetireReply struct {
	regspec.ChannelRow
	Removed postActionResults `json:"removed"`
}

func (r *Registrar) retireChannel(sys actorbase.Sys, ctx context.Context, principal string, source channel.ID, p ChannelRetire) (ChannelRetireReply, error) {
	if p.ChannelID == "" {
		return ChannelRetireReply{}, invalid("channel_id required")
	}
	if p.ChannelID == protocol.C0ChannelID {
		return ChannelRetireReply{}, reserved("c0 cannot be retired")
	}
	row, found, err := r.registry.GetChannelDesired(ctx, p.ChannelID)
	if !found && err == nil {
		return ChannelRetireReply{}, notFound("channel")
	}
	if err != nil {
		return ChannelRetireReply{}, err
	}
	if row.OwnerPrincipal != principal && source != protocol.C0ChannelID {
		return ChannelRetireReply{}, denied("channel owner or core required")
	}
	hasPresentChild, err := r.registry.store.PresentChildExists(ctx, p.ChannelID)
	if err != nil {
		return ChannelRetireReply{}, err
	}
	if hasPresentChild {
		return ChannelRetireReply{}, conflict("channel has active child channels")
	}
	if err := r.registry.store.RetireChannelAndPeer(ctx, p.ChannelID, peerDeclID(p.ChannelID), r.now().UnixMilli()); err != nil {
		return ChannelRetireReply{}, err
	}
	row.Status = regspec.ChannelRetired
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: p.ChannelID, AllPrincipals: true})
	}
	removed := r.removeChannelEdges(sys, ctx, row)
	return ChannelRetireReply{ChannelRow: row, Removed: removed}, nil
}

func (r *Registrar) removeChannelEdges(sys actorbase.Sys, ctx context.Context, row regspec.ChannelRow) postActionResults {
	payload := map[string]any{"decl_id": peerDeclID(row.ID)}
	core := r.callActor(sys, ctx, actor.SystemActorID, "channel.remove_actor", payload)
	parent := any("n/a")
	if row.ParentID != protocol.C0ChannelID {
		if instances, ok := r.facts.(ChannelInstancesResolver); ok {
			ids, err := instances.DeclaredInstances(ctx, protocol.C0ChannelID, peerDeclID(row.ParentID))
			if err != nil {
				parent = errorValue(err)
			} else if len(ids) != 1 {
				parent = map[string]string{"error_code": "receiver_unavailable", "detail": "parent peeractor unavailable"}
			} else {
				parent = r.callActor(sys, ctx, ids[0], "channel.remove_actor", payload)
			}
		}
	}
	return postActionResults{Core: core, Parent: parent}
}

type PrincipalRegisterReply struct {
	regspec.PrincipalRow
	HomeChannelID channel.ID        `json:"home_channel_id"`
	Introduced    postActionResults `json:"introduced"`
}

func (r *Registrar) registerPrincipal(sys actorbase.Sys, ctx context.Context, p PrincipalRegister) (PrincipalRegisterReply, error) {
	p.Email = strings.TrimSpace(p.Email)
	if p.Email == "" || p.SecretHash == "" {
		return PrincipalRegisterReply{}, invalid("email and secret_hash required")
	}
	id := strings.TrimSpace(p.ID)
	if id == protocol.RootPrincipalID {
		return PrincipalRegisterReply{}, reserved("root principal id is reserved")
	}
	if id != "" {
		if err := ValidateName(id); err != nil {
			return PrincipalRegisterReply{}, invalid(err.Error())
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
		return PrincipalRegisterReply{}, err
	}
	if found {
		return PrincipalRegisterReply{}, conflict("email or principal already exists")
	}
	if id == "" {
		id = uuid.NewString()
	}
	row = regspec.PrincipalRow{ID: id, Kind: actor.KindHuman, Email: p.Email, DisplayName: p.DisplayName, Status: regspec.PrincipalPresent, CreatedAt: now}
	var home regspec.ChannelRow
	err = r.registry.store.InTx(ctx, func(tx *store.Tx) error {
		if err := tx.InsertPrincipal(ctx, row); err != nil {
			return err
		}
		if err := tx.InsertPasswordCredential(ctx, id, p.SecretHash, now); err != nil {
			return err
		}
		var created bool
		var err error
		home, created, err = r.provisionChannel(ctx, tx, id, protocol.C0ChannelID, id, "", nil, nil)
		if err != nil {
			return err
		}
		if !created {
			return conflict("principal home already exists")
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			return PrincipalRegisterReply{}, conflict("email or principal already exists")
		}
		return PrincipalRegisterReply{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: home.ID, Principal: id})
	}
	introduced := r.introduceChannel(sys, ctx, home, nil)
	return PrincipalRegisterReply{PrincipalRow: row, HomeChannelID: home.ID, Introduced: introduced}, nil
}

func (r *Registrar) retirePrincipal(ctx context.Context, caller string, source channel.ID, p PrincipalRetire) (regspec.PrincipalRow, error) {
	if p.PrincipalID == "" {
		return regspec.PrincipalRow{}, invalid("principal_id required")
	}
	if p.PrincipalID == protocol.RootPrincipalID {
		return regspec.PrincipalRow{}, reserved("root cannot be retired")
	}
	if caller != p.PrincipalID && source != protocol.C0ChannelID {
		return regspec.PrincipalRow{}, denied("principal or core required")
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

func (r *Registrar) setCredential(ctx context.Context, caller string, source channel.ID, p CredentialSet) (CredentialReply, error) {
	if p.PrincipalID == "" || p.SecretHash == "" {
		return CredentialReply{}, invalid("principal_id and secret_hash required")
	}
	if caller != p.PrincipalID && source != protocol.C0ChannelID {
		return CredentialReply{}, denied("principal or core required")
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
	if systemDecl(p.ID) {
		return regspec.DeclRow{}, reserved("system declaration is reserved")
	}
	if systemClass(p.Class) {
		return regspec.DeclRow{}, reserved("system class is reserved")
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
		if existing.Status == regspec.DeclPresent && existing.Name == p.Name && existing.Description == p.Description && existing.Owner == owner && existing.DefaultClass == p.Class && existing.Visibility == p.Visibility && jsonEqual(existing.Config, p.Config) {
			return existing, nil
		}
		return regspec.DeclRow{}, conflict("declaration id already exists")
	}
	if err != nil {
		return regspec.DeclRow{}, err
	}
	row := regspec.DeclRow{ID: p.ID, Name: p.Name, Description: p.Description, Owner: owner, DefaultClass: p.Class, Config: cloneJSON(p.Config), Status: regspec.DeclPresent, Visibility: p.Visibility, CreatedAt: now, UpdatedAt: now}
	if err := r.registry.store.InsertDecl(ctx, row); err != nil {
		return regspec.DeclRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{AllChannels: true})
	}
	return row, nil
}

func systemClass(class string) bool {
	return class == SvcActorClass || class == PeerActorClass || class == RegistrarClass
}
func systemDecl(id string) bool {
	return id == SvcActorDeclID || id == CoreActorDeclID || id == RegistrarSeatDeclID || strings.HasPrefix(id, PeerActorDeclPrefix)
}

func (r *Registrar) editDecl(ctx context.Context, caller string, source channel.ID, p DeclEdit) (regspec.DeclRow, error) {
	if p.ID == "" {
		return regspec.DeclRow{}, invalid("id required")
	}
	if systemDecl(p.ID) {
		return regspec.DeclRow{}, reserved("system declaration is reserved")
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
	if systemClass(row.DefaultClass) || systemDecl(row.ID) {
		return regspec.DeclRow{}, reserved("system declaration is reserved")
	}
	if row.Owner != caller && source != protocol.C0ChannelID {
		return regspec.DeclRow{}, denied("declaration owner required")
	}
	before := row
	before.Config = cloneJSON(row.Config)
	if p.Name != nil {
		row.Name = strings.TrimSpace(*p.Name)
	}
	if p.Description != nil {
		row.Description = *p.Description
	}
	if p.Class != nil {
		next := strings.TrimSpace(*p.Class)
		if systemClass(next) {
			return regspec.DeclRow{}, reserved("system class is reserved")
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
	if row.Name == before.Name && row.Description == before.Description && row.DefaultClass == before.DefaultClass && row.Visibility == before.Visibility && jsonEqual(row.Config, before.Config) {
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

func (r *Registrar) revokeDecl(ctx context.Context, caller string, source channel.ID, p DeclRevoke) (regspec.DeclRow, error) {
	if p.ID == "" {
		return regspec.DeclRow{}, invalid("id required")
	}
	if systemDecl(p.ID) {
		return regspec.DeclRow{}, reserved("system declaration is reserved")
	}
	row, found, err := r.registry.store.GetDecl(ctx, p.ID)
	if !found && err == nil {
		return regspec.DeclRow{}, notFound("declaration")
	}
	if err != nil {
		return regspec.DeclRow{}, err
	}
	if systemClass(row.DefaultClass) || systemDecl(row.ID) {
		return regspec.DeclRow{}, reserved("system declaration is reserved")
	}
	if row.Owner != caller && source != protocol.C0ChannelID {
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
	if systemDecl(p.DeclID) {
		return regspec.OverlayRow{}, reserved("system declaration is reserved")
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
	if systemClass(decl.DefaultClass) || systemDecl(decl.ID) {
		return regspec.OverlayRow{}, reserved("system declaration is reserved")
	}
	if r.classes == nil {
		return regspec.OverlayRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
	}
	if err := r.classes.ValidateConfig(decl.DefaultClass, p.Config); err != nil {
		return regspec.OverlayRow{}, invalid(err.Error())
	}
	return r.writeOverlay(ctx, p)
}

func (r *Registrar) writeOverlay(ctx context.Context, p OverlaySet) (regspec.OverlayRow, error) {
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
	if systemDecl(p.DeclID) {
		return Confirmation{}, reserved("system declaration is reserved")
	}
	if p.ChannelID != source {
		return Confirmation{}, denied("overlay target must equal source channel")
	}
	decl, ok, err := r.registry.GetDecl(ctx, p.DeclID)
	if err != nil {
		return Confirmation{}, err
	}
	if !ok || decl.Status != regspec.DeclPresent {
		return Confirmation{}, notFound("declaration")
	}
	if systemClass(decl.DefaultClass) || systemDecl(decl.ID) {
		return Confirmation{}, reserved("system declaration is reserved")
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

func (r *Registrar) retireDevice(ctx context.Context, owner string, source channel.ID, p DeviceRetire) (regspec.DeviceRow, error) {
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
	if row.OwnerPrincipal != owner && source != protocol.C0ChannelID {
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

func (r *Registrar) validateTemplateBody(ctx context.Context, body regspec.TemplateBody) error {
	return r.registry.store.InTx(ctx, func(tx *store.Tx) error {
		if err := r.validateTemplateDeclarations(ctx, tx, body.Declarations); err != nil {
			return err
		}
		if body.Profile != nil {
			if body.Profile.Serving != nil && *body.Profile.Serving != 0 && *body.Profile.Serving != 1 {
				return invalid("serving must be 0 or 1")
			}
			return r.validateEndpoints(ctx, tx, body.Profile.Endpoints)
		}
		return nil
	})
}

func (r *Registrar) validateTemplateDeclarations(ctx context.Context, tx *store.Tx, declarations []regspec.TemplateDeclaration) error {
	seen := make(map[string]struct{}, len(declarations))
	for _, item := range declarations {
		if item.DeclID == "" {
			return invalid("template declaration id required")
		}
		if _, duplicate := seen[item.DeclID]; duplicate {
			return invalid("duplicate template declaration")
		}
		seen[item.DeclID] = struct{}{}
		decl, ok, err := tx.GetDecl(ctx, item.DeclID)
		if err != nil {
			return err
		}
		if !ok || decl.Status != regspec.DeclPresent {
			return invalid("template declaration not found")
		}
		if strings.HasPrefix(item.DeclID, PeerActorDeclPrefix) {
			if item.Config != nil {
				return invalid("peer declaration config is fixed")
			}
			if decl.DefaultClass != PeerActorClass {
				return invalid("peer declaration class is invalid")
			}
			continue
		}
		if systemDecl(item.DeclID) || systemClass(decl.DefaultClass) {
			return invalid("system declaration is not allowed in channel recipes")
		}
	}
	return nil
}
func (r *Registrar) registerChannelTemplate(ctx context.Context, owner string, p ChannelTemplateRegister) (regspec.ChannelTemplateRow, error) {
	p.ID = strings.TrimSpace(p.ID)
	p.Name = strings.TrimSpace(p.Name)
	if p.ID == "" || p.Name == "" {
		return regspec.ChannelTemplateRow{}, invalid("id and name required")
	}
	if p.Visibility == "" {
		p.Visibility = "private"
	}
	if p.Visibility != "private" && p.Visibility != "public" {
		return regspec.ChannelTemplateRow{}, invalid("invalid visibility")
	}
	if err := r.validateTemplateBody(ctx, p.Body); err != nil {
		return regspec.ChannelTemplateRow{}, err
	}
	raw, err := json.Marshal(p.Body)
	if err != nil {
		return regspec.ChannelTemplateRow{}, err
	}
	now := r.now().UnixMilli()
	row := regspec.ChannelTemplateRow{ID: p.ID, Name: p.Name, Description: p.Description, Owner: owner, Status: regspec.DeclPresent, Visibility: p.Visibility, Body: raw, CreatedAt: now, UpdatedAt: now}
	if err := r.registry.store.InsertChannelTemplate(ctx, row); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return regspec.ChannelTemplateRow{}, conflict("channel template id already exists")
		}
		return regspec.ChannelTemplateRow{}, err
	}
	return row, nil
}
func (r *Registrar) editChannelTemplate(ctx context.Context, caller string, p ChannelTemplateEdit) (regspec.ChannelTemplateRow, error) {
	row, ok, err := r.registry.GetChannelTemplate(ctx, p.ID)
	if err != nil {
		return regspec.ChannelTemplateRow{}, err
	}
	if !ok || row.Status != regspec.DeclPresent {
		return regspec.ChannelTemplateRow{}, notFound("channel template")
	}
	if row.Owner != caller {
		return regspec.ChannelTemplateRow{}, denied("channel template owner required")
	}
	if p.Name != nil {
		row.Name = strings.TrimSpace(*p.Name)
	}
	if p.Description != nil {
		row.Description = *p.Description
	}
	if p.Visibility != nil {
		row.Visibility = *p.Visibility
	}
	if row.Name == "" || (row.Visibility != "private" && row.Visibility != "public") {
		return regspec.ChannelTemplateRow{}, invalid("invalid channel template")
	}
	if p.Body != nil {
		if err := r.validateTemplateBody(ctx, *p.Body); err != nil {
			return regspec.ChannelTemplateRow{}, err
		}
		row.Body, _ = json.Marshal(*p.Body)
	}
	row.UpdatedAt = r.now().UnixMilli()
	return row, r.registry.store.UpdateChannelTemplate(ctx, row)
}
func (r *Registrar) revokeChannelTemplate(ctx context.Context, caller string, p ChannelTemplateRevoke) (regspec.ChannelTemplateRow, error) {
	row, ok, err := r.registry.GetChannelTemplate(ctx, p.ID)
	if err != nil {
		return regspec.ChannelTemplateRow{}, err
	}
	if !ok {
		return regspec.ChannelTemplateRow{}, notFound("channel template")
	}
	if row.Owner != caller {
		return regspec.ChannelTemplateRow{}, denied("channel template owner required")
	}
	if row.Status == regspec.DeclRevoked {
		return row, nil
	}
	row.Status = regspec.DeclRevoked
	row.UpdatedAt = r.now().UnixMilli()
	return row, r.registry.store.RevokeChannelTemplate(ctx, row.ID, row.UpdatedAt)
}

func (r *Registrar) setChannelProfile(ctx context.Context, source channel.ID, p ChannelProfileSet) (regspec.ChannelRow, error) {
	if p.ChannelID == "" {
		return regspec.ChannelRow{}, invalid("channel_id required")
	}
	if p.ChannelID == protocol.C0ChannelID {
		return regspec.ChannelRow{}, reserved("c0 profile is fixed")
	}
	if source != protocol.C0ChannelID && source != p.ChannelID {
		return regspec.ChannelRow{}, denied("profile target must equal source channel or core")
	}
	if p.Serving == nil || (*p.Serving != 0 && *p.Serving != 1) {
		return regspec.ChannelRow{}, invalid("serving must be 0 or 1")
	}
	var rows []regspec.EndpointRow
	now := r.now().UnixMilli()
	err := r.registry.store.InTx(ctx, func(tx *store.Tx) error {
		if _, ok, err := tx.GetChannel(ctx, p.ChannelID); err != nil {
			return err
		} else if !ok {
			return notFound("channel")
		}
		if err := r.validateEndpoints(ctx, tx, p.Endpoints); err != nil {
			return err
		}
		for name, endpoint := range p.Endpoints {
			meta, _ := json.Marshal(map[string]any{"examples": endpoint.Examples, "schema": endpoint.Schema})
			rows = append(rows, regspec.EndpointRow{ChannelID: p.ChannelID, Name: name, Description: endpoint.Description, Receiver: endpoint.Receiver, Meta: meta, UpdatedAt: now})
		}
		return tx.ReplaceProfile(ctx, p.ChannelID, p.Description, *p.Serving, rows)
	})
	if err != nil {
		return regspec.ChannelRow{}, err
	}
	row, ok, err := r.registry.GetChannelDesired(ctx, p.ChannelID)
	if err != nil {
		return regspec.ChannelRow{}, err
	}
	if !ok {
		return regspec.ChannelRow{}, notFound("channel")
	}
	return row, nil
}

func (r *Registrar) channelView(ctx context.Context, row regspec.ChannelRow) (regspec.ChannelRow, error) {
	var spec GenesisSpec
	if err := json.Unmarshal(row.Spec, &spec); err != nil {
		return regspec.ChannelRow{}, err
	}
	recipe := regspec.TemplateBody{Profile: &spec.Profile}
	for _, declaration := range spec.Declarations {
		if declaration.DeclID == SvcActorDeclID || declaration.DeclID == CoreActorDeclID {
			continue
		}
		item := regspec.TemplateDeclaration{DeclID: declaration.DeclID}
		if !strings.HasPrefix(declaration.DeclID, PeerActorDeclPrefix) {
			item.Config = cloneJSON(declaration.Rendered.Config)
		}
		recipe.Declarations = append(recipe.Declarations, item)
	}
	endpoints, err := r.registry.ListEndpoints(ctx, row.ID)
	if err != nil {
		return regspec.ChannelRow{}, err
	}
	profile := regspec.ChannelProfile{Description: stringPtr(row.Description), Serving: intPtr(row.Serving), Endpoints: map[string]regspec.EndpointSpec{}}
	for _, endpoint := range endpoints {
		spec := regspec.EndpointSpec{Description: endpoint.Description, Receiver: endpoint.Receiver}
		if len(endpoint.Meta) > 0 {
			var meta struct {
				Examples []json.RawMessage `json:"examples"`
				Schema   json.RawMessage   `json:"schema"`
			}
			_ = json.Unmarshal(endpoint.Meta, &meta)
			spec.Examples = meta.Examples
			spec.Schema = meta.Schema
		}
		profile.Endpoints[endpoint.Name] = spec
	}
	row.Recipe = &recipe
	row.Profile = &profile
	return row, nil
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

// WARNING (2026-08-13, known and accepted by the owner): this read word
// returns DeviceRow as-is, and that row carries the device admission secret
// in cleartext (see the warning on regspec.DeviceRow.Key). Every principal
// able to send device.list therefore reads every device secret. Deliberate:
// secrets have no first-class carrier yet — they ride the ordinary kv store,
// and the secret axis is a later batch; confidentiality is not a leading
// constraint at this stage. The fix is a reply type that omits that field.
//
// Second, older debt: this bypasses Registry and reaches into store directly.
// A NEW read path must not follow that: add the Registry door instead (obs
// did, see ListPrincipals/ListDevices).
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
func stringPtr(v string) *string                  { return &v }
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
