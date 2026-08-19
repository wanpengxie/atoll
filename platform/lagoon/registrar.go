package lagoon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon/internal/store"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
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
	rootExists, err := r.registry.store.PrincipalExistsWithKind(ctx, channelspec.RootPrincipalID, actor.KindHuman)
	if err != nil {
		return err
	}
	localExists, err := r.registry.store.DeviceExists(ctx, channelspec.LocalDeviceID)
	if err != nil {
		return err
	}
	if !rootExists || !localExists {
		return errors.New("lagoon: installation identity incomplete")
	}
	if err := ValidateName(string(channelspec.C0ChannelID)); err != nil {
		return fmt.Errorf("lagoon: invalid c0 channel name: %w", err)
	}
	now := r.now().UnixMilli()
	if err := r.registry.store.UpsertSteward(ctx, channelspec.StewardPrincipalID, now); err != nil {
		return err
	}
	c0Exists, err := r.registry.store.ChannelExists(ctx, channelspec.C0ChannelID)
	if err != nil {
		return err
	}
	if c0Exists {
		// c0 exists: its row is the owner's to change and is left untouched.
		// Only refuse to start on a genesis this build cannot read.
		row, found, err := r.registry.GetChannelDesired(ctx, channelspec.C0ChannelID)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("lagoon: c0 registry genesis missing — wipe the installation and start again")
		}
		if err := validateStoredC0Genesis(row.Spec); err != nil {
			return fmt.Errorf("lagoon: c0 registry genesis invalid (%w) — wipe the installation and start again", err)
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
		spec.Profile = regspec.ChannelProfile{Description: &description, Serving: &serving, Endpoints: map[string]regspec.EndpointSpec{}}
		raw, err := json.Marshal(spec)
		if err != nil {
			return err
		}
		if err := r.registry.store.InsertSystemChannel(ctx, regspec.ChannelRow{
			ID: channelspec.C0ChannelID, Name: string(channelspec.C0ChannelID), Type: "group",
			Status: regspec.ChannelPresent, OwnerPrincipal: channelspec.RootPrincipalID, Description: description, Serving: serving, Spec: raw, CreatedAt: spec.CreatedAt,
		}); err != nil {
			return err
		}
	}
	decls := []regspec.DeclRow{
		{ID: SvcActorDeclID, Name: "Service Actor", Owner: channelspec.RootPrincipalID, DefaultClass: SvcActorClass, Config: json.RawMessage(`{}`), Status: regspec.DeclPresent, Visibility: "private", Singleton: true, CreatedAt: now, UpdatedAt: now},
		{ID: RegistrarDeclID, Name: "Registrar Seat", Owner: channelspec.RootPrincipalID, DefaultClass: ClassRegistrar, Config: json.RawMessage(`{}`), Status: regspec.DeclPresent, Visibility: "private", Singleton: true, CreatedAt: now, UpdatedAt: now},
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

// validateStoredC0Genesis is the whole of what a start asks of an existing
// c0: that this build can read its genesis and that it is c0's. Anything
// else means the installation predates this build's shape — there is no
// upgrade path before 1.0; the operator wipes and starts again.
func validateStoredC0Genesis(raw json.RawMessage) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var stored GenesisSpec
	if err := decoder.Decode(&stored); err != nil {
		return err
	}
	if stored.ChannelID != channelspec.C0ChannelID {
		return errors.New("channel_id is not c0")
	}
	if stored.Type != "group" {
		return errors.New("type is not group")
	}
	if stored.OwnerPrincipal != channelspec.RootPrincipalID {
		return errors.New("owner is not root")
	}
	return nil
}

func Def(registrar *Registrar) actorbase.Def {
	return actorbase.Def{Manifest: introspect.Manifest{Class: "registrar", Interfaces: []string{"actor"}, Words: map[string]introspect.WordSpec{}}, New: func() (actorbase.Proc, error) {
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
	caller := actorbase.EffectiveCaller(msg)
	principal := r.resolvePrincipal(msg.Ctx(), caller.Channel, caller.Actor, sys, msg)
	if principal == "" {
		return
	}
	// The lobby doors are judged by the calling channel: register and login
	// are spoken from the lobby and from nowhere else. Everything else the
	// lobby could say never reaches here — c0's svcactor exposes only these
	// two endpoints to it.
	if LobbyWord(word) != (caller.Channel == channelspec.LobbyChannelID) {
		_, _ = sys.Fail(msg, string(CodePermissionDenied), "lobby speaks register and login only, and only the lobby speaks them")
		return
	}
	if word == WordPrincipalCreate && caller.Channel == channelspec.LobbyChannelID && !r.registry.openRegistration {
		_, _ = sys.Fail(msg, "endpoint_not_found", "this node is not accepting self-registration, so new principals cannot be created from the lobby; an existing principal must create the account")
		return
	}
	value, err := r.execute(sys, msg.Ctx(), principal, caller.Channel, word, msg.Payload)
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
	_, _ = sys.Reply(msg, Reply{Value: rawValue})
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
	if err := actorbase.DecodeStrict(raw, out); err != nil {
		return &Error{Code: CodeInvalidArgs, Detail: "invalid JSON payload"}
	}
	return nil
}

func decodeClosed(raw json.RawMessage, out any) error {
	return actorbase.DecodeStrict(raw, out)
}

// decodeArgs decodes one word's closed argument struct and, when that fails,
// says why and what the word wanted instead. Strict decoding rejects a payload
// for reasons only the decoder knows — an unknown field, an array where an
// object belongs — so replacing its account with a fixed sentence leaves the
// sender to guess which of those it was. That is not hypothetical: one observed
// session burned two extra attempts on a bare "invalid JSON payload" from this
// path, and in the same session corrected an agent.ask payload on the first
// retry, because that path had kept the decoder's own message.
func decodeArgs(raw json.RawMessage, out any, shape string) error {
	err := decodeClosed(raw, out)
	if err == nil {
		return nil
	}
	detail := "invalid payload: " + err.Error()
	if shape != "" {
		detail += "; this word takes " + shape
	}
	return invalid(detail)
}

// kindOrUnknown names a class's kind for a message, staying legible when the
// class is not one this node runs — the caller is being told why a transition
// was refused, and "unknown class" as one half of that sentence is still more
// use than no sentence.
// configRejection relays a class's own verdict on a config and points at where
// the shape can be read. The class knows exactly what it refused and says so;
// what it cannot know is that a caller who got here has no other way to learn
// the field set — the shapes live in Go types, and system.class.list is the
// one word that publishes them.
// The unknown-class case is told apart with the catalog itself rather than by
// matching the registry's error, so this stays behind the ClassCatalog
// interface that keeps lagoon independent of any particular class registry.
func (r *Registrar) configRejection(class string, err error) error {
	if _, known := r.classes.LookupClassKind(class); !known {
		return invalid(fmt.Sprintf("class %q is not one this node can run; list the runnable classes, each with the config shape it accepts, using system.class.list", class))
	}
	return invalid(fmt.Sprintf("%s; read the accepted config shape for class %q with system.class.list", err.Error(), class))
}

func kindOrUnknown(kind actor.Kind, known bool) string {
	if !known {
		return "unknown class"
	}
	return string(kind)
}

// nameRule states the name law from ValidateName instead of only enforcing it.
// "lagoon: invalid name" names the package that refused and nothing a caller
// can act on; the first name an agent tried here was a non-ASCII one, which
// the rule rules out in its first clause.
func nameRule(what, got string) string {
	return fmt.Sprintf("%s %q is not a valid name: use 1-63 characters of lowercase a-z, 0-9 or '-', starting and ending with a letter or digit (no spaces, uppercase, or non-ASCII)", what, got)
}

const (
	shapeRecipe = `{"name": "<1-63 chars, lowercase a-z 0-9 and '-'>", "recipe": {"declarations": [{"decl_id": "<an actor template id>"}], "profile": {"description": "...", "serving": 0 or 1, "svc_agent": "<a decl_id listed in declarations>"}}}`

	shapeChannelTemplate = `{"id": "...", "name": "...", "visibility": "public" or "private", "body": {"declarations": [{"decl_id": "..."}], "profile": {...}}}`

	shapeChannelTemplateEdit = `{"id": "..."} plus any of name, description, visibility, body`

	shapeChannelProfile = `{"channel_id": "...", "description": "...", "serving": 0 or 1}`
)

func decodeEmpty(raw json.RawMessage) error {
	var body struct{}
	if err := actorbase.DecodeStrictEmpty(raw, &body); err != nil {
		return invalid("this word takes no parameters; send an empty object: " + err.Error())
	}
	return nil
}

func (r *Registrar) execute(sys actorbase.Sys, ctx context.Context, principal string, source channel.ID, word Word, raw json.RawMessage) (any, error) {
	switch word {
	case WordChannelCreate:
		var p ChannelCreate
		if err := decodeArgs(raw, &p, shapeRecipe); err != nil {
			return nil, err
		}
		return r.createChannel(sys, ctx, principal, source, p)
	case WordChannelTemplateCreate:
		var p ChannelTemplateRegister
		if err := decodeArgs(raw, &p, shapeChannelTemplate); err != nil {
			return nil, err
		}
		return r.registerChannelTemplate(ctx, principal, p)
	case WordChannelTemplateSet:
		var p ChannelTemplateEdit
		if err := decodeArgs(raw, &p, shapeChannelTemplateEdit); err != nil {
			return nil, err
		}
		return r.editChannelTemplate(ctx, principal, p)
	case WordChannelTemplateDelete:
		var p ChannelTemplateRevoke
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.revokeChannelTemplate(ctx, principal, p)
	case WordChannelSet:
		var p ChannelProfileSet
		if err := decodeArgs(raw, &p, shapeChannelProfile); err != nil {
			return nil, err
		}
		return r.setChannelProfile(ctx, source, p)
	case WordChannelDelete:
		var p ChannelRetire
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.retireChannel(sys, ctx, principal, source, p)
	case WordPrincipalCreate:
		var p PrincipalRegister
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.registerPrincipal(sys, ctx, p)
	case WordPrincipalLogin:
		var p PrincipalLogin
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.loginPrincipal(ctx, p)
	case WordPrincipalDelete:
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
	case WordActorTemplateCreate:
		var p DeclRegister
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.registerDecl(ctx, principal, p)
	case WordActorTemplateSet:
		var p DeclEdit
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.editDecl(ctx, principal, source, p)
	case WordActorTemplateDelete:
		var p DeclRevoke
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.revokeDecl(ctx, principal, source, p)
	case WordActorOverlaySet:
		var p OverlaySet
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.setOverlay(ctx, principal, source, p)
	case WordActorOverlayDelete:
		var p OverlayClear
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.clearOverlay(ctx, source, p)
	case WordDeviceCreate:
		var p DeviceMint
		if err := decodePayload(raw, &p); err != nil {
			return nil, err
		}
		return r.createDevice(ctx, principal, p.Name)
	case WordDeviceDelete:
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
			return nil, notFound("channel", string(p.ChannelID), "system.channel.list")
		}
		return r.channelView(ctx, row)
	case WordPrincipalList:
		if err := decodeEmpty(raw); err != nil {
			return nil, err
		}
		return r.registry.store.ListPrincipals(ctx)
	case WordActorTemplateList:
		if err := decodeEmpty(raw); err != nil {
			return nil, err
		}
		return r.registry.ListDecls(ctx)
	case WordActorTemplateGet:
		var p DeclRevoke
		if err := decodeClosed(raw, &p); err != nil || p.ID == "" {
			return nil, invalid("id required: name the template to act on; list them with system.actor.template.list")
		}
		row, ok, err := r.registry.GetDecl(ctx, p.ID)
		if err != nil {
			return nil, err
		}
		if !ok || row.Status != regspec.DeclPresent {
			return nil, notFound("declaration", p.ID, "system.actor.template.list")
		}
		return row, nil
	case WordChannelTemplateList:
		if err := decodeEmpty(raw); err != nil {
			return nil, err
		}
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
			return nil, notFound("channel template", p.ID, "system.channel.template.list")
		}
		return row, nil
	case WordDeviceList:
		if err := decodeEmpty(raw); err != nil {
			return nil, err
		}
		return r.readDevices(ctx)
	case WordClassList:
		if err := decodeEmpty(raw); err != nil {
			return nil, err
		}
		return r.readClasses()
	case WordPrincipalGet:
		if err := decodeEmpty(raw); err != nil {
			return nil, err
		}
		return r.readPrincipal(ctx, principal)
	default:
		return nil, invalid(fmt.Sprintf("%q is not a word this registry answers; call actor.describe on the system door to see the ones it does", word))
	}
}

func invalid(detail string) error { return &Error{Code: CodeInvalidArgs, Detail: detail} }

// notFound names the id that was not found and where the caller can get a
// real one. The previous signature took only a noun, so "channel not found"
// was the most it could ever say — the sender learned neither which of the
// ids it sent was wrong nor how to obtain a correct one, and had to go
// probing. The listing word is part of the message because the registry is
// the only place these ids exist; reconstructing one by hand is how a caller
// arrives here in the first place.
func notFound(noun, id, listWord string) error {
	detail := noun + " not found"
	if id != "" {
		detail = fmt.Sprintf("%s %q not found", noun, id)
	}
	if listWord != "" {
		detail += "; list the current ones with " + listWord
	}
	return &Error{Code: CodeNotFound, Detail: detail}
}
func conflict(detail string) error { return &Error{Code: CodeConflictExists, Detail: detail} }
func denied(detail string) error   { return &Error{Code: CodePermissionDenied, Detail: detail} }
func reserved(detail string) error { return &Error{Code: CodeReserved, Detail: detail} }

type ChannelCreateReply struct {
	ChannelID channel.ID `json:"channel_id"`
}

func (r *Registrar) createChannel(sys actorbase.Sys, ctx context.Context, owner string, source channel.ID, p ChannelCreate) (ChannelCreateReply, error) {
	var row regspec.ChannelRow
	var created bool
	err := r.registry.store.InTx(ctx, func(tx *store.Tx) error {
		var err error
		row, created, err = r.provisionChannel(ctx, tx, owner, source, p.Name, p.Recipe)
		return err
	})
	if err != nil {
		return ChannelCreateReply{}, err
	}
	if !created {
		return ChannelCreateReply{ChannelID: row.ID}, nil
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: row.ID})
	}
	r.postChannelEdges(sys, row, message.TypeSystemMemberCreate)
	return ChannelCreateReply{ChannelID: row.ID}, nil
}

func (r *Registrar) provisionChannel(ctx context.Context, tx *store.Tx, owner string, parent channel.ID, name string, body regspec.TemplateBody) (regspec.ChannelRow, bool, error) {
	if name == "" {
		return regspec.ChannelRow{}, false, invalid("name required: a channel needs a name, 1-63 chars of lowercase a-z, 0-9 or '-'")
	}
	if err := ValidateName(name); err != nil {
		return regspec.ChannelRow{}, false, invalid(nameRule("channel name", name))
	}
	if parent == "" {
		return regspec.ChannelRow{}, false, invalid("parent required: a channel is always created under the channel the request came from, and that channel could not be determined")
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
		return regspec.ChannelRow{}, false, notFound("parent channel", string(parent), "system.channel.list")
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
	declarations := make([]GenesisDeclaration, 0, len(body.Declarations)+2)
	overlays := make([]regspec.OverlayRow, 0, len(body.Declarations))
	declarationKinds := make(map[string]actor.Kind, len(body.Declarations)+1)
	svc, err := r.renderSystem(SvcActorClass, json.RawMessage(`{}`))
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	declarations = append(declarations, GenesisDeclaration{DeclID: SvcActorDeclID, Kind: actor.KindPeer, Rendered: svc})
	declarationKinds[SvcActorDeclID] = actor.KindPeer
	for _, item := range body.Declarations {
		if item.DeclID == "" || declarationKinds[item.DeclID] != "" {
			return regspec.ChannelRow{}, false, invalid("recipe declaration id is empty or duplicated")
		}
		decl, ok, err := tx.GetDecl(ctx, item.DeclID)
		if err != nil {
			return regspec.ChannelRow{}, false, err
		}
		if !ok || decl.Status != regspec.DeclPresent {
			return regspec.ChannelRow{}, false, notFound("declaration", item.DeclID, "system.actor.template.list")
		}
		if decl.Visibility != "public" && decl.Owner != owner {
			return regspec.ChannelRow{}, false, denied(fmt.Sprintf("declaration %q is private and owned by %q, not you; use a public declaration, one you own, or create your own with system.actor.template.create and visibility \"public\"", item.DeclID, decl.Owner))
		}
		config := decl.Config
		if len(item.Config) > 0 {
			config = item.Config
			overlays = append(overlays, regspec.OverlayRow{DeclID: item.DeclID, ChannelID: id, Config: cloneJSON(item.Config), UpdatedAt: now})
		}
		if r.classes == nil {
			return regspec.ChannelRow{}, false, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
		}
		if err := r.classes.ValidateConfig(decl.DefaultClass, config); err != nil {
			return regspec.ChannelRow{}, false, r.configRejection(decl.DefaultClass, err)
		}
		kind, ok := r.classes.LookupClassKind(decl.DefaultClass)
		if !ok {
			return regspec.ChannelRow{}, false, invalid(fmt.Sprintf("declaration %q names class %q, which this node cannot run; list the runnable classes with system.class.list", item.DeclID, decl.DefaultClass))
		}
		placement, ok := r.classes.LookupClassPlacement(decl.DefaultClass)
		if !ok {
			return regspec.ChannelRow{}, false, invalid(fmt.Sprintf("class %q declares no valid placement, so no host can be chosen for it; this is a defect in the class itself rather than in your request", decl.DefaultClass))
		}
		rendered := channelspec.RenderedSnapshot{Class: decl.DefaultClass, Config: cloneJSON(config), Placement: channelspec.Placement{Kind: placement}, Singleton: decl.Singleton}
		if placement == channelspec.PlacementDaemon {
			rendered.Placement.DesiredHost = channelspec.LocalDeviceID
		}
		rendered, err = rendered.Seal()
		if err != nil {
			return regspec.ChannelRow{}, false, err
		}
		declarations = append(declarations, GenesisDeclaration{DeclID: item.DeclID, Kind: kind, Rendered: rendered})
		declarationKinds[item.DeclID] = kind
	}
	if parent != channelspec.C0ChannelID {
		if declarationKinds[string(parent)] != "" {
			return regspec.ChannelRow{}, false, invalid(fmt.Sprintf("a recipe must not list %q, the peer back to its own parent channel: the registry mints that itself", string(parent)))
		}
		parentDecl, ok, err := tx.GetDecl(ctx, string(parent))
		if err != nil {
			return regspec.ChannelRow{}, false, err
		}
		if !ok || parentDecl.Status != regspec.DeclPresent {
			return regspec.ChannelRow{}, false, notFound("parent peer declaration", string(parent), "system.actor.template.list")
		}
		parentRendered, err := r.renderSystem(PeerActorClass, targetConfig(parent))
		if err != nil {
			return regspec.ChannelRow{}, false, err
		}
		declarations = append(declarations, GenesisDeclaration{DeclID: string(parent), Kind: actor.KindPeer, Rendered: parentRendered})
		declarationKinds[string(parent)] = actor.KindPeer
	}
	if err := validateServiceProfile(profile, declarationKinds); err != nil {
		return regspec.ChannelRow{}, false, err
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
	for _, overlay := range overlays {
		if err := tx.UpsertOverlay(ctx, overlay); err != nil {
			return regspec.ChannelRow{}, false, err
		}
	}
	if err := tx.InsertBinding(ctx, regspec.BindingRow{ChannelID: row.ID, DeviceID: channelspec.LocalDeviceID, AttachedAt: now}); err != nil {
		return regspec.ChannelRow{}, false, err
	}
	peerID := string(id)
	if err := tx.InsertDecl(ctx, regspec.DeclRow{ID: peerID, Name: qualified, Owner: owner, DefaultClass: PeerActorClass, Config: targetConfig(id), Status: regspec.DeclPresent, Visibility: "public", CreatedAt: now, UpdatedAt: now}); err != nil {
		return regspec.ChannelRow{}, false, err
	}
	return row, true, nil
}

func (r *Registrar) renderSystem(class string, config json.RawMessage) (channelspec.RenderedSnapshot, error) {
	return (channelspec.RenderedSnapshot{Class: class, Config: config, Placement: channelspec.Placement{Kind: channelspec.PlacementServer}}).Seal()
}
func intPtr(v int) *int { return &v }
func targetConfig(id channel.ID) json.RawMessage {
	raw, _ := json.Marshal(map[string]channel.ID{"channel": id})
	return raw
}

// declaredIn lists what the recipe actually declared. Every rejection in
// validateServiceProfile is a mismatch between a name the profile used and the
// declarations above it, and the sender cannot see that set from the refusal
// alone — it wrote the payload, but not necessarily the template ids inside it.
func declaredIn(kinds map[string]actor.Kind) string {
	if len(kinds) == 0 {
		return "this recipe declares no members at all"
	}
	names := make([]string, 0, len(kinds))
	for name, kind := range kinds {
		names = append(names, fmt.Sprintf("%s (%s)", name, kind))
	}
	sort.Strings(names)
	return "this recipe declares: " + strings.Join(names, ", ")
}

func validateServiceProfile(profile regspec.ChannelProfile, kinds map[string]actor.Kind) error {
	for name, endpoint := range profile.Endpoints {
		if strings.TrimSpace(name) == "" || name == introspect.QueryDescribe || name == "agent.ask" || strings.HasPrefix(name, "svcactor.") || strings.HasPrefix(name, "system.") {
			return invalid(fmt.Sprintf("service endpoint name %q is empty or reserved: an endpoint may not be named actor.describe or agent.ask, nor start with svcactor. or system., because those are already answered by the door itself", name))
		}
		kind, ok := kinds[endpoint.Receiver]
		if !ok {
			return invalid(fmt.Sprintf("endpoint %q names receiver %q, which is not in this recipe; %s", name, endpoint.Receiver, declaredIn(kinds)))
		}
		if kind != actor.KindTool && kind != actor.KindAgent && kind != actor.KindHuman {
			return invalid(fmt.Sprintf("endpoint %q names receiver %q, a %s declaration; only tool, agent and human declarations can answer an endpoint", name, endpoint.Receiver, kind))
		}
	}
	if profile.SvcAgent != nil && *profile.SvcAgent != "default" {
		if kinds[*profile.SvcAgent] != actor.KindAgent {
			return invalid(fmt.Sprintf("svc_agent %q must name an agent declaration listed in this recipe's declarations; %s. Either add that declaration, name one of them, or use \"default\" for the first active agent", *profile.SvcAgent, declaredIn(kinds)))
		}
	}
	return nil
}

func (r *Registrar) postChannelEdges(sys actorbase.Sys, row regspec.ChannelRow, word string) {
	raw, _ := json.Marshal(map[string]any{"decl_id": string(row.ID)})
	if word == message.TypeSystemMemberDelete {
		raw, _ = json.Marshal(map[string]any{"member": "peer:" + string(row.ID)})
	}
	_, _ = sys.Post(behavior.RequestSpec{Type: word, Audience: message.Audience{actor.SystemActorID}, Payload: raw})
	if row.ParentID != channelspec.C0ChannelID {
		_, _ = sys.Post(behavior.RequestSpec{Type: word, Audience: message.Audience{actor.ActorID("peer:" + string(row.ParentID))}, Payload: raw})
	}
}

type ChannelRetireReply struct {
	regspec.ChannelRow
}

func (r *Registrar) retireChannel(sys actorbase.Sys, ctx context.Context, principal string, source channel.ID, p ChannelRetire) (ChannelRetireReply, error) {
	if p.ChannelID == "" {
		return ChannelRetireReply{}, invalid("channel_id required: name the channel to act on; list them with system.channel.list")
	}
	if p.ChannelID == channelspec.C0ChannelID {
		return ChannelRetireReply{}, reserved("c0 cannot be retired")
	}
	row, found, err := r.registry.GetChannelDesired(ctx, p.ChannelID)
	if !found && err == nil {
		return ChannelRetireReply{}, notFound("channel", string(p.ChannelID), "system.channel.list")
	}
	if err != nil {
		return ChannelRetireReply{}, err
	}
	if row.OwnerPrincipal != principal && source != channelspec.C0ChannelID {
		return ChannelRetireReply{}, denied(fmt.Sprintf("channel %q is owned by %q and you are acting as %q; only its owner, or a caller in the registry channel, may retire it", p.ChannelID, row.OwnerPrincipal, principal))
	}
	hasPresentChild, err := r.registry.store.PresentChildExists(ctx, p.ChannelID)
	if err != nil {
		return ChannelRetireReply{}, err
	}
	if hasPresentChild {
		return ChannelRetireReply{}, conflict("channel has active child channels")
	}
	if err := r.registry.store.RetireChannelAndPeer(ctx, p.ChannelID, string(p.ChannelID), r.now().UnixMilli()); err != nil {
		return ChannelRetireReply{}, err
	}
	row.Status = regspec.ChannelRetired
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: p.ChannelID, AllPrincipals: true})
	}
	r.postChannelEdges(sys, row, message.TypeSystemMemberDelete)
	return ChannelRetireReply{ChannelRow: row}, nil
}

type PrincipalRegisterReply struct {
	PrincipalID   string     `json:"principal_id"`
	HomeChannelID channel.ID `json:"home_channel_id"`
}

func (r *Registrar) registerPrincipal(sys actorbase.Sys, ctx context.Context, p PrincipalRegister) (PrincipalRegisterReply, error) {
	p.Email = strings.TrimSpace(p.Email)
	if p.Email == "" || p.SecretHash == "" {
		return PrincipalRegisterReply{}, invalid("email and secret_hash are both required to create a principal")
	}
	id := strings.TrimSpace(p.ID)
	if id == channelspec.RootPrincipalID {
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
		home, created, err = r.provisionChannel(ctx, tx, id, channelspec.C0ChannelID, id, regspec.TemplateBody{})
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
	r.postChannelEdges(sys, home, message.TypeSystemMemberCreate)
	return PrincipalRegisterReply{PrincipalID: row.ID, HomeChannelID: home.ID}, nil
}

// loginPrincipal is the lobby's second door: a guest presents an email and
// password and, if they match an active credential, learns which principal it
// is. The session itself is the portal's to mint from a "completed" reply —
// nothing here hands out anything but the principal id.
func (r *Registrar) loginPrincipal(ctx context.Context, p PrincipalLogin) (PrincipalLoginReply, error) {
	p.Email = strings.TrimSpace(p.Email)
	if p.Email == "" || p.Password == "" {
		return PrincipalLoginReply{}, invalid("email and password are both required to sign in")
	}
	id, ok, err := r.registry.VerifyCredential(ctx, p.Email, p.Password)
	if err != nil {
		return PrincipalLoginReply{}, err
	}
	if !ok {
		return PrincipalLoginReply{}, &Error{Code: CodeInvalidCredentials, Detail: "invalid credentials"}
	}
	return PrincipalLoginReply{PrincipalID: id}, nil
}

func (r *Registrar) retirePrincipal(ctx context.Context, caller string, source channel.ID, p PrincipalRetire) (regspec.PrincipalRow, error) {
	if p.PrincipalID == "" {
		return regspec.PrincipalRow{}, invalid("principal_id required: name the principal to act on; list them with system.principal.list")
	}
	if p.PrincipalID == channelspec.RootPrincipalID {
		return regspec.PrincipalRow{}, reserved("root cannot be retired")
	}
	if caller != p.PrincipalID && source != channelspec.C0ChannelID {
		return regspec.PrincipalRow{}, denied(fmt.Sprintf("you are acting as %q and may only retire your own principal; retiring %q requires acting as that principal or calling from the registry channel", caller, p.PrincipalID))
	}
	row, err := r.updatePrincipalStatus(ctx, p.PrincipalID, regspec.PrincipalRetired)
	return row, err
}

func (r *Registrar) updatePrincipalStatus(ctx context.Context, id string, status regspec.PrincipalStatus) (regspec.PrincipalRow, error) {
	row, found, err := r.registry.store.GetPrincipal(ctx, id)
	if !found && err == nil {
		return regspec.PrincipalRow{}, notFound("principal", id, "system.principal.list")
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
		return CredentialReply{}, invalid("principal_id and secret_hash are both required to replace a credential")
	}
	if caller != p.PrincipalID && source != channelspec.C0ChannelID {
		return CredentialReply{}, denied(fmt.Sprintf("you are acting as %q and may only change your own credential; changing %q requires acting as that principal or calling from the registry channel", caller, p.PrincipalID))
	}
	principalKind, found, err := r.registry.store.PrincipalKind(ctx, p.PrincipalID)
	if !found && err == nil {
		return CredentialReply{}, notFound("principal", p.PrincipalID, "system.principal.list")
	}
	if err != nil {
		return CredentialReply{}, err
	}
	if principalKind != actor.KindHuman {
		return CredentialReply{}, denied(fmt.Sprintf("principal %q is a %s, and only a human principal signs in with a credential", p.PrincipalID, principalKind))
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
		return regspec.DeclRow{}, invalid("id, name and class are all required: class decides what the declaration runs, and system.class.list shows which classes this node has")
	}
	if systemDecl(p.ID) {
		return regspec.DeclRow{}, reserved("system declaration is reserved")
	}
	if r.systemClass(p.Class) {
		return regspec.DeclRow{}, reserved("system class is reserved")
	}
	if p.Visibility == "" {
		p.Visibility = "private"
	}
	if p.Visibility != "private" && p.Visibility != "public" {
		return regspec.DeclRow{}, invalid(fmt.Sprintf("visibility %q is not valid: the only values are public (anyone may build from this declaration) and private (only you may, and this is the default)", p.Visibility))
	}
	if r.classes == nil {
		return regspec.DeclRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
	}
	if err := r.classes.ValidateConfig(p.Class, p.Config); err != nil {
		return regspec.DeclRow{}, r.configRejection(p.Class, err)
	}
	now := r.now().UnixMilli()
	existing, found, err := r.registry.store.GetDecl(ctx, p.ID)
	if err == nil && found {
		if existing.Status == regspec.DeclPresent && existing.Name == p.Name && existing.Description == p.Description && existing.Owner == owner && existing.DefaultClass == p.Class && existing.Visibility == p.Visibility && existing.Singleton == p.Singleton && jsonEqual(existing.Config, p.Config) {
			return existing, nil
		}
		return regspec.DeclRow{}, conflict("declaration id already exists")
	}
	if err != nil {
		return regspec.DeclRow{}, err
	}
	row := regspec.DeclRow{ID: p.ID, Name: p.Name, Description: p.Description, Owner: owner, DefaultClass: p.Class, Config: cloneJSON(p.Config), Status: regspec.DeclPresent, Visibility: p.Visibility, Singleton: p.Singleton, CreatedAt: now, UpdatedAt: now}
	if err := r.registry.store.InsertDecl(ctx, row); err != nil {
		return regspec.DeclRow{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{AllChannels: true})
	}
	return row, nil
}

func (r *Registrar) systemClass(class string) bool {
	if r.classes == nil {
		return false
	}
	kind, ok := r.classes.LookupClassKind(class)
	return ok && (kind == actor.KindPeer || kind == actor.KindSystem)
}
func systemDecl(id string) bool {
	return id == SvcActorDeclID || id == RegistrarDeclID
}

// systemCompanion names the declarations genesis materialises itself; a
// recipe neither introduces nor projects them. Peer handles are user-facing
// recipe entries and are not companions.
func systemCompanion(id string) bool {
	return id == SvcActorDeclID || id == RegistrarDeclID
}

func (r *Registrar) editDecl(ctx context.Context, caller string, source channel.ID, p DeclEdit) (regspec.DeclRow, error) {
	if p.ID == "" {
		return regspec.DeclRow{}, invalid("id required: name the template to act on; list them with system.actor.template.list")
	}
	if systemDecl(p.ID) {
		return regspec.DeclRow{}, reserved("system declaration is reserved")
	}
	row, found, err := r.registry.store.GetDecl(ctx, p.ID)
	if !found && err == nil {
		return regspec.DeclRow{}, notFound("declaration", p.ID, "system.actor.template.list")
	}
	if err != nil {
		return regspec.DeclRow{}, err
	}
	if row.Status != regspec.DeclPresent {
		return regspec.DeclRow{}, notFound("declaration", p.ID, "system.actor.template.list")
	}
	if r.systemClass(row.DefaultClass) || systemDecl(row.ID) {
		return regspec.DeclRow{}, reserved("system declaration is reserved")
	}
	if row.Owner != caller && source != channelspec.C0ChannelID {
		return regspec.DeclRow{}, denied(fmt.Sprintf("declaration %q is owned by %q and you are acting as %q; only its owner, or a caller in the registry channel, may change it", row.ID, row.Owner, caller))
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
		if r.systemClass(next) {
			return regspec.DeclRow{}, reserved("system class is reserved")
		}
		if r.classes == nil {
			return regspec.DeclRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
		}
		oldKind, oldOK := r.classes.LookupClassKind(row.DefaultClass)
		newKind, newOK := r.classes.LookupClassKind(next)
		if !oldOK || !newOK || oldKind != newKind {
			return regspec.DeclRow{}, invalid(fmt.Sprintf("cannot change class from %q to %q: that would change the declaration from a %s into a %s, and a declaration's kind is fixed once minted. Create a new declaration instead", row.DefaultClass, next, kindOrUnknown(oldKind, oldOK), kindOrUnknown(newKind, newOK)))
		}
		row.DefaultClass = next
	}
	if p.Config != nil {
		row.Config = cloneJSON(p.Config)
	}
	if p.Visibility != nil {
		row.Visibility = *p.Visibility
	}
	if p.Singleton != nil {
		row.Singleton = *p.Singleton
	}
	if row.Name == "" || (row.Visibility != "private" && row.Visibility != "public") {
		return regspec.DeclRow{}, invalid(fmt.Sprintf("the declaration would be left invalid: name is %q and visibility is %q, but a declaration needs a non-empty name and a visibility of public or private", row.Name, row.Visibility))
	}
	if r.classes == nil {
		return regspec.DeclRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
	}
	if err := r.classes.ValidateConfig(row.DefaultClass, row.Config); err != nil {
		return regspec.DeclRow{}, r.configRejection(row.DefaultClass, err)
	}
	if row.Name == before.Name && row.Description == before.Description && row.DefaultClass == before.DefaultClass && row.Visibility == before.Visibility && row.Singleton == before.Singleton && jsonEqual(row.Config, before.Config) {
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
		return regspec.DeclRow{}, invalid("id required: name the template to act on; list them with system.actor.template.list")
	}
	if systemDecl(p.ID) {
		return regspec.DeclRow{}, reserved("system declaration is reserved")
	}
	row, found, err := r.registry.store.GetDecl(ctx, p.ID)
	if !found && err == nil {
		return regspec.DeclRow{}, notFound("declaration", p.ID, "system.actor.template.list")
	}
	if err != nil {
		return regspec.DeclRow{}, err
	}
	if r.systemClass(row.DefaultClass) || systemDecl(row.ID) {
		return regspec.DeclRow{}, reserved("system declaration is reserved")
	}
	if row.Owner != caller && source != channelspec.C0ChannelID {
		return regspec.DeclRow{}, denied(fmt.Sprintf("declaration %q is owned by %q and you are acting as %q; only its owner, or a caller in the registry channel, may change it", row.ID, row.Owner, caller))
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
		return regspec.OverlayRow{}, invalid("decl_id and channel_id are both required: the overlay names which declaration, in which channel")
	}
	if systemDecl(p.DeclID) {
		return regspec.OverlayRow{}, reserved("system declaration is reserved")
	}
	if p.ChannelID != source {
		return regspec.OverlayRow{}, denied(fmt.Sprintf("an overlay may only be set on the channel it is sent from: this request names channel_id %q but arrived from %q. Send it from the target channel instead", p.ChannelID, source))
	}
	decl, ok, err := r.registry.GetDecl(ctx, p.DeclID)
	if err != nil {
		return regspec.OverlayRow{}, err
	}
	if !ok || decl.Status != regspec.DeclPresent {
		return regspec.OverlayRow{}, notFound("declaration", p.DeclID, "system.actor.template.list")
	}
	if r.systemClass(decl.DefaultClass) || systemDecl(decl.ID) {
		return regspec.OverlayRow{}, reserved("system declaration is reserved")
	}
	if r.classes == nil {
		return regspec.OverlayRow{}, &Error{Code: CodeResultUnknown, Detail: "class catalog unavailable"}
	}
	if err := r.classes.ValidateConfig(decl.DefaultClass, p.Config); err != nil {
		return regspec.OverlayRow{}, r.configRejection(decl.DefaultClass, err)
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
		return Confirmation{}, invalid("decl_id and channel_id are both required: the overlay names which declaration, in which channel")
	}
	if systemDecl(p.DeclID) {
		return Confirmation{}, reserved("system declaration is reserved")
	}
	if p.ChannelID != source {
		return Confirmation{}, denied(fmt.Sprintf("an overlay may only be set on the channel it is sent from: this request names channel_id %q but arrived from %q. Send it from the target channel instead", p.ChannelID, source))
	}
	decl, ok, err := r.registry.GetDecl(ctx, p.DeclID)
	if err != nil {
		return Confirmation{}, err
	}
	if !ok || decl.Status != regspec.DeclPresent {
		return Confirmation{}, notFound("declaration", p.DeclID, "system.actor.template.list")
	}
	if r.systemClass(decl.DefaultClass) || systemDecl(decl.ID) {
		return Confirmation{}, reserved("system declaration is reserved")
	}
	if err := r.registry.store.DeleteOverlay(ctx, p.DeclID, p.ChannelID); err != nil {
		return Confirmation{}, err
	}
	if r.registry.onCommit != nil {
		r.registry.onCommit(Change{ChannelID: p.ChannelID})
	}
	return Confirmation{Word: WordActorOverlayDelete, TargetID: p.DeclID, Status: "cleared"}, nil
}

func (r *Registrar) createDevice(ctx context.Context, owner, name string) (regspec.DeviceRow, error) {
	if err := ValidateName(name); err != nil {
		return regspec.DeviceRow{}, invalid(nameRule("device name", name))
	}
	if row, found, err := r.registry.GetDeviceByName(ctx, name); err != nil {
		return regspec.DeviceRow{}, err
	} else if found {
		if row.OwnerPrincipal == owner && row.Status == regspec.DevicePresent {
			return row, nil
		}
		return regspec.DeviceRow{}, conflict("device name already exists")
	}
	row := regspec.DeviceRow{ID: uuid.NewString(), OwnerPrincipal: owner, Name: name, Key: uuid.NewString(), Status: regspec.DevicePresent, CreatedAt: r.now().UnixMilli()}
	err := r.registry.store.InsertDevice(ctx, row)
	return row, err
}

func (r *Registrar) retireDevice(ctx context.Context, owner string, source channel.ID, p DeviceRetire) (regspec.DeviceRow, error) {
	if p.DeviceID == "" {
		return regspec.DeviceRow{}, invalid("device_id required: name the device to act on; list them with system.device.list")
	}
	if p.DeviceID == channelspec.LocalDeviceID {
		return regspec.DeviceRow{}, reserved("local device cannot be retired")
	}
	row, ok, err := r.registry.GetDevice(ctx, p.DeviceID)
	if err != nil {
		return regspec.DeviceRow{}, err
	}
	if !ok {
		return regspec.DeviceRow{}, notFound("device", p.DeviceID, "system.device.list")
	}
	if row.OwnerPrincipal != owner && source != channelspec.C0ChannelID {
		return regspec.DeviceRow{}, denied(fmt.Sprintf("device %q belongs to %q and you are acting as %q; only its owner, or a caller in the registry channel, may retire it", p.DeviceID, row.OwnerPrincipal, owner))
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
	if p.DeviceID == channelspec.LocalDeviceID {
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
		return invalid("channel_id and device_id are both required: a binding names which device, to which channel")
	}
	if p.ChannelID != source {
		return denied(fmt.Sprintf("a device may only be bound to the channel the request comes from: this names channel_id %q but arrived from %q. Send it from the target channel instead", p.ChannelID, source))
	}
	ch, ok, err := r.registry.GetChannelDesired(ctx, p.ChannelID)
	if err != nil {
		return err
	}
	if !ok || ch.Status != regspec.ChannelPresent {
		return notFound("channel", string(p.ChannelID), "system.channel.list")
	}
	device, ok, err := r.registry.GetDevice(ctx, p.DeviceID)
	if err != nil {
		return err
	}
	if !ok || device.Status != regspec.DevicePresent {
		return notFound("device", p.DeviceID, "system.device.list")
	}
	if p.DeviceID != channelspec.LocalDeviceID && device.OwnerPrincipal != owner {
		return denied(fmt.Sprintf("device %q belongs to %q and you are acting as %q; only its owner may bind it", p.DeviceID, device.OwnerPrincipal, owner))
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
			kinds := make(map[string]actor.Kind, len(body.Declarations))
			for _, item := range body.Declarations {
				decl, ok, err := tx.GetDecl(ctx, item.DeclID)
				if err != nil {
					return err
				}
				if !ok || r.classes == nil {
					return invalid(fmt.Sprintf("declaration %q cannot be resolved right now, so the template cannot be checked against it", item.DeclID))
				}
				kind, ok := r.classes.LookupClassKind(decl.DefaultClass)
				if !ok {
					return invalid(fmt.Sprintf("declaration %q names class %q, which this node cannot run; list the runnable classes with system.class.list", item.DeclID, decl.DefaultClass))
				}
				kinds[item.DeclID] = kind
			}
			return validateServiceProfile(*body.Profile, kinds)
		}
		return nil
	})
}

func (r *Registrar) validateTemplateDeclarations(ctx context.Context, tx *store.Tx, declarations []regspec.TemplateDeclaration) error {
	seen := make(map[string]struct{}, len(declarations))
	for _, item := range declarations {
		if item.DeclID == "" {
			return invalid("every entry in declarations needs a non-empty decl_id")
		}
		if _, duplicate := seen[item.DeclID]; duplicate {
			return invalid(fmt.Sprintf("declaration %q is listed more than once; each decl_id may appear at most once", item.DeclID))
		}
		seen[item.DeclID] = struct{}{}
		decl, ok, err := tx.GetDecl(ctx, item.DeclID)
		if err != nil {
			return err
		}
		if !ok || decl.Status != regspec.DeclPresent {
			return invalid(fmt.Sprintf("declaration %q is not present; list the available ones with system.actor.template.list", item.DeclID))
		}
		if decl.DefaultClass == PeerActorClass {
			if item.Config != nil {
				return invalid(fmt.Sprintf("declaration %q is a peer, whose config is minted by the registry and cannot be overridden here; drop the config field for this entry", item.DeclID))
			}
			if decl.DefaultClass != PeerActorClass {
				return invalid("peer declaration class is invalid")
			}
			continue
		}
		if systemDecl(item.DeclID) || r.systemClass(decl.DefaultClass) {
			return invalid("system declaration is not allowed in channel recipes")
		}
	}
	return nil
}
func (r *Registrar) registerChannelTemplate(ctx context.Context, owner string, p ChannelTemplateRegister) (regspec.ChannelTemplateRow, error) {
	p.ID = strings.TrimSpace(p.ID)
	p.Name = strings.TrimSpace(p.Name)
	if p.ID == "" || p.Name == "" {
		return regspec.ChannelTemplateRow{}, invalid("id and name are both required to create a channel template")
	}
	if p.Visibility == "" {
		p.Visibility = "private"
	}
	if p.Visibility != "private" && p.Visibility != "public" {
		return regspec.ChannelTemplateRow{}, invalid(fmt.Sprintf("visibility %q is not valid: the only values are public (anyone may build from this template) and private (only you may, and this is the default)", p.Visibility))
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
		return regspec.ChannelTemplateRow{}, notFound("channel template", p.ID, "system.channel.template.list")
	}
	if row.Owner != caller {
		return regspec.ChannelTemplateRow{}, denied(fmt.Sprintf("channel template %q is owned by %q and you are acting as %q; only its owner may change it", p.ID, row.Owner, caller))
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
		return regspec.ChannelTemplateRow{}, invalid(fmt.Sprintf("the template would be left invalid: name is %q and visibility is %q, but a template needs a non-empty name and a visibility of public or private", row.Name, row.Visibility))
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
		return regspec.ChannelTemplateRow{}, notFound("channel template", p.ID, "system.channel.template.list")
	}
	if row.Owner != caller {
		return regspec.ChannelTemplateRow{}, denied(fmt.Sprintf("channel template %q is owned by %q and you are acting as %q; only its owner may change it", p.ID, row.Owner, caller))
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
		return regspec.ChannelRow{}, invalid("channel_id required: name the channel to act on; list them with system.channel.list")
	}
	if p.ChannelID == channelspec.C0ChannelID {
		return regspec.ChannelRow{}, reserved("c0 profile is fixed")
	}
	if source != channelspec.C0ChannelID && source != p.ChannelID {
		return regspec.ChannelRow{}, denied(fmt.Sprintf("a channel profile may only be changed from that channel or from the registry channel: this names channel_id %q but arrived from %q", p.ChannelID, source))
	}
	if p.Serving == nil || (*p.Serving != 0 && *p.Serving != 1) {
		return regspec.ChannelRow{}, invalid("serving must be 0 or 1")
	}
	err := r.registry.store.InTx(ctx, func(tx *store.Tx) error {
		if _, ok, err := tx.GetChannel(ctx, p.ChannelID); err != nil {
			return err
		} else if !ok {
			return notFound("channel", string(p.ChannelID), "system.channel.list")
		}
		return tx.ReplaceProfile(ctx, p.ChannelID, p.Description, *p.Serving)
	})
	if err != nil {
		return regspec.ChannelRow{}, err
	}
	row, ok, err := r.registry.GetChannelDesired(ctx, p.ChannelID)
	if err != nil {
		return regspec.ChannelRow{}, err
	}
	if !ok {
		return regspec.ChannelRow{}, notFound("channel", string(p.ChannelID), "system.channel.list")
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
		if systemCompanion(declaration.DeclID) {
			continue
		}
		item := regspec.TemplateDeclaration{DeclID: declaration.DeclID, Config: cloneJSON(declaration.Rendered.Config)}
		recipe.Declarations = append(recipe.Declarations, item)
	}
	profile := spec.Profile
	profile.Description = stringPtr(row.Description)
	profile.Serving = intPtr(row.Serving)
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
// able to request the system device listing therefore reads every device secret. Deliberate:
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

// ClassRow is one runnable class as a declaration author needs to see it: what
// to name in `class`, what kind of member that mints, and what `config` may
// contain. The schema is the class's own published contract, not a copy — a
// copy here would drift from the parser that actually rejects config.
type ClassRow struct {
	Class        string          `json:"class"`
	Kind         string          `json:"kind"`
	Placement    string          `json:"placement"`
	ConfigSchema json.RawMessage `json:"config_schema,omitempty"`
}

func (r *Registrar) readClasses() ([]ClassRow, error) {
	if r.classes == nil {
		return nil, &Error{Code: CodeResultUnknown, Detail: "the class catalog is not attached to this registry, so the runnable classes cannot be listed right now"}
	}
	names := r.classes.Classes()
	out := make([]ClassRow, 0, len(names))
	for _, class := range names {
		row := ClassRow{Class: class}
		if kind, ok := r.classes.LookupClassKind(class); ok {
			row.Kind = string(kind)
		}
		if placement, ok := r.classes.LookupClassPlacement(class); ok {
			row.Placement = string(placement)
		}
		if schema, ok := r.classes.ClassConfigSchema(class); ok {
			row.ConfigSchema = schema
		}
		out = append(out, row)
	}
	return out, nil
}

func (r *Registrar) readPrincipal(ctx context.Context, id string) (regspec.PrincipalRow, error) {
	row, found, err := r.registry.store.GetPresentPrincipal(ctx, id)
	if !found && err == nil {
		return regspec.PrincipalRow{}, notFound("principal", id, "system.principal.list")
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
