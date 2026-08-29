package introspect

import (
	"encoding/json"
	"strings"

	"github.com/wanpengxie/atoll/protocol/message"
)

// SystemWordSpecs is the descriptive half of the closed system vocabulary.
// The protocol package owns which words exist and where they live; the
// manifest layer owns the human-facing contract projected by actor.describe.
func SystemWordSpecs() map[string]WordSpec {
	return map[string]WordSpec{
		message.TypeSystemChannelCreate: systemWordWithExamples(
			"Create a child channel from this channel. You must explicitly choose which active actors from the current channel are copied into its genesis; names such as root or steward are not actor ids.",
			"name (string), recipe (object), initial_actor_ids (array of full actor ids from system.member.list; [] explicitly creates no copied seats). A channel whose recipe names no svc_agent accepts nothing through its service door afterwards.",
			objectSchema(channelNameSchema+","+recipeSchema+","+initialActorIDsSchema, "name", "recipe", "initial_actor_ids"),
			recipeExampleMinimal, recipeExampleServing),
		message.TypeSystemChannelGet:        systemWord("Get one channel's registered facts.", "channel_id (string).", objectSchema(`"channel_id":{"type":"string"}`, "channel_id")),
		message.TypeSystemChannelList:       systemWord("List registered channels, optionally below one parent.", "parent_id (optional string).", objectSchema(`"parent_id":{"type":"string"}`)),
		message.TypeSystemChannelSet:        systemWord("Update a channel's profile.", "channel_id, description (strings), serving (integer 0 or 1); default_storage_device_id optionally names the attached device whose channel directory the file UI opens first.", objectSchema(`"channel_id":{"type":"string"},"description":{"type":"string"},"serving":{"type":"integer","enum":[0,1]},"default_storage_device_id":{"type":"string"}`, "channel_id", "description", "serving")),
		message.TypeSystemChannelDeviceList: systemWord("List the devices attached to the channel this request comes from. This is the authoritative source for file, terminal, and desired_host device choices; system.device.list is only the space-wide inventory.", "No parameters.", objectSchema("")),
		message.TypeSystemChannelDelete:     systemWord("Retire a channel.", "channel_id (string).", objectSchema(`"channel_id":{"type":"string"}`, "channel_id")),
		message.TypeSystemChannelTemplateCreate: systemWordWithExamples(
			"Create a reusable channel template. Its body is the same recipe system.channel.create takes inline.",
			"id, name (strings), body (object); description and visibility optional. visibility defaults to private, and a private template can only be used by its owner.",
			objectSchema(`"id":{"type":"string"},"name":{"type":"string"},"description":{"type":"string"},"visibility":{"type":"string","enum":["private","public"]},`+strings.Replace(recipeSchema, `"recipe":`, `"body":`, 1), "id", "name", "body"),
			templateBodyExample),
		message.TypeSystemChannelTemplateGet:    systemWord("Get one channel template.", "id (string).", objectSchema(`"id":{"type":"string"}`, "id")),
		message.TypeSystemChannelTemplateList:   systemWord("List channel templates visible to the caller.", "No parameters.", objectSchema("")),
		message.TypeSystemChannelTemplateSet:    systemWord("Update fields of a channel template.", "id (string); name, description, visibility, body are optional.", objectSchema(`"id":{"type":"string"},"name":{"type":"string"},"description":{"type":"string"},"visibility":{"type":"string"},"body":{"type":"object"}`, "id")),
		message.TypeSystemChannelTemplateDelete: systemWord("Retire a channel template.", "id (string).", objectSchema(`"id":{"type":"string"}`, "id")),
		message.TypeSystemActorTemplateCreate:   systemWord("Create an actor declaration template.", "id, name, class (strings); description, config, visibility, singleton optional.", objectSchema(`"id":{"type":"string"},"name":{"type":"string"},"description":{"type":"string"},"class":{"type":"string"},"config":{"type":"object"},"visibility":{"type":"string","enum":["private","public"]},"singleton":{"type":"boolean"}`, "id", "name", "class")),
		message.TypeSystemActorTemplateGet:      systemWord("Get one actor declaration template.", "id (string).", objectSchema(`"id":{"type":"string"}`, "id")),
		message.TypeSystemActorTemplateList:     systemWord("List actor declaration templates visible to the caller.", "No parameters.", objectSchema("")),
		message.TypeSystemActorTemplateSet:      systemWord("Update fields of an actor declaration template.", "id (string); name, description, class, config, visibility, singleton optional.", objectSchema(`"id":{"type":"string"},"name":{"type":"string"},"description":{"type":"string"},"class":{"type":"string"},"config":{"type":"object"},"visibility":{"type":"string"},"singleton":{"type":"boolean"}`, "id")),
		message.TypeSystemActorTemplateDelete:   systemWord("Retire an actor declaration template.", "id (string).", objectSchema(`"id":{"type":"string"}`, "id")),
		message.TypeSystemActorOverlaySet:       systemWord("Set a channel-specific config overlay for an actor declaration.", "decl_id, channel_id (strings), config (object).", objectSchema(`"decl_id":{"type":"string"},"channel_id":{"type":"string"},"config":{"type":"object"}`, "decl_id", "channel_id", "config")),
		message.TypeSystemActorOverlayDelete:    systemWord("Remove a channel-specific actor declaration overlay.", "decl_id, channel_id (strings).", objectSchema(`"decl_id":{"type":"string"},"channel_id":{"type":"string"}`, "decl_id", "channel_id")),
		message.TypeSystemPrincipalCreate:       systemWord("Create a principal account.", "email, secret_hash (strings); id and display_name optional.", objectSchema(`"id":{"type":"string"},"email":{"type":"string"},"secret_hash":{"type":"string"},"display_name":{"type":"string"}`, "email", "secret_hash")),
		message.TypeSystemPrincipalLogin:        systemWord("Authenticate a principal by email and password.", "email, password (strings).", objectSchema(`"email":{"type":"string"},"password":{"type":"string"}`, "email", "password")),
		message.TypeSystemPrincipalDelete:       systemWord("Retire a principal account.", "principal_id (string).", objectSchema(`"principal_id":{"type":"string"}`, "principal_id")),
		message.TypeSystemPrincipalGet:          systemWord("Get YOUR OWN principal facts. You are the effective caller, so this answers about you — it is NOT a lookup of whoever sent you the request you are handling. Use system.member.get or system.principal.list to learn about somebody else.", "No parameters.", objectSchema("")),
		message.TypeSystemPrincipalList:         systemWord("List principals visible to the caller.", "No parameters.", objectSchema("")),
		message.TypeSystemCredentialSet:         systemWord("Replace a principal's credential hash.", "principal_id, secret_hash (strings).", objectSchema(`"principal_id":{"type":"string"},"secret_hash":{"type":"string"}`, "principal_id", "secret_hash")),
		message.TypeSystemDeviceCreate:          systemWord("Create a device owned by the caller.", "name (string).", objectSchema(`"name":{"type":"string"}`, "name")),
		message.TypeSystemDeviceAttach:          systemWord("Attach a device to a channel.", "device_id, channel_id (strings).", objectSchema(`"device_id":{"type":"string"},"channel_id":{"type":"string"}`, "device_id", "channel_id")),
		message.TypeSystemDeviceDetach:          systemWord("Detach a device from a channel.", "device_id, channel_id (strings).", objectSchema(`"device_id":{"type":"string"},"channel_id":{"type":"string"}`, "device_id", "channel_id")),
		message.TypeSystemDeviceList:            systemWord("List devices visible to the caller.", "No parameters.", objectSchema("")),
		message.TypeSystemDeviceDelete:          systemWord("Retire a device.", "device_id (string).", objectSchema(`"device_id":{"type":"string"}`, "device_id")),
		message.TypeSystemClassList:             systemWord("List the actor classes this node can run, each with the config shape its declarations must follow. Read this before writing an actor template: class decides what config means, and config is decoded strictly, so a field the schema does not list is rejected. A class with no config_schema takes no config at all.", "No parameters.", objectSchema("")),
		message.TypeSystemMemberCreate:          systemWord("Create a member of the channel this request reaches, from an actor declaration. Sent to your own system door it acts on your channel; sent to a peer it acts on that peer's channel.", "decl_id (string): an actor template visible to you. desired_host (string, optional): the stable id of one of this channel's attached devices; discover them with system.channel.device.list. Omitted placement uses local-device when attached, or the sole attached device; otherwise it is rejected instead of guessed. Only classes that run on a device accept it.", objectSchema(`"decl_id":{"type":"string"},"desired_host":{"type":"string"}`, "decl_id")),
		message.TypeSystemMemberAdmit:           systemWord("Admit a registered human principal as a human member of the channel this request reaches. Agent principals are rejected; agents and tools enter through their declarations.", "principal (string): a human principal id, not an actor id.", objectSchema(`"principal":{"type":"string"}`, "principal")),
		message.TypeSystemMemberList:            systemWord("List the current members and presence facts of the channel this request reaches. Sent to your own system door it lists your channel; sent to a peer it lists that peer's channel.", "No parameters.", objectSchema("")),
		message.TypeSystemMemberGet:             systemWord("Get one member's membership and presence facts.", "member (string actor id or unambiguous segment).", objectSchema(`"member":{"type":"string"}`, "member")),
		message.TypeSystemMemberDelete:          systemWord("Remove a member from the channel this request reaches.", "Exactly one of: member (string actor id or unambiguous segment), or decl_id (string): the actor template the member was seated from, which removes that template's single seat and fails as ambiguous if it has more than one.", objectSchema(`"member":{"type":"string"},"decl_id":{"type":"string"}`)),
		message.TypeSystemMemberRestart:         systemWord("Restart a live member's cell generation.", "member (string actor id or unambiguous segment).", objectSchema(`"member":{"type":"string"}`, "member")),
		message.TypeSystemMemberRestartAll: systemWord(
			"Break-glass recovery: give every working member of THIS channel a fresh term, in place. Nothing is deleted — the ledger, membership and workspace are untouched, and each member keeps its actor id. Only agent and tool members restart; humans, the system actor, the registrar, the service door and channel peers are left alone, because none of them holds work a restart would recover. A member that fails to restart does not stop the others, and the answer names who restarted, who failed and who was skipped. NOTE: this does not close the requests those members were serving — those stay open until their own deadline.",
			"No parameters.",
			objectSchema("")),
		message.TypeSystemTimerSet: systemWord(
			"Set an alarm for yourself: at the chosen instant the substrate delivers a message you compose now, authored by you and addressed to you. An agent that receives one wakes into an ordinary turn; other actors receive it as an event. The alarm belongs to the caller — subject may name another member on system.timer.list only. Channel-local: this word does not cross a channel membrane, so a caller from another channel asks a member HERE to act instead of arming an alarm it could never own.",
			"exactly one of duration_ms (positive integer, milliseconds from now) or fire_at (integer, absolute unix milliseconds); msg_type (string, the type of the message the alarm will deliver); payload (optional object, delivered byte for byte); home (optional \"durable\" or \"memory\", default durable — durable survives a restart, memory does not).",
			objectSchema(`"duration_ms":{"type":"integer","minimum":1},"fire_at":{"type":"integer"},"msg_type":{"type":"string"},"payload":{"type":"object"},"home":{"type":"string","enum":["durable","memory"]},"subject":{"type":"string"}`, "msg_type")),
		message.TypeSystemTimerCancel: systemWord(
			"Cancel one of your pending alarms. existed=false means it had already fired, never existed, or is not yours — the three are deliberately indistinguishable, so no caller can probe for another member's alarms.",
			"timer_id (string).",
			objectSchema(`"timer_id":{"type":"string"},"subject":{"type":"string"}`, "timer_id")),
		message.TypeSystemTimerList: systemWord(
			"List pending alarms, earliest first. Answers with the coordinates only (timer_id, fire_at, msg_type, home) and never the payload: this reads which alarms exist, not what they will say. subject may name any active member of this channel — a channel is one permission boundary.",
			"subject (optional full actor id; omit for your own alarms).",
			objectSchema(`"subject":{"type":"string"}`)),
		message.TypeSystemLogRecent:      systemWord("Read the most recent complete conversation turns as their projected ledger rows (request, terminal, latest provisional of an open request); housekeeping words such as actor.describe, agent.context and system.* are excluded.", "limit (integer from 1 through 20): how many complete conversation turns.", objectSchema(`"limit":{"type":"integer","minimum":1,"maximum":20}`, "limit")),
		message.TypeSystemMemberCreated:  systemWord("Record that a channel member was created.", "Event payload is emitted by the platform; callers do not send it.", objectSchema(``)),
		message.TypeSystemMemberDeleted:  systemWord("Record that a channel member was deleted.", "Event payload is emitted by the platform; callers do not send it.", objectSchema(``)),
		message.TypeSystemChannelInbound: systemWord("Record that a peer request entered the channel.", "Event payload is emitted by the platform; callers do not send it.", objectSchema(``)),
	}
}

func systemWord(description, params string, schema json.RawMessage) WordSpec {
	return WordSpec{Description: description + "\nParameters: " + params, InputSchema: schema}
}

// systemWordWithExamples is for the words whose argument is a nested structure
// rather than a few scalars. A schema describes such an argument correctly and
// still leaves a caller guessing at the arrangement; a worked example does not.
// The recipe words are the ones that cost real attempts to guess: one observed
// session spent five failed submissions on system.channel.create alone.
func systemWordWithExamples(description, params string, schema json.RawMessage, examples ...string) WordSpec {
	spec := systemWord(description, params, schema)
	for _, example := range examples {
		spec.Examples = append(spec.Examples, json.RawMessage(example))
	}
	return spec
}

// channelNameSchema carries the name law from lagoon.ValidateName as a pattern.
// The law is enforced there and stated here; a caller that reads this cannot
// send the rejected form, and one that cannot read it has no other source —
// the previous schema said only "string", and the first thing an agent tried
// was a non-ASCII name.
const channelNameSchema = `"name":{"type":"string","pattern":"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$","description":"1-63 chars, lowercase a-z, 0-9 and '-', not starting or ending with '-'"}`

const initialActorIDsSchema = `"initial_actor_ids":{"type":"array","description":"explicit active members copied from the source channel; use complete ids returned by system.member.list, and send [] for none","maxItems":64,"uniqueItems":true,"items":{"type":"string"}}`

// recipeSchema is the inline channel recipe: the declarations a new channel is
// born with, plus the service profile that decides whether anyone outside can
// reach it. It is spelled out here because it exists nowhere a caller can read
// — regspec.TemplateBody is a Go type, and the word previously published it as
// a bare object.
const recipeSchema = `"recipe":{"type":"object","additionalProperties":false,"description":"what the new channel is born with","properties":{` +
	`"declarations":{"type":"array","description":"members minted at genesis; each decl_id must name an actor template visible to you (public, or one you own)","items":{"type":"object","additionalProperties":false,"required":["decl_id"],"properties":{"decl_id":{"type":"string"},"config":{"type":"object","description":"per-channel config overlay for this declaration"}}}},` +
	`"profile":{"type":"object","additionalProperties":false,"description":"how the channel serves callers from outside","properties":{` +
	`"description":{"type":"string"},` +
	`"default_storage_device_id":{"type":"string","description":"attached device whose channel directory file views open first; defaults to local-device"},` +
	`"serving":{"type":"integer","enum":[0,1],"description":"1 makes the channel reachable through its service door"},` +
	`"svc_agent":{"type":"string","description":"which agent answers agent.ask from outside. Must be a decl_id listed in declarations above and that declaration must be an agent, or the literal \"default\" to use the first active agent. Omit it and the channel answers nothing from outside."},` +
	`"endpoints":{"type":"object","description":"extra words the service door accepts, mapped to the declaration that answers them","additionalProperties":{"type":"object","required":["receiver"],"properties":{"description":{"type":"string"},"receiver":{"type":"string"},"schema":{"type":"object"},"examples":{"type":"array"}}}}}}}}`

const recipeExampleMinimal = `{"name":"research","recipe":{"declarations":[],"profile":{"description":"a channel with no members and no service door"}},"initial_actor_ids":[]}`

const recipeExampleServing = `{"name":"research","recipe":{"declarations":[{"decl_id":"my-analyst"}],"profile":{"description":"analysis workspace","serving":1,"svc_agent":"my-analyst"}},"initial_actor_ids":["human:root:1787128257816","agent:steward:1787487131255"]}`

const templateBodyExample = `{"id":"team-channel","name":"Team channel","visibility":"public","body":{"declarations":[{"decl_id":"my-analyst"}],"profile":{"description":"a team workspace","serving":1,"svc_agent":"my-analyst"}}}`

func objectSchema(properties string, required ...string) json.RawMessage {
	shape := map[string]any{"type": "object", "additionalProperties": false}
	if properties != "" {
		var props map[string]any
		_ = json.Unmarshal([]byte("{"+properties+"}"), &props)
		shape["properties"] = props
	} else {
		shape["properties"] = map[string]any{}
	}
	if len(required) > 0 {
		shape["required"] = required
	}
	raw, _ := json.Marshal(shape)
	return raw
}
