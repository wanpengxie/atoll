package introspect

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/protocol/message"
)

// SystemWordSpecs is the descriptive half of the closed system vocabulary.
// The protocol package owns which words exist and where they live; the
// manifest layer owns the human-facing contract projected by actor.describe.
func SystemWordSpecs() map[string]WordSpec {
	return map[string]WordSpec{
		message.TypeSystemChannelCreate:         systemWord("Create a channel from an inline recipe.", "name (string), recipe (object).", objectSchema(`"name":{"type":"string"},"recipe":{"type":"object"}`, "name", "recipe")),
		message.TypeSystemChannelGet:            systemWord("Get one channel's registered facts.", "channel_id (string).", objectSchema(`"channel_id":{"type":"string"}`, "channel_id")),
		message.TypeSystemChannelList:           systemWord("List registered channels, optionally below one parent.", "parent_id (optional string).", objectSchema(`"parent_id":{"type":"string"}`)),
		message.TypeSystemChannelSet:            systemWord("Update a channel's profile.", "channel_id, description (strings), serving (integer 0 or 1).", objectSchema(`"channel_id":{"type":"string"},"description":{"type":"string"},"serving":{"type":"integer","enum":[0,1]}`, "channel_id", "description", "serving")),
		message.TypeSystemChannelDelete:         systemWord("Retire a channel.", "channel_id (string).", objectSchema(`"channel_id":{"type":"string"}`, "channel_id")),
		message.TypeSystemChannelTemplateCreate: systemWord("Create a reusable channel template.", "id, name (strings), body (object); description and visibility optional.", objectSchema(`"id":{"type":"string"},"name":{"type":"string"},"description":{"type":"string"},"visibility":{"type":"string","enum":["private","public"]},"body":{"type":"object"}`, "id", "name", "body")),
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
		message.TypeSystemPrincipalGet:          systemWord("Get the effective caller's principal facts.", "No parameters.", objectSchema("")),
		message.TypeSystemPrincipalList:         systemWord("List principals visible to the caller.", "No parameters.", objectSchema("")),
		message.TypeSystemCredentialSet:         systemWord("Replace a principal's credential hash.", "principal_id, secret_hash (strings).", objectSchema(`"principal_id":{"type":"string"},"secret_hash":{"type":"string"}`, "principal_id", "secret_hash")),
		message.TypeSystemDeviceCreate:          systemWord("Create a device owned by the caller.", "name (string).", objectSchema(`"name":{"type":"string"}`, "name")),
		message.TypeSystemDeviceAttach:          systemWord("Attach a device to a channel.", "device_id, channel_id (strings).", objectSchema(`"device_id":{"type":"string"},"channel_id":{"type":"string"}`, "device_id", "channel_id")),
		message.TypeSystemDeviceDetach:          systemWord("Detach a device from a channel.", "device_id, channel_id (strings).", objectSchema(`"device_id":{"type":"string"},"channel_id":{"type":"string"}`, "device_id", "channel_id")),
		message.TypeSystemDeviceList:            systemWord("List devices visible to the caller.", "No parameters.", objectSchema("")),
		message.TypeSystemDeviceDelete:          systemWord("Retire a device.", "device_id (string).", objectSchema(`"device_id":{"type":"string"}`, "device_id")),
		message.TypeSystemMemberCreate:          systemWord("Create this channel's member from an actor declaration.", "decl_id (string).", objectSchema(`"decl_id":{"type":"string"}`, "decl_id")),
		message.TypeSystemMemberAdmit:           systemWord("Admit a principal as a human member of this channel.", "principal (string).", objectSchema(`"principal":{"type":"string"}`, "principal")),
		message.TypeSystemMemberList:            systemWord("List this channel's current members and presence facts.", "No parameters.", objectSchema("")),
		message.TypeSystemMemberGet:             systemWord("Get one member's membership and presence facts.", "member (string actor id or unambiguous segment).", objectSchema(`"member":{"type":"string"}`, "member")),
		message.TypeSystemMemberDelete:          systemWord("Remove a member from this channel.", "member (string actor id or unambiguous segment).", objectSchema(`"member":{"type":"string"}`, "member")),
		message.TypeSystemMemberRestart:         systemWord("Restart a live member's cell generation.", "member (string actor id or unambiguous segment).", objectSchema(`"member":{"type":"string"}`, "member")),
		message.TypeSystemLogRecent:             systemWord("Read the most recent request and response ledger rows.", "limit (integer from 1 through 5).", objectSchema(`"limit":{"type":"integer","minimum":1,"maximum":5}`, "limit")),
		message.TypeSystemMemberCreated:         systemWord("Record that a channel member was created.", "Event payload is emitted by the platform; callers do not send it.", objectSchema(``)),
		message.TypeSystemMemberDeleted:         systemWord("Record that a channel member was deleted.", "Event payload is emitted by the platform; callers do not send it.", objectSchema(``)),
		message.TypeSystemChannelInbound:        systemWord("Record that a peer request entered the channel.", "Event payload is emitted by the platform; callers do not send it.", objectSchema(``)),
	}
}

func systemWord(description, params string, schema json.RawMessage) WordSpec {
	return WordSpec{Description: description + "\nParameters: " + params, InputSchema: schema}
}

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
