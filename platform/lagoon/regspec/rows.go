// Package regspec owns the row shapes stored in the channel-zero registry.
// It is a data-only leaf package; registry storage and lagoon business rules
// depend on these types, never the other way around.
package regspec

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type ChannelStatus string

const (
	ChannelPresent ChannelStatus = "present"
	ChannelRetired ChannelStatus = "retired"
)

type PrincipalStatus string

const (
	PrincipalPresent PrincipalStatus = "present"
	PrincipalRetired PrincipalStatus = "retired"
)

type DeclStatus string

const (
	DeclPresent DeclStatus = "present"
	DeclRevoked DeclStatus = "revoked"
)

type DeviceStatus string

const (
	DevicePresent DeviceStatus = "present"
	DeviceRetired DeviceStatus = "retired"
)

type CredentialStatus string

const (
	CredentialActive  CredentialStatus = "active"
	CredentialRetired CredentialStatus = "retired"
)

type ChannelRow struct {
	ID             channel.ID      `json:"id"`
	ParentID       channel.ID      `json:"parent_id,omitempty"`
	Name           string          `json:"name"`
	QualifiedName  string          `json:"qualified_name"`
	Type           string          `json:"type"`
	Status         ChannelStatus   `json:"status"`
	OwnerPrincipal string          `json:"owner_principal"`
	Spec           json.RawMessage `json:"spec"`
	CreatedAt      int64           `json:"created_at"`
}

type PrincipalRow struct {
	ID          string          `json:"id"`
	Kind        actor.Kind      `json:"kind"`
	Email       string          `json:"email,omitempty"`
	DisplayName string          `json:"display_name,omitempty"`
	Status      PrincipalStatus `json:"status"`
	CreatedAt   int64           `json:"created_at"`
}

type DeclRow struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Owner        string          `json:"owner"`
	DefaultClass string          `json:"default_class"`
	Config       json.RawMessage `json:"config,omitempty"`
	Status       DeclStatus      `json:"status"`
	Visibility   string          `json:"visibility"`
	CreatedAt    int64           `json:"created_at"`
	UpdatedAt    int64           `json:"updated_at"`
}

type OverlayRow struct {
	DeclID    string          `json:"decl_id"`
	ChannelID channel.ID      `json:"channel_id"`
	Config    json.RawMessage `json:"config,omitempty"`
	UpdatedAt int64           `json:"updated_at"`
}

type DeviceRow struct {
	ID             string `json:"id"`
	OwnerPrincipal string `json:"owner_principal"`
	Name           string `json:"name"`
	// WARNING (2026-08-13, known and accepted by the owner): Key is the
	// device's admission secret in cleartext, and it carries a json tag —
	// ANY read path that serializes this row as-is leaks it. The device.list
	// word does exactly that today (registrar.readDevices returns the row
	// unchanged), so every principal able to send that word can read every
	// device secret.
	//
	// This is deliberate, not an oversight. Secrets are not a first-class
	// carrier yet: everything secret-shaped (this key, an actor's config
	// credentials) rides the ordinary kv store, and hardening the whole
	// secret axis is a later batch (coral C2 "credentials end-to-end").
	// Confidentiality is not a leading constraint at this stage, while
	// seeing the key during install and debugging is genuinely useful.
	//
	// The fix, when the secret axis is built, is a reply type for that word
	// that omits this field (the credential side already does this).
	// Until then: a NEW read path must never emit this row as-is. The obs
	// daemons word builds its own projection without this field
	// (platform/lagoon/obsview.go).
	Key       string       `json:"key"`
	Status    DeviceStatus `json:"status"`
	CreatedAt int64        `json:"created_at"`
}

type BindingRow struct {
	ChannelID  channel.ID `json:"channel_id"`
	DeviceID   string     `json:"device_id"`
	AttachedAt int64      `json:"attached_at"`
}
