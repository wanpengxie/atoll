package contract

import "encoding/json"

// Path and query DTOs make non-body request inputs part of the generated
// contract instead of collapsing them into anonymous "path"/"query" objects.
type ChannelPath struct {
	ChannelID string `json:"chID"`
}

type ChannelResourcePath struct {
	ChannelID  string `json:"chID"`
	ResourceID string `json:"rid"`
}

type ChannelActorPath struct {
	ChannelID string `json:"chID"`
	ActorID   string `json:"actorID"`
}

type ChannelDeclarationPath struct {
	ChannelID     string `json:"chID"`
	DeclarationID string `json:"declID"`
}

type DeclarationPath struct {
	DeclarationID string `json:"declID"`
}

type DaemonPath struct {
	DaemonID string `json:"id"`
}

type ChannelDaemonPath struct {
	ChannelID string `json:"chID"`
	DaemonID  string `json:"id"`
}

type ChannelListQuery struct {
	ParentID *string `json:"parent_id,omitempty"`
}

type MessagePageQuery struct {
	AfterSeq *int64 `json:"after_seq,omitempty"`
	Limit    *int   `json:"limit,omitempty"`
}

type ResourceListQuery struct {
	Prefix string `json:"prefix,omitempty"`
	Cursor string `json:"cursor,omitempty"`
	Limit  *int   `json:"limit,omitempty"`
}

// REST response DTOs. Handlers return these exact types, so changing a wire
// field necessarily changes the generated golden schema.
type Principal struct {
	ID          string `json:"id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
}

type OK struct {
	OK bool `json:"ok"`
}

type Channel struct {
	ID             string  `json:"id"`
	Name           string  `json:"name"`
	Type           string  `json:"type"`
	Status         string  `json:"status"`
	OwnerPrincipal string  `json:"owner_principal"`
	CreatedAt      int64   `json:"created_at"`
	ParentID       *string `json:"parent_id"`
	Changed        *bool   `json:"changed,omitempty"`
	DefaultAgent   string  `json:"default_agent,omitempty"`
}

type ChannelList struct {
	Channels []Channel `json:"channels"`
}

type ChannelDeletion struct {
	Status  string `json:"status,omitempty"`
	Changed bool   `json:"changed"`
}

type Candidate struct {
	UserID      string `json:"user_id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
}

type CandidateList struct {
	Candidates []Candidate `json:"candidates"`
}

type MessageRow struct {
	Envelope   json.RawMessage `json:"envelope"`
	Seq        int64           `json:"seq"`
	IsTerminal bool            `json:"is_terminal"`
}

type MessagePage struct {
	Messages          []MessageRow `json:"messages"`
	ScannedThroughSeq int64        `json:"scanned_through_seq"`
}

type Membership struct {
	ActorID string `json:"actor_id"`
	Changed bool   `json:"changed"`
}

type IntroduceActorResponse struct {
	Changed   bool     `json:"changed"`
	ActorID   string   `json:"actor_id,omitempty"`
	Instances []string `json:"instances"`
}

type RemoveActorResponse struct {
	Changed bool     `json:"changed"`
	Removed []string `json:"removed"`
}

type DeclarationInstance struct {
	ChannelID  string `json:"channel_id"`
	InstanceID string `json:"instance_id"`
}

type Declaration struct {
	ID         string                `json:"id"`
	Name       string                `json:"name"`
	Owner      string                `json:"owner"`
	Class      string                `json:"class"`
	Visibility string                `json:"visibility"`
	CreatedAt  int64                 `json:"created_at"`
	UpdatedAt  int64                 `json:"updated_at,omitempty"`
	Instances  []DeclarationInstance `json:"instances"`
}

type DeclarationList struct {
	Declarations []Declaration `json:"decls"`
}

type DeclarationMutation struct {
	Updated string `json:"updated,omitempty"`
	Deleted string `json:"deleted,omitempty"`
}

type Daemon struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	APIKey    string `json:"api_key,omitempty"`
	CreatedAt int64  `json:"created_at,omitempty"`
	Online    *bool  `json:"online,omitempty"`
}

type DaemonList struct {
	Daemons []Daemon `json:"daemons"`
}

type DaemonDeletion struct {
	OK                 bool   `json:"ok"`
	AuthorityCommitted bool   `json:"authority_committed"`
	Convergence        string `json:"convergence"`
	Diagnostics        any    `json:"diagnostics"`
}

type DaemonBinding struct {
	Bound            bool     `json:"bound"`
	Changed          bool     `json:"changed"`
	ClearedInstances []string `json:"cleared_instances,omitempty"`
}

type DeclarationOverlay struct {
	Updated string `json:"updated"`
}
