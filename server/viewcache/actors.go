package viewcache

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ActorSnapshot is a server-side projection of actor/type readiness from
// the synced channel log. The daemon-local actor_registry remains the
// write-side truth; this view is eventually consistent via view-sync.
type ActorSnapshot struct {
	ChannelID string         `json:"channel_id"`
	Actors    []ActorViewRow `json:"actors"`
}

// ActorViewRow is the SDK/list_actors HTTP shape.
type ActorViewRow struct {
	ActorID           string          `json:"actor_id"`
	Kind              string          `json:"kind,omitempty"`
	Binding           string          `json:"binding,omitempty"`
	DisplayName       string          `json:"display_name,omitempty"`
	Ready             bool            `json:"ready"`
	ReadyReason       string          `json:"ready_reason,omitempty"`
	ReadyDetail       json.RawMessage `json:"ready_detail,omitempty"`
	LastReadyAt       int64           `json:"last_ready_at,omitempty"`
	LastStateChangeAt int64           `json:"last_state_change_at,omitempty"`
	Types             []TypeViewRow   `json:"types,omitempty"`
}

// TypeViewRow is one installed type grouped under its handler actor.
type TypeViewRow struct {
	Type           string   `json:"type"`
	AllowedKinds   []string `json:"allowed_kinds,omitempty"`
	HandlerBinding string   `json:"handler_binding,omitempty"`
	MaxPendingMs   int64    `json:"max_pending_ms,omitempty"`
}

// ActorSnapshot reads system.type.installed, system.actor.*, and
// actor.readiness.changed events from the server view cache and folds
// them into a compact actor list.
func (s *Service) ActorSnapshot(ctx context.Context, channelID channel.ID) (ActorSnapshot, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT envelope_json
		  FROM view_cache_messages
		 WHERE channel_id = ?
		   AND (
		        envelope_json LIKE '%"type":"system.actor.registered"%'
		     OR envelope_json LIKE '%"type":"system.actor.deregistered"%'
		     OR envelope_json LIKE '%"type":"system.type.installed"%'
		     OR envelope_json LIKE '%"type":"actor.readiness.changed"%'
		   )
		 ORDER BY seq ASC`, string(channelID))
	if err != nil {
		return ActorSnapshot{}, fmt.Errorf("viewcache: actor snapshot query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	actors := map[string]*ActorViewRow{}
	type actorTypeSet map[string]TypeViewRow
	typesByActor := map[string]actorTypeSet{}

	ensureActor := func(id string) *ActorViewRow {
		if id == "" {
			id = "_unknown"
		}
		if existing := actors[id]; existing != nil {
			return existing
		}
		row := &ActorViewRow{
			ActorID:     id,
			ReadyReason: "unknown",
			ReadyDetail: json.RawMessage(`{}`),
		}
		actors[id] = row
		return row
	}

	for rows.Next() {
		var envJSON string
		if err := rows.Scan(&envJSON); err != nil {
			return ActorSnapshot{}, fmt.Errorf("viewcache: actor snapshot scan: %w", err)
		}
		var env message.Envelope
		if err := json.Unmarshal([]byte(envJSON), &env); err != nil {
			return ActorSnapshot{}, fmt.Errorf("viewcache: actor snapshot unmarshal envelope: %w", err)
		}
		switch env.Type {
		case "system.actor.registered":
			var payload struct {
				ActorID      string `json:"actor_id"`
				ActorKind    string `json:"actor_kind"`
				ActorBinding string `json:"actor_binding"`
				DisplayName  string `json:"display_name"`
			}
			if err := json.Unmarshal(env.Payload, &payload); err != nil {
				continue
			}
			a := ensureActor(payload.ActorID)
			a.Kind = payload.ActorKind
			a.Binding = payload.ActorBinding
			a.DisplayName = payload.DisplayName
		case "system.actor.deregistered":
			var payload struct {
				ActorID string `json:"actor_id"`
			}
			if err := json.Unmarshal(env.Payload, &payload); err == nil && payload.ActorID != "" {
				delete(actors, payload.ActorID)
				delete(typesByActor, payload.ActorID)
			}
		case "system.type.installed":
			var payload struct {
				Type           string   `json:"type"`
				AllowedKinds   []string `json:"allowed_kinds"`
				HandlerActorID string   `json:"handler_actor_id"`
				HandlerBinding string   `json:"handler_binding"`
				MaxPendingMs   int64    `json:"max_pending_ms"`
			}
			if err := json.Unmarshal(env.Payload, &payload); err != nil || payload.HandlerActorID == "" {
				continue
			}
			a := ensureActor(payload.HandlerActorID)
			if a.Kind == "" {
				a.Kind = "tool"
			}
			if a.Binding == "" {
				a.Binding = payload.HandlerBinding
			}
			set := typesByActor[payload.HandlerActorID]
			if set == nil {
				set = actorTypeSet{}
				typesByActor[payload.HandlerActorID] = set
			}
			set[payload.Type] = TypeViewRow{
				Type:           payload.Type,
				AllowedKinds:   append([]string(nil), payload.AllowedKinds...),
				HandlerBinding: payload.HandlerBinding,
				MaxPendingMs:   payload.MaxPendingMs,
			}
		case "actor.readiness.changed":
			var payload struct {
				ActorID   string `json:"actor_id"`
				ChangedAt int64  `json:"changed_at"`
				Current   struct {
					Ready             bool            `json:"ready"`
					Reason            string          `json:"reason"`
					Detail            json.RawMessage `json:"detail"`
					LastReadyAt       int64           `json:"last_ready_at"`
					LastStateChangeAt int64           `json:"last_state_change_at"`
				} `json:"current"`
			}
			if err := json.Unmarshal(env.Payload, &payload); err != nil || payload.ActorID == "" {
				continue
			}
			a := ensureActor(payload.ActorID)
			a.Ready = payload.Current.Ready
			a.ReadyReason = payload.Current.Reason
			if len(payload.Current.Detail) > 0 {
				a.ReadyDetail = append(json.RawMessage(nil), payload.Current.Detail...)
			}
			a.LastReadyAt = payload.Current.LastReadyAt
			a.LastStateChangeAt = payload.Current.LastStateChangeAt
			if a.LastStateChangeAt == 0 {
				a.LastStateChangeAt = payload.ChangedAt
			}
		}
	}
	if err := rows.Err(); err != nil {
		return ActorSnapshot{}, fmt.Errorf("viewcache: actor snapshot rows: %w", err)
	}

	out := make([]ActorViewRow, 0, len(actors))
	for id, a := range actors {
		if set := typesByActor[id]; len(set) > 0 {
			a.Types = make([]TypeViewRow, 0, len(set))
			for _, t := range set {
				a.Types = append(a.Types, t)
			}
			sort.Slice(a.Types, func(i, j int) bool { return a.Types[i].Type < a.Types[j].Type })
		}
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ActorID < out[j].ActorID })
	return ActorSnapshot{ChannelID: string(channelID), Actors: out}, nil
}
