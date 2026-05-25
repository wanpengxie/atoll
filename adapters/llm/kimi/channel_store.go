package kimi

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/wanpengxie/ActOS/kernel/adapter"

	_ "modernc.org/sqlite" // register driver
)

// ChannelStore is a read-only live view onto the channel-local sqlite
// authoritative state (actor_registry / type_registry). The worker
// process opens it via COAGENT_CHANNEL_DB and snapshots channel state
// on demand — replacing the prior `worker-context.json` static file
// produced by the daemon at spawn time.
//
// "Live" here means "queried at the call site": ChannelStore does NOT
// cache rows. Callers decide their own freshness window (typically
// once at worker boot for tool / prompt build; later turns may
// re-query if the LLM tool layer supports dynamic registration —
// today go-kimi `AdditionalTools` is fixed at agent build time, so
// snapshots taken mid-session are informational only).
//
// channel.sqlite is opened with `mode=ro` so the worker cannot
// accidentally mutate channel state through this seam.
type ChannelStore struct {
	db   *sql.DB
	path string
}

// OpenChannelStore opens channel.sqlite at path in read-only mode.
// Empty path returns (nil, nil) — caller can treat absent store as
// "no channel context" and fall through to the legacy prompt path.
func OpenChannelStore(path string) (*ChannelStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	// Build a sqlite3 DSN with mode=ro so the connection rejects any
	// write attempt (defense-in-depth — the worker code path only
	// queries, but a future caller error would be caught at the driver).
	dsn := "file:" + path + "?" + url.Values{
		"mode":  []string{"ro"},
		"cache": []string{"shared"},
	}.Encode()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("kimi: open channel store %q: %w", path, err)
	}
	// Sanity ping — surface bad path immediately instead of on first query.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("kimi: ping channel store %q: %w", path, err)
	}
	return &ChannelStore{db: db, path: path}, nil
}

// Close releases the sqlite handle.
func (s *ChannelStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Snapshot reads the current actor_registry + type_registry rows. The
// returned ChannelContext mirrors the legacy JSON-file shape so kimi
// bridge consumers do not have to fork. Device actor connection state is
// intentionally not exposed through this channel store; server/devicebus
// owns the live route.
func (s *ChannelStore) Snapshot(ctx context.Context, channelID, channelType string) (ChannelContext, error) {
	out := ChannelContext{
		ChannelID:   channelID,
		ChannelType: channelType,
	}
	if s == nil || s.db == nil {
		return out, nil
	}

	if actors, err := s.listActors(ctx); err != nil {
		return out, err
	} else {
		out.Actors = actors
	}

	if types, err := s.listTypes(ctx); err != nil {
		return out, err
	} else {
		out.Types = types
	}
	if catalogs, err := s.listDeclarationCatalogs(ctx); err != nil {
		return out, err
	} else {
		mergeDeclarationCatalogs(&out, catalogs)
	}

	return out, nil
}

func (s *ChannelStore) listActors(ctx context.Context) ([]ActorInfo, error) {
	const q = `SELECT actor_id, actor_kind,
	                  COALESCE(actor_binding, ''),
	                  COALESCE(display_name, ''),
	                  COALESCE(ready_state, 'unknown'),
	                  COALESCE(ready_reason, 'unknown'),
	                  COALESCE(ready_detail, '{}'),
	                  COALESCE(last_ready_at, 0),
	                  COALESCE(last_state_change_at, 0)
	             FROM actor_registry
	            WHERE deregistered_at IS NULL
	         ORDER BY actor_id ASC`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("kimi: channel store list actors: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []ActorInfo
	for rows.Next() {
		var a ActorInfo
		var readyState, readyDetail string
		if err := rows.Scan(&a.ActorID, &a.Kind, &a.Binding, &a.DisplayName,
			&readyState, &a.ReadyReason, &readyDetail, &a.LastReadyAt, &a.LastStateChangeAt); err != nil {
			return nil, fmt.Errorf("kimi: channel store scan actor: %w", err)
		}
		a.Ready = readyState == "ready"
		a.ReadyDetail = json.RawMessage(readyDetail)
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kimi: channel store actor rows: %w", err)
	}
	return out, nil
}

func (s *ChannelStore) listDeclarationCatalogs(ctx context.Context) ([]adapter.DeclarationCatalog, error) {
	const q = `SELECT key, value
	             FROM adapter_state
	            WHERE substr(key, 1, 8) = 'adapter:'
	         ORDER BY key ASC`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("kimi: channel store list declaration catalogs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []adapter.DeclarationCatalog
	suffix := ":" + adapter.DeclarationConventionStateKey
	for rows.Next() {
		var (
			key string
			raw []byte
		)
		if err := rows.Scan(&key, &raw); err != nil {
			return nil, fmt.Errorf("kimi: channel store scan declaration catalog: %w", err)
		}
		if !strings.HasSuffix(key, suffix) {
			continue
		}
		var catalog adapter.DeclarationCatalog
		if err := json.Unmarshal(raw, &catalog); err != nil {
			return nil, fmt.Errorf("kimi: channel store decode declaration catalog %q: %w", key, err)
		}
		out = append(out, catalog)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kimi: channel store declaration catalog rows: %w", err)
	}
	return out, nil
}

func mergeDeclarationCatalogs(out *ChannelContext, catalogs []adapter.DeclarationCatalog) {
	if out == nil || len(catalogs) == 0 {
		return
	}
	actorIndex := make(map[string]int, len(out.Actors))
	for i := range out.Actors {
		actorIndex[strings.TrimSpace(out.Actors[i].ActorID)] = i
	}
	typeIndex := make(map[string]int, len(out.Types))
	for i := range out.Types {
		typeIndex[strings.TrimSpace(out.Types[i].Type)] = i
	}
	for _, catalog := range catalogs {
		actorID := strings.TrimSpace(string(catalog.ActorID))
		if actorID != "" {
			if idx, ok := actorIndex[actorID]; ok {
				out.Actors[idx].Description = catalog.Description
				out.Actors[idx].SkillDoc = catalog.SkillDoc
			}
		}
		for typeName, doc := range catalog.Types {
			idx, ok := typeIndex[strings.TrimSpace(typeName)]
			if !ok {
				continue
			}
			if actorID != "" && strings.TrimSpace(out.Types[idx].HandlerActorID) != actorID {
				continue
			}
			out.Types[idx].Description = doc.Description
			out.Types[idx].PayloadExample = cloneRawJSON(doc.PayloadExample)
			out.Types[idx].PayloadFields = append([]adapter.FieldDoc(nil), doc.PayloadFields...)
			out.Types[idx].ErrorCodes = append([]adapter.ErrorDoc(nil), doc.ErrorCodes...)
			out.Types[idx].Notes = doc.Notes
		}
	}
}

func (s *ChannelStore) listTypes(ctx context.Context) ([]TypeInfo, error) {
	const q = `SELECT type,
	                  COALESCE(handler_actor_id, ''),
	                  handler_binding,
	                  allowed_kinds,
	                  COALESCE(max_pending_ms, 0)
	             FROM type_registry
	            WHERE install_status = 'installed'
	         ORDER BY type ASC`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("kimi: channel store list types: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []TypeInfo
	for rows.Next() {
		var (
			t              TypeInfo
			allowedKindsJS string
		)
		if err := rows.Scan(&t.Type, &t.HandlerActorID, &t.HandlerBinding, &allowedKindsJS, &t.MaxPendingMs); err != nil {
			return nil, fmt.Errorf("kimi: channel store scan type: %w", err)
		}
		// allowed_kinds is stored as a JSON array string (e.g. `["request","response"]`)
		// per runtime/store/schema.go::type_registry. Tolerate empty / malformed
		// rows by leaving AllowedKinds nil.
		if strings.TrimSpace(allowedKindsJS) != "" {
			var kinds []string
			if err := json.Unmarshal([]byte(allowedKindsJS), &kinds); err == nil {
				t.AllowedKinds = kinds
			}
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kimi: channel store type rows: %w", err)
	}
	return out, nil
}
