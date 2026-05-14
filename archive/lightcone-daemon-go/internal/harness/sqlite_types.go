package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// MemoryTypeLookup is an in-memory cache of type_registry rows with
// pre-compiled JSON schemas. The harness reads from this rather than
// hitting sqlite per request — schema compilation is expensive enough
// to amortise once at channel start-up + on each install.
//
// Build one of these via LoadTypeLookup; tests may construct directly
// via NewMemoryTypeLookup.
type MemoryTypeLookup struct {
	mu    sync.RWMutex
	types map[string]*pkgharness.TypeInfo
}

// NewMemoryTypeLookup builds an empty cache. Callers seed it via Put or
// via the LoadTypeLookup helper that reads from sqlite.
func NewMemoryTypeLookup() *MemoryTypeLookup {
	return &MemoryTypeLookup{types: make(map[string]*pkgharness.TypeInfo)}
}

// Put inserts (or overwrites) a TypeInfo entry. Caller compiled schemas
// already; tests use this when constructing fixtures.
func (m *MemoryTypeLookup) Put(info *pkgharness.TypeInfo) {
	if info == nil || info.Type == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.types[info.Type] = info
}

// Get returns the TypeInfo entry for typeName, (nil, false) when absent.
func (m *MemoryTypeLookup) Get(typeName string) (*pkgharness.TypeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.types[typeName]
	return t, ok
}

// LoadTypeLookup reads every row out of type_registry, compiles the
// per-kind schemas and returns a populated MemoryTypeLookup. Bindings
// call this at channel startup; callers that mutate type_registry (e.g.
// type migration) should rebuild the lookup afterwards.
func LoadTypeLookup(ctx context.Context, db *sql.DB) (*MemoryTypeLookup, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT type, allowed_kinds, schemas_by_kind, handler_binding,
		        terminal_convention, handler_actor_id
		   FROM type_registry`,
	)
	if err != nil {
		return nil, fmt.Errorf("load_type_lookup: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := NewMemoryTypeLookup()
	for rows.Next() {
		var (
			typeName       string
			allowedKindsJS string
			schemasJSON    string
			binding        string
			terminal       string
			handlerActor   sql.NullString
		)
		if err := rows.Scan(&typeName, &allowedKindsJS, &schemasJSON, &binding, &terminal, &handlerActor); err != nil {
			return nil, fmt.Errorf("load_type_lookup: scan: %w", err)
		}
		var allowed []string
		if err := json.Unmarshal([]byte(allowedKindsJS), &allowed); err != nil {
			return nil, fmt.Errorf("load_type_lookup: parse allowed_kinds for %q: %w", typeName, err)
		}
		kinds := make([]v4types.Kind, 0, len(allowed))
		for _, k := range allowed {
			kinds = append(kinds, v4types.Kind(k))
		}
		compiled, cerr := compileSchemas(typeName, schemasJSON)
		if cerr != nil {
			return nil, cerr
		}
		info := &pkgharness.TypeInfo{
			Type:               typeName,
			AllowedKinds:       kinds,
			HandlerBinding:     binding,
			TerminalConvention: terminal,
			Schemas:            compiled,
		}
		if handlerActor.Valid {
			info.HandlerActorID = handlerActor.String
		}
		out.Put(info)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load_type_lookup: rows: %w", err)
	}
	return out, nil
}

// compileSchemas compiles each per-kind JSON Schema doc inside the
// `schemas_by_kind` JSON object. The returned map has v4types.Kind keys
// (matching the harness's lookup signature) and nil values for any
// per-kind entry that fails to compile — the harness treats nil as
// "no schema declared" so a malformed entry does not block writes
// (Install already pre-validates schemas; a malformed entry here is a
// programming bug worth surfacing in logs but not crashing the binding).
func compileSchemas(typeName, schemasJSON string) (map[v4types.Kind]*jsonschema.Schema, error) {
	if strings.TrimSpace(schemasJSON) == "" {
		return nil, errors.New("schemas_by_kind is empty")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(schemasJSON), &raw); err != nil {
		return nil, fmt.Errorf("compile_schemas[%s]: parse: %w", typeName, err)
	}
	out := make(map[v4types.Kind]*jsonschema.Schema, len(raw))
	for kind, doc := range raw {
		url := fmt.Sprintf("type://%s/%s", typeName, kind)
		c := jsonschema.NewCompiler()
		if err := c.AddResource(url, strings.NewReader(string(doc))); err != nil {
			return nil, fmt.Errorf("compile_schemas[%s.%s]: add resource: %w", typeName, kind, err)
		}
		schema, err := c.Compile(url)
		if err != nil {
			return nil, fmt.Errorf("compile_schemas[%s.%s]: compile: %w", typeName, kind, err)
		}
		out[v4types.Kind(kind)] = schema
	}
	return out, nil
}
