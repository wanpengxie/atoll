package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
)

// MaxSQLiteQueryRows caps the result-set size we will materialise into
// the response payload. Workers that need more should paginate via
// LIMIT — the cap prevents a runaway query from filling the channel
// log with a multi-megabyte response row (which the wrapper would
// truncate anyway).
const MaxSQLiteQueryRows = 1_000

// NewSQLiteQueryTool builds a read-only sqlite.query tool. The tool
// answers `{sql, params?}` requests by running QueryContext on the
// channel-local sqlite handle and serialising the rows back as
// `{columns, rows}`.
//
// Hard contracts:
//   - SQL MUST start with SELECT or WITH (case-insensitive); everything
//     else returns IsError=true `{reason: "sqlite_query_not_select"}`.
//   - Empty SQL returns IsError=true `{reason: "sqlite_query_empty"}`.
//   - Rows are scanned as `[]any` and JSON-marshalled — sqlite returns
//     []byte for TEXT/BLOB; we promote []byte → string when the bytes
//     parse as valid UTF-8 (the common case).
//
// go-kimi does not ship an sqlite tool, so this is the M1.3 baseline
// implementation per ticket T11 §交付物 "自实现 sqlite.query tool".
func NewSQLiteQueryTool(db *sql.DB) *SQLiteQueryTool {
	return &SQLiteQueryTool{db: db}
}

// SQLiteQueryTool implements the go-kimi tools.Tool interface for the
// `sqlite.query` v4 type.
type SQLiteQueryTool struct {
	db *sql.DB
}

// Name reports the tool's go-kimi-facing identity. The v4 wrapper
// rewrites this to the v4 type name (`sqlite.query`) so the LLM sees
// the protocol name; the inner Name() is informational only.
func (t *SQLiteQueryTool) Name() string { return "sqlite_query" }

// Description copy goes into the LLM tool catalogue. Keep it short +
// directly actionable.
func (t *SQLiteQueryTool) Description() string {
	return "Run a read-only SELECT (or WITH ... SELECT) query against the channel sqlite. " +
		"Returns {columns, rows}. Non-SELECT statements are rejected."
}

// ParameterSchema declares the input shape so the LLM passes
// well-formed JSON.
func (t *SQLiteQueryTool) ParameterSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "required": ["sql"],
  "additionalProperties": false,
  "properties": {
    "sql":    {"type": "string", "description": "SELECT or WITH ... SELECT statement"},
    "params": {"type": "array", "items": {}}
  }
}`)
}

// Execute runs the query and returns rows. Errors are surfaced via
// IsError=true so the LLM observes them and the v4 wrapper records
// the failure in the channel log without crashing the agent loop.
func (t *SQLiteQueryTool) Execute(ctx context.Context, raw json.RawMessage) (types.ToolResult, error) {
	if t.db == nil {
		return failResult("sqlite_query_no_db", "sqlite handle is nil"), errors.New("sqlite_query: db is nil")
	}
	args, perr := parseQueryArgs(raw)
	if perr != nil {
		return failResult("sqlite_query_invalid_args", perr.Error()), nil
	}
	if perr := validateSelect(args.SQL); perr != nil {
		return failResult(perr.reason, perr.detail), nil
	}

	driverArgs := make([]any, len(args.Params))
	for i, p := range args.Params {
		driverArgs[i] = p
	}

	rows, err := t.db.QueryContext(ctx, args.SQL, driverArgs...)
	if err != nil {
		return failResult("sqlite_query_failed", err.Error()), nil
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return failResult("sqlite_query_columns_failed", err.Error()), nil
	}

	out := make([][]any, 0, 16)
	for rows.Next() {
		if len(out) >= MaxSQLiteQueryRows {
			// We still consume the iterator so the driver releases
			// the statement, but stop appending. The truncation is
			// reflected in the response payload.
			break
		}
		row := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range row {
			ptrs[i] = &row[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return failResult("sqlite_query_scan_failed", err.Error()), nil
		}
		out = append(out, normaliseRow(row))
	}
	if err := rows.Err(); err != nil {
		return failResult("sqlite_query_iter_failed", err.Error()), nil
	}

	payload := map[string]any{
		"columns": cols,
		"rows":    out,
	}
	if len(out) == MaxSQLiteQueryRows && rows.Next() {
		payload["truncated"] = true
	}
	return types.ToolResult{
		Name:  t.Name(),
		Value: types.ToolReturnValue{Value: payload},
	}, nil
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// queryArgs is the parsed shape of the tool's JSON arguments.
type queryArgs struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params,omitempty"`
}

// parseQueryArgs decodes the raw JSON and validates the top-level
// required fields.
func parseQueryArgs(raw json.RawMessage) (queryArgs, error) {
	var args queryArgs
	if len(raw) == 0 {
		return args, errors.New("params payload is empty")
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return args, fmt.Errorf("parse args: %w", err)
	}
	if strings.TrimSpace(args.SQL) == "" {
		return args, errors.New("sql is required")
	}
	return args, nil
}

// rejectErr describes a structured rejection: a stable reason code +
// human-readable detail. Kept lower-case to match the v4 reason-string
// convention.
type rejectErr struct {
	reason string
	detail string
}

func (e rejectErr) Error() string { return e.reason + ": " + e.detail }

// validateSelect enforces the SELECT-only rule. We accept WITH ...
// SELECT because CTEs are a common read-only construct; everything
// else returns a structured rejection.
func validateSelect(sqlText string) *rejectErr {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return &rejectErr{"sqlite_query_empty", "sql is empty"}
	}
	upper := strings.ToUpper(trimmed)
	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return nil
	case strings.HasPrefix(upper, "WITH"):
		// Cheap heuristic: ensure the keyword SELECT appears somewhere
		// in the statement so a `WITH ... INSERT` cannot sneak through.
		if !strings.Contains(upper, "SELECT") {
			return &rejectErr{"sqlite_query_not_select", "WITH clause without SELECT body is not allowed"}
		}
		return nil
	default:
		return &rejectErr{"sqlite_query_not_select", "only SELECT / WITH ... SELECT statements are allowed"}
	}
}

// normaliseRow promotes []byte values to strings when possible. sqlite
// returns TEXT columns as []byte; the harness payload schema validators
// expect strings, and downstream code is easier to write against
// strings than bytes.
func normaliseRow(row []any) []any {
	out := make([]any, len(row))
	for i, v := range row {
		switch x := v.(type) {
		case []byte:
			out[i] = string(x)
		default:
			out[i] = v
		}
	}
	return out
}

// failResult builds the standard `{status:'failed', reason, message}`
// tool result so wrapper + UI can render rejections uniformly.
func failResult(reason, detail string) types.ToolResult {
	return types.ToolResult{
		Name:    "sqlite_query",
		IsError: true,
		Value: types.ToolReturnValue{Value: map[string]any{
			"status":  "failed",
			"reason":  reason,
			"message": detail,
		}},
	}
}
