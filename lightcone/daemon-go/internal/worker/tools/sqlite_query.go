package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
)

// MaxSQLiteQueryRows caps the result-set size we will materialise into
// the response payload. Workers that need more should paginate via
// LIMIT — the cap prevents a runaway query from filling the channel
// log with a multi-megabyte response row (which the wrapper would
// truncate anyway).
const MaxSQLiteQueryRows = 1_000

// defaultSQLiteQueryTimeout caps a single sqlite.query Execute. Aligned
// with the M1.3 type_registry MaxPendingMs default (5s) per the harness
// stalled-timeout convention — but applied here as a driver-level
// deadline so a runaway SELECT cannot keep the worker conn busy past
// the budget (claude 96-2 major: T103 / FIX D).
const defaultSQLiteQueryTimeout = 5 * time.Second

// NewSQLiteQueryTool builds a read-only sqlite.query tool. The tool
// answers `{sql, params?}` requests by running QueryContext on the
// caller-supplied sqlite handle and serialising the rows back as
// `{columns, rows}`. Each Execute is wrapped in a per-call
// `context.WithTimeout(defaultSQLiteQueryTimeout)` so a slow query
// surfaces a deadline_exceeded error instead of hanging the worker.
//
// Hard contracts:
//   - The supplied `db` MUST be a read-only `*sql.DB` (opened with
//     `OpenOptions{ReadOnly: true}` / DSN `mode=ro`). This is the
//     primary defence — SQLite rejects any DML at the driver level,
//     so a DML CTE (`WITH x AS (DELETE ... RETURNING) SELECT ...`)
//     fails regardless of validator quality. See R2-FIX-7 (t113).
//   - SQL MUST start with SELECT or WITH (case-insensitive); everything
//     else returns IsError=true `{reason: "sqlite_query_not_select"}`.
//   - The validator also tokenises the SQL (skipping string literals
//     and comments) and rejects any DML keyword at any nesting depth.
//     Belt-and-suspenders against the ro driver — if the catalog wiring
//     ever regresses to a writable handle, the validator still holds.
//   - Empty SQL returns IsError=true `{reason: "sqlite_query_empty"}`.
//   - Rows are scanned as `[]any` and JSON-marshalled — sqlite returns
//     []byte for TEXT/BLOB; we promote []byte → string when the bytes
//     parse as valid UTF-8 (the common case).
//
// go-kimi does not ship an sqlite tool, so this is the M1.3 baseline
// implementation per ticket T11 §交付物 "自实现 sqlite.query tool".
func NewSQLiteQueryTool(db *sql.DB) *SQLiteQueryTool {
	return &SQLiteQueryTool{db: db, timeout: defaultSQLiteQueryTimeout}
}

// SQLiteQueryTool implements the go-kimi tools.Tool interface for the
// `sqlite.query` v4 type.
type SQLiteQueryTool struct {
	db *sql.DB
	// timeout caps one Execute call. Constructor defaults to
	// defaultSQLiteQueryTimeout; tests set a short value directly to
	// exercise the deadline path.
	timeout time.Duration
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

	// Per-query deadline (FIX D / claude 96-2 major). We bound the
	// driver call directly here — type_registry MaxPendingMs is the
	// harness's stalled-pending observer, not a driver deadline. A
	// nonpositive timeout (test forgetting to set the field) falls
	// back to the documented default.
	queryTimeout := t.timeout
	if queryTimeout <= 0 {
		queryTimeout = defaultSQLiteQueryTimeout
	}
	qctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	driverArgs := make([]any, len(args.Params))
	copy(driverArgs, args.Params)

	rows, err := t.db.QueryContext(qctx, args.SQL, driverArgs...)
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

// dmlKeywords is the closed set of statement-level keywords the
// validator rejects at any nesting depth. SQLite ≥3.35 supports DML
// CTEs (`WITH x AS (DELETE ... RETURNING) SELECT * FROM x`) — the old
// `HasPrefix("WITH") && Contains("SELECT")` heuristic let those slip
// through. Tokenising the statement and refusing any of these names
// closes the LLM prompt-injection write path even if the underlying
// `*sql.DB` were ever wired to a writable handle by mistake. See
// R2-FIX-7 (t113).
var dmlKeywords = map[string]struct{}{
	"INSERT":   {},
	"UPDATE":   {},
	"DELETE":   {},
	"DROP":     {},
	"ALTER":    {},
	"CREATE":   {},
	"REPLACE":  {},
	"TRUNCATE": {},
}

// validateSelect enforces the SELECT-only rule. It accepts statements
// starting with SELECT or WITH (CTE) and rejects everything else.
// Additionally it tokenises the SQL — skipping string literals,
// quoted identifiers, and `--` / `/* */` comments — and rejects any
// DML keyword (INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/REPLACE/TRUNCATE)
// at any nesting depth. Belt-and-suspenders behind the ro `*sql.DB`
// driver-level enforcement (R2-FIX-7).
func validateSelect(sqlText string) *rejectErr {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return &rejectErr{"sqlite_query_empty", "sql is empty"}
	}
	upper := strings.ToUpper(trimmed)
	switch {
	case strings.HasPrefix(upper, "SELECT"), strings.HasPrefix(upper, "WITH"):
		// fall through to tokeniser
	default:
		return &rejectErr{"sqlite_query_not_select", "only SELECT / WITH ... SELECT statements are allowed"}
	}
	if kw := scanForDMLKeyword(trimmed); kw != "" {
		return &rejectErr{
			"sqlite_query_not_select",
			"DML keyword " + kw + " is not allowed (including inside CTE / subquery)",
		}
	}
	if strings.HasPrefix(upper, "WITH") {
		// Cheap heuristic: ensure the keyword SELECT appears somewhere
		// in the statement so an empty `WITH` shell with no read body
		// still falls back to a structured rejection.
		if !strings.Contains(upper, "SELECT") {
			return &rejectErr{"sqlite_query_not_select", "WITH clause without SELECT body is not allowed"}
		}
	}
	return nil
}

// scanForDMLKeyword tokenises sqlText and returns the first DML
// keyword it observes (uppercased), or "" if the statement is clean.
//
// Tokenisation rules (sqlite lexical conventions):
//   - `--` starts a line comment that ends at the next newline.
//   - `/* ... */` is a block comment (non-nesting per the SQL spec).
//   - `'...'` is a string literal; `''` inside is an escaped quote.
//   - `"..."` (and `[...]` / `` `...` `` per sqlite legacy) are quoted
//     identifiers; we treat their contents as opaque, not keywords.
//   - Outside the above, runs of `[A-Za-z_][A-Za-z0-9_]*` are tokens.
//
// We do NOT attempt to parse the AST — a single keyword match at any
// depth is enough to reject. Conversely, keywords that appear inside
// string literals (e.g. `SELECT 'DELETE FROM x' AS s`) are correctly
// ignored.
func scanForDMLKeyword(sqlText string) string {
	const (
		stateNormal = iota
		stateLineComment
		stateBlockComment
		stateSingleQuote
		stateDoubleQuote
		stateBacktick
		stateBracket
	)
	state := stateNormal
	var token strings.Builder
	flush := func() string {
		if token.Len() == 0 {
			return ""
		}
		kw := strings.ToUpper(token.String())
		token.Reset()
		if _, ok := dmlKeywords[kw]; ok {
			return kw
		}
		return ""
	}
	r := []byte(sqlText)
	for i := 0; i < len(r); i++ {
		c := r[i]
		switch state {
		case stateNormal:
			// Start of a comment?
			if c == '-' && i+1 < len(r) && r[i+1] == '-' {
				if kw := flush(); kw != "" {
					return kw
				}
				state = stateLineComment
				i++
				continue
			}
			if c == '/' && i+1 < len(r) && r[i+1] == '*' {
				if kw := flush(); kw != "" {
					return kw
				}
				state = stateBlockComment
				i++
				continue
			}
			// Start of a quoted span?
			switch c {
			case '\'':
				if kw := flush(); kw != "" {
					return kw
				}
				state = stateSingleQuote
				continue
			case '"':
				if kw := flush(); kw != "" {
					return kw
				}
				state = stateDoubleQuote
				continue
			case '`':
				if kw := flush(); kw != "" {
					return kw
				}
				state = stateBacktick
				continue
			case '[':
				if kw := flush(); kw != "" {
					return kw
				}
				state = stateBracket
				continue
			}
			// Identifier character?
			if isIdentChar(c) {
				token.WriteByte(c)
				continue
			}
			if kw := flush(); kw != "" {
				return kw
			}
		case stateLineComment:
			if c == '\n' {
				state = stateNormal
			}
		case stateBlockComment:
			if c == '*' && i+1 < len(r) && r[i+1] == '/' {
				state = stateNormal
				i++
			}
		case stateSingleQuote:
			if c == '\'' {
				// Doubled '' is an embedded quote, not a terminator.
				if i+1 < len(r) && r[i+1] == '\'' {
					i++
					continue
				}
				state = stateNormal
			}
		case stateDoubleQuote:
			if c == '"' {
				if i+1 < len(r) && r[i+1] == '"' {
					i++
					continue
				}
				state = stateNormal
			}
		case stateBacktick:
			if c == '`' {
				if i+1 < len(r) && r[i+1] == '`' {
					i++
					continue
				}
				state = stateNormal
			}
		case stateBracket:
			// sqlite `[ident]` quotes have no escape; first `]` closes.
			if c == ']' {
				state = stateNormal
			}
		}
	}
	if kw := flush(); kw != "" {
		return kw
	}
	return ""
}

// isIdentChar reports whether b can appear in a SQL identifier token
// (ASCII-only — SQLite accepts unicode identifiers but DML keywords
// are pure ASCII so the check is safe).
func isIdentChar(b byte) bool {
	switch {
	case b >= 'A' && b <= 'Z':
		return true
	case b >= 'a' && b <= 'z':
		return true
	case b >= '0' && b <= '9':
		return true
	case b == '_':
		return true
	}
	return false
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
