package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

type resourceRegistry struct {
	db    *sql.DB
	nowMs func() int64
}

func newResourceRegistry(db *sql.DB) *resourceRegistry {
	return &resourceRegistry{db: db, nowMs: func() int64 { return time.Now().UnixMilli() }}
}

func (r *resourceRegistry) Resolve(ctx context.Context, id resource.ResourceID) (resourcespec.ResourceMeta, bool, error) {
	var kind, createdBy string
	var createdAt int64
	err := r.db.QueryRowContext(ctx,
		`SELECT kind, created_by, created_at FROM resources WHERE resource_id=?`, string(id),
	).Scan(&kind, &createdBy, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return resourcespec.ResourceMeta{}, false, nil
	}
	if err != nil {
		return resourcespec.ResourceMeta{}, false, fmt.Errorf("store: resource resolve %q: %w", id, err)
	}
	return resourcespec.ResourceMeta{Kind: resourcespec.ResourceKind(kind), CreatedBy: actor.ActorID(createdBy), CreatedAt: createdAt}, true, nil
}

func (r *resourceRegistry) Create(ctx context.Context, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, initial []byte) error {
	if id == "" || creator == "" {
		return errors.New("store: resource create: id and creator required")
	}
	res, err := r.db.ExecContext(ctx,
		`INSERT INTO resources (resource_id, kind, bytes, created_by, created_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(resource_id) DO NOTHING`, string(id), string(kind), initial, string(creator), r.nowMs())
	if err != nil {
		return fmt.Errorf("store: resource create %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: resource create rows %q: %w", id, err)
	}
	if n == 0 {
		return resourcespec.ErrAlreadyExists
	}
	return nil
}

func (r *resourceRegistry) Delete(ctx context.Context, id resource.ResourceID) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM resources WHERE resource_id=?`, string(id))
	if err != nil {
		return fmt.Errorf("store: resource delete %q: %w", id, err)
	}
	return nil
}

func (r *resourceRegistry) List(ctx context.Context, prefix string, limit int, cursor string) ([]resourcespec.ResourceRow, string, error) {
	if limit <= 0 {
		return nil, "", errors.New("store: resource list: limit must be positive")
	}
	var afterCreatedAt int64
	var afterID string
	if cursor != "" {
		var ok bool
		afterCreatedAt, afterID, ok = decodeListCursor(cursor)
		if !ok {
			return nil, "", fmt.Errorf("store: resource list: %w", resourcespec.ErrMalformedCursor)
		}
	}
	q := `SELECT resource_id, kind, created_by, created_at FROM resources
	      WHERE (created_at > ? OR (created_at = ? AND resource_id > ?))`
	args := []any{afterCreatedAt, afterCreatedAt, afterID}
	if like, ok := likePrefix(prefix); ok {
		q += ` AND resource_id LIKE ? ESCAPE '\'`
		args = append(args, like)
	}
	q += ` ORDER BY created_at, resource_id LIMIT ?`
	args = append(args, limit)
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("store: resource list: %w", err)
	}
	defer rows.Close()
	var out []resourcespec.ResourceRow
	var lastAt int64
	var lastID string
	for rows.Next() {
		var id, kind, createdBy string
		if err := rows.Scan(&id, &kind, &createdBy, &lastAt); err != nil {
			return nil, "", err
		}
		lastID = id
		out = append(out, resourcespec.ResourceRow{ID: resource.ResourceID(id), Meta: resourcespec.ResourceMeta{
			Kind: resourcespec.ResourceKind(kind), CreatedBy: actor.ActorID(createdBy), CreatedAt: lastAt,
		}})
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) == limit {
		next = encodeListCursor(lastAt, lastID)
	}
	return out, next, nil
}

func likePrefix(prefix string) (string, bool) {
	if prefix == "" {
		return "", false
	}
	var b strings.Builder
	for _, ch := range prefix {
		if ch == '\\' || ch == '%' || ch == '_' {
			b.WriteByte('\\')
		}
		b.WriteRune(ch)
	}
	b.WriteByte('%')
	return b.String(), true
}

func encodeListCursor(createdAt int64, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(createdAt, 10) + "\x00" + id))
}

func decodeListCursor(cursor string) (int64, string, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, "", false
	}
	parts := strings.SplitN(string(raw), "\x00", 2)
	if len(parts) != 2 {
		return 0, "", false
	}
	n, err := strconv.ParseInt(parts[0], 10, 64)
	return n, parts[1], err == nil
}

type kvDriver struct{ db *sql.DB }

func newKVDriver(db *sql.DB) *kvDriver { return &kvDriver{db: db} }

func (d *kvDriver) Read(ctx context.Context, id resource.ResourceID) ([]byte, bool, error) {
	var raw []byte
	var isNull bool
	err := d.db.QueryRowContext(ctx, `SELECT bytes, bytes IS NULL FROM resources WHERE resource_id=?`, string(id)).Scan(&raw, &isNull)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: kv read %q: %w", id, err)
	}
	if isNull {
		return nil, false, nil
	}
	if raw == nil {
		raw = []byte{}
	}
	return raw, true, nil
}

func (d *kvDriver) Write(ctx context.Context, id resource.ResourceID, value []byte) error {
	res, err := d.db.ExecContext(ctx, `UPDATE resources SET bytes=? WHERE resource_id=?`, value, string(id))
	if err != nil {
		return fmt.Errorf("store: kv write %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return fmt.Errorf("store: kv write %q: resource vanished", id)
	}
	return nil
}

func (d *kvDriver) Delete(context.Context, resource.ResourceID) error { return nil }

var _ resourcespec.Registry = (*resourceRegistry)(nil)
var _ resourcespec.Driver = (*kvDriver)(nil)
