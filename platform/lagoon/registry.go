package lagoon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"golang.org/x/crypto/bcrypt"

	_ "modernc.org/sqlite"
)

type Change struct {
	ChannelID   channel.ID
	Principal   string
	AllChannels bool
}

// Registry is the read face over the seven registry tables. Its concrete type
// also owns registrar's private write helpers, but no exported method mutates
// registry state.
type Registry struct {
	db       *sql.DB
	onCommit func(Change)
}

func Open(path string, onCommit func(Change)) (*Registry, error) {
	u := &url.URL{Scheme: "file", Path: path}
	// Reserve registrar write intent at BeginTx; registry owns its file and lock
	// domain independently of every channel ledger.
	db, err := sql.Open("sqlite", u.String()+"?mode=rw&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		return nil, fmt.Errorf("lagoon: open registry: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return &Registry{db: db, onCommit: onCommit}, nil
}

func (r *Registry) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

func (r *Registry) VerifyCredential(ctx context.Context, email, presented string) (string, bool, error) {
	var id, hash string
	err := r.db.QueryRowContext(ctx, `SELECT p.id,c.secret_hash FROM principals p
		JOIN credentials c ON c.principal_id=p.id AND c.kind='password' AND c.status='active'
		WHERE p.email=? AND p.kind='human' AND p.status='present'`, email).Scan(&id, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(presented)) != nil {
		return "", false, nil
	}
	return id, true, nil
}

func (r *Registry) ResolveDeviceKey(ctx context.Context, key string) (string, bool, error) {
	var id string
	err := r.db.QueryRowContext(ctx, `SELECT id FROM devices WHERE key=? AND status='present'`, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return id, err == nil, err
}

func (r *Registry) GetDevice(ctx context.Context, id string) (DeviceRow, bool, error) {
	row, err := scanDevice(r.db.QueryRowContext(ctx, `SELECT id,owner_principal,name,key,status,created_at FROM devices WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return DeviceRow{}, false, nil
	}
	return row, err == nil, err
}

func (r *Registry) GetDeviceFact(ctx context.Context, id string) (DeviceStatus, bool, error) {
	var status DeviceStatus
	err := r.db.QueryRowContext(ctx, `SELECT status FROM devices WHERE id=?`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return status, err == nil, err
}

func (r *Registry) GetPrincipalStatus(ctx context.Context, id string) (PrincipalStatus, bool, error) {
	var status PrincipalStatus
	err := r.db.QueryRowContext(ctx, `SELECT status FROM principals WHERE id=?`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return status, err == nil, err
}

func (r *Registry) ListChannels(ctx context.Context) ([]ChannelRow, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id,parent_id,name,type,status,owner_principal,spec_json,created_at FROM channels ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChannelRow
	for rows.Next() {
		row, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (r *Registry) ListPresentChannels(ctx context.Context) ([]ChannelRow, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id,parent_id,name,type,status,owner_principal,spec_json,created_at FROM channels WHERE status='present' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChannelRow
	for rows.Next() {
		row, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (r *Registry) GetChannelDesired(ctx context.Context, id channel.ID) (ChannelRow, bool, error) {
	row, err := scanChannel(r.db.QueryRowContext(ctx, `SELECT id,parent_id,name,type,status,owner_principal,spec_json,created_at FROM channels WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return ChannelRow{}, false, nil
	}
	return row, err == nil, err
}

func (r *Registry) GetDecl(ctx context.Context, id string) (DeclRow, bool, error) {
	row, err := scanDecl(r.db.QueryRowContext(ctx, `SELECT id,name,owner,default_class,config_json,status,visibility,created_at,updated_at FROM decls WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return DeclRow{}, false, nil
	}
	return row, err == nil, err
}

func (r *Registry) ListDecls(ctx context.Context) ([]DeclRow, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id,name,owner,default_class,config_json,status,visibility,created_at,updated_at FROM decls WHERE status='present' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeclRow
	for rows.Next() {
		row, err := scanDecl(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (r *Registry) GetOverlays(ctx context.Context, ch channel.ID) ([]OverlayRow, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT o.decl_id,o.channel_id,o.config_json,o.updated_at
		FROM decl_overlays o JOIN decls d ON d.id=o.decl_id JOIN channels c ON c.id=o.channel_id
		WHERE o.channel_id=? AND d.status='present' AND c.status='present' ORDER BY o.decl_id`, ch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OverlayRow
	for rows.Next() {
		var v OverlayRow
		var raw sql.NullString
		if err := rows.Scan(&v.DeclID, &v.ChannelID, &raw, &v.UpdatedAt); err != nil {
			return nil, err
		}
		if raw.Valid {
			v.Config = json.RawMessage(raw.String)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (r *Registry) IsBound(ctx context.Context, ch channel.ID, device string) (bool, error) {
	var ok bool
	err := r.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM bindings b JOIN devices d ON d.id=b.device_id
		WHERE b.channel_id=? AND b.device_id=? AND d.status='present')`, ch, device).Scan(&ok)
	return ok, err
}

func (r *Registry) ListBoundDevices(ctx context.Context, ch channel.ID) ([]DeviceRow, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT d.id,d.owner_principal,d.name,d.key,d.status,d.created_at
		FROM bindings b JOIN devices d ON d.id=b.device_id
		WHERE b.channel_id=? AND d.status='present' ORDER BY d.id`, ch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeviceRow
	for rows.Next() {
		row, err := scanDevice(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// ListBoundDeviceIDs is the narrow placement boundary used by channel homes;
// device credentials and other registry columns never cross it.
func (r *Registry) ListBoundDeviceIDs(ctx context.Context, ch channel.ID) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT d.id
		FROM bindings b JOIN devices d ON d.id=b.device_id
		WHERE b.channel_id=? AND d.status='present' ORDER BY d.id`, ch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (r *Registry) listPrincipals(ctx context.Context) ([]PrincipalRow, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id,kind,email,display_name,status,created_at FROM principals WHERE status='present' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PrincipalRow
	for rows.Next() {
		row, err := scanPrincipal(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

type scanner interface{ Scan(...any) error }

func scanChannel(s scanner) (ChannelRow, error) {
	var row ChannelRow
	var parent sql.NullString
	var spec string
	err := s.Scan(&row.ID, &parent, &row.Name, &row.Type, &row.Status, &row.OwnerPrincipal, &spec, &row.CreatedAt)
	if parent.Valid {
		row.ParentID = channel.ID(parent.String)
	}
	row.Spec = json.RawMessage(spec)
	return row, err
}

func scanPrincipal(s scanner) (PrincipalRow, error) {
	var row PrincipalRow
	var kind string
	var email, display sql.NullString
	err := s.Scan(&row.ID, &kind, &email, &display, &row.Status, &row.CreatedAt)
	switch actor.Kind(kind) {
	case actor.KindHuman, actor.KindAgent:
		row.Kind = actor.Kind(kind)
	default:
		if err == nil {
			err = fmt.Errorf("lagoon: invalid principal kind %q", kind)
		}
	}
	if email.Valid {
		row.Email = email.String
	}
	if display.Valid {
		row.DisplayName = display.String
	}
	return row, err
}

func scanDecl(s scanner) (DeclRow, error) {
	var row DeclRow
	var raw sql.NullString
	err := s.Scan(&row.ID, &row.Name, &row.Owner, &row.DefaultClass, &raw, &row.Status, &row.Visibility, &row.CreatedAt, &row.UpdatedAt)
	if raw.Valid {
		row.Config = json.RawMessage(raw.String)
	}
	return row, err
}

func scanDevice(s scanner) (DeviceRow, error) {
	var row DeviceRow
	err := s.Scan(&row.ID, &row.OwnerPrincipal, &row.Name, &row.Key, &row.Status, &row.CreatedAt)
	return row, err
}
