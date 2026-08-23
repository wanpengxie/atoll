// Package store is the sole runtime storage implementation for the c0
// registry. It owns SQL, scanning, database setup, and constraint
// classification; lagoon owns the business meaning of the returned facts.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	channelColumns   = "channels.id,channels.parent_id,channels.name,channels.type,channels.status,channels.owner_principal,channels.description,channels.serving,channels.spec_json,channels.created_at"
	principalColumns = "principals.id,principals.kind,principals.email,principals.display_name,principals.status,principals.created_at"
	declColumns      = "decls.id,decls.name,decls.description,decls.owner,decls.default_class,decls.config_json,decls.status,decls.visibility,decls.singleton,decls.created_at,decls.updated_at"
	deviceColumns    = "devices.id,devices.owner_principal,devices.name,devices.key,devices.status,devices.created_at"
	overlayColumns   = "decl_overlays.decl_id,decl_overlays.channel_id,decl_overlays.config_json,decl_overlays.updated_at"
	templateColumns  = "channel_templates.id,channel_templates.name,channel_templates.description,channel_templates.owner,channel_templates.status,channel_templates.visibility,channel_templates.body_json,channel_templates.created_at,channel_templates.updated_at"
)

var ErrConflict = errors.New("registry store: unique or primary-key conflict")

type Store struct{ db *sql.DB }

type Tx struct{ tx *sql.Tx }

func (s *Store) InTx(ctx context.Context, fn func(*Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(&Tx{tx: tx}); err != nil {
		return err
	}
	return tx.Commit()
}

func Open(path string) (*Store, error) {
	u := &url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", u.String()+"?mode=rw&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("lagoon: open registry: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code() {
		case sqlite3.SQLITE_CONSTRAINT_UNIQUE, sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY:
			return errors.Join(ErrConflict, err)
		}
	}
	return err
}

func (s *Store) CredentialHash(ctx context.Context, email string) (string, string, bool, error) {
	var id, hash string
	err := s.db.QueryRowContext(ctx, `SELECT principals.id,credentials.secret_hash FROM principals
		JOIN credentials ON credentials.principal_id=principals.id AND credentials.kind='password' AND credentials.status='active'
		WHERE principals.email=? AND principals.kind='human' AND principals.status='present'`, email).Scan(&id, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	return id, hash, err == nil, err
}

func (s *Store) ResolveDeviceKey(ctx context.Context, key string) (string, bool, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM devices WHERE key=? AND status='present'`, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return id, err == nil, err
}

func (s *Store) GetDevice(ctx context.Context, id string) (regspec.DeviceRow, bool, error) {
	row, err := scanDevice(s.db.QueryRowContext(ctx, `SELECT `+deviceColumns+` FROM devices WHERE devices.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.DeviceRow{}, false, nil
	}
	return row, err == nil, err
}

func (s *Store) GetDeviceStatus(ctx context.Context, id string) (regspec.DeviceStatus, bool, error) {
	var status regspec.DeviceStatus
	err := s.db.QueryRowContext(ctx, `SELECT status FROM devices WHERE id=?`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return status, err == nil, err
}

func (s *Store) GetPrincipalStatus(ctx context.Context, id string) (regspec.PrincipalStatus, bool, error) {
	var status regspec.PrincipalStatus
	err := s.db.QueryRowContext(ctx, `SELECT status FROM principals WHERE id=?`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return status, err == nil, err
}

func (s *Store) ListChannels(ctx context.Context) ([]regspec.ChannelRow, error) {
	return s.listChannels(ctx, `SELECT `+channelColumns+` FROM channels ORDER BY channels.id`)
}

func (s *Store) FindChannels(ctx context.Context, parent channel.ID, name string) ([]regspec.ChannelRow, error) {
	return s.listChannels(ctx, `SELECT `+channelColumns+` FROM channels WHERE channels.parent_id=? AND channels.name=? ORDER BY channels.id`, parent, name)
}

func (s *Store) PresentChildExists(ctx context.Context, parent channel.ID) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channels WHERE parent_id=? AND status='present')`, parent).Scan(&exists)
	return exists, err
}

func (s *Store) listChannels(ctx context.Context, query string, args ...any) ([]regspec.ChannelRow, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []regspec.ChannelRow
	for rows.Next() {
		row, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) GetChannel(ctx context.Context, id channel.ID) (regspec.ChannelRow, bool, error) {
	row, err := scanChannel(s.db.QueryRowContext(ctx, `SELECT `+channelColumns+` FROM channels WHERE channels.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.ChannelRow{}, false, nil
	}
	return row, err == nil, err
}

func (t *Tx) GetChannel(ctx context.Context, id channel.ID) (regspec.ChannelRow, bool, error) {
	row, err := scanChannel(t.tx.QueryRowContext(ctx, `SELECT `+channelColumns+` FROM channels WHERE channels.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.ChannelRow{}, false, nil
	}
	return row, err == nil, err
}

func (t *Tx) ListChannels(ctx context.Context) ([]regspec.ChannelRow, error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT `+channelColumns+` FROM channels ORDER BY channels.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []regspec.ChannelRow
	for rows.Next() {
		row, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (t *Tx) FindChannels(ctx context.Context, parent channel.ID, name string) ([]regspec.ChannelRow, error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT `+channelColumns+` FROM channels WHERE channels.parent_id=? AND channels.name=? ORDER BY channels.id`, parent, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []regspec.ChannelRow
	for rows.Next() {
		row, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) GetDecl(ctx context.Context, id string) (regspec.DeclRow, bool, error) {
	row, err := scanDecl(s.db.QueryRowContext(ctx, `SELECT `+declColumns+` FROM decls WHERE decls.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.DeclRow{}, false, nil
	}
	return row, err == nil, err
}

func (t *Tx) GetDecl(ctx context.Context, id string) (regspec.DeclRow, bool, error) {
	row, err := scanDecl(t.tx.QueryRowContext(ctx, `SELECT `+declColumns+` FROM decls WHERE decls.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.DeclRow{}, false, nil
	}
	return row, err == nil, err
}

// GetDevice inside the transaction: a recipe that names which machine runs a
// seat has to be checked against the device table in the same transaction that
// writes the channel, or the check answers about a world that no longer exists
// by the time the row lands.
func (t *Tx) GetDevice(ctx context.Context, id string) (regspec.DeviceRow, bool, error) {
	row, err := scanDevice(t.tx.QueryRowContext(ctx, `SELECT `+deviceColumns+` FROM devices WHERE devices.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.DeviceRow{}, false, nil
	}
	return row, err == nil, err
}

func (t *Tx) GetOverlay(ctx context.Context, declID string, ch channel.ID) (regspec.OverlayRow, bool, error) {
	row, err := scanOverlay(t.tx.QueryRowContext(ctx, `SELECT `+overlayColumns+` FROM decl_overlays WHERE decl_overlays.decl_id=? AND decl_overlays.channel_id=?`, declID, ch))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.OverlayRow{}, false, nil
	}
	return row, err == nil, err
}

func (s *Store) ReplaceProfile(ctx context.Context, ch channel.ID, description string, serving int) error {
	return s.InTx(ctx, func(tx *Tx) error {
		return tx.ReplaceProfile(ctx, ch, description, serving)
	})
}

func (tx *Tx) ReplaceProfile(ctx context.Context, ch channel.ID, description string, serving int) error {
	_, err := tx.tx.ExecContext(ctx, `UPDATE channels SET description=?,serving=? WHERE id=?`, description, serving, ch)
	return err
}

func (s *Store) GetChannelTemplate(ctx context.Context, id string) (regspec.ChannelTemplateRow, bool, error) {
	row, err := scanTemplate(s.db.QueryRowContext(ctx, `SELECT `+templateColumns+` FROM channel_templates WHERE channel_templates.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.ChannelTemplateRow{}, false, nil
	}
	return row, err == nil, err
}

func (t *Tx) GetChannelTemplate(ctx context.Context, id string) (regspec.ChannelTemplateRow, bool, error) {
	row, err := scanTemplate(t.tx.QueryRowContext(ctx, `SELECT `+templateColumns+` FROM channel_templates WHERE channel_templates.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.ChannelTemplateRow{}, false, nil
	}
	return row, err == nil, err
}

func (s *Store) ListChannelTemplates(ctx context.Context) ([]regspec.ChannelTemplateRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+templateColumns+` FROM channel_templates ORDER BY channel_templates.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []regspec.ChannelTemplateRow
	for rows.Next() {
		row, err := scanTemplate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) InsertChannelTemplate(ctx context.Context, row regspec.ChannelTemplateRow) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO channel_templates(id,name,description,owner,status,visibility,body_json,created_at,updated_at) VALUES(?,?,?,?,'present',?,?,?,?)`, row.ID, row.Name, nullableText(row.Description), row.Owner, row.Visibility, string(row.Body), row.CreatedAt, row.UpdatedAt)
	return classify(err)
}

func (s *Store) UpdateChannelTemplate(ctx context.Context, row regspec.ChannelTemplateRow) error {
	_, err := s.db.ExecContext(ctx, `UPDATE channel_templates SET name=?,description=?,visibility=?,body_json=?,updated_at=? WHERE id=?`, row.Name, nullableText(row.Description), row.Visibility, string(row.Body), row.UpdatedAt, row.ID)
	return err
}

func (s *Store) RevokeChannelTemplate(ctx context.Context, id string, at int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE channel_templates SET status='revoked',updated_at=? WHERE id=?`, at, id)
	return err
}

func (s *Store) ListDecls(ctx context.Context) ([]regspec.DeclRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+declColumns+` FROM decls WHERE decls.status='present' ORDER BY decls.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeclRows(rows)
}

func scanDeclRows(rows *sql.Rows) ([]regspec.DeclRow, error) {
	var out []regspec.DeclRow
	for rows.Next() {
		row, err := scanDecl(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) ListPresentOverlays(ctx context.Context) ([]regspec.OverlayRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+overlayColumns+`
		FROM decl_overlays JOIN decls ON decls.id=decl_overlays.decl_id JOIN channels ON channels.id=decl_overlays.channel_id
		WHERE decls.status='present' AND channels.status='present' ORDER BY decl_overlays.decl_id,decl_overlays.channel_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []regspec.OverlayRow
	for rows.Next() {
		row, err := scanOverlay(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) GetOverlays(ctx context.Context, ch channel.ID) ([]regspec.OverlayRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+overlayColumns+`
		FROM decl_overlays JOIN decls ON decls.id=decl_overlays.decl_id JOIN channels ON channels.id=decl_overlays.channel_id
		WHERE decl_overlays.channel_id=? AND decls.status='present' AND channels.status='present' ORDER BY decl_overlays.decl_id`, ch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []regspec.OverlayRow
	for rows.Next() {
		row, err := scanOverlay(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) GetOverlay(ctx context.Context, declID string, ch channel.ID) (regspec.OverlayRow, bool, error) {
	row, err := scanOverlay(s.db.QueryRowContext(ctx, `SELECT `+overlayColumns+` FROM decl_overlays WHERE decl_overlays.decl_id=? AND decl_overlays.channel_id=?`, declID, ch))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.OverlayRow{}, false, nil
	}
	return row, err == nil, err
}

func (s *Store) IsBound(ctx context.Context, ch channel.ID, device string) (bool, error) {
	var ok bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM bindings JOIN devices ON devices.id=bindings.device_id
		WHERE bindings.channel_id=? AND bindings.device_id=? AND devices.status='present')`, ch, device).Scan(&ok)
	return ok, err
}

func (s *Store) ListBoundDeviceIDs(ctx context.Context, ch channel.ID) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT devices.id
		FROM bindings JOIN devices ON devices.id=bindings.device_id
		WHERE bindings.channel_id=? AND devices.status='present' ORDER BY devices.id`, ch)
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

func (s *Store) ListDevices(ctx context.Context) ([]regspec.DeviceRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+deviceColumns+` FROM devices WHERE devices.status='present' ORDER BY devices.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []regspec.DeviceRow
	for rows.Next() {
		row, err := scanDevice(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// GetDeviceByName includes retired rows: daemon names are never reusable.
func (s *Store) GetDeviceByName(ctx context.Context, name string) (regspec.DeviceRow, bool, error) {
	row, err := scanDevice(s.db.QueryRowContext(ctx, `SELECT `+deviceColumns+` FROM devices WHERE devices.name=? ORDER BY devices.created_at,devices.id LIMIT 1`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.DeviceRow{}, false, nil
	}
	return row, err == nil, err
}

func (s *Store) ListPrincipals(ctx context.Context) ([]regspec.PrincipalRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+principalColumns+` FROM principals WHERE principals.status='present' ORDER BY principals.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []regspec.PrincipalRow
	for rows.Next() {
		row, err := scanPrincipal(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) GetPrincipal(ctx context.Context, id string) (regspec.PrincipalRow, bool, error) {
	row, err := scanPrincipal(s.db.QueryRowContext(ctx, `SELECT `+principalColumns+` FROM principals WHERE principals.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.PrincipalRow{}, false, nil
	}
	return row, err == nil, err
}

func (t *Tx) GetPrincipal(ctx context.Context, id string) (regspec.PrincipalRow, bool, error) {
	row, err := scanPrincipal(t.tx.QueryRowContext(ctx, `SELECT `+principalColumns+` FROM principals WHERE principals.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.PrincipalRow{}, false, nil
	}
	return row, err == nil, err
}

func (s *Store) GetPrincipalByEmail(ctx context.Context, email string) (regspec.PrincipalRow, bool, error) {
	row, err := scanPrincipal(s.db.QueryRowContext(ctx, `SELECT `+principalColumns+` FROM principals WHERE principals.email=?`, email))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.PrincipalRow{}, false, nil
	}
	return row, err == nil, err
}

func (s *Store) GetPresentPrincipal(ctx context.Context, id string) (regspec.PrincipalRow, bool, error) {
	row, err := scanPrincipal(s.db.QueryRowContext(ctx, `SELECT `+principalColumns+` FROM principals WHERE principals.id=? AND principals.status='present'`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.PrincipalRow{}, false, nil
	}
	return row, err == nil, err
}

func (s *Store) PrincipalExistsWithKind(ctx context.Context, id string, kind actor.Kind) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM principals WHERE id=? AND kind=?)`, id, kind).Scan(&exists)
	return exists, err
}

func (s *Store) DeviceExists(ctx context.Context, id string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM devices WHERE id=?)`, id).Scan(&exists)
	return exists, err
}

func (s *Store) ChannelExists(ctx context.Context, id channel.ID) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channels WHERE id=?)`, id).Scan(&exists)
	return exists, err
}

func (s *Store) UpsertSteward(ctx context.Context, id string, at int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO principals(id,kind,email,display_name,status,created_at) VALUES(?,'agent',NULL,'Steward','present',?) ON CONFLICT(id) DO UPDATE SET kind='agent',email=NULL,display_name='Steward',status='present'`, id, at)
	return classify(err)
}

// InsertSystemChannel carves c0's registry row once. There is no upsert:
// a start never rewrites an existing c0 row.
func (s *Store) InsertSystemChannel(ctx context.Context, row regspec.ChannelRow) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO channels(id,parent_id,name,type,status,owner_principal,description,serving,spec_json,created_at) VALUES(?,NULL,?,'group','present',?,?,?,?,?)`, row.ID, row.Name, row.OwnerPrincipal, row.Description, row.Serving, string(row.Spec), row.CreatedAt)
	return classify(err)
}

func (s *Store) UpsertSystemDecl(ctx context.Context, row regspec.DeclRow) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO decls(id,name,description,owner,default_class,config_json,status,visibility,singleton,created_at,updated_at) VALUES(?,?,?,?,?,?,'present',?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,description=excluded.description,owner=excluded.owner,default_class=excluded.default_class,config_json=excluded.config_json,status='present',visibility=excluded.visibility,singleton=excluded.singleton,updated_at=excluded.updated_at`, row.ID, row.Name, nullableText(row.Description), row.Owner, row.DefaultClass, nullableJSON(row.Config), row.Visibility, row.Singleton, row.CreatedAt, row.UpdatedAt)
	return classify(err)
}

func (s *Store) InsertChannel(ctx context.Context, row regspec.ChannelRow) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO channels(id,parent_id,name,type,status,owner_principal,description,serving,spec_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, row.ID, row.ParentID, row.Name, row.Type, row.Status, row.OwnerPrincipal, row.Description, row.Serving, string(row.Spec), row.CreatedAt)
	return classify(err)
}

func (t *Tx) InsertChannel(ctx context.Context, row regspec.ChannelRow) error {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO channels(id,parent_id,name,type,status,owner_principal,description,serving,spec_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, row.ID, row.ParentID, row.Name, row.Type, row.Status, row.OwnerPrincipal, row.Description, row.Serving, string(row.Spec), row.CreatedAt)
	return classify(err)
}

func (t *Tx) InsertBinding(ctx context.Context, row regspec.BindingRow) error {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO bindings(channel_id,device_id,attached_at) VALUES(?,?,?)`, row.ChannelID, row.DeviceID, row.AttachedAt)
	return classify(err)
}

func (t *Tx) InsertDecl(ctx context.Context, row regspec.DeclRow) error {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO decls(id,name,description,owner,default_class,config_json,status,visibility,singleton,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, row.ID, row.Name, nullableText(row.Description), row.Owner, row.DefaultClass, nullableJSON(row.Config), row.Status, row.Visibility, row.Singleton, row.CreatedAt, row.UpdatedAt)
	return classify(err)
}

func (t *Tx) UpdateDeclConfig(ctx context.Context, id string, config json.RawMessage) error {
	_, err := t.tx.ExecContext(ctx, `UPDATE decls SET config_json=? WHERE id=?`, nullableJSON(config), id)
	return err
}

func (t *Tx) UpdateOverlayConfig(ctx context.Context, declID string, channelID channel.ID, config json.RawMessage) error {
	_, err := t.tx.ExecContext(ctx, `UPDATE decl_overlays SET config_json=? WHERE decl_id=? AND channel_id=?`, nullableJSON(config), declID, channelID)
	return err
}

func (t *Tx) InsertPrincipal(ctx context.Context, row regspec.PrincipalRow) error {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO principals(id,kind,email,display_name,status,created_at) VALUES(?,?,?,?,?,?)`, row.ID, row.Kind, row.Email, nullableText(row.DisplayName), row.Status, row.CreatedAt)
	return classify(err)
}

func (t *Tx) UpsertOverlay(ctx context.Context, row regspec.OverlayRow) error {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO decl_overlays(decl_id,channel_id,config_json,updated_at) VALUES(?,?,?,?)
		ON CONFLICT(decl_id,channel_id) DO UPDATE SET config_json=excluded.config_json,updated_at=excluded.updated_at`, row.DeclID, row.ChannelID, string(row.Config), row.UpdatedAt)
	return err
}

func (t *Tx) InsertPasswordCredential(ctx context.Context, principalID, hash string, at int64) error {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO credentials(principal_id,kind,secret_hash,status,rotated_at) VALUES(?,'password',?,'active',?)`, principalID, hash, at)
	return classify(err)
}

func (s *Store) RetireChannelAndPeer(ctx context.Context, id channel.ID, peerDecl string, at int64) error {
	return s.InTx(ctx, func(tx *Tx) error {
		if _, err := tx.tx.ExecContext(ctx, `UPDATE channels SET status='retired' WHERE id=?`, id); err != nil {
			return err
		}
		_, err := tx.tx.ExecContext(ctx, `UPDATE decls SET status='revoked',updated_at=? WHERE id=?`, at, peerDecl)
		return err
	})
}

func (s *Store) UpdateChannelStatus(ctx context.Context, id channel.ID, status regspec.ChannelStatus) error {
	_, err := s.db.ExecContext(ctx, `UPDATE channels SET status=? WHERE id=?`, status, id)
	return err
}

func (s *Store) InsertPrincipal(ctx context.Context, row regspec.PrincipalRow) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO principals(id,kind,email,display_name,status,created_at) VALUES(?,?,?,?,?,?)`, row.ID, row.Kind, row.Email, nullableText(row.DisplayName), row.Status, row.CreatedAt)
	return classify(err)
}

func (s *Store) InsertPasswordCredential(ctx context.Context, principalID, hash string, at int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO credentials(principal_id,kind,secret_hash,status,rotated_at) VALUES(?,'password',?,'active',?)`, principalID, hash, at)
	return classify(err)
}

func (s *Store) UpdatePrincipalStatus(ctx context.Context, id string, status regspec.PrincipalStatus) error {
	_, err := s.db.ExecContext(ctx, `UPDATE principals SET status=? WHERE id=?`, status, id)
	return err
}

func (s *Store) PrincipalKind(ctx context.Context, id string) (actor.Kind, bool, error) {
	var kind actor.Kind
	err := s.db.QueryRowContext(ctx, `SELECT kind FROM principals WHERE id=?`, id).Scan(&kind)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return kind, err == nil, err
}

func (s *Store) PasswordCredential(ctx context.Context, principalID string) (string, regspec.CredentialStatus, int64, bool, error) {
	var hash string
	var status regspec.CredentialStatus
	var at int64
	err := s.db.QueryRowContext(ctx, `SELECT secret_hash,status,rotated_at FROM credentials WHERE principal_id=? AND kind='password'`, principalID).Scan(&hash, &status, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", 0, false, nil
	}
	return hash, status, at, err == nil, err
}

func (s *Store) UpsertPasswordCredential(ctx context.Context, principalID, hash string, at int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO credentials(principal_id,kind,secret_hash,status,rotated_at) VALUES(?,'password',?,'active',?) ON CONFLICT(principal_id,kind) DO UPDATE SET secret_hash=excluded.secret_hash,status='active',rotated_at=excluded.rotated_at`, principalID, hash, at)
	return classify(err)
}

func (s *Store) InsertDecl(ctx context.Context, row regspec.DeclRow) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO decls(id,name,description,owner,default_class,config_json,status,visibility,singleton,created_at,updated_at) VALUES(?,?,?,?,?,?,'present',?,?,?,?)`, row.ID, row.Name, nullableText(row.Description), row.Owner, row.DefaultClass, nullableJSON(row.Config), row.Visibility, row.Singleton, row.CreatedAt, row.UpdatedAt)
	return classify(err)
}

func (s *Store) UpdateDecl(ctx context.Context, row regspec.DeclRow) error {
	_, err := s.db.ExecContext(ctx, `UPDATE decls SET name=?,description=?,default_class=?,config_json=?,visibility=?,singleton=?,updated_at=? WHERE id=?`, row.Name, nullableText(row.Description), row.DefaultClass, nullableJSON(row.Config), row.Visibility, row.Singleton, row.UpdatedAt, row.ID)
	return err
}

func (s *Store) RevokeDecl(ctx context.Context, id string, at int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE decls SET status='revoked',updated_at=? WHERE id=?`, at, id)
	return err
}

func (s *Store) UpsertOverlay(ctx context.Context, row regspec.OverlayRow) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO decl_overlays(decl_id,channel_id,config_json,updated_at) VALUES(?,?,?,?) ON CONFLICT(decl_id,channel_id) DO UPDATE SET config_json=excluded.config_json,updated_at=excluded.updated_at`, row.DeclID, row.ChannelID, nullableJSON(row.Config), row.UpdatedAt)
	return classify(err)
}

func (s *Store) DeleteOverlay(ctx context.Context, declID string, ch channel.ID) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM decl_overlays WHERE decl_id=? AND channel_id=?`, declID, ch)
	return err
}

func (s *Store) InsertDevice(ctx context.Context, row regspec.DeviceRow) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO devices(id,owner_principal,name,key,status,created_at) VALUES(?,?,?,?,?,?)`, row.ID, row.OwnerPrincipal, row.Name, row.Key, row.Status, row.CreatedAt)
	return classify(err)
}

func (s *Store) UpdateDeviceStatus(ctx context.Context, id string, status regspec.DeviceStatus) error {
	_, err := s.db.ExecContext(ctx, `UPDATE devices SET status=? WHERE id=?`, status, id)
	return err
}

func (s *Store) InsertBinding(ctx context.Context, row regspec.BindingRow) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO bindings(channel_id,device_id,attached_at) VALUES(?,?,?)`, row.ChannelID, row.DeviceID, row.AttachedAt)
	return classify(err)
}

func (s *Store) InsertBindingIfAbsent(ctx context.Context, row regspec.BindingRow) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO bindings(channel_id,device_id,attached_at) VALUES(?,?,?) ON CONFLICT(channel_id,device_id) DO NOTHING`, row.ChannelID, row.DeviceID, row.AttachedAt)
	return classify(err)
}

func (s *Store) Binding(ctx context.Context, ch channel.ID, deviceID string) (regspec.BindingRow, bool, error) {
	var row regspec.BindingRow
	err := s.db.QueryRowContext(ctx, `SELECT channel_id,device_id,attached_at FROM bindings WHERE channel_id=? AND device_id=?`, ch, deviceID).Scan(&row.ChannelID, &row.DeviceID, &row.AttachedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return regspec.BindingRow{}, false, nil
	}
	return row, err == nil, err
}

func (s *Store) DeleteBinding(ctx context.Context, ch channel.ID, deviceID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM bindings WHERE channel_id=? AND device_id=?`, ch, deviceID)
	return err
}

type scanner interface{ Scan(...any) error }

func scanChannel(s scanner) (regspec.ChannelRow, error) {
	var row regspec.ChannelRow
	var parent sql.NullString
	var spec string
	err := s.Scan(&row.ID, &parent, &row.Name, &row.Type, &row.Status, &row.OwnerPrincipal, &row.Description, &row.Serving, &spec, &row.CreatedAt)
	if parent.Valid {
		row.ParentID = channel.ID(parent.String)
	}
	row.Spec = json.RawMessage(spec)
	return row, err
}

func scanTemplate(s scanner) (regspec.ChannelTemplateRow, error) {
	var row regspec.ChannelTemplateRow
	var description sql.NullString
	var body string
	err := s.Scan(&row.ID, &row.Name, &description, &row.Owner, &row.Status, &row.Visibility, &body, &row.CreatedAt, &row.UpdatedAt)
	if description.Valid {
		row.Description = description.String
	}
	row.Body = json.RawMessage(body)
	return row, err
}

func scanPrincipal(s scanner) (regspec.PrincipalRow, error) {
	var row regspec.PrincipalRow
	var kind string
	var email, display sql.NullString
	err := s.Scan(&row.ID, &kind, &email, &display, &row.Status, &row.CreatedAt)
	if err != nil {
		return regspec.PrincipalRow{}, err
	}
	switch actor.Kind(kind) {
	case actor.KindHuman, actor.KindAgent:
		row.Kind = actor.Kind(kind)
	default:
		return regspec.PrincipalRow{}, fmt.Errorf("lagoon: invalid principal kind %q", kind)
	}
	if email.Valid {
		row.Email = email.String
	}
	if display.Valid {
		row.DisplayName = display.String
	}
	return row, nil
}

func scanDecl(s scanner) (regspec.DeclRow, error) {
	var row regspec.DeclRow
	var description, raw sql.NullString
	err := s.Scan(&row.ID, &row.Name, &description, &row.Owner, &row.DefaultClass, &raw, &row.Status, &row.Visibility, &row.Singleton, &row.CreatedAt, &row.UpdatedAt)
	if description.Valid {
		row.Description = description.String
	}
	if raw.Valid {
		row.Config = json.RawMessage(raw.String)
	}
	return row, err
}

func scanDevice(s scanner) (regspec.DeviceRow, error) {
	var row regspec.DeviceRow
	err := s.Scan(&row.ID, &row.OwnerPrincipal, &row.Name, &row.Key, &row.Status, &row.CreatedAt)
	return row, err
}

func scanOverlay(s scanner) (regspec.OverlayRow, error) {
	var row regspec.OverlayRow
	var raw sql.NullString
	err := s.Scan(&row.DeclID, &row.ChannelID, &raw, &row.UpdatedAt)
	if raw.Valid {
		row.Config = json.RawMessage(raw.String)
	}
	return row, err
}

func nullableText(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nullableJSON(v json.RawMessage) any {
	if len(v) == 0 {
		return nil
	}
	return string(v)
}
