// Package boot is the sealed installation phase. It creates c0 through the
// ordinary channel store opener, writes the fixed registry image in physical
// order, and publishes the install marker last. No running component imports
// an installation write capability from this package.
package boot

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"golang.org/x/crypto/bcrypt"

	_ "modernc.org/sqlite"
)

type Config struct {
	ChannelDir   string
	RootPassword string
	Now          func() time.Time
}

type Result struct {
	C0DBPath       string
	RegistryDBPath string
	C0Genesis      lagoon.GenesisSpec
	Installed      bool
	RootPassword   string
}

const registryDBName = "registry.db"

var registryDDL = [...]string{
	`CREATE TABLE channels (
  id TEXT PRIMARY KEY, parent_id TEXT REFERENCES channels(id),
  name TEXT NOT NULL, type TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('present','retired')),
  owner_principal TEXT NOT NULL REFERENCES principals(id),
  spec_json TEXT NOT NULL, created_at INTEGER NOT NULL)`,
	`CREATE TABLE principals (
  id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('human','agent')),
  email TEXT UNIQUE CHECK((kind='human') = (email IS NOT NULL)),
  display_name TEXT,
  status TEXT NOT NULL CHECK(status IN ('present','retired')),
  created_at INTEGER NOT NULL)`,
	`CREATE TABLE credentials (
  principal_id TEXT NOT NULL REFERENCES principals(id),
  kind TEXT NOT NULL CHECK(kind IN ('password')),
  secret_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('active','retired')),
  rotated_at INTEGER, PRIMARY KEY (principal_id, kind))`,
	`CREATE TABLE decls (
  id TEXT PRIMARY KEY, name TEXT NOT NULL,
  owner TEXT NOT NULL REFERENCES principals(id),
  default_class TEXT NOT NULL, config_json TEXT,
  status TEXT NOT NULL CHECK(status IN ('present','revoked')),
  visibility TEXT NOT NULL DEFAULT 'private',
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
	`CREATE TABLE decl_overlays (
  decl_id TEXT NOT NULL REFERENCES decls(id) ON DELETE CASCADE,
  channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
  config_json TEXT, updated_at INTEGER NOT NULL,
  PRIMARY KEY (decl_id, channel_id))`,
	`CREATE TABLE devices (
  id TEXT PRIMARY KEY,
  owner_principal TEXT NOT NULL REFERENCES principals(id),
  name TEXT NOT NULL, key TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK(status IN ('present','retired')),
  created_at INTEGER NOT NULL)`,
	`CREATE TABLE bindings (
  channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
  device_id TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  attached_at INTEGER NOT NULL,
  PRIMARY KEY (channel_id, device_id))`,
	`CREATE TABLE atoll_install (
  id INTEGER PRIMARY KEY CHECK(id=1),
  installed_at INTEGER NOT NULL)`,
}

func Ensure(ctx context.Context, cfg Config) (Result, error) {
	c0Path, err := channelhost.DBPath(cfg.ChannelDir, protocol.C0ChannelID)
	if err != nil {
		return Result{}, err
	}
	channelDir := filepath.Clean(cfg.ChannelDir)
	registryParent := filepath.Dir(channelDir)
	if registryParent == channelDir {
		return Result{}, errors.New("boot: channel directory must have a distinct parent for registry database")
	}
	registryPath := filepath.Join(registryParent, registryDBName)
	installed, err := hasMarker(ctx, registryPath)
	if err != nil {
		return Result{}, err
	}
	if installed {
		if err := prepareStartup(ctx, c0Path, registryPath, time.Now()); err != nil {
			return Result{}, err
		}
		genesis, err := readC0Genesis(ctx, c0Path)
		if err != nil {
			return Result{}, err
		}
		return Result{C0DBPath: c0Path, RegistryDBPath: registryPath, C0Genesis: genesis}, nil
	}
	if err := removeUnpublished(c0Path, registryPath); err != nil {
		return Result{}, err
	}
	password := cfg.RootPassword
	if password == "" {
		password = uuid.NewString()
	}
	now := time.Now
	if cfg.Now != nil {
		now = cfg.Now
	}
	if err := install(ctx, c0Path, registryPath, password, now()); err != nil {
		return Result{}, err
	}
	genesis, err := readC0Genesis(ctx, c0Path)
	if err != nil {
		return Result{}, err
	}
	return Result{C0DBPath: c0Path, RegistryDBPath: registryPath, C0Genesis: genesis, Installed: true, RootPassword: password}, nil
}

func readC0Genesis(ctx context.Context, path string) (lagoon.GenesisSpec, error) {
	cs, err := runtime.OpenChannel(ctx, protocol.C0ChannelID, path, runtime.OpenChannelOptions{ReadOnly: true, MustExist: true})
	if err != nil {
		return lagoon.GenesisSpec{}, fmt.Errorf("boot: open c0 genesis: %w", err)
	}
	defer cs.Close()
	physical, found, err := cs.Genesis.ReadGenesis(ctx)
	if err != nil {
		return lagoon.GenesisSpec{}, fmt.Errorf("boot: read c0 genesis: %w", err)
	}
	if !found {
		return lagoon.GenesisSpec{}, errors.New("boot: c0 physical genesis missing")
	}
	return lagoon.GenesisSpec{
		ChannelID:          channel.ID(physical.ChannelID),
		Type:               physical.Type,
		OwnerPrincipal:     physical.OwnerPrincipal,
		CreatedAt:          physical.CreatedAt,
		ParentID:           channel.ID(physical.ParentChannelID),
		InitiatorPrincipal: physical.InitiatorPrincipal,
	}, nil
}

// prepareStartup verifies installation-only identities without rewriting any
// credential or device row, and repairs the two c0 composition seats through
// the ordinary channel actor store before the membrane is opened.
func prepareStartup(ctx context.Context, c0Path, registryPath string, now time.Time) error {
	db, err := openSQLite(registryPath, false)
	if err != nil {
		return err
	}
	checks := []struct {
		noun  string
		query string
		arg   string
	}{
		{"root principal", `SELECT EXISTS(SELECT 1 FROM principals WHERE id=? AND kind='human')`, protocol.RootPrincipalID},
		{"root credential", `SELECT EXISTS(SELECT 1 FROM credentials WHERE principal_id=? AND kind='password')`, protocol.RootPrincipalID},
		{"local device", `SELECT EXISTS(SELECT 1 FROM devices WHERE id=?)`, protocol.LocalDeviceID},
	}
	for _, check := range checks {
		var present bool
		if err := db.QueryRowContext(ctx, check.query, check.arg).Scan(&present); err != nil {
			_ = db.Close()
			return fmt.Errorf("boot: verify %s: %w", check.noun, err)
		}
		if !present {
			_ = db.Close()
			return fmt.Errorf("boot: installed world is missing %s", check.noun)
		}
	}
	if err := db.Close(); err != nil {
		return err
	}
	cs, err := runtime.OpenChannel(ctx, protocol.C0ChannelID, c0Path, runtime.OpenChannelOptions{MustExist: true})
	if err != nil {
		return fmt.Errorf("boot: open installed c0: %w", err)
	}
	defer cs.Close()
	rows, err := cs.Actors.ListActive(ctx)
	if err != nil {
		return err
	}
	var rootSeat, registrarSeat bool
	for _, row := range rows {
		rootSeat = rootSeat || (row.Kind == actor.KindHuman && row.Principal == protocol.RootPrincipalID)
		registrarSeat = registrarSeat || (row.Kind == actor.KindTool && row.SourceDeclID == lagoon.RegistrarSeatDeclID)
	}
	stamp := now.UnixMilli()
	if !registrarSeat {
		if _, err := cs.Actors.Insert(ctx, storespec.ActorDraft{Kind: actor.KindTool, SourceDeclID: lagoon.RegistrarSeatDeclID, CreatedAt: stamp, Definition: storespec.ActorDefinition{Class: lagoon.RegistrarClass, Config: json.RawMessage(`{}`)}, Placement: storespec.NewServerPlacement()}); err != nil {
			return fmt.Errorf("boot: repair registrar seat: %w", err)
		}
	}
	if !rootSeat {
		if _, err := cs.Actors.Insert(ctx, storespec.ActorDraft{Kind: actor.KindHuman, Principal: protocol.RootPrincipalID, CreatedAt: stamp, Definition: storespec.ActorDefinition{Class: "human"}, Placement: storespec.NewServerPlacement()}); err != nil {
			return fmt.Errorf("boot: repair c0 owner seat: %w", err)
		}
	}
	return nil
}

func hasMarker(ctx context.Context, path string) (bool, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	db, err := openSQLite(path, false)
	if err != nil {
		return false, err
	}
	defer db.Close()
	var installed int
	err = db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM atoll_install WHERE id=1)`).Scan(&installed)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return false, nil
		}
		return false, err
	}
	return installed == 1, nil
}

func removeUnpublished(paths ...string) error {
	for _, path := range paths {
		for _, suffix := range []string{"", "-wal", "-shm", ".tombstone"} {
			if err := os.Remove(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("boot: remove half-installed database %q: %w", path+suffix, err)
			}
		}
	}
	return nil
}

func install(ctx context.Context, c0Path, registryPath, password string, now time.Time) error {
	stamp := now.UnixMilli()
	cs, err := runtime.OpenChannel(ctx, protocol.C0ChannelID, c0Path, runtime.OpenChannelOptions{})
	if err != nil {
		return fmt.Errorf("boot: create c0: %w", err)
	}
	genesis := storespec.ChannelGenesis{ChannelID: string(protocol.C0ChannelID), Type: "group", OwnerPrincipal: protocol.RootPrincipalID, CreatedAt: stamp}
	if err := cs.Genesis.CreateGenesis(ctx, genesis); err != nil {
		_ = cs.Close()
		return fmt.Errorf("boot: write c0 genesis: %w", err)
	}
	_, err = cs.Actors.Insert(ctx, storespec.ActorDraft{Kind: actor.KindTool, SourceDeclID: lagoon.RegistrarSeatDeclID, CreatedAt: stamp, Definition: storespec.ActorDefinition{Class: lagoon.RegistrarClass, Config: json.RawMessage(`{}`)}, Placement: storespec.NewServerPlacement()})
	if err != nil {
		_ = cs.Close()
		return fmt.Errorf("boot: seat registrar: %w", err)
	}
	_, err = cs.Actors.Insert(ctx, storespec.ActorDraft{Kind: actor.KindHuman, Principal: protocol.RootPrincipalID, CreatedAt: stamp, Definition: storespec.ActorDefinition{Class: "human"}, Placement: storespec.NewServerPlacement()})
	if err != nil {
		_ = cs.Close()
		return fmt.Errorf("boot: seat c0 owner: %w", err)
	}
	if err := cs.Close(); err != nil {
		return fmt.Errorf("boot: close c0 install phase: %w", err)
	}

	db, err := openSQLite(registryPath, true)
	if err != nil {
		return err
	}
	defer db.Close()
	for _, ddl := range registryDDL {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("boot: registry DDL: %w", err)
		}
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO principals(id,kind,email,display_name,status,created_at) VALUES(?,'human',?,'Root','present',?)`, protocol.RootPrincipalID, "root@atoll.local", stamp); err != nil {
		return fmt.Errorf("boot: root principal: %w", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO credentials(principal_id,kind,secret_hash,status,rotated_at) VALUES(?,'password',?,'active',?)`, protocol.RootPrincipalID, string(hash), stamp); err != nil {
		return fmt.Errorf("boot: root credential: %w", err)
	}
	c0spec := lagoon.GenesisSpec{ChannelID: protocol.C0ChannelID, Type: "group", OwnerPrincipal: protocol.RootPrincipalID, CreatedAt: stamp}
	raw, err := json.Marshal(c0spec)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO channels(id,parent_id,name,type,status,owner_principal,spec_json,created_at) VALUES(?,NULL,?,'group','present',?,?,?)`, protocol.C0ChannelID, protocol.C0ChannelID, protocol.RootPrincipalID, string(raw), stamp); err != nil {
		return fmt.Errorf("boot: c0 channel row: %w", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO principals(id,kind,email,display_name,status,created_at) VALUES(?,'agent',NULL,'Steward','present',?)`, protocol.StewardPrincipalID, stamp); err != nil {
		return fmt.Errorf("boot: steward principal: %w", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO decls(id,name,owner,default_class,config_json,status,visibility,created_at,updated_at) VALUES(?,?,?,?,'{}','present','public',?,?)`, lagoon.SpaceToolDeclID, "Space Tool", protocol.RootPrincipalID, lagoon.SpaceToolClass, stamp, stamp); err != nil {
		return fmt.Errorf("boot: space-tool decl: %w", err)
	}
	const localDeviceName = "local-device"
	if err := lagoon.ValidateDeviceName(localDeviceName); err != nil {
		return fmt.Errorf("boot: local device name: %w", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO devices(id,owner_principal,name,key,status,created_at) VALUES(?,?,?,?,'present',?)`, protocol.LocalDeviceID, protocol.RootPrincipalID, localDeviceName, uuid.NewString(), stamp); err != nil {
		return fmt.Errorf("boot: local device: %w", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO atoll_install(id,installed_at) VALUES(1,?)`, stamp); err != nil {
		return fmt.Errorf("boot: publish install marker: %w", err)
	}
	return nil
}

func openSQLite(path string, create bool) (*sql.DB, error) {
	u := &url.URL{Scheme: "file", Path: path}
	mode := "rw"
	if create {
		mode = "rwc"
	}
	// Use the same write-intent posture as the running registry opener.
	db, err := sql.Open("sqlite", u.String()+"?mode="+mode+"&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// RegistryDDLCount is a test-visible inventory, not a second schema surface.
func RegistryDDLCount() int { return len(registryDDL) - 1 }
