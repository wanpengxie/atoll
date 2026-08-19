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
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
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
	// StewardClass is the agent class boot carves as c0's steward. Empty = codex.
	// It is an input to the carver, not a mechanism: boot writes what it is told.
	StewardClass string
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
  description TEXT NOT NULL DEFAULT '',
  serving INTEGER NOT NULL DEFAULT 1 CHECK(serving IN (0,1)),
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
  description TEXT,
  owner TEXT NOT NULL REFERENCES principals(id),
  default_class TEXT NOT NULL, config_json TEXT,
  status TEXT NOT NULL CHECK(status IN ('present','revoked')),
  visibility TEXT NOT NULL DEFAULT 'private',
	  singleton INTEGER NOT NULL DEFAULT 0 CHECK(singleton IN (0,1)),
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
	`CREATE TABLE decl_overlays (
  decl_id TEXT NOT NULL REFERENCES decls(id) ON DELETE CASCADE,
  channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
  config_json TEXT, updated_at INTEGER NOT NULL,
  PRIMARY KEY (decl_id, channel_id))`,
	`CREATE TABLE channel_templates (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT,
  owner TEXT NOT NULL REFERENCES principals(id),
  status TEXT NOT NULL CHECK(status IN ('present','revoked')),
  visibility TEXT NOT NULL DEFAULT 'private', body_json TEXT NOT NULL,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
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
	c0Path, err := channelhost.DBPath(cfg.ChannelDir, channelspec.C0ChannelID)
	if err != nil {
		return Result{}, err
	}
	channelDir := filepath.Clean(cfg.ChannelDir)
	registryParent := filepath.Dir(channelDir)
	if registryParent == channelDir {
		return Result{}, errors.New("boot: channel directory must have a distinct parent for registry database")
	}
	registryPath := filepath.Join(registryParent, registryDBName)
	lobbyPath, err := channelhost.DBPath(cfg.ChannelDir, channelspec.LobbyChannelID)
	if err != nil {
		return Result{}, err
	}
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
	if err := removeUnpublished(c0Path, lobbyPath, registryPath); err != nil {
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
	steward := strings.TrimSpace(cfg.StewardClass)
	if steward == "" {
		steward = "codex"
	}
	if err := install(ctx, c0Path, registryPath, password, steward, now()); err != nil {
		return Result{}, err
	}
	genesis, err := readC0Genesis(ctx, c0Path)
	if err != nil {
		return Result{}, err
	}
	return Result{C0DBPath: c0Path, RegistryDBPath: registryPath, C0Genesis: genesis, Installed: true, RootPassword: password}, nil
}

func readC0Genesis(ctx context.Context, path string) (lagoon.GenesisSpec, error) {
	cs, err := runtime.OpenChannel(ctx, channelspec.C0ChannelID, path, runtime.OpenChannelOptions{ReadOnly: true, MustExist: true})
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

// prepareStartup verifies the immutable installation image. There are no
// schema migrations or legacy-shape repairs before 1.0.
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
		{"root principal", `SELECT EXISTS(SELECT 1 FROM principals WHERE id=? AND kind='human')`, channelspec.RootPrincipalID},
		{"root credential", `SELECT EXISTS(SELECT 1 FROM credentials WHERE principal_id=? AND kind='password')`, channelspec.RootPrincipalID},
		{"local device", `SELECT EXISTS(SELECT 1 FROM devices WHERE id=?)`, channelspec.LocalDeviceID},
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

func install(ctx context.Context, c0Path, registryPath, password, stewardClass string, now time.Time) error {
	stamp := now.UnixMilli()
	if err := lagoon.ValidateName(string(channelspec.C0ChannelID)); err != nil {
		return fmt.Errorf("boot: invalid c0 channel name: %w", err)
	}
	daemonPlacement, err := storespec.NewDaemonPlacement(channelspec.LocalDeviceID)
	if err != nil {
		return err
	}
	lobbyPath, err := channelhost.DBPath(filepath.Dir(c0Path), channelspec.LobbyChannelID)
	if err != nil {
		return err
	}
	if err := writePhysicalChannel(ctx, c0Path, storespec.ChannelGenesis{ChannelID: string(channelspec.C0ChannelID), Type: "group", OwnerPrincipal: channelspec.RootPrincipalID, CreatedAt: stamp}, []storespec.ActorDraft{
		{Kind: actor.KindSystem, SourceDeclID: lagoon.RegistrarDeclID, CreatedAt: stamp, Definition: storespec.ActorDefinition{Class: lagoon.ClassRegistrar, Config: json.RawMessage(`{}`)}, Placement: storespec.NewServerPlacement()},
		{Kind: actor.KindPeer, SourceDeclID: lagoon.SvcActorDeclID, CreatedAt: stamp, Definition: storespec.ActorDefinition{Class: lagoon.SvcActorClass, Config: json.RawMessage(`{}`)}, Placement: storespec.NewServerPlacement()},
		{Kind: actor.KindHuman, Principal: channelspec.RootPrincipalID, CreatedAt: stamp, Definition: storespec.ActorDefinition{Class: "human"}, Placement: storespec.NewServerPlacement()},
		{Kind: actor.KindAgent, Principal: channelspec.StewardPrincipalID, SourceDeclID: lagoon.StableBootstrapDeclID(channelspec.RootPrincipalID, "steward"), CreatedAt: stamp, Definition: storespec.ActorDefinition{Class: stewardClass, Config: stewardConfig()}, Placement: daemonPlacement},
		{Kind: actor.KindPeer, SourceDeclID: string(channelspec.LobbyChannelID), CreatedAt: stamp, Definition: storespec.ActorDefinition{Class: lagoon.PeerActorClass, Config: targetConfig(channelspec.LobbyChannelID)}, Placement: storespec.NewServerPlacement()},
	}); err != nil {
		return fmt.Errorf("boot: write c0: %w", err)
	}
	if err := writePhysicalChannel(ctx, lobbyPath, storespec.ChannelGenesis{ChannelID: string(channelspec.LobbyChannelID), Type: "group", OwnerPrincipal: channelspec.RootPrincipalID, ParentChannelID: string(channelspec.C0ChannelID), InitiatorPrincipal: channelspec.RootPrincipalID, CreatedAt: stamp}, []storespec.ActorDraft{
		{Kind: actor.KindPeer, SourceDeclID: lagoon.SvcActorDeclID, CreatedAt: stamp, Definition: storespec.ActorDefinition{Class: lagoon.SvcActorClass, Config: json.RawMessage(`{}`)}, Placement: storespec.NewServerPlacement()},
		// The lobby is outside the trust domain: only the guest speaks there.
		// root is not a lobby member; c0 reaches the lobby through its handle.
		{Kind: actor.KindHuman, Principal: channelspec.GuestPrincipalID, CreatedAt: stamp, Definition: storespec.ActorDefinition{Class: "human"}, Placement: storespec.NewServerPlacement()},
	}); err != nil {
		return fmt.Errorf("boot: write lobby: %w", err)
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
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO principals(id,kind,email,display_name,status,created_at) VALUES(?,'human',?,'Root','present',?)`, channelspec.RootPrincipalID, "root@atoll.local", stamp); err != nil {
		return fmt.Errorf("boot: root principal: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO credentials(principal_id,kind,secret_hash,status,rotated_at) VALUES(?,'password',?,'active',?)`, channelspec.RootPrincipalID, string(hash), stamp); err != nil {
		return fmt.Errorf("boot: root credential: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO principals(id,kind,email,display_name,status,created_at) VALUES(?,'agent',NULL,'Steward','present',?)`, channelspec.StewardPrincipalID, stamp); err != nil {
		return fmt.Errorf("boot: steward principal: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO principals(id,kind,email,display_name,status,created_at) VALUES(?,'human',?,'Guest','present',?)`, channelspec.GuestPrincipalID, "guest@atoll.local", stamp); err != nil {
		return fmt.Errorf("boot: guest principal: %w", err)
	}
	svcSnapshot, err := sealSnapshot(lagoon.SvcActorClass, json.RawMessage(`{}`), channelspec.PlacementServer)
	if err != nil {
		return err
	}
	registrarSnapshot, err := sealSnapshot(lagoon.ClassRegistrar, json.RawMessage(`{}`), channelspec.PlacementServer)
	if err != nil {
		return err
	}
	c0Description := "Atoll core registry and administration channel."
	c0Serving := 1
	c0spec := lagoon.GenesisSpec{ChannelID: channelspec.C0ChannelID, Type: "group", OwnerPrincipal: channelspec.RootPrincipalID, CreatedAt: stamp, Declarations: []lagoon.GenesisDeclaration{{DeclID: lagoon.SvcActorDeclID, Kind: actor.KindPeer, Rendered: svcSnapshot}, {DeclID: lagoon.RegistrarDeclID, Kind: actor.KindSystem, Rendered: registrarSnapshot}}, Profile: regspec.ChannelProfile{Description: &c0Description, Serving: &c0Serving, Endpoints: map[string]regspec.EndpointSpec{}}}
	raw, err := json.Marshal(c0spec)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO channels(id,parent_id,name,type,status,owner_principal,description,serving,spec_json,created_at) VALUES(?,NULL,?,'group','present',?, ?,1,?,?)`, channelspec.C0ChannelID, channelspec.C0ChannelID, channelspec.RootPrincipalID, "Atoll core registry and administration channel.", string(raw), stamp); err != nil {
		return fmt.Errorf("boot: c0 channel row: %w", err)
	}
	lobbyDescription := "Registration lobby for unauthenticated guests."
	lobbyServing := 0
	lobbySpec := lagoon.GenesisSpec{ChannelID: channelspec.LobbyChannelID, Type: "group", OwnerPrincipal: channelspec.RootPrincipalID, CreatedAt: stamp, ParentID: channelspec.C0ChannelID, InitiatorPrincipal: channelspec.RootPrincipalID, Declarations: []lagoon.GenesisDeclaration{{DeclID: lagoon.SvcActorDeclID, Kind: actor.KindPeer, Rendered: svcSnapshot}}, Profile: regspec.ChannelProfile{Description: &lobbyDescription, Serving: &lobbyServing, Endpoints: map[string]regspec.EndpointSpec{}}}
	lobbyRaw, err := json.Marshal(lobbySpec)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO channels(id,parent_id,name,type,status,owner_principal,description,serving,spec_json,created_at) VALUES(?,?,'lobby','group','present',?, ?,0,?,?)`, channelspec.LobbyChannelID, channelspec.C0ChannelID, channelspec.RootPrincipalID, "Registration lobby for unauthenticated guests.", string(lobbyRaw), stamp); err != nil {
		return fmt.Errorf("boot: lobby channel row: %w", err)
	}
	decls := []struct {
		id, name, class, visibility string
		config                      json.RawMessage
	}{
		{lagoon.RegistrarDeclID, "Registrar Seat", lagoon.ClassRegistrar, "private", json.RawMessage(`{}`)},
		{lagoon.SvcActorDeclID, "Service Actor", lagoon.SvcActorClass, "private", json.RawMessage(`{}`)},
		{string(channelspec.LobbyChannelID), "c0.lobby", lagoon.PeerActorClass, "public", targetConfig(channelspec.LobbyChannelID)},
		{lagoon.StableBootstrapDeclID(channelspec.RootPrincipalID, "steward"), "Steward", stewardClass, "private", stewardConfig()},
	}
	for _, decl := range decls {
		if _, err := tx.ExecContext(ctx, `INSERT INTO decls(id,name,description,owner,default_class,config_json,status,visibility,created_at,updated_at) VALUES(?,?,NULL,?,?,?,'present',?,?,?)`, decl.id, decl.name, channelspec.RootPrincipalID, decl.class, string(decl.config), decl.visibility, stamp, stamp); err != nil {
			return fmt.Errorf("boot: system declaration %s: %w", decl.id, err)
		}
	}
	const localDeviceName = "local-device"
	if err := lagoon.ValidateName(localDeviceName); err != nil {
		return fmt.Errorf("boot: local device name: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO devices(id,owner_principal,name,key,status,created_at) VALUES(?,?,?,?,'present',?)`, channelspec.LocalDeviceID, channelspec.RootPrincipalID, localDeviceName, uuid.NewString(), stamp); err != nil {
		return fmt.Errorf("boot: local device: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO bindings(channel_id,device_id,attached_at) VALUES(?,?,?)`, channelspec.C0ChannelID, channelspec.LocalDeviceID, stamp); err != nil {
		return fmt.Errorf("boot: c0 local binding: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO atoll_install(id,installed_at) VALUES(1,?)`, stamp); err != nil {
		return fmt.Errorf("boot: publish install marker: %w", err)
	}
	return tx.Commit()
}

func writePhysicalChannel(ctx context.Context, path string, genesis storespec.ChannelGenesis, actors []storespec.ActorDraft) error {
	cs, err := runtime.OpenChannel(ctx, channel.ID(genesis.ChannelID), path, runtime.OpenChannelOptions{})
	if err != nil {
		return err
	}
	defer cs.Close()
	if err := cs.Genesis.CreateGenesis(ctx, genesis); err != nil {
		return err
	}
	for _, draft := range actors {
		if _, err := cs.Actors.Insert(ctx, draft); err != nil {
			return err
		}
	}
	return nil
}

func targetConfig(id channel.ID) json.RawMessage {
	raw, _ := json.Marshal(map[string]channel.ID{"channel": id})
	return raw
}
func sealSnapshot(class string, config json.RawMessage, placement channelspec.PlacementKind) (channelspec.RenderedSnapshot, error) {
	return (channelspec.RenderedSnapshot{Class: class, Config: config, Placement: channelspec.Placement{Kind: placement}}).Seal()
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
