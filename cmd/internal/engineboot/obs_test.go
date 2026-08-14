package engineboot

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/boot"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/obs"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type censusPanicHost struct {
	bundle       channelhost.Bundle
	serving      bool
	acquireCalls int
	acquired     []channel.ID
}

func (h *censusPanicHost) Acquire(id channel.ID) (channelhost.Bundle, bool) {
	h.acquireCalls++
	h.acquired = append(h.acquired, id)
	return h.bundle, h.serving
}

func (*censusPanicHost) Census(context.Context) ([]channelhost.CensusEntry, error) {
	panic("obs production adapter touched Census")
}

func (h *censusPanicHost) closeBundle() { h.serving = false }

type obsBundleStub struct{ view channelhost.View }

func (b obsBundleStub) Generation() uint64                { return 1 }
func (b obsBundleStub) Gateway() channelhost.GatewayHitch { return nil }
func (b obsBundleStub) Call() channelhost.Caller          { return nil }
func (b obsBundleStub) View() channelhost.View            { return b.view }

type obsViewStub struct{ roster []channelspec.ObsRosterRow }

func (v obsViewStub) Roster(context.Context) ([]channelspec.ObsRosterRow, error) {
	return v.roster, nil
}
func (obsViewStub) HumanRoster(context.Context) ([]channelspec.HumanRosterEntry, error) {
	return nil, nil
}
func (obsViewStub) DeclaredInstances(context.Context, string) ([]actor.ActorID, error) {
	return nil, nil
}
func (obsViewStub) HasDeclaredInstance(context.Context, string) (bool, error) { return false, nil }
func (obsViewStub) ResolvePrincipal(context.Context, string) (actor.ActorID, bool, error) {
	return "", false, nil
}
func (obsViewStub) OwnerPrincipal(context.Context) (string, bool, error) { return "", false, nil }
func (obsViewStub) ReadVisibleAfterSeq(context.Context, channel.Reader, int64, int) ([]storespec.StoredRow, int64, error) {
	return nil, 0, nil
}
func (obsViewStub) ActorFacts(context.Context, actor.ActorID) (channelspec.ActorFacts, bool, error) {
	return channelspec.ActorFacts{}, false, nil
}
func (obsViewStub) IsBound(context.Context, string) (bool, error) { return false, nil }

type productionAdapterRegistry struct{}

func (productionAdapterRegistry) PrincipalPresent(context.Context, string) (bool, error) {
	return true, nil
}
func (productionAdapterRegistry) Channels(context.Context, *string) ([]obs.Row, bool, error) {
	return []obs.Row{{Key: "c0", Declared: json.RawMessage(`{"id":"c0"}`)}}, true, nil
}
func (productionAdapterRegistry) Channel(context.Context, string) (obs.Row, bool, error) {
	return obs.Row{Declared: json.RawMessage(`{"id":"c0"}`)}, true, nil
}
func (productionAdapterRegistry) Principals(context.Context) ([]obs.Row, bool, error) {
	return nil, true, nil
}
func (productionAdapterRegistry) Daemons(context.Context) ([]obs.Row, bool, error) {
	return nil, true, nil
}
func (productionAdapterRegistry) Decls(context.Context) ([]obs.Row, bool, error) {
	return nil, true, nil
}

type obsDaemonHostStub map[string]bool

func (h obsDaemonHostStub) DaemonOnline(id string) bool { return h[id] }

func openObsGoldenRegistry(t *testing.T) *lagoon.Registry {
	t.Helper()
	const stamp = int64(1_700_000_000_000)
	installed, err := boot.Ensure(context.Background(), boot.Config{
		ChannelDir: filepath.Join(t.TempDir(), "channels"), RootPassword: "obs-golden-password",
		Now: func() time.Time { return time.UnixMilli(stamp) },
	})
	if err != nil {
		t.Fatal(err)
	}
	u := &url.URL{Scheme: "file", Path: installed.RegistryDBPath}
	db, err := sql.Open("sqlite", u.String()+"?mode=rw&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	const channelID = "c/ 频道"
	if _, err := db.ExecContext(context.Background(), `INSERT INTO channels(id,parent_id,name,type,status,owner_principal,spec_json,created_at) VALUES(?,?,'child','group','present',?,'{}',?)`, channelID, protocol.C0ChannelID, protocol.RootPrincipalID, stamp+1); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE devices SET name='desk' WHERE id=?`, protocol.LocalDeviceID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	registry, err := lagoon.Open(installed.RegistryDBPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	return registry
}

func TestProductionAdaptersSixObservationWordsHaveCompleteGoldenJSON(t *testing.T) {
	const channelID = "c/ 频道"
	registry := openObsGoldenRegistry(t)
	host := &censusPanicHost{serving: true, bundle: obsBundleStub{view: obsViewStub{roster: []channelspec.ObsRosterRow{
		{
			ID: "actor-z", Kind: actor.KindTool, DeclID: "decl-z", Bound: false,
			Device: channelspec.DeviceState{Kind: channelspec.DeviceMalformed},
		},
		{
			ID: "actor-a", Kind: actor.KindHuman, Bound: true,
			Device: channelspec.DeviceState{Kind: channelspec.DeviceKnown, Online: false, ReceivedAt: 8123},
		},
	}}}}
	plane := obs.New(obs.Config{
		Registry: registryObsAdapter{registry: registry},
		Channels: channelObsAdapter{host: host},
		Daemons:  daemonObsAdapter{host: obsDaemonHostStub{}},
		Now:      func() int64 { return 9000 },
	})
	escapedChannel := url.PathEscape(channelID)
	tests := []struct {
		name, path, query, golden string
	}{
		{
			name: "space channels", path: "/obs/space/channels", query: "parent_id=c0",
			golden: `{"subject":"space/channels","kind":"channels","complete":true,"items":[{"key":"c/ 频道","declared":{"id":"c/ 频道","parent_id":"c0","name":"child","qualified_name":"c0.child","type":"group","status":"present","owner_principal":"root","created_at":1700000000001},"actual":{"measures":[{"name":"open","value":true,"unknown":false,"observed_at":9000,"since":null}]}}]}`,
		},
		{
			name: "space principals", path: "/obs/space/principals",
			golden: `{"subject":"space/principals","kind":"principals","complete":true,"items":[{"key":"root","declared":{"id":"root","kind":"human","email":"root@atoll.local","display_name":"Root","status":"present","created_at":1700000000000},"actual":null},{"key":"steward","declared":{"id":"steward","kind":"agent","display_name":"Steward","status":"present","created_at":1700000000000},"actual":null}]}`,
		},
		{
			name: "space daemons", path: "/obs/space/daemons",
			golden: `{"subject":"space/daemons","kind":"daemons","complete":true,"items":[{"key":"local-device","declared":{"id":"local-device","owner_principal":"root","name":"desk","status":"present","created_at":1700000000000},"actual":{"measures":[{"name":"online","value":false,"unknown":false,"observed_at":9000,"since":null}]}}]}`,
		},
		{
			name: "space decls", path: "/obs/space/decls",
			golden: `{"subject":"space/decls","kind":"decls","complete":true,"items":[{"key":"space-tool","declared":{"id":"space-tool","name":"Space Tool","owner":"root","default_class":"space-tool","config":{},"status":"present","visibility":"public","created_at":1700000000000,"updated_at":1700000000000},"actual":null}]}`,
		},
		{
			name: "channel profile", path: "/obs/channel/" + escapedChannel + "/profile",
			golden: `{"subject":"channel/c/ 频道/profile","kind":"profile","complete":true,"items":[{"declared":{"id":"c/ 频道","parent_id":"c0","name":"child","qualified_name":"c0.child","type":"group","status":"present","owner_principal":"root","created_at":1700000000001},"actual":{"measures":[{"name":"open","value":true,"unknown":false,"observed_at":9000,"since":null}]}}]}`,
		},
		{
			name: "channel actors", path: "/obs/channel/" + escapedChannel + "/actors",
			golden: `{"subject":"channel/c/ 频道/actors","kind":"actors","complete":true,"items":[{"key":"actor-a","declared":{"id":"actor-a","kind":"human"},"actual":{"measures":[{"name":"bound","value":true,"unknown":false,"observed_at":9000,"since":null},{"name":"device_online","value":false,"unknown":false,"observed_at":8123,"since":null}]}},{"key":"actor-z","declared":{"id":"actor-z","kind":"tool","decl_id":"decl-z"},"actual":{"measures":[{"name":"bound","value":false,"unknown":false,"observed_at":9000,"since":null},{"name":"device_online","value":null,"unknown":true,"reason":"read_failed","observed_at":9000,"since":null}]}}]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			answer, err := plane.Pull(context.Background(), protocol.RootPrincipalID, test.path, test.query)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(answer)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != test.golden {
				t.Fatalf("golden mismatch\n got: %s\nwant: %s", raw, test.golden)
			}
		})
	}
	if len(host.acquired) != 3 {
		t.Fatalf("production channel adapter acquisitions=%v, want channels/profile/actors", host.acquired)
	}
	for i, got := range host.acquired {
		if got != channel.ID(channelID) {
			t.Fatalf("production channel adapter acquire[%d]=%q, want decoded id %q", i, got, channelID)
		}
	}
}

func TestProductionRegistryAdapterEmptySubjectKeepsItemsArray(t *testing.T) {
	registry := openObsGoldenRegistry(t)
	plane := obs.New(obs.Config{Registry: registryObsAdapter{registry: registry}})
	answer, err := plane.Pull(context.Background(), protocol.RootPrincipalID, "/obs/channel/missing/profile", "")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(answer)
	if err != nil {
		t.Fatal(err)
	}
	const golden = `{"subject":"channel/missing/profile","kind":"profile","complete":true,"items":[]}`
	if string(raw) != golden {
		t.Fatalf("golden mismatch\n got: %s\nwant: %s", raw, golden)
	}
}

func TestUnreadableDeviceTestimonyBecomesUnknownOnProductionObservationWire(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `{"online":null}`, `{"online":0}`, `{"other":1}`} {
		t.Run(raw, func(t *testing.T) {
			if parsed, ok := introspect.ParseDevicePresence([]byte(raw)); ok {
				t.Fatalf("testimony %s parsed as known value %+v", raw, parsed)
			}
			host := &censusPanicHost{serving: true, bundle: obsBundleStub{view: obsViewStub{roster: []channelspec.ObsRosterRow{{
				ID: "actor:a", Kind: actor.KindHuman, Bound: true,
				Device: channelspec.DeviceState{Kind: channelspec.DeviceMalformed},
			}}}}}
			plane := obs.New(obs.Config{
				Registry: productionAdapterRegistry{}, Channels: channelObsAdapter{host: host},
				Now: func() int64 { return 321 },
			})
			answer, err := plane.Pull(context.Background(), "root", "/obs/channel/c0/actors", "")
			if err != nil {
				t.Fatal(err)
			}
			measure := answer.Items[0].Actual.Measures[1]
			if measure.Name != "device_online" || !measure.Unknown || measure.Reason != "read_failed" || string(measure.Value) != "null" {
				t.Fatalf("testimony %s final measure=%+v", raw, measure)
			}
			wire, err := json.Marshal(answer)
			if err != nil {
				t.Fatal(err)
			}
			var shape struct {
				Items []struct {
					Actual struct {
						Measures []map[string]json.RawMessage `json:"measures"`
					} `json:"actual"`
				} `json:"items"`
			}
			if err := json.Unmarshal(wire, &shape); err != nil {
				t.Fatal(err)
			}
			device := shape.Items[0].Actual.Measures[1]
			if value, exists := device["value"]; !exists || string(value) != "null" {
				t.Fatalf("testimony %s wire device measure=%s: value must be explicit null", raw, wire)
			}
		})
	}
}

func TestProductionChannelAdapterAnswersOpenWithoutCensus(t *testing.T) {
	host := &censusPanicHost{serving: true}
	plane := obs.New(obs.Config{
		Registry: productionAdapterRegistry{}, Channels: channelObsAdapter{host: host},
		Now: func() int64 { return 10 },
	})
	answer, err := plane.Pull(context.Background(), "root", "/obs/space/channels", "")
	if err != nil {
		t.Fatal(err)
	}
	if host.acquireCalls != 1 || len(answer.Items) != 1 || len(answer.Items[0].Actual.Measures) != 1 {
		t.Fatalf("answer=%+v acquire calls=%d", answer, host.acquireCalls)
	}
	measure := answer.Items[0].Actual.Measures[0]
	if measure.Name != "open" || string(measure.Value) != "true" || measure.Unknown {
		t.Fatalf("open measure=%+v", measure)
	}
}

func TestProductionChannelAdapterRosterAcquiresOnceAndMarshalsOwnerProjection(t *testing.T) {
	host := &censusPanicHost{serving: true, bundle: obsBundleStub{view: obsViewStub{roster: []channelspec.ObsRosterRow{{
		ID: "tool:a", Kind: actor.KindTool, DeclID: "decl-a", Name: "Ticket Booker", Description: "Books a ticket", Bound: true,
		Device: channelspec.DeviceState{Kind: channelspec.DeviceKnown, Online: false, ReceivedAt: 9},
	}}}}}
	rows, serving, err := (channelObsAdapter{host: host}).Roster(context.Background(), "c0")
	if err != nil || !serving || host.acquireCalls != 1 || len(rows) != 1 {
		t.Fatalf("roster=(%+v,%v,%v) acquire calls=%d", rows, serving, err, host.acquireCalls)
	}
	if string(rows[0].Declared) != `{"id":"tool:a","kind":"tool","decl_id":"decl-a","name":"Ticket Booker","description":"Books a ticket"}` || !rows[0].Bound || rows[0].Device.Kind != obs.DeviceKnown || rows[0].Device.Online {
		t.Fatalf("adapted row=%+v declared=%s", rows[0], rows[0].Declared)
	}
}

func TestActorsReturnsNotServingAfterFixtureClosesPresentChannel(t *testing.T) {
	host := &censusPanicHost{serving: true, bundle: obsBundleStub{view: obsViewStub{}}}
	// The fixture closes the only published bundle without changing registry
	// presence, which is the otherwise-unreachable production state under test.
	host.closeBundle()
	plane := obs.New(obs.Config{Registry: productionAdapterRegistry{}, Channels: channelObsAdapter{host: host}})
	_, err := plane.Pull(context.Background(), "root", "/obs/channel/c0/actors", "")
	var typed *obs.Error
	if !errors.As(err, &typed) || typed.Kind != obs.ErrNotServing {
		t.Fatalf("error=%v", err)
	}
}
