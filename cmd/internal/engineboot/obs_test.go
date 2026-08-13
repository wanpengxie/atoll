package engineboot

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/obs"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type censusPanicHost struct {
	bundle       channelhost.Bundle
	serving      bool
	acquireCalls int
}

func (h *censusPanicHost) Acquire(channel.ID) (channelhost.Bundle, bool) {
	h.acquireCalls++
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
		ID: "tool:a", Kind: actor.KindTool, DeclID: "decl-a", Bound: true,
		Device: channelspec.DeviceState{Kind: channelspec.DeviceKnown, Online: false, ReceivedAt: 9},
	}}}}}
	rows, serving, err := (channelObsAdapter{host: host}).Roster(context.Background(), "c0")
	if err != nil || !serving || host.acquireCalls != 1 || len(rows) != 1 {
		t.Fatalf("roster=(%+v,%v,%v) acquire calls=%d", rows, serving, err, host.acquireCalls)
	}
	if string(rows[0].Declared) != `{"id":"tool:a","kind":"tool","decl_id":"decl-a"}` || !rows[0].Bound || rows[0].Device.Kind != obs.DeviceKnown || rows[0].Device.Online {
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
