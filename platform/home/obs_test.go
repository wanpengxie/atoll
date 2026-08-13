package home

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

func TestRosterAlwaysSynthesizesSystemActor(t *testing.T) {
	h := openAdmissionHome(t, "obs-roster")
	rows, err := h.View().Roster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for i, row := range rows {
		if i > 0 && rows[i-1].ID > row.ID {
			t.Fatalf("roster is not sorted: %+v", rows)
		}
		if row.ID == actor.SystemActorID {
			found = true
			if row.Kind != actor.KindSystem || row.DeclID != "" || !row.Bound || row.Device.Kind != channelspec.DeviceAbsent {
				t.Fatalf("system row=%+v", row)
			}
		}
	}
	if !found {
		t.Fatalf("system actor missing from roster: %+v", rows)
	}
}

func TestRosterDeviceProjectionPreservesFourTestimonyStates(t *testing.T) {
	kind := actorrt.ObsKind(introspect.ObsDevicePresence)
	tests := []struct {
		name     string
		snapshot presence.Snapshot
		want     channelspec.DeviceState
	}{
		{"absent", presence.Snapshot{L3: map[actorrt.ObsKind]presence.Testimony{}}, channelspec.DeviceState{Kind: channelspec.DeviceAbsent}},
		{"known true", presence.Snapshot{L3: map[actorrt.ObsKind]presence.Testimony{kind: {Val: introspect.MarshalDevicePresence(true), ReceivedAt: 10}}}, channelspec.DeviceState{Kind: channelspec.DeviceKnown, Online: true, ReceivedAt: 10}},
		{"known false", presence.Snapshot{L3: map[actorrt.ObsKind]presence.Testimony{kind: {Val: introspect.MarshalDevicePresence(false), ReceivedAt: 11}}}, channelspec.DeviceState{Kind: channelspec.DeviceKnown, Online: false, ReceivedAt: 11}},
		{"stale", presence.Snapshot{L3: map[actorrt.ObsKind]presence.Testimony{kind: {Val: introspect.MarshalDevicePresence(true), ReceivedAt: 12, StaleFromPriorLife: true}}}, channelspec.DeviceState{Kind: channelspec.DeviceStale, ReceivedAt: 12}},
		{"malformed", presence.Snapshot{L3: map[actorrt.ObsKind]presence.Testimony{kind: {Val: []byte("bad"), ReceivedAt: 13}}}, channelspec.DeviceState{Kind: channelspec.DeviceMalformed}},
		{"null document", presence.Snapshot{L3: map[actorrt.ObsKind]presence.Testimony{kind: {Val: []byte(`null`), ReceivedAt: 14}}}, channelspec.DeviceState{Kind: channelspec.DeviceMalformed}},
		{"empty object", presence.Snapshot{L3: map[actorrt.ObsKind]presence.Testimony{kind: {Val: []byte(`{}`), ReceivedAt: 15}}}, channelspec.DeviceState{Kind: channelspec.DeviceMalformed}},
		{"null online", presence.Snapshot{L3: map[actorrt.ObsKind]presence.Testimony{kind: {Val: []byte(`{"online":null}`), ReceivedAt: 16}}}, channelspec.DeviceState{Kind: channelspec.DeviceMalformed}},
		{"wrong online type", presence.Snapshot{L3: map[actorrt.ObsKind]presence.Testimony{kind: {Val: []byte(`{"online":0}`), ReceivedAt: 17}}}, channelspec.DeviceState{Kind: channelspec.DeviceMalformed}},
		{"missing online", presence.Snapshot{L3: map[actorrt.ObsKind]presence.Testimony{kind: {Val: []byte(`{"other":1}`), ReceivedAt: 18}}}, channelspec.DeviceState{Kind: channelspec.DeviceMalformed}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := rosterDeviceState(test.snapshot); got != test.want {
				t.Fatalf("state=%+v want=%+v", got, test.want)
			}
		})
	}
}
