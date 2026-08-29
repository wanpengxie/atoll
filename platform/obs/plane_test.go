package obs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type registryStub struct {
	present           bool
	presentErr        error
	channels          []Row
	channelsComplete  bool
	channelsErr       error
	channel           Row
	channelFound      bool
	channelErr        error
	channelDevices    []Row
	channelDevicesOK  bool
	channelDevicesErr error
	principals        []Row
	principalsOK      bool
	principalsErr     error
	daemons           []Row
	daemonsOK         bool
	daemonsErr        error
	decls             []Row
	declsOK           bool
	declsErr          error
	seenParent        *string
	seenChannel       string
}

func (s *registryStub) PrincipalPresent(context.Context, string) (bool, error) {
	return s.present, s.presentErr
}
func (s *registryStub) Channels(_ context.Context, parent *string) ([]Row, bool, error) {
	s.seenParent = parent
	return s.channels, s.channelsComplete, s.channelsErr
}
func (s *registryStub) Channel(_ context.Context, id string) (Row, bool, error) {
	s.seenChannel = id
	return s.channel, s.channelFound, s.channelErr
}
func (s *registryStub) ChannelDevices(_ context.Context, id string) ([]Row, bool, error) {
	s.seenChannel = id
	return s.channelDevices, s.channelDevicesOK, s.channelDevicesErr
}
func (s *registryStub) Principals(context.Context) ([]Row, bool, error) {
	return s.principals, s.principalsOK, s.principalsErr
}
func (s *registryStub) Daemons(context.Context) ([]Row, bool, error) {
	return s.daemons, s.daemonsOK, s.daemonsErr
}
func (s *registryStub) Decls(context.Context) ([]Row, bool, error) {
	return s.decls, s.declsOK, s.declsErr
}

type channelStub struct {
	open       map[string]bool
	roster     []RosterEntry
	serving    bool
	rosterErr  error
	rosterSeen string
}

func (s *channelStub) Open(id string) bool { return s.open[id] }
func (s *channelStub) Roster(_ context.Context, id string) ([]RosterEntry, bool, error) {
	s.rosterSeen = id
	return s.roster, s.serving, s.rosterErr
}

type daemonStub map[string]bool

func (s daemonStub) Online(id string) bool             { return s[id] }
func (s daemonStub) OnlineInChannel(id, _ string) bool { return s[id] }

func TestSixObservationWordsHaveCompleteGoldenJSON(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		query    string
		registry *registryStub
		channels *channelStub
		daemons  daemonStub
		golden   string
	}{
		{
			name: "space channels", path: "/obs/space/channels", query: "parent_id=c0",
			registry: &registryStub{present: true, channelsComplete: true, channels: []Row{
				{Key: "b", Declared: json.RawMessage(`{"id":"b","name":"beta"}`)},
				{Key: "a", Declared: json.RawMessage(`{"id":"a","name":"alpha"}`)},
			}},
			channels: &channelStub{open: map[string]bool{"a": false, "b": true}},
			golden:   `{"subject":"space/channels","kind":"channels","complete":true,"items":[{"key":"a","declared":{"id":"a","name":"alpha"},"actual":{"measures":[{"name":"open","value":false,"unknown":false,"observed_at":1000,"since":null}]}},{"key":"b","declared":{"id":"b","name":"beta"},"actual":{"measures":[{"name":"open","value":true,"unknown":false,"observed_at":1000,"since":null}]}}]}`,
		},
		{
			name: "space principals", path: "/obs/space/principals",
			registry: &registryStub{present: true, principalsOK: true, principals: []Row{{Key: "p", Declared: json.RawMessage(`{"id":"p","kind":"human"}`)}}},
			golden:   `{"subject":"space/principals","kind":"principals","complete":true,"items":[{"key":"p","declared":{"id":"p","kind":"human"},"actual":null}]}`,
		},
		{
			name: "space daemons", path: "/obs/space/daemons",
			registry: &registryStub{present: true, daemonsOK: true, daemons: []Row{{Key: "d", Declared: json.RawMessage(`{"id":"d","name":"desk"}`)}}},
			daemons:  daemonStub{"d": false},
			golden:   `{"subject":"space/daemons","kind":"daemons","complete":true,"items":[{"key":"d","declared":{"id":"d","name":"desk"},"actual":{"measures":[{"name":"online","value":false,"unknown":false,"observed_at":1000,"since":null}]}}]}`,
		},
		{
			name: "space decls", path: "/obs/space/decls",
			registry: &registryStub{present: true, declsOK: true, decls: []Row{{Key: "x", Declared: json.RawMessage(`{"id":"x","config":{}}`)}}},
			golden:   `{"subject":"space/decls","kind":"decls","complete":true,"items":[{"key":"x","declared":{"id":"x","config":{}},"actual":null}]}`,
		},
		{
			name: "channel profile", path: "/obs/channel/c%2F%E9%A2%91%E9%81%93/profile",
			registry: &registryStub{present: true, channelFound: true, channel: Row{Declared: json.RawMessage(`{"id":"c/频道","qualified_name":"c0.channel"}`)}},
			channels: &channelStub{open: map[string]bool{"c/频道": true}},
			golden:   `{"subject":"channel/c/频道/profile","kind":"profile","complete":true,"items":[{"declared":{"id":"c/频道","qualified_name":"c0.channel"},"actual":{"measures":[{"name":"open","value":true,"unknown":false,"observed_at":1000,"since":null}]}}]}`,
		},
		{
			name: "channel actors", path: "/obs/channel/c%2F%20%E9%A2%91%E9%81%93/actors",
			registry: &registryStub{present: true, channelFound: true, channel: Row{Declared: json.RawMessage(`{"id":"c0"}`)}},
			channels: &channelStub{serving: true, roster: []RosterEntry{
				{Key: "z", Declared: json.RawMessage(`{"id":"z","kind":"tool"}`), Bound: false, Device: DeviceState{Kind: DeviceAbsent}},
				{Key: "a", Declared: json.RawMessage(`{"id":"a","kind":"human"}`), Bound: true, Device: DeviceState{Kind: DeviceKnown, Online: false, ReceivedAt: 77}},
			}},
			golden: `{"subject":"channel/c/ 频道/actors","kind":"actors","complete":true,"items":[{"key":"a","declared":{"id":"a","kind":"human"},"actual":{"measures":[{"name":"bound","value":true,"unknown":false,"observed_at":1000,"since":null},{"name":"device_online","value":false,"unknown":false,"observed_at":77,"since":null}]}},{"key":"z","declared":{"id":"z","kind":"tool"},"actual":{"measures":[{"name":"bound","value":false,"unknown":false,"observed_at":1000,"since":null},{"name":"device_online","value":null,"unknown":true,"reason":"no_testimony","observed_at":1000,"since":null}]}}]}`,
		},
		{
			name: "channel devices", path: "/obs/channel/c0/devices",
			registry: &registryStub{present: true, channelDevicesOK: true, channelDevices: []Row{{Key: "device-a", Declared: json.RawMessage(`{"device_id":"device-a","name":"Laptop","default_storage":true}`)}}},
			daemons:  daemonStub{"device-a": true},
			golden:   `{"subject":"channel/c0/devices","kind":"devices","complete":true,"items":[{"key":"device-a","declared":{"device_id":"device-a","name":"Laptop","default_storage":true},"actual":{"measures":[{"name":"online","value":true,"unknown":false,"observed_at":1000,"since":null}]}}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plane := New(Config{Registry: test.registry, Channels: test.channels, Daemons: test.daemons, Now: func() int64 { return 1000 }})
			answer, err := plane.Pull(context.Background(), "principal", test.path, test.query)
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
			if test.name == "space channels" {
				if test.registry.seenParent == nil || *test.registry.seenParent != "c0" {
					t.Fatalf("registry parent filter=%v, want c0", test.registry.seenParent)
				}
			}
			if test.name == "channel actors" {
				const decoded = "c/ 频道"
				if test.registry.seenChannel != decoded || test.channels.rosterSeen != decoded {
					t.Fatalf("decoded channel id registry=%q membrane=%q, want %q", test.registry.seenChannel, test.channels.rosterSeen, decoded)
				}
			}
		})
	}
}

func TestEmptySubjectAndPartialListStayInsideObservation(t *testing.T) {
	registry := &registryStub{present: true, channelFound: false, principalsOK: false, principals: []Row{{Key: "p", Declared: json.RawMessage(`{"id":"p"}`)}}}
	plane := New(Config{Registry: registry, Channels: &channelStub{}, Now: func() int64 { return 1 }})
	empty, err := plane.Pull(context.Background(), "principal", "/obs/channel/missing/profile", "")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(empty)
	if string(raw) != `{"subject":"channel/missing/profile","kind":"profile","complete":true,"items":[]}` {
		t.Fatalf("empty observation=%s", raw)
	}
	emptyActors, err := plane.Pull(context.Background(), "principal", "/obs/channel/missing/actors", "")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(emptyActors)
	if string(raw) != `{"subject":"channel/missing/actors","kind":"actors","complete":true,"items":[]}` {
		t.Fatalf("empty actors observation=%s", raw)
	}
	partial, err := plane.Pull(context.Background(), "principal", "/obs/space/principals", "")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(partial)
	if string(raw) != `{"subject":"space/principals","kind":"principals","complete":false,"items":[{"key":"p","declared":{"id":"p"},"actual":null}]}` {
		t.Fatalf("partial observation=%s", raw)
	}
}

func TestAddressAndQueryParserRejectsEveryNonCanonicalShape(t *testing.T) {
	valid, err := parseAddress("/obs/channel/opaque%2F%E9%A2%91%E9%81%93/actors", "")
	if err != nil || valid.channel != "opaque/频道" || valid.subject != "channel/opaque/频道/actors" {
		t.Fatalf("valid address=(%+v,%v)", valid, err)
	}
	for _, test := range []struct {
		path, query string
		kind        ErrorKind
	}{
		{"/space/channels", "", ErrBadAddress},
		{"/obs/space/channels/", "", ErrBadAddress},
		{"/obs/space//channels", "", ErrBadAddress},
		{"/obs/channel/c%2fpart/profile", "", ErrBadAddress},
		{"/obs/channel/c/%70rofile", "", ErrBadAddress},
		{"/obs/space/future", "", ErrUnknownKind},
		{"/obs/channel/c/future", "", ErrUnknownKind},
		{"/obs/space/channels", "parent_id=", ErrBadQuery},
		{"/obs/space/channels", "parent_id=a&parent_id=b", ErrBadQuery},
		{"/obs/space/channels", "parent_id=a&extra=b", ErrBadQuery},
		{"/obs/space/channels", "parent_id=a;b", ErrBadQuery},
		{"/obs/space/principals", "parent_id=c0", ErrBadQuery},
	} {
		_, err := parseAddress(test.path, test.query)
		var typed *Error
		if !errors.As(err, &typed) || typed.Kind != test.kind {
			t.Errorf("parseAddress(%q,%q) err=%v, want %s", test.path, test.query, err, test.kind)
		}
	}
}

func TestDeviceTestimonyFourStateConversion(t *testing.T) {
	plane := New(Config{Now: func() int64 { return 999 }})
	tests := []struct {
		state  DeviceState
		golden string
	}{
		{DeviceState{Kind: DeviceKnown, Online: true, ReceivedAt: 7}, `{"name":"device_online","value":true,"unknown":false,"observed_at":7,"since":null}`},
		{DeviceState{Kind: DeviceKnown, Online: false, ReceivedAt: 8}, `{"name":"device_online","value":false,"unknown":false,"observed_at":8,"since":null}`},
		{DeviceState{Kind: DeviceAbsent}, `{"name":"device_online","value":null,"unknown":true,"reason":"no_testimony","observed_at":999,"since":null}`},
		{DeviceState{Kind: DeviceStale, ReceivedAt: 6}, `{"name":"device_online","value":null,"unknown":true,"reason":"stale_testimony","observed_at":999,"since":null}`},
		{DeviceState{Kind: DeviceMalformed}, `{"name":"device_online","value":null,"unknown":true,"reason":"read_failed","observed_at":999,"since":null}`},
	}
	for _, test := range tests {
		raw, _ := json.Marshal(plane.deviceMeasure(test.state))
		if string(raw) != test.golden {
			t.Errorf("state=%s got=%s want=%s", test.state.Kind, raw, test.golden)
		}
	}
}

func TestAuthorizationFailureAndReadFailureMatrix(t *testing.T) {
	plane := New(Config{Registry: &registryStub{present: false}})
	_, err := plane.Pull(context.Background(), "principal", "/obs/space/decls", "")
	assertErrorKind(t, err, ErrForbidden)
	_, err = plane.Pull(context.Background(), "", "/obs/space/decls", "")
	assertErrorKind(t, err, ErrUnauthed)

	for _, test := range []struct {
		name string
		err  error
		want ErrorKind
	}{
		{"canceled", context.Canceled, ErrCanceled},
		{"timeout", context.DeadlineExceeded, ErrTimeout},
		{"overloaded", &Error{Kind: ErrOverloaded, Detail: "busy"}, ErrOverloaded},
		{"internal", errors.New("broken"), ErrInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := &registryStub{present: true, declsErr: test.err}
			_, err := New(Config{Registry: registry}).Pull(context.Background(), "principal", "/obs/space/decls", "")
			assertErrorKind(t, err, test.want)
		})
	}

	registry := &registryStub{present: true, channelFound: true, channel: Row{Declared: json.RawMessage(`{"id":"c"}`)}}
	_, err = New(Config{Registry: registry, Channels: &channelStub{serving: false}}).Pull(context.Background(), "principal", "/obs/channel/c/actors", "")
	assertErrorKind(t, err, ErrNotServing)
}

func TestCanceledContextStopsBeforePureMemoryLeaves(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	registry := &registryStub{present: true}
	_, err := New(Config{Registry: registry}).Pull(ctx, "principal", "/obs/space/decls", "")
	assertErrorKind(t, err, ErrCanceled)
}

func assertErrorKind(t *testing.T, err error, want ErrorKind) {
	t.Helper()
	var typed *Error
	if !errors.As(err, &typed) || typed.Kind != want {
		t.Fatalf("error=%v, want kind %s", err, want)
	}
}

func TestErrorStringAndUnwrap(t *testing.T) {
	cause := errors.New("cause")
	err := &Error{Kind: ErrInternal, Detail: "detail", Cause: cause}
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "detail") {
		t.Fatalf("typed error=%v", err)
	}
}
