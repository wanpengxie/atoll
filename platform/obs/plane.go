package obs

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"strings"
	"time"
)

const (
	reasonNoTestimony    = "no_testimony"
	reasonStaleTestimony = "stale_testimony"
	reasonReadFailed     = "read_failed"
)

type Plane struct {
	registry RegistryReader
	channels ChannelLocator
	daemons  DaemonReader
	now      func() int64
}

func New(cfg Config) *Plane {
	now := cfg.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	return &Plane{registry: cfg.Registry, channels: cfg.Channels, daemons: cfg.Daemons, now: now}
}

func (p *Plane) Pull(ctx context.Context, principal, escapedPath, rawQuery string) (Observation, error) {
	if err := contextError(ctx); err != nil {
		return Observation{}, err
	}
	if principal == "" {
		return Observation{}, newError(ErrUnauthed, "principal required", nil)
	}
	if p == nil || p.registry == nil {
		return Observation{}, newError(ErrInternal, "registry reader not wired", nil)
	}
	present, err := p.registry.PrincipalPresent(ctx, principal)
	if err != nil {
		return Observation{}, classify(ctx, err)
	}
	if !present {
		return Observation{}, newError(ErrForbidden, "principal is not present", nil)
	}
	address, err := parseAddress(escapedPath, rawQuery)
	if err != nil {
		return Observation{}, err
	}

	switch address.kind {
	case "channels":
		return p.pullChannels(ctx, address)
	case "principals":
		return p.pullPure(ctx, address, p.registry.Principals)
	case "daemons":
		return p.pullDaemons(ctx, address)
	case "decls":
		return p.pullPure(ctx, address, p.registry.Decls)
	case "profile":
		return p.pullProfile(ctx, address)
	case "actors":
		return p.pullActors(ctx, address)
	default:
		return Observation{}, newError(ErrUnknownKind, "unknown observation kind", nil)
	}
}

type address struct {
	subject string
	kind    string
	channel string
	parent  *string
}

func parseAddress(escapedPath, rawQuery string) (address, error) {
	if !strings.HasPrefix(escapedPath, "/obs/") {
		return address{}, newError(ErrBadAddress, "address must start with /obs/", nil)
	}
	escapedSegments := strings.Split(strings.TrimPrefix(escapedPath, "/obs/"), "/")
	segments := make([]string, len(escapedSegments))
	for i, escaped := range escapedSegments {
		if escaped == "" {
			return address{}, newError(ErrBadAddress, "empty address segment", nil)
		}
		decoded, err := url.PathUnescape(escaped)
		if err != nil || url.PathEscape(decoded) != escaped {
			return address{}, newError(ErrBadAddress, "non-canonical address encoding", err)
		}
		segments[i] = decoded
	}

	var out address
	switch {
	case len(segments) == 2 && segments[0] == "space":
		switch segments[1] {
		case "channels", "principals", "daemons", "decls":
			out = address{subject: "space/" + segments[1], kind: segments[1]}
		default:
			return address{}, newError(ErrUnknownKind, "unknown space observation kind", nil)
		}
	case len(segments) == 3 && segments[0] == "channel":
		switch segments[2] {
		case "profile", "actors":
			out = address{subject: "channel/" + segments[1] + "/" + segments[2], kind: segments[2], channel: segments[1]}
		default:
			return address{}, newError(ErrUnknownKind, "unknown channel observation kind", nil)
		}
	default:
		return address{}, newError(ErrBadAddress, "invalid observation address shape", nil)
	}

	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return address{}, newError(ErrBadQuery, "malformed query", err)
	}
	if out.kind != "channels" {
		if len(query) != 0 {
			return address{}, newError(ErrBadQuery, "query is not accepted for this observation", nil)
		}
		return out, nil
	}
	if len(query) == 0 {
		return out, nil
	}
	values, ok := query["parent_id"]
	if !ok || len(query) != 1 || len(values) != 1 || values[0] == "" {
		return address{}, newError(ErrBadQuery, "parent_id must appear once with a non-empty value", nil)
	}
	parent := values[0]
	out.parent = &parent
	return out, nil
}

func (p *Plane) pullChannels(ctx context.Context, a address) (Observation, error) {
	rows, complete, err := p.registry.Channels(ctx, a.parent)
	if err != nil {
		return Observation{}, classify(ctx, err)
	}
	items := make([]Item, 0, len(rows))
	for _, row := range rows {
		if err := contextError(ctx); err != nil {
			return Observation{}, err
		}
		if err := validRow(row); err != nil {
			return Observation{}, err
		}
		if p.channels == nil {
			return Observation{}, newError(ErrInternal, "channel locator not wired", nil)
		}
		open := p.channels.Open(row.Key)
		items = append(items, Item{Key: row.Key, Declared: cloneRaw(row.Declared), Actual: actual(boolMeasure("open", open, p.now()))})
	}
	return observation(a, complete, items), nil
}

func (p *Plane) pullPure(ctx context.Context, a address, read func(context.Context) ([]Row, bool, error)) (Observation, error) {
	rows, complete, err := read(ctx)
	if err != nil {
		return Observation{}, classify(ctx, err)
	}
	items := make([]Item, 0, len(rows))
	for _, row := range rows {
		if err := contextError(ctx); err != nil {
			return Observation{}, err
		}
		if err := validRow(row); err != nil {
			return Observation{}, err
		}
		items = append(items, Item{Key: row.Key, Declared: cloneRaw(row.Declared), Actual: nil})
	}
	return observation(a, complete, items), nil
}

func (p *Plane) pullDaemons(ctx context.Context, a address) (Observation, error) {
	rows, complete, err := p.registry.Daemons(ctx)
	if err != nil {
		return Observation{}, classify(ctx, err)
	}
	items := make([]Item, 0, len(rows))
	for _, row := range rows {
		if err := contextError(ctx); err != nil {
			return Observation{}, err
		}
		if err := validRow(row); err != nil {
			return Observation{}, err
		}
		if p.daemons == nil {
			return Observation{}, newError(ErrInternal, "daemon reader not wired", nil)
		}
		online := p.daemons.Online(row.Key)
		items = append(items, Item{Key: row.Key, Declared: cloneRaw(row.Declared), Actual: actual(boolMeasure("online", online, p.now()))})
	}
	return observation(a, complete, items), nil
}

func (p *Plane) pullProfile(ctx context.Context, a address) (Observation, error) {
	row, found, err := p.registry.Channel(ctx, a.channel)
	if err != nil {
		return Observation{}, classify(ctx, err)
	}
	if !found {
		return observation(a, true, nil), nil
	}
	if err := validRow(row); err != nil {
		return Observation{}, err
	}
	if err := contextError(ctx); err != nil {
		return Observation{}, err
	}
	if p.channels == nil {
		return Observation{}, newError(ErrInternal, "channel locator not wired", nil)
	}
	open := p.channels.Open(a.channel)
	item := Item{Declared: cloneRaw(row.Declared), Actual: actual(boolMeasure("open", open, p.now()))}
	return observation(a, true, []Item{item}), nil
}

func (p *Plane) pullActors(ctx context.Context, a address) (Observation, error) {
	_, found, err := p.registry.Channel(ctx, a.channel)
	if err != nil {
		return Observation{}, classify(ctx, err)
	}
	if !found {
		return observation(a, true, nil), nil
	}
	if err := contextError(ctx); err != nil {
		return Observation{}, err
	}
	if p.channels == nil {
		return Observation{}, newError(ErrInternal, "channel locator not wired", nil)
	}
	rows, serving, err := p.channels.Roster(ctx, a.channel)
	if err != nil {
		return Observation{}, classify(ctx, err)
	}
	if !serving {
		return Observation{}, newError(ErrNotServing, "channel is not serving", nil)
	}
	items := make([]Item, 0, len(rows))
	for _, row := range rows {
		if err := contextError(ctx); err != nil {
			return Observation{}, err
		}
		if err := validRow(Row{Key: row.Key, Declared: row.Declared}); err != nil {
			return Observation{}, err
		}
		measures := []Measure{boolMeasure("bound", row.Bound, p.now()), p.deviceMeasure(row.Device)}
		slices.SortFunc(measures, func(left, right Measure) int { return strings.Compare(left.Name, right.Name) })
		items = append(items, Item{Key: row.Key, Declared: cloneRaw(row.Declared), Actual: &Actual{Measures: measures}})
	}
	return observation(a, true, items), nil
}

func (p *Plane) deviceMeasure(state DeviceState) Measure {
	switch state.Kind {
	case DeviceKnown:
		return boolMeasureAt("device_online", state.Online, state.ReceivedAt)
	case DeviceAbsent:
		return unknownMeasure("device_online", reasonNoTestimony, p.now())
	case DeviceStale:
		return unknownMeasure("device_online", reasonStaleTestimony, p.now())
	case DeviceMalformed:
		return unknownMeasure("device_online", reasonReadFailed, p.now())
	default:
		return unknownMeasure("device_online", reasonReadFailed, p.now())
	}
}

func observation(a address, complete bool, items []Item) Observation {
	if items == nil {
		items = make([]Item, 0)
	}
	slices.SortFunc(items, func(left, right Item) int { return strings.Compare(left.Key, right.Key) })
	return Observation{Subject: a.subject, Kind: a.kind, Complete: complete, Items: items}
}

func actual(measures ...Measure) *Actual {
	if measures == nil {
		measures = make([]Measure, 0)
	}
	slices.SortFunc(measures, func(left, right Measure) int { return strings.Compare(left.Name, right.Name) })
	return &Actual{Measures: measures}
}

func boolMeasure(name string, value bool, observedAt int64) Measure {
	return boolMeasureAt(name, value, observedAt)
}

func boolMeasureAt(name string, value bool, observedAt int64) Measure {
	raw := json.RawMessage("false")
	if value {
		raw = json.RawMessage("true")
	}
	return Measure{Name: name, Value: raw, ObservedAt: observedAt}
}

func unknownMeasure(name, reason string, observedAt int64) Measure {
	return Measure{Name: name, Value: json.RawMessage("null"), Unknown: true, Reason: reason, ObservedAt: observedAt}
}

func validRow(row Row) error {
	if !json.Valid(row.Declared) {
		return newError(ErrInternal, "owner returned malformed declared projection", nil)
	}
	return nil
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	switch ctx.Err() {
	case context.Canceled:
		return newError(ErrCanceled, "observation canceled", context.Canceled)
	case context.DeadlineExceeded:
		return newError(ErrTimeout, "observation timed out", context.DeadlineExceeded)
	default:
		return nil
	}
}

func classify(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := contextError(ctx); ctxErr != nil {
		return ctxErr
	}
	var typed *Error
	if errors.As(err, &typed) {
		return typed
	}
	if errors.Is(err, context.Canceled) {
		return newError(ErrCanceled, "observation canceled", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return newError(ErrTimeout, "observation timed out", err)
	}
	return newError(ErrInternal, "observation read failed", err)
}

func newError(kind ErrorKind, detail string, cause error) *Error {
	return &Error{Kind: kind, Detail: detail, Cause: cause}
}
