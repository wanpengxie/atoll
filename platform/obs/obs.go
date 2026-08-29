// Package obs owns the process-local observation plane: authorization, address
// parsing, dispatch, and the common answer shape. Projection knowledge remains
// with the mechanisms that own each observation word.
package obs

import (
	"context"
	"encoding/json"
)

type Observation struct {
	Subject  string `json:"subject"`
	Kind     string `json:"kind"`
	Complete bool   `json:"complete"`
	Items    []Item `json:"items"`
}

type Item struct {
	Key      string          `json:"key,omitempty"`
	Declared json.RawMessage `json:"declared"`
	Actual   *Actual         `json:"actual"`
}

type Actual struct {
	Measures []Measure `json:"measures"`
}

type Measure struct {
	Name       string          `json:"name"`
	Value      json.RawMessage `json:"value"`
	Unknown    bool            `json:"unknown"`
	Reason     string          `json:"reason,omitempty"`
	ObservedAt int64           `json:"observed_at"`
	Since      *int64          `json:"since"`
}

type ErrorKind string

const (
	ErrBadAddress  ErrorKind = "bad_address"
	ErrUnknownKind ErrorKind = "unknown_kind"
	ErrBadQuery    ErrorKind = "bad_query"
	ErrUnauthed    ErrorKind = "unauthenticated"
	ErrForbidden   ErrorKind = "forbidden"
	ErrNotServing  ErrorKind = "not_serving"
	ErrTimeout     ErrorKind = "timeout"
	ErrCanceled    ErrorKind = "canceled"
	ErrOverloaded  ErrorKind = "overloaded"
	ErrInternal    ErrorKind = "internal"
)

type Error struct {
	Kind   ErrorKind
	Detail string
	Cause  error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Detail != "" {
		return string(e.Kind) + ": " + e.Detail
	}
	if e.Cause != nil {
		return string(e.Kind) + ": " + e.Cause.Error()
	}
	return string(e.Kind)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type Config struct {
	Registry RegistryReader
	Channels ChannelLocator
	Daemons  DaemonReader
	Now      func() int64
}

type Row struct {
	Key      string
	Declared json.RawMessage
}

type RegistryReader interface {
	PrincipalPresent(ctx context.Context, principal string) (bool, error)
	Channels(ctx context.Context, parent *string) (rows []Row, complete bool, err error)
	Channel(ctx context.Context, id string) (row Row, found bool, err error)
	ChannelDevices(ctx context.Context, id string) ([]Row, bool, error)
	Principals(ctx context.Context) ([]Row, bool, error)
	Daemons(ctx context.Context) ([]Row, bool, error)
	Decls(ctx context.Context) ([]Row, bool, error)
}

type ChannelLocator interface {
	Open(id string) bool
	Roster(ctx context.Context, id string) (rows []RosterEntry, serving bool, err error)
}

type RosterEntry struct {
	Key      string
	Declared json.RawMessage
	Bound    bool
	Device   DeviceState
}

type DeviceStateKind string

const (
	DeviceKnown     DeviceStateKind = "known"
	DeviceAbsent    DeviceStateKind = "absent"
	DeviceStale     DeviceStateKind = "stale"
	DeviceMalformed DeviceStateKind = "malformed"
)

type DeviceState struct {
	Kind       DeviceStateKind
	Online     bool
	ReceivedAt int64
}

type DaemonReader interface {
	Online(daemonID string) bool
	OnlineInChannel(daemonID, channelID string) bool
}
