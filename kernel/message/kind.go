// Package message defines the v4 message envelope, message kind /
// visibility enums, the three closed reason sets (harness reject /
// install / terminal failure), and the RFC 8785 canonical hash function
// used by the Message-Write Harness.
//
// Authoritative spec:
//
//   - Envelope fields           — L0 §2.1 (.dalek/pm/proto-layer0.md)
//   - Kind closed set           — L0 §3.1 invariant I7 + proto-layer0 §3
//   - Visibility enum           — L0 §2.4
//   - Reason closed sets        — L1 §10.3
//   - Canonical hash            — L2 §1.4.10.2
//
// The package depends on kernel/actor for L0 sender identity primitives.
package message

import (
	"database/sql/driver"
	"fmt"
)

// Kind is the v4 message ADT classifier (event / request / response).
//
// Once a message is written, kind is immutable (L0 §2.2 — "kind 写入定死").
type Kind string

// Kind enum — closed set per L0 §3.1 invariant I7.
const (
	KindEvent    Kind = "event"
	KindRequest  Kind = "request"
	KindResponse Kind = "response"
)

// AllKinds enumerates every valid Kind value, in spec order.
var AllKinds = []Kind{KindEvent, KindRequest, KindResponse}

// String returns the wire form.
func (k Kind) String() string { return string(k) }

// Value implements driver.Valuer for SQL TEXT boundaries.
func (k Kind) Value() (driver.Value, error) { return string(k), nil }

// Scan implements sql.Scanner for SQL TEXT boundaries.
func (k *Kind) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*k = ""
		return nil
	case string:
		*k = Kind(v)
		return nil
	case []byte:
		*k = Kind(string(v))
		return nil
	default:
		return fmt.Errorf("message.Kind: scan unsupported %T", src)
	}
}

// Visibility is the envelope `visibility` field — 3-value closed set
// covering who in the channel can query-see this message.
//
// Authoritative spec: proto-layer0 §2.4 (round-3 cluster F). Once
// written, visibility is immutable.
//
// Semantics:
//   - public  — visible to every channel member.
//   - private — only sender + actors in audience may see this message.
//   - system  — protocol-internal metadata / intermediate output
//     (e.g. agent.text progress bubbles, placement notices, bootstrap
//     events). View fanout suppresses these from the default UI view;
//     they still persist in the channel message log (audit trail).
type Visibility string

// Visibility enum — closed set per proto-layer0 §2.4.
const (
	VisibilityPublic  Visibility = "public"
	VisibilityPrivate Visibility = "private"
	VisibilitySystem  Visibility = "system"
)

// AllVisibilities enumerates every valid Visibility value, in spec order.
var AllVisibilities = []Visibility{VisibilityPublic, VisibilityPrivate, VisibilitySystem}

// String returns the wire form.
func (v Visibility) String() string { return string(v) }

// Value implements driver.Valuer for SQL TEXT boundaries.
func (v Visibility) Value() (driver.Value, error) { return string(v), nil }

// Scan implements sql.Scanner for SQL TEXT boundaries.
func (v *Visibility) Scan(src any) error {
	switch x := src.(type) {
	case nil:
		*v = ""
		return nil
	case string:
		*v = Visibility(x)
		return nil
	case []byte:
		*v = Visibility(string(x))
		return nil
	default:
		return fmt.Errorf("message.Visibility: scan unsupported %T", src)
	}
}
