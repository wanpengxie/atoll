package accessdoor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// --- Stat ---

func TestDoorStat(t *testing.T) {
	t.Run("owner sees full ops without grants", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true, role: storespec.RoleOwner})
		res, err := d.stat(context.Background(), "owner", "r1")
		if err != nil || res.Reject != "" || len(res.Ops) != len(objectOps) {
			t.Fatalf("owner stat = (%+v,%v), want full ops", res, err)
		}
	})
	t.Run("not found", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: false}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		res, err := d.stat(context.Background(), "a", "r1")
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if res.Reject != QueryNotFound {
			t.Fatalf("reject = %q, want %q", res.Reject, QueryNotFound)
		}
	})

	t.Run("zero rights masquerades as not_found", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: false, membersAllow: false}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		res, err := d.stat(context.Background(), "a", "r1")
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if res.Reject != QueryNotFound {
			t.Fatalf("reject = %q, want %q (zero-rights masquerade)", res.Reject, QueryNotFound)
		}
	})

	t.Run("holder sees meta + effective ops, never a coord field", func(t *testing.T) {
		meta := resourcespec.ResourceMeta{
			Kind: resourcespec.KindFile, CreatedAt: 42,
			PlacementKind:     resourcespec.PlacementDaemonLocal,
			PlacementDaemonID: "", PlacementCoord: "should-never-surface",
			CreatedBy: "creator",
		}
		reg := &fakeRegistry{resolveExists: true, resolveMeta: meta, actorAllows: true}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		res, err := d.stat(context.Background(), "a", "r1")
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if res.Reject != "" {
			t.Fatalf("reject = %q, want none", res.Reject)
		}
		if res.Meta.CreatedAt != 42 || res.Meta.CreatedBy != "creator" || res.Meta.Kind != resourcespec.KindFile || res.Meta.PlacementKind != resourcespec.PlacementDaemonLocal {
			t.Fatalf("meta = %+v", res.Meta)
		}
		if len(res.Ops) == 0 {
			t.Fatalf("expected non-empty effective ops for an ActorAllows holder")
		}
		// StatMeta structurally has no PlacementCoord field — the compiler
		// itself enforces this (there is no res.Meta.PlacementCoord to even
		// reference); this test documents the intent alongside the type.
	})

	t.Run("resolve error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{resolveErr: errors.New("db down")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		_, err := d.stat(context.Background(), "a", "r1")
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})

	t.Run("effectiveOps computation error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllowsErr: errors.New("x")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		_, err := d.stat(context.Background(), "a", "r1")
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})
}

// --- List ---

func rowWithActorGrant(id resource.ResourceID, kind resourcespec.ResourceKind, createdAt int64, grantee string, ops ...access.Operation) resourcespec.ResourceRow {
	return resourcespec.ResourceRow{
		ID:   id,
		Meta: resourcespec.ResourceMeta{Kind: kind, CreatedAt: createdAt},
		Grants: []access.Grant{
			{GranteeKind: access.GranteeActor, Grantee: actor.ActorID(grantee), Ops: ops},
		},
	}
}

func TestDoorList(t *testing.T) {
	t.Run("owner sees zero-grant rows with full ops", func(t *testing.T) {
		reg := &fakeRegistry{listRows: []resourcespec.ResourceRow{{ID: "orphan", Meta: metaKV()}}}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true, role: storespec.RoleOwner})
		page, err := d.list(context.Background(), "owner", ListQuery{})
		if err != nil || len(page.Entries) != 1 || page.Entries[0].ID != "orphan" || len(page.Entries[0].Ops) != len(objectOps) {
			t.Fatalf("owner list = (%+v,%v), want orphan with full ops", page, err)
		}
	})
	t.Run("filters rows the caller has zero rights on", func(t *testing.T) {
		reg := &fakeRegistry{
			listRows: []resourcespec.ResourceRow{
				rowWithActorGrant("r1", resourcespec.KindKV, 1, "a", access.OpRead),
				rowWithActorGrant("r2", resourcespec.KindKV, 2, "someone-else", access.OpRead),
			},
		}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		page, err := d.list(context.Background(), "a", ListQuery{})
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if len(page.Entries) != 1 || page.Entries[0].ID != "r1" {
			t.Fatalf("entries = %+v, want only r1", page.Entries)
		}
	})

	t.Run("overlay-only grants are projected — List matches Invoke/Stat authorization", func(t *testing.T) {
		// The caller has ZERO durable grants on the row; its rights live only
		// in the session overlay (a forked grantee / forked-creator
		// convenience grant). List must surface the row, or an overlay-granted
		// caller cannot discover a resource it can access.
		reg := &fakeRegistry{
			listRows: []resourcespec.ResourceRow{
				rowWithActorGrant("r1", resourcespec.KindKV, 1, "someone-else", access.OpRead),
			},
		}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		d.deps.Overlay = &fakeGrantOverlay{allows: true}
		page, err := d.list(context.Background(), "a", ListQuery{})
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if len(page.Entries) != 1 || page.Entries[0].ID != "r1" {
			t.Fatalf("entries = %+v, want r1 visible via overlay", page.Entries)
		}
		if len(page.Entries[0].Ops) == 0 {
			t.Fatalf("overlay-granted ops missing from projection: %+v", page.Entries[0])
		}
	})

	t.Run("empty Entries with non-empty Next when every scanned row is invisible", func(t *testing.T) {
		reg := &fakeRegistry{
			listRows: []resourcespec.ResourceRow{
				rowWithActorGrant("r1", resourcespec.KindKV, 1, "someone-else", access.OpRead),
			},
			listNextCursor: "raw-cursor-from-registry",
		}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		page, err := d.list(context.Background(), "a", ListQuery{})
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if len(page.Entries) != 0 {
			t.Fatalf("entries = %+v, want empty (all rows invisible)", page.Entries)
		}
		if page.Next == "" {
			t.Fatalf("Next must stay non-empty — caller must keep pulling past an all-invisible page")
		}
	})

	t.Run("members grant counts only for a current member", func(t *testing.T) {
		row := resourcespec.ResourceRow{
			ID:   "r1",
			Meta: resourcespec.ResourceMeta{Kind: resourcespec.KindKV, CreatedAt: 1},
			Grants: []access.Grant{
				{GranteeKind: access.GranteeMembers, Ops: []access.Operation{access.OpRead}},
			},
		}
		reg := &fakeRegistry{listRows: []resourcespec.ResourceRow{row}}

		t.Run("member sees it", func(t *testing.T) {
			d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
			page, err := d.list(context.Background(), "a", ListQuery{})
			if err != nil {
				t.Fatalf("unexpected Go error: %v", err)
			}
			if len(page.Entries) != 1 {
				t.Fatalf("entries = %+v, want r1 visible to a member", page.Entries)
			}
		})
		t.Run("non-member does not", func(t *testing.T) {
			d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: false})
			page, err := d.list(context.Background(), "a", ListQuery{})
			if err != nil {
				t.Fatalf("unexpected Go error: %v", err)
			}
			if len(page.Entries) != 0 {
				t.Fatalf("entries = %+v, want none for a non-member", page.Entries)
			}
		})
	})

	t.Run("limit normalization: non-positive defaults, over-ceiling caps", func(t *testing.T) {
		reg := &fakeRegistry{}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		if _, err := d.list(context.Background(), "a", ListQuery{Limit: 0}); err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if _, err := d.list(context.Background(), "a", ListQuery{Limit: -5}); err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if _, err := d.list(context.Background(), "a", ListQuery{Limit: 10_000}); err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if len(reg.listCalls) != 3 {
			t.Fatalf("expected 3 List calls, got %d", len(reg.listCalls))
		}
		if reg.listCalls[0].limit != defaultListLimit || reg.listCalls[1].limit != defaultListLimit {
			t.Fatalf("non-positive limits must normalize to the default: calls = %+v", reg.listCalls)
		}
		if reg.listCalls[2].limit != maxListLimit {
			t.Fatalf("over-ceiling limit must cap at maxListLimit: calls = %+v", reg.listCalls)
		}
	})

	t.Run("cursor round-trips through the door's own prefix-fingerprinted envelope", func(t *testing.T) {
		reg := &fakeRegistry{listNextCursor: "raw-registry-cursor-1"}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		page, err := d.list(context.Background(), "a", ListQuery{Prefix: "doc:"})
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if page.Next == "" {
			t.Fatalf("expected a non-empty Next cursor")
		}
		// Feed the returned cursor back in with the SAME prefix — must decode
		// to the raw registry cursor Registry.List saw.
		if _, err := d.list(context.Background(), "a", ListQuery{Prefix: "doc:", Cursor: page.Next}); err != nil {
			t.Fatalf("unexpected Go error on second call: %v", err)
		}
		if got := reg.listCalls[1].cursor; got != "raw-registry-cursor-1" {
			t.Fatalf("second call's registry cursor = %q, want the first call's raw nextCursor", got)
		}
	})

	t.Run("prefix change on an old cursor is bad_cursor", func(t *testing.T) {
		reg := &fakeRegistry{listNextCursor: "raw-registry-cursor-1"}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		page, err := d.list(context.Background(), "a", ListQuery{Prefix: "doc:"})
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		page2, err := d.list(context.Background(), "a", ListQuery{Prefix: "img:", Cursor: page.Next})
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if page2.Reject != QueryBadCursor {
			t.Fatalf("reject = %q, want bad_cursor for a prefix-changed cursor", page2.Reject)
		}
	})

	t.Run("garbage cursor string is bad_cursor, never a Go error", func(t *testing.T) {
		reg := &fakeRegistry{}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		page, err := d.list(context.Background(), "a", ListQuery{Cursor: "not-valid-base64!!!"})
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if page.Reject != QueryBadCursor {
			t.Fatalf("reject = %q, want bad_cursor", page.Reject)
		}
		if len(reg.listCalls) != 0 {
			t.Fatalf("Registry.List must not run against an undecodable cursor")
		}
	})

	t.Run("Registry.List's own malformed-cursor sentinel maps to bad_cursor, not a Go error", func(t *testing.T) {
		reg := &fakeRegistry{listErr: resourcespec.ErrMalformedCursor}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		page, err := d.list(context.Background(), "a", ListQuery{})
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if page.Reject != QueryBadCursor {
			t.Fatalf("reject = %q, want bad_cursor", page.Reject)
		}
	})

	t.Run("Registry.List infra error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{listErr: errors.New("db down")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		_, err := d.list(context.Background(), "a", ListQuery{})
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})

	t.Run("membership error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{err: errors.New("mem down")})
		_, err := d.list(context.Background(), "a", ListQuery{})
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})
}

// TestListFrameCapBound pins 期11 spec §3.7's "max limit 显式绑定 16MiB 帧
// cap" — a full maxListLimit-entry page, each entry carrying a worst-case-ish
// id/kind/ops shape, must fit well inside one ipc frame. This is a HARD
// assertion (the bound is load-bearing for the wire path, §3.3/§5), not a
// documentation comment.
func TestListFrameCapBound(t *testing.T) {
	page := ListPage{Entries: make([]ListEntry, maxListLimit)}
	for i := range page.Entries {
		page.Entries[i] = ListEntry{
			ID:   resource.ResourceID("workspace/some/reasonably/deep/path/resource-identifier-0000000000"),
			Kind: resourcespec.KindFile,
			Ops:  OpSet{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete},
		}
	}
	raw, err := json.Marshal(page)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(raw) >= ipc.MaxFrameBytes {
		t.Fatalf("a full maxListLimit page marshals to %d bytes, want < ipc.MaxFrameBytes (%d)", len(raw), ipc.MaxFrameBytes)
	}
}

// --- ingressCreate structural rules ---

func TestIngressCreate(t *testing.T) {
	tests := []struct {
		name    string
		id      resource.ResourceID
		spec    resourcespec.CreateSpec
		initial []byte
		wantErr bool
	}{
		{name: "empty id", id: "", spec: resourcespec.CreateSpec{Kind: resourcespec.KindKV}, wantErr: true},
		{name: "kind out of closed set", id: "r1", spec: resourcespec.CreateSpec{Kind: resourcespec.ResourceKind("bogus")}, wantErr: true},
		{name: "dir + with_content is a conflicting combo", id: "r1", spec: resourcespec.CreateSpec{Kind: resourcespec.KindFile, Dir: true, WithContent: true}, wantErr: true},
		{name: "file with non-nil initial bytes", id: "r1", spec: resourcespec.CreateSpec{Kind: resourcespec.KindFile}, initial: []byte("smuggled"), wantErr: true},
		{name: "well-formed kv", id: "r1", spec: resourcespec.CreateSpec{Kind: resourcespec.KindKV}, initial: []byte("v")},
		{name: "well-formed empty file (dir)", id: "r1", spec: resourcespec.CreateSpec{Kind: resourcespec.KindFile, Dir: true}},
		{name: "well-formed with-content file declaration", id: "r1", spec: resourcespec.CreateSpec{Kind: resourcespec.KindFile, WithContent: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ingressCreate(tc.id, tc.spec, tc.initial)
			if tc.wantErr {
				if !errors.Is(err, ErrMalformed) {
					t.Fatalf("err = %v, want ErrMalformed", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
		})
	}
}

// --- cursor codec unit tests ---

func TestQueryCursorCodec(t *testing.T) {
	t.Run("empty cursor decodes to start-from-beginning", func(t *testing.T) {
		raw, ok := decodeQueryCursor("doc:", "")
		if !ok || raw != "" {
			t.Fatalf("raw=%q ok=%v, want empty/true", raw, ok)
		}
	})

	t.Run("round-trip preserves the registry cursor and prefix", func(t *testing.T) {
		enc := encodeQueryCursor("doc:", "created_at\x00id")
		raw, ok := decodeQueryCursor("doc:", enc)
		if !ok || raw != "created_at\x00id" {
			t.Fatalf("raw=%q ok=%v, want the original registry cursor", raw, ok)
		}
	})

	t.Run("wrong prefix fails", func(t *testing.T) {
		enc := encodeQueryCursor("doc:", "x")
		if _, ok := decodeQueryCursor("img:", enc); ok {
			t.Fatalf("expected decode failure for a mismatched prefix")
		}
	})

	t.Run("malformed base64 fails", func(t *testing.T) {
		if _, ok := decodeQueryCursor("doc:", "!!!not base64!!!"); ok {
			t.Fatalf("expected decode failure for invalid base64")
		}
	})
}
