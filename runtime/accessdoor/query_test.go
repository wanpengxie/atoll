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
)

// --- Stat ---

func TestDoorStat(t *testing.T) {
	t.Run("owner sees full ops without grants", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true, isOwner: true})
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

	t.Run("a non-member's zero rights masquerade as not_found", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: false})
		res, err := d.stat(context.Background(), "a", "r1")
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if res.Reject != QueryNotFound {
			t.Fatalf("reject = %q, want %q (zero-rights masquerade)", res.Reject, QueryNotFound)
		}
	})

	t.Run("a member sees meta + effective ops, never a coord field", func(t *testing.T) {
		meta := resourcespec.ResourceMeta{
			Kind: resourcespec.KindFile, CreatedAt: 42,
			PlacementKind:     resourcespec.PlacementDaemonLocal,
			PlacementDaemonID: "", PlacementCoord: "should-never-surface",
			CreatedBy: "creator",
		}
		reg := &fakeRegistry{resolveExists: true, resolveMeta: meta}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
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
		if len(res.Ops) != 2 {
			t.Fatalf("ops = %v, want {read,write} for a non-creator member (delete is creator ∨ owner, PM-D3)", res.Ops)
		}
		// StatMeta structurally has no PlacementCoord field — the compiler
		// itself enforces this (there is no res.Meta.PlacementCoord to even
		// reference); this test documents the intent alongside the type.
	})

	t.Run("the creator's Stat echoes delete too (PM-D3)", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKVBy("a")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		res, err := d.stat(context.Background(), "a", "r1")
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if len(res.Ops) != len(objectOps) {
			t.Fatalf("ops = %v, want the full object set for the creator", res.Ops)
		}
	})

	t.Run("resolve error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{resolveErr: errors.New("db down")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		_, err := d.stat(context.Background(), "a", "r1")
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})

	t.Run("facts error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{err: errors.New("x")})
		_, err := d.stat(context.Background(), "a", "r1")
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})
}

// --- List ---

func rowBy(id resource.ResourceID, kind resourcespec.ResourceKind, createdAt int64, creator string) resourcespec.ResourceRow {
	return resourcespec.ResourceRow{
		ID:   id,
		Meta: resourcespec.ResourceMeta{Kind: kind, CreatedAt: createdAt, CreatedBy: actor.ActorID(creator)},
	}
}

func TestDoorList(t *testing.T) {
	t.Run("owner sees every row with full ops", func(t *testing.T) {
		reg := &fakeRegistry{listRows: []resourcespec.ResourceRow{{ID: "orphan", Meta: metaKV()}}}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true, isOwner: true})
		page, err := d.list(context.Background(), "owner", ListQuery{})
		if err != nil || len(page.Entries) != 1 || page.Entries[0].ID != "orphan" || len(page.Entries[0].Ops) != len(objectOps) {
			t.Fatalf("owner list = (%+v,%v), want orphan with full ops", page, err)
		}
	})

	t.Run("a member sees every row (membrane-uniform), delete echoed only on own rows", func(t *testing.T) {
		reg := &fakeRegistry{
			listRows: []resourcespec.ResourceRow{
				rowBy("r1", resourcespec.KindKV, 1, "a"),
				rowBy("r2", resourcespec.KindKV, 2, "someone-else"),
			},
		}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		page, err := d.list(context.Background(), "a", ListQuery{})
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if len(page.Entries) != 2 {
			t.Fatalf("entries = %+v, want both rows visible to a member", page.Entries)
		}
		if len(page.Entries[0].Ops) != len(objectOps) {
			t.Fatalf("own row ops = %v, want full set (creator, PM-D3)", page.Entries[0].Ops)
		}
		if len(page.Entries[1].Ops) != 2 {
			t.Fatalf("foreign row ops = %v, want {read,write} only", page.Entries[1].Ops)
		}
	})

	t.Run("a non-member sees empty Entries with non-empty Next", func(t *testing.T) {
		reg := &fakeRegistry{
			listRows: []resourcespec.ResourceRow{
				rowBy("r1", resourcespec.KindKV, 1, "someone-else"),
			},
			listNextCursor: "raw-cursor-from-registry",
		}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: false})
		page, err := d.list(context.Background(), "a", ListQuery{})
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if len(page.Entries) != 0 {
			t.Fatalf("entries = %+v, want empty (non-member sees nothing)", page.Entries)
		}
		if page.Next == "" {
			t.Fatalf("Next must stay non-empty — caller must keep pulling past an all-invisible page")
		}
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
			Ops:  OpSet{access.OpRead, access.OpWrite, access.OpDelete},
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
		{name: "well-formed empty file (dir)", id: "daemon://host/r1", spec: resourcespec.CreateSpec{Kind: resourcespec.KindFile, Dir: true}},
		{name: "well-formed with-content file declaration", id: "daemon://host/r1", spec: resourcespec.CreateSpec{Kind: resourcespec.KindFile, WithContent: true}},
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
