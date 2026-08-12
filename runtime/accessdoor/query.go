package accessdoor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

type QueryReject string

const (
	QueryNotFound  QueryReject = "resource_not_found"
	QueryBadCursor QueryReject = "bad_cursor"
)

type OpSet []access.Operation

func opSetFromEffective(eff map[access.Operation]bool) OpSet {
	out := make(OpSet, 0, len(objectOps))
	for _, op := range objectOps {
		if eff[op] {
			out = append(out, op)
		}
	}
	return out
}

type StatMeta struct {
	Kind      resourcespec.ResourceKind
	CreatedAt int64
	CreatedBy actor.ActorID
	Size      int64
}

type StatResult struct {
	Meta   StatMeta
	Ops    OpSet
	Reject QueryReject
}

type ListQuery struct {
	Prefix string
	Limit  int
	Cursor string
}

type ListEntry struct {
	ID   resource.ResourceID
	Kind resourcespec.ResourceKind
	Ops  OpSet
}

type ListPage struct {
	Entries []ListEntry
	Next    string
	Reject  QueryReject
}

const (
	defaultListLimit = 50
	maxListLimit     = 500
)

func normalizeListLimit(n int) int {
	if n <= 0 {
		return defaultListLimit
	}
	if n > maxListLimit {
		return maxListLimit
	}
	return n
}

func ingressCreate(id resource.ResourceID, spec resourcespec.CreateSpec, initial []byte) error {
	if err := checkResourceID(id); err != nil {
		return err
	}
	if !resourcespec.ValidKind(spec.Kind) {
		return fmt.Errorf("%w: invalid create kind", ErrMalformed)
	}
	if spec.Kind == resourcespec.KindFile {
		if initial != nil {
			return fmt.Errorf("%w: file bytes use the byte path", ErrMalformed)
		}
		if _, err := resourcespec.ParseFileAddress(string(id)); err != nil {
			return fmt.Errorf("%w: %v", ErrMalformed, err)
		}
	}
	return nil
}

func (d *door) create(ctx context.Context, caller actor.ActorID, id resource.ResourceID, spec resourcespec.CreateSpec, initial []byte) (Outcome, error) {
	facts, err := d.deps.Authority.ResourceActorFacts(ctx, caller)
	if err != nil {
		return Outcome{}, err
	}
	if !facts.Active {
		return Outcome{RejectReason: access.AccessDenied}, nil
	}
	if spec.Kind == resourcespec.KindKV {
		if err := d.deps.Registry.Create(ctx, id, resourcespec.KindKV, caller, initial); err != nil {
			return createVerdict(ctx, err)
		}
		return Outcome{}, nil
	}
	address, _ := resourcespec.ParseFileAddress(string(id))
	mount, err := d.storageMount(ctx, address.Host)
	if err != nil {
		return Outcome{}, err
	}
	if spec.WithContent {
		route, err := d.resolveFileRoute(ctx, caller, id, access.OpWrite)
		if err != nil {
			return Outcome{}, err
		}
		return Outcome{Route: route}, nil
	}
	if d.deps.Files == nil {
		return Outcome{}, errors.New("accessdoor: file control unavailable")
	}
	if err := d.deps.Files.Create(ctx, mount.DaemonID, address.Path); err != nil {
		return executeFailure(ctx, err)
	}
	return Outcome{}, nil
}

func (d *door) stat(ctx context.Context, caller actor.ActorID, id resource.ResourceID) (StatResult, error) {
	if address, err := resourcespec.ParseFileAddress(string(id)); err == nil {
		if _, err := d.authorizeMember(ctx, caller); err != nil {
			return StatResult{Reject: QueryNotFound}, nil
		}
		mount, err := d.storageMount(ctx, address.Host)
		if err != nil {
			return StatResult{}, err
		}
		if d.deps.Files == nil {
			return StatResult{}, errors.New("accessdoor: file control unavailable")
		}
		info, found, err := d.deps.Files.Stat(ctx, mount.DaemonID, address.Path)
		if err != nil {
			return StatResult{}, err
		}
		if !found {
			return StatResult{Reject: QueryNotFound}, nil
		}
		return StatResult{Meta: StatMeta{Kind: resourcespec.KindFile, Size: info.Size}, Ops: OpSet{access.OpRead, access.OpWrite, access.OpDelete}}, nil
	}
	meta, found, err := d.deps.Registry.Resolve(ctx, id)
	if err != nil {
		return StatResult{}, err
	}
	if !found {
		return StatResult{Reject: QueryNotFound}, nil
	}
	facts, err := d.deps.Authority.ResourceActorFacts(ctx, caller)
	if err != nil {
		return StatResult{}, err
	}
	ops := opSetFromEffective(effectiveOps(caller, facts.Active, facts.Owner, meta.CreatedBy))
	if len(ops) == 0 {
		return StatResult{Reject: QueryNotFound}, nil
	}
	return StatResult{Meta: StatMeta{Kind: meta.Kind, CreatedAt: meta.CreatedAt, CreatedBy: meta.CreatedBy}, Ops: ops}, nil
}

func fileListPrefix(raw string) (host, path string, ok bool) {
	const head = "daemon://"
	if !strings.HasPrefix(raw, head) {
		return "", "", false
	}
	rest := strings.TrimPrefix(raw, head)
	i := strings.IndexByte(rest, '/')
	if i <= 0 {
		return "", "", false
	}
	return rest[:i], rest[i+1:], true
}

func (d *door) list(ctx context.Context, caller actor.ActorID, q ListQuery) (ListPage, error) {
	if host, prefix, ok := fileListPrefix(q.Prefix); ok {
		if q.Cursor != "" {
			return ListPage{Reject: QueryBadCursor}, nil
		}
		if _, err := d.authorizeMember(ctx, caller); err != nil {
			return ListPage{}, nil
		}
		mount, err := d.storageMount(ctx, host)
		if err != nil {
			return ListPage{}, err
		}
		if d.deps.Files == nil {
			return ListPage{}, errors.New("accessdoor: file control unavailable")
		}
		rows, err := d.deps.Files.List(ctx, mount.DaemonID, prefix)
		if err != nil {
			return ListPage{}, err
		}
		limit := normalizeListLimit(q.Limit)
		if len(rows) > limit {
			rows = rows[:limit]
		}
		entries := make([]ListEntry, 0, len(rows))
		for _, row := range rows {
			entries = append(entries, ListEntry{ID: resource.ResourceID("daemon://" + host + "/" + row.Path), Kind: resourcespec.KindFile, Ops: OpSet{access.OpRead, access.OpWrite, access.OpDelete}})
		}
		return ListPage{Entries: entries}, nil
	}
	limit := normalizeListLimit(q.Limit)
	rows, next, err := d.deps.Registry.List(ctx, q.Prefix, limit, q.Cursor)
	if errors.Is(err, resourcespec.ErrMalformedCursor) {
		return ListPage{Reject: QueryBadCursor}, nil
	}
	if err != nil {
		return ListPage{}, err
	}
	facts, err := d.deps.Authority.ResourceActorFacts(ctx, caller)
	if err != nil {
		return ListPage{}, err
	}
	entries := make([]ListEntry, 0, len(rows))
	for _, row := range rows {
		ops := opSetFromEffective(effectiveOps(caller, facts.Active, facts.Owner, row.Meta.CreatedBy))
		if len(ops) > 0 {
			entries = append(entries, ListEntry{ID: row.ID, Kind: row.Meta.Kind, Ops: ops})
		}
	}
	return ListPage{Entries: entries, Next: next}, nil
}
