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
	NodeType  FileNodeType
	Size      int64
	// ModifiedAt is Unix milliseconds, zero when the device reported none.
	ModifiedAt int64
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
	ID       resource.ResourceID
	Kind     resourcespec.ResourceKind
	Ops      OpSet
	NodeType FileNodeType
	// Size is the byte count, which a channel's file listing gets from the
	// device's filesystem in the same breath as the name. Other kinds leave it
	// zero, exactly as stat already does for them — every entry carries its
	// Kind, so a reader never has to guess whether a size means anything.
	Size int64
	// ModifiedAt is Unix milliseconds, zero when the device reported none.
	ModifiedAt int64
}

type ListPage struct {
	Entries []ListEntry
	Next    string
	Reject  QueryReject
}

func fileNodeOps(nodeType FileNodeType) OpSet {
	if nodeType == "" || nodeType == FileNodeRegular {
		return OpSet{access.OpRead, access.OpWrite, access.OpDelete}
	}
	// A directory (and an unsupported physical node) has no byte stream. The
	// query itself provides navigation; delete is its only object operation.
	return OpSet{access.OpDelete}
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
		if spec.NodeType == "" {
			spec.NodeType = resourcespec.FileNodeRegular
		}
		if spec.NodeType != resourcespec.FileNodeRegular && spec.NodeType != resourcespec.FileNodeDirectory {
			return fmt.Errorf("%w: invalid file node type", ErrMalformed)
		}
		if spec.NodeType == resourcespec.FileNodeDirectory && spec.WithContent {
			return fmt.Errorf("%w: a directory cannot carry content", ErrMalformed)
		}
		if initial != nil {
			return fmt.Errorf("%w: file bytes use the byte path", ErrMalformed)
		}
		if _, err := resourcespec.ParseFileAddress(string(id)); err != nil {
			return fmt.Errorf("%w: %v", ErrMalformed, err)
		}
	} else if _, err := resourcespec.ParseFileAddress(string(id)); err == nil {
		return fmt.Errorf("%w: kv id must not be a file address", ErrMalformed)
	} else if spec.NodeType != "" {
		return fmt.Errorf("%w: node type belongs to file create", ErrMalformed)
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
	address, file, err := d.fileAddress(id)
	if err != nil {
		return Outcome{}, err
	}
	if !file {
		return Outcome{}, fmt.Errorf("%w: file address required", ErrMalformed)
	}
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
	nodeType := spec.NodeType
	if nodeType == "" {
		nodeType = resourcespec.FileNodeRegular
	}
	if err := d.deps.Files.Create(ctx, mount.DaemonID, address.Path, nodeType); err != nil {
		return executeFailure(ctx, err)
	}
	return Outcome{}, nil
}

func (d *door) stat(ctx context.Context, caller actor.ActorID, id resource.ResourceID) (StatResult, error) {
	id, err := d.normalizeFileName(ctx, caller, id)
	if err != nil {
		return StatResult{}, err
	}
	address, file, addressErr := d.fileAddress(id)
	if addressErr != nil {
		return StatResult{}, addressErr
	}
	if file {
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
		return StatResult{Meta: StatMeta{Kind: resourcespec.KindFile, NodeType: info.NodeType, Size: info.Size, ModifiedAt: info.ModifiedAt}, Ops: fileNodeOps(info.NodeType)}, nil
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

func (d *door) list(ctx context.Context, caller actor.ActorID, q ListQuery) (ListPage, error) {
	normalized, err := d.normalizeFilePrefix(ctx, caller, q.Prefix)
	if err != nil {
		return ListPage{}, err
	}
	q.Prefix = normalized
	if strings.HasPrefix(q.Prefix, resourcespec.DaemonScheme+"://") {
		prefix, err := resourcespec.ParseFilePrefix(q.Prefix)
		if err != nil {
			return ListPage{}, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		if d.deps.ChannelName == "" || prefix.Channel != d.deps.ChannelName {
			return ListPage{}, fmt.Errorf("%w: file address names a different channel", ErrMalformed)
		}
		if _, err := d.authorizeMember(ctx, caller); err != nil {
			return ListPage{}, nil
		}
		mount, err := d.storageMount(ctx, prefix.Host)
		if err != nil {
			return ListPage{}, err
		}
		if d.deps.Files == nil {
			return ListPage{}, errors.New("accessdoor: file control unavailable")
		}
		rows, next, err := d.deps.Files.List(ctx, mount.DaemonID, prefix.Path, normalizeListLimit(q.Limit), q.Cursor)
		if errors.Is(err, ErrMalformedFileCursor) {
			return ListPage{Reject: QueryBadCursor}, nil
		}
		if err != nil {
			return ListPage{}, err
		}
		entries := make([]ListEntry, 0, len(rows))
		for _, row := range rows {
			address, err := resourcespec.FormatFileAddress(resourcespec.FileAddress{Scheme: resourcespec.DaemonScheme, Host: prefix.Host, Channel: prefix.Channel, Path: row.Path})
			if err != nil {
				return ListPage{}, err
			}
			entries = append(entries, ListEntry{ID: resource.ResourceID(address), Kind: resourcespec.KindFile, Ops: fileNodeOps(row.NodeType), NodeType: row.NodeType, Size: row.Size, ModifiedAt: row.ModifiedAt})
		}
		return ListPage{Entries: entries, Next: next}, nil
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
