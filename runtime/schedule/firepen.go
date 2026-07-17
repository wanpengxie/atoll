package schedule

import (
	"context"
	"strings"

	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

type timerFirePen struct {
	store     timerspec.TimerStore
	authority storespec.ActorAuthority
	channelID channel.ID
}

func NewTimerFirePen(store timerspec.TimerStore, authority storespec.ActorAuthority, channelID channel.ID) TimerFirePen {
	return timerFirePen{store: store, authority: authority, channelID: channelID}
}

func (p timerFirePen) Fire(ctx context.Context, row timerspec.TimerRow, env *message.Envelope) (timerspec.FireOutcome, error) {
	if row.Type == "" || strings.HasPrefix(row.Type, message.ReservedTypePrefix) {
		return 0, FireRejected{Reason: "harness_reserved_type_unauthorized_sender", Detail: row.Type}
	}
	control, ok, err := p.authority.LookupActive(ctx, row.AuthorID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, FireRejected{Reason: "author_not_member", Detail: string(row.AuthorID)}
	}
	verdict, err := p.authority.CheckAuthor(ctx, storespec.AuthorStamp{ID: row.AuthorID, BirthVersion: control.CurrentDeclVersion})
	if err != nil {
		return 0, err
	}
	if verdict != storespec.AuthorOK {
		return 0, FireRejected{Reason: "author_not_member", Detail: string(row.AuthorID)}
	}
	welded := *env
	welded.Sender = message.Sender{ID: row.AuthorID, Kind: control.Kind}
	welded.ChannelID = p.channelID
	if welded.Visibility == "" {
		welded.Visibility = message.VisibilityPublic
	}
	return p.store.FireAndMark(ctx, row.ID, &welded)
}
