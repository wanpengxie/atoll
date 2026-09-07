package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const logQueryColumns = `id, ts, ts_received, channel_id,
	sender_kind, sender_id, kind, type, payload,
	COALESCE(parent_id,''), COALESCE(correlation_id,''),
	visibility, audience, expires_at, is_terminal, seq`

func (m *messages) ReadVisibleExchanges(ctx context.Context, before, head int64, limit int) (storespec.ExchangePage, error) {
	if limit < 1 || limit > 256 || before < 0 || head < 0 {
		return storespec.ExchangePage{}, fmt.Errorf("store: invalid exchange window")
	}
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return storespec.ExchangePage{}, err
	}
	defer tx.Rollback()
	var current int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM messages`).Scan(&current); err != nil {
		return storespec.ExchangePage{}, err
	}
	if head == 0 || head > current {
		head = current
	}
	end := head
	if before > 0 {
		end = min(end, before-1)
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+logQueryColumns+` FROM messages
		WHERE seq <= ? AND visibility <> 'system'
		AND kind <> 'response' ORDER BY seq DESC LIMIT ?`, end, limit+1)
	if err != nil {
		return storespec.ExchangePage{}, err
	}
	var starts []storespec.StoredRow
	for rows.Next() {
		row, err := scanEnvelopeRows(rows)
		if err != nil {
			rows.Close()
			return storespec.ExchangePage{}, err
		}
		starts = append(starts, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return storespec.ExchangePage{}, err
	}
	out := storespec.ExchangePage{HeadSeq: head, HasOlder: len(starts) > limit}
	if out.HasOlder {
		starts = starts[:limit]
	}
	for _, start := range starts {
		exchange := []storespec.StoredRow{start}
		if start.Envelope.Kind == message.KindRequest {
			// Terminal first; when absent use latest provisional. Select before
			// filtering visibility: a private terminal must not resurrect an
			// obsolete public progress message as a supposedly open response.
			response, err := scanEnvelope(tx.QueryRowContext(ctx, `SELECT `+logQueryColumns+` FROM messages
				WHERE parent_id = ? AND kind = 'response' AND seq <= ?
				ORDER BY is_terminal DESC, seq DESC LIMIT 1`, start.Envelope.ID, head))
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return storespec.ExchangePage{}, err
			}
			if err == nil && response.Envelope.Visibility != message.VisibilitySystem {
				exchange = append(exchange, response)
			}
		}
		out.Exchanges = append(out.Exchanges, exchange)
	}
	if err := tx.Commit(); err != nil {
		return storespec.ExchangePage{}, err
	}
	return out, nil
}

func (m *messages) FindVisibleSeq(ctx context.Context, seq int64) (storespec.StoredRow, bool, error) {
	row, err := scanEnvelope(m.db.QueryRowContext(ctx, `SELECT `+logQueryColumns+`
		FROM messages WHERE seq = ? AND visibility <> 'system'`, seq))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.StoredRow{}, false, nil
	}
	return row, err == nil, err
}

func (m *messages) FindVisibleID(ctx context.Context, id string) (storespec.StoredRow, bool, error) {
	row, err := scanEnvelope(m.db.QueryRowContext(ctx, `SELECT `+logQueryColumns+` FROM messages WHERE id=? AND visibility <> 'system'`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.StoredRow{}, false, nil
	}
	return row, err == nil, err
}

func (m *messages) ReadVisibleReply(ctx context.Context, id string, head int64) (storespec.StoredRow, bool, error) {
	row, err := scanEnvelope(m.db.QueryRowContext(ctx, `SELECT `+logQueryColumns+` FROM messages WHERE parent_id=? AND kind='response' AND seq<=? ORDER BY is_terminal DESC,seq DESC LIMIT 1`, id, head))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.StoredRow{}, false, nil
	}
	if err == nil && row.Envelope.Visibility == message.VisibilitySystem {
		return storespec.StoredRow{}, false, nil
	}
	return row, err == nil, err
}

func (m *messages) ReadVisibleWindow(ctx context.Context, cursor, head int64, after bool, limit int) (storespec.MessageWindow, error) {
	if cursor < 1 || head < 0 || limit < 1 || limit > 256 {
		return storespec.MessageWindow{}, fmt.Errorf("store: invalid message window")
	}
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return storespec.MessageWindow{}, err
	}
	defer tx.Rollback()
	var current int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM messages`).Scan(&current); err != nil {
		return storespec.MessageWindow{}, err
	}
	if head == 0 || head > current {
		head = current
	}
	op, order := "<", "DESC"
	if after {
		op, order = ">", "ASC"
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+logQueryColumns+` FROM messages AS m
		WHERE seq `+op+` ? AND seq <= ? AND visibility <> 'system'
		AND (kind <> 'response' OR (
			EXISTS (SELECT 1 FROM messages AS p WHERE p.id=m.parent_id AND p.kind='request' AND p.visibility <> 'system' AND p.seq <= ?)
			AND seq=(SELECT r.seq FROM messages AS r WHERE r.parent_id=m.parent_id AND r.kind='response' AND r.seq <= ? ORDER BY r.is_terminal DESC,r.seq DESC LIMIT 1)
		)) ORDER BY seq `+order+` LIMIT ?`, cursor, head, head, head, limit+1)
	if err != nil {
		return storespec.MessageWindow{}, err
	}
	out := storespec.MessageWindow{HeadSeq: head}
	for rows.Next() {
		row, err := scanEnvelopeRows(rows)
		if err != nil {
			rows.Close()
			return storespec.MessageWindow{}, err
		}
		out.Rows = append(out.Rows, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return storespec.MessageWindow{}, err
	}
	out.HasMore = len(out.Rows) > limit
	if out.HasMore {
		out.Rows = out.Rows[:limit]
	}
	if err := tx.Commit(); err != nil {
		return storespec.MessageWindow{}, err
	}
	return out, nil
}
