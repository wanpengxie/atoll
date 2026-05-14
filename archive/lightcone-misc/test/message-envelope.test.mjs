import assert from 'node:assert/strict';
import test from 'node:test';

import { insertMessage } from '../src/db/index.js';
import { formatMessage } from '../src/internal/index.js';

function fakeDbReturning(row, calls) {
  return {
    async execute(sql, params = []) {
      calls.push({ sql, params });
      if (sql.trim().startsWith('INSERT INTO messages')) {
        return [{ insertId: 42 }];
      }
      return [[row]];
    },
  };
}

test('insertMessage writes canonical envelope fields only', async () => {
  const row = {
    seq: 42,
    id: 'msg-canonical',
    team_id: 'team-a',
    channel_id: null,
    sender_id: 'user-a',
    sender_kind: 'human',
    payload_type: 'user.text',
    payload_body: JSON.stringify({ text: 'hello' }),
    content: 'hello',
    thread_id: null,
    envelope_json: JSON.stringify({ sender: { kind: 'human', id: 'user-a', name: 'User A' } }),
    created_at: '2026-05-06 00:00:00',
    updated_at: '2026-05-06 00:00:00',
  };
  const calls = [];
  const inserted = await insertMessage(fakeDbReturning(row, calls), {
    id: 'msg-canonical',
    teamId: 'team-a',
    senderKind: 'human',
    senderId: 'user-a',
    payloadType: 'user.text',
    payloadBody: { text: 'hello' },
    envelope: { sender: { kind: 'human', id: 'user-a', name: 'User A' } },
    content: 'hello',
  });

  assert.equal(inserted.id, 'msg-canonical');
  assert.match(calls[0].sql, /sender_kind, payload_type, payload_body/);
  assert.equal(calls[0].params[3], 'user-a');
  assert.equal(calls[0].params[4], 'human');
  assert.equal(calls[0].params[5], 'user.text');

  const formatted = formatMessage(row);
  assert.equal(formatted.content, 'hello');
  assert.equal(formatted.senderKind, 'human');
  assert.equal(formatted.payloadType, 'user.text');
  assert.deepEqual(formatted.payloadBody, { text: 'hello' });
  assert.equal(formatted.envelope.sender.name, 'User A');
});

test('insertMessage dedupes daemon message.append retries by request id', async () => {
  const row = {
    seq: 9,
    id: 'msg-existing',
    channel_id: 'channel-a',
    daemon_request_id: 'req-existing',
    sender_id: 'user-a',
    sender_kind: 'human',
    payload_type: 'user.text',
    payload_body: JSON.stringify({ text: 'hello once' }),
    content: 'hello once',
    envelope_json: JSON.stringify({ sender: { kind: 'human', id: 'user-a', name: 'User A' } }),
    created_at: '2026-05-06 00:00:00',
    updated_at: '2026-05-06 00:00:00',
  };
  const calls = [];
  const db = {
    async execute(sql, params = []) {
      calls.push({ sql, params });
      if (sql.trim().startsWith('INSERT INTO messages')) {
        const err = new Error('Duplicate entry');
        err.code = 'ER_DUP_ENTRY';
        err.errno = 1062;
        throw err;
      }
      if (sql.includes('daemon_request_id')) {
        return [[row]];
      }
      return [[]];
    },
  };

  const inserted = await insertMessage(db, {
    id: 'msg-retry',
    channelId: 'channel-a',
    senderKind: 'human',
    senderId: 'user-a',
    payloadType: 'user.text',
    payloadBody: { text: 'hello once' },
    envelope: { sender: { kind: 'human', id: 'user-a', name: 'User A' } },
    content: 'hello once',
    daemonRequestId: 'req-existing',
  });

  assert.equal(inserted.id, 'msg-existing');
  assert.equal(inserted.__deduped, true);
  assert.match(calls[0].sql, /daemon_request_id/);
  assert.equal(calls[0].params.at(-1), 'req-existing');
  assert.match(calls[1].sql, /daemon_request_id/);
});

test('formatMessage exposes envelope fields from the server view cache', () => {
  const formatted = formatMessage({
    seq: 7,
    id: 'msg-envelope',
    team_id: null,
    channel_id: 'channel-a',
    sender_id: 'user-a',
    sender_kind: 'human',
    payload_type: 'user.text',
    payload_body: JSON.stringify({ text: 'hello' }),
    content: 'hello',
    parent_id: 'parent-a',
    correlation_id: 'corr-a',
    task_id: 'task-a',
    thread_id: 'thread-a',
    audience: JSON.stringify(['channel']),
    not_before: null,
    origin: 'external',
    expires_at: 1770000000000,
    ts_received: 1760000000000,
    envelope_json: JSON.stringify({
      id: 'msg-envelope',
      sender: { kind: 'human', id: 'user-a', name: 'User A' },
      mentions: ['agent:channel-agent'],
    }),
    created_at: '2026-05-06 00:00:00',
    updated_at: '2026-05-06 00:00:00',
  });

  assert.equal(formatted.payloadType, 'user.text');
  assert.deepEqual(formatted.payloadBody, { text: 'hello' });
  assert.equal(formatted.correlationId, 'corr-a');
  assert.deepEqual(formatted.envelope.mentions, ['agent:channel-agent']);
  assert.equal(formatted.envelope.sender.kind, 'human');
});
