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

test('insertMessage keeps legacy message writes valid when envelope fields are absent', async () => {
  const row = {
    seq: 42,
    id: 'legacy-msg',
    team_id: 'team-a',
    channel_id: null,
    sender_type: 'user',
    sender_id: 'user-a',
    sender_name: 'User A',
    message_type: 'chat',
    content: 'legacy hello',
    thread_id: null,
    mentions: null,
    created_at: '2026-05-06 00:00:00',
    updated_at: '2026-05-06 00:00:00',
  };
  const calls = [];
  const inserted = await insertMessage(fakeDbReturning(row, calls), {
    id: 'legacy-msg',
    teamId: 'team-a',
    senderType: 'user',
    senderId: 'user-a',
    senderName: 'User A',
    messageType: 'chat',
    content: 'legacy hello',
  });

  assert.equal(inserted.id, 'legacy-msg');
  assert.match(calls[0].sql, /sender_kind, payload_type, payload_body/);
  assert.equal(calls[0].params[7], null);
  assert.equal(calls[0].params[8], null);
  assert.equal(calls[0].params[9], null);

  const formatted = formatMessage(row);
  assert.equal(formatted.content, 'legacy hello');
  assert.equal(formatted.payloadType, null);
  assert.equal(formatted.envelope, null);
});

test('formatMessage exposes envelope fields from the server view cache', () => {
  const formatted = formatMessage({
    seq: 7,
    id: 'msg-envelope',
    team_id: null,
    channel_id: 'channel-a',
    sender_type: 'human',
    sender_id: 'user-a',
    sender_name: 'User A',
    sender_kind: 'human',
    message_type: 'chat',
    payload_type: 'user.text',
    payload_body: JSON.stringify({ text: 'hello' }),
    content: 'hello',
    parent_id: 'parent-a',
    correlation_id: 'corr-a',
    task_id: 'task-a',
    thread_id: 'thread-a',
    audience: JSON.stringify(['channel']),
    mentions: JSON.stringify(['agent:channel-agent']),
    not_before: null,
    origin: 'external',
    expires_at: 1770000000000,
    ts_received: 1760000000000,
    envelope_json: JSON.stringify({ id: 'msg-envelope', sender: { kind: 'human', id: 'user-a' } }),
    created_at: '2026-05-06 00:00:00',
    updated_at: '2026-05-06 00:00:00',
  });

  assert.equal(formatted.payloadType, 'user.text');
  assert.deepEqual(formatted.payloadBody, { text: 'hello' });
  assert.equal(formatted.correlationId, 'corr-a');
  assert.deepEqual(formatted.mentions, ['agent:channel-agent']);
  assert.equal(formatted.envelope.sender.kind, 'human');
});
