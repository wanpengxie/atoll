import assert from 'node:assert/strict';
import test from 'node:test';

import { PayloadType, SenderKind, TriggerDecision } from '@coagent/payload-types';
import { TriggerGateway } from '../src/trigger-gateway.js';

const channel = { agentName: 'channel-agent' };

function messageEvent(senderKind, payloadType, options = {}) {
  return {
    type: payloadType,
    payload: {
      message: {
        senderKind,
        senderId: options.senderId ?? `${senderKind}:sender`,
        payload: {
          type: payloadType,
          body: options.body ?? {},
        },
        mentions: options.mentions ?? null,
      },
    },
  };
}

test('trigger gateway applies the V0 default three-state rules', () => {
  const gateway = new TriggerGateway();
  const cases = [
    ['user text mentions self', messageEvent(SenderKind.HUMAN, PayloadType.USER_TEXT, { mentions: ['agent:channel-agent'] }), TriggerDecision.REACT],
    ['user text no mention', messageEvent(SenderKind.HUMAN, PayloadType.USER_TEXT), TriggerDecision.REACT],
    ['dispatch completed', messageEvent(SenderKind.EXTERNAL, PayloadType.DISPATCH_COMPLETED), TriggerDecision.REACT],
    ['dispatch failed', messageEvent(SenderKind.EXTERNAL, PayloadType.DISPATCH_FAILED), TriggerDecision.REACT],
    ['dispatch rejected', messageEvent(SenderKind.EXTERNAL, PayloadType.DISPATCH_REJECTED), TriggerDecision.REACT],
    ['dispatch accepted', messageEvent(SenderKind.EXTERNAL, PayloadType.DISPATCH_ACCEPTED), TriggerDecision.LOG_ONLY],
    ['agent dispatch start mentions self', messageEvent(SenderKind.AGENT, PayloadType.DISPATCH_START, { mentions: ['agent:channel-agent'] }), TriggerDecision.REACT],
    ['agent dispatch start to external', messageEvent(SenderKind.AGENT, PayloadType.DISPATCH_START), TriggerDecision.LOG_ONLY],
    ['self check due', messageEvent(SenderKind.AGENT, PayloadType.DISPATCH_SELF_CHECK_DUE), TriggerDecision.REACT],
    ['cron tick', { type: 'cron.tick', payload: { schedule_id: 'schedule-1' } }, TriggerDecision.REACT],
    ['channel config updated', { type: 'channel.config.updated', payload: { diff: {} } }, TriggerDecision.REACT],
    ['workdir artifacts change', { type: 'workdir.changed', payload: { path: 'artifacts/a.png' } }, TriggerDecision.LOG_ONLY],
    ['workdir notes change', { type: 'workdir.changed', payload: { path: 'notes/tasks/2026-05-06-camping.md' } }, TriggerDecision.LOG_ONLY],
    ['workdir agents change', { type: 'workdir.changed', payload: { path: 'agents/channel-agent/session.id' } }, TriggerDecision.BLOCK],
    ['agent text echo', messageEvent(SenderKind.AGENT, PayloadType.AGENT_TEXT), TriggerDecision.LOG_ONLY],
    ['task opened', messageEvent(SenderKind.AGENT, PayloadType.TASK_OPENED), TriggerDecision.LOG_ONLY],
    ['task closed', messageEvent(SenderKind.AGENT, PayloadType.TASK_CLOSED), TriggerDecision.LOG_ONLY],
    ['task appended', messageEvent(SenderKind.AGENT, PayloadType.TASK_APPENDED), TriggerDecision.LOG_ONLY],
    ['self memo', messageEvent(SenderKind.AGENT, PayloadType.SELF_MEMO), TriggerDecision.LOG_ONLY],
    ['heartbeat', { type: 'heartbeat', payload: {} }, TriggerDecision.BLOCK],
    ['metric', { type: 'metric.cpu', payload: {} }, TriggerDecision.BLOCK],
  ];

  for (const [name, event, expected] of cases) {
    assert.equal(gateway.evaluate(event, channel).decision, expected, name);
  }
});

test('trigger gateway does not treat channel-agent literal as self for custom agent names', () => {
  const gateway = new TriggerGateway();
  const customChannel = { agentName: 'alice' };

  assert.equal(
    gateway.evaluate(
      messageEvent(SenderKind.AGENT, PayloadType.DISPATCH_START, { mentions: ['agent:channel-agent'] }),
      customChannel,
    ).decision,
    TriggerDecision.LOG_ONLY,
  );
  assert.equal(
    gateway.evaluate(
      messageEvent(SenderKind.AGENT, PayloadType.DISPATCH_START, { mentions: ['agent:alice'] }),
      customChannel,
    ).decision,
    TriggerDecision.REACT,
  );
});

test('trigger gateway dispatch returns stable outcome and delivery shape', async () => {
  const calls = [];
  const gateway = new TriggerGateway({
    onReact: async (_channel, _event, outcome) => {
      calls.push(['react', outcome.reason]);
      return { ok: false, reason: 'delivery refused' };
    },
    onLogOnly: async (_channel, _event, outcome) => {
      calls.push(['log_only', outcome.reason]);
      return { ok: true };
    },
    onBlock: async (_channel, _event, outcome) => {
      calls.push(['block', outcome.reason]);
      return { ok: true };
    },
  });

  const reactOutcome = { decision: TriggerDecision.REACT, reason: 'test_react' };
  const logOnlyOutcome = { decision: TriggerDecision.LOG_ONLY, reason: 'test_log_only' };
  const blockOutcome = { decision: TriggerDecision.BLOCK, reason: 'test_block' };

  const reactResult = await gateway.dispatch({ channel, event: { type: 'user.message.posted' }, outcome: reactOutcome });
  const logOnlyResult = await gateway.dispatch({ channel, event: { type: 'agent.text' }, outcome: logOnlyOutcome });
  const blockResult = await gateway.dispatch({ channel, event: { type: 'metric.cpu' }, outcome: blockOutcome });

  assert.deepEqual(reactResult, {
    outcome: reactOutcome,
    delivery: { ok: false, reason: 'delivery refused' },
  });
  assert.deepEqual(logOnlyResult, {
    outcome: logOnlyOutcome,
    delivery: null,
  });
  assert.deepEqual(blockResult, {
    outcome: blockOutcome,
    delivery: null,
  });
  assert.deepEqual(calls, [
    ['react', 'test_react'],
    ['log_only', 'test_log_only'],
    ['block', 'test_block'],
  ]);
});
