import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const testDir = path.dirname(fileURLToPath(import.meta.url));
const packageDir = path.resolve(testDir, '..');

const mockConfig = {
  channel_type: 'mock',
  display_name: 'Mock Channel',
  description: 'Snapshot fixture channel type.',
  dispatch_table: [
    {
      sender_kind: 'human',
      payload_type: 'user.text',
      protocol: 'agentic',
      handler: 'handle_mock_user_text',
      description: 'Decide whether to reply or open a tracked mock task.',
    },
    {
      sender_kind: 'agent',
      payload_type: 'dispatch.self_check_due',
      protocol: 'hybrid',
      handler: 'handle_mock_self_check',
      description: 'Check the dispatch before choosing a next branch.',
    },
  ],
  business_clis: [
    {
      name: 'mockctl',
      purpose: 'Mock business CLI used only by prompt tests.',
      commands: ['mockctl publish --draft <path>', 'mockctl status --id <id>'],
    },
  ],
  dispatch_types: [
    {
      type: 'mock.publish',
      description: 'Publish a mock artifact asynchronously.',
      target: 'external:mock',
      params: ['draft_ref'],
    },
  ],
  task_types: [
    { type: 'mock.series', description: 'Tracked mock content series.' },
  ],
  invariants: ['Mock tasks must keep docs updated before closing.'],
  business_sop: [
    {
      name: 'handle_mock_self_check',
      protocol: 'hybrid',
      steps: [
        'Run dispatch check.',
        'Renew once if still pending.',
        'Append the linked task timeline when terminal.',
      ],
    },
  ],
  capabilities: ['Publish mock artifacts.', 'Track mock series tasks.'],
};

function env(overrides = {}) {
  return {
    channelId: 'channel-mock',
    channelName: 'Mock Channel',
    channelType: 'mock',
    workspaceId: 'workspace-1',
    workdir: '/tmp/channel-mock',
    agentName: 'channel-agent',
    sessionId: '11111111-1111-4111-8111-111111111111',
    sessionIdPath: '/tmp/channel-mock/agents/channel-agent/session.id',
    daemonSocket: '/tmp/daemon.sock',
    daemonHttp: '',
    daemonToken: '',
    capabilitySet: { cli_binaries: [] },
    channelTypeConfig: mockConfig,
    ...overrides,
  };
}

test('system prompt renders complete snapshot for mock channel-type config', async () => {
  const { buildSystemPrompt } = await import(path.join(packageDir, 'dist', 'prompt', 'system-prompt.js'));
  const snapshot = readFileSync(path.join(testDir, 'fixtures', 'system-prompt.mock.snap'), 'utf8');

  assert.equal(`${buildSystemPrompt(env())}\n`, snapshot);
});

test('system prompt excludes dynamic channel context while user turn injects it', async () => {
  const { buildSystemPrompt, buildUserTurn } = await import(path.join(packageDir, 'dist', 'prompt', 'system-prompt.js'));
  const firstEnv = env({
    channelId: 'channel-a',
    channelName: 'Channel A',
    workspaceId: 'workspace-a',
    workdir: '/tmp/channel-a',
    agentName: 'agent-a',
  });
  const secondEnv = env({
    channelId: 'channel-b',
    channelName: 'Channel B',
    workspaceId: 'workspace-b',
    workdir: '/var/tmp/channel-b',
    agentName: 'agent-b',
  });

  const first = buildSystemPrompt(firstEnv);
  const second = buildSystemPrompt(secondEnv);

  assert.equal(first, second);
  assert.doesNotMatch(first, /(?:^|\n)(?:channel_id|channel_name|agent_name|workspace_id|workdir):/);
  assert.doesNotMatch(first, /channel-a|Channel A|workspace-a|\/tmp\/channel-a|agent-a/);

  const turn = buildUserTurn({ type: 'user.message.posted', payload: { text: 'hello' } }, firstEnv);
  assert.match(turn, /<channel_context>\nchannel_id: channel-a/);
  assert.match(turn, /channel_name: Channel A/);
  assert.match(turn, /agent_name: agent-a/);
  assert.match(turn, /workspace_id: workspace-a/);
  assert.match(turn, /workdir: \/tmp\/channel-a/);
});

test('channel-type config validator accepts shipped fixtures and rejects malformed entries', async () => {
  const {
    parseChannelTypeConfigText,
    validateChannelTypeConfig,
  } = await import(path.join(packageDir, 'dist', 'prompt', 'channel-type-config.js'));

  for (const channelType of ['echo', 'demo', 'xhs-creator']) {
    const raw = readFileSync(path.join(packageDir, 'dist', 'prompt', 'channel-types', `${channelType}.yaml`), 'utf8');
    const config = parseChannelTypeConfigText(raw);
    assert.equal(config.channel_type, channelType);
    assert.equal(config.dispatch_table.length > 0, true);
  }

  assert.throws(
    () => validateChannelTypeConfig({ channel_type: 'broken' }),
    /display_name is required|dispatch_table/,
  );
  assert.throws(
    () => validateChannelTypeConfig({
      ...mockConfig,
      dispatch_table: [{ ...mockConfig.dispatch_table[0], protocol: 'manual' }],
    }),
    /protocol/,
  );
});

test('xhs-creator channel type renders business prompt and missing configs fail fast', async () => {
  const { buildSystemPrompt } = await import(path.join(packageDir, 'dist', 'prompt', 'system-prompt.js'));
  const { genericChannelTypeConfig, loadChannelTypeConfig } = await import(path.join(packageDir, 'dist', 'prompt', 'channel-type-config.js'));

  const prompt = buildSystemPrompt(env({ channelType: 'xhs-creator', channelTypeConfig: undefined }));
  assert.match(prompt, /XHS Creator Channel/);
  assert.match(prompt, /xhs publish --title/);
  assert.match(prompt, /xhs\.record_note/);
  assert.match(prompt, /note\.publish/);
  assert.match(prompt, /topic\.series/);
  assert.doesNotMatch(prompt, /Generic channel type fallback/);

  const genericFixture = genericChannelTypeConfig('unit-fixture');
  assert.equal(genericFixture.channel_type, 'unit-fixture');
  assert.match(genericFixture.description, /Generic channel type fallback/);

  for (const load of [
    () => loadChannelTypeConfig('unknown'),
    () => buildSystemPrompt(env({ channelType: 'unknown', channelTypeConfig: undefined })),
  ]) {
    assert.throws(
      load,
      (error) => {
        assert.equal(error.name, 'ChannelTypeConfigNotFound');
        assert.equal(error.code, 'channel_type_config_not_found');
        assert.match(error.message, /unknown/);
        return true;
      },
    );
  }
});

test('base prompt is identical across echo and demo channel types', async () => {
  const { buildPromptParts } = await import(path.join(packageDir, 'dist', 'prompt', 'system-prompt.js'));

  const echo = buildPromptParts(env({ channelType: 'echo', channelTypeConfig: undefined }));
  const demo = buildPromptParts(env({ channelType: 'demo', channelTypeConfig: undefined }));

  assert.equal(echo.basePrompt, demo.basePrompt);
  assert.notEqual(echo.channelConfigPrompt, demo.channelConfigPrompt);
  assert.match(echo.channelConfigPrompt, /Echo Channel/);
  assert.match(demo.channelConfigPrompt, /Self-Trampolining Demo Channel/);
  assert.doesNotMatch(echo.basePrompt, /Echo Channel|Self-Trampolining Demo Channel|demo\.step|echo\.ping/);
});
