import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import type { AgentEnv } from '../types/env.js';

function templatePath(filename: string): string {
  return fileURLToPath(new URL(`./templates/${filename}`, import.meta.url));
}

function readTemplate(filename: string): string {
  return readFileSync(templatePath(filename), 'utf8').trim();
}

function renderTemplate(template: string, values: Record<string, string>): string {
  let output = template;
  for (const [key, value] of Object.entries(values)) {
    output = output.replaceAll(`{{${key}}}`, value);
  }
  return output;
}

function commandList(env: AgentEnv): string {
  const commands = [
    '- `coagent-msg send --content <text-or-path> [--attachments <a,b>]`',
    '- `coagent-msg check [--since <iso8601>] [--limit N]`',
    '- `coagent-msg search --keyword <kw> [--limit N]`',
    '- `coagent-kernel schedule-cron --cron <expr> --reason <text> [--payload <json>]`',
    '- `coagent-kernel schedule-at --at <iso8601> --reason <text> [--payload <json>]`',
    '- `coagent-kernel list-schedules`',
    '- `coagent-kernel cancel-schedule --id <schedule_id>`',
    '- `coagent-kernel channel-info`',
    '- `coagent-kernel member-list`',
    '- `coagent-kernel capability-list`',
  ];

  if (env.capabilitySet.cli_binaries.includes('xhs')) {
    commands.push('- `xhs publish --title <title> --content <path> --images <a,b> [--tags <a,b>]`');
    commands.push('- `xhs search <keyword> [--limit N]`');
    commands.push('- `xhs get-my-recent [--limit N]`');
    commands.push('- `xhs get-note --note-id <id>`');
    commands.push('- `xhs publish-status --note-id <id>`');
  }

  return commands.join('\n');
}

export function buildSystemPrompt(env: AgentEnv): string {
  const values = {
    AGENT_NAME: env.agentName,
    CHANNEL_NAME: env.channelName || env.channelId,
    CLI_COMMANDS: commandList(env),
    WORKDIR: path.resolve(env.workdir),
  };

  const sections = [
    renderTemplate(readTemplate('identity.md'), values),
    renderTemplate(readTemplate('workspace-guide.md'), values),
    renderTemplate(readTemplate('protocol.md'), values),
    renderTemplate(readTemplate('xhs-flow.md'), values),
    renderTemplate(readTemplate('reflection.md'), values),
  ];

  return sections.join('\n\n');
}

export function buildUserTurn(event: unknown): string {
  return [
    'Handle the following daemon trigger event.',
    'Use channel CLI commands when you need to send a reply, inspect channel state, schedule work, or run business actions.',
    'If no visible action is needed, finish quietly after checking the event.',
    '',
    JSON.stringify(event, null, 2),
  ].join('\n');
}
