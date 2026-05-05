import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { Command, InvalidArgumentError } from 'commander';
import { PayloadType, SenderKind } from '@coagent/payload-types';
import { parseCsv, parseJsonOption, parsePositiveInteger } from '../../lib/arg-parsers.js';
import { requireChannelId, resolveWorkdir } from '../../lib/channel-fs.js';
import { configureDaemonRpcEnv } from '../../lib/coagent-env.js';
import { CliError } from '../../lib/errors.js';
import { callDaemonRpc } from '../../lib/rpc.js';
import { writeSuccess } from '../../lib/output.js';

const DEFAULT_LIMIT = 20;

function readExplicitTextFile(input: string, flag: string): string {
  const filePath = String(input ?? '').trim();
  if (!filePath) throw new CliError('invalid_arguments', `${flag} requires a file path`, 2);
  try {
    return readFileSync(filePath, 'utf8');
  } catch (error) {
    const message = error instanceof Error ? error.message : 'unknown error';
    throw new CliError('invalid_arguments', `${flag} could not be read: ${message}`, 2);
  }
}

function rejectBothOptions(
  options: Record<string, unknown>,
  literalKey: string,
  fileKey: string,
  literalFlag: string,
  fileFlag: string,
): void {
  if (options[literalKey] != null && options[fileKey] != null) {
    throw new CliError('invalid_arguments', `${literalFlag} and ${fileFlag} cannot be used together`, 2);
  }
}

function parseDurationMs(value: string): number {
  const text = String(value ?? '').trim();
  const numeric = Number(text);
  if (Number.isFinite(numeric) && numeric > 0) return numeric;
  const match = text.match(/^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/i);
  if (!match) {
    throw new InvalidArgumentError('must be a positive duration like 30s, 10m, 2h, or milliseconds');
  }
  const amount = Number(match[1]);
  const unit = match[2].toLowerCase();
  const multipliers: Record<string, number> = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 };
  return Math.round(amount * multipliers[unit]);
}

function parseTimeMs(value: string): number {
  const text = String(value ?? '').trim();
  const durationMatch = text.match(/^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/i);
  if (durationMatch) return Date.now() + parseDurationMs(text);
  const numeric = Number(text);
  if (Number.isFinite(numeric)) return numeric;
  const parsed = Date.parse(text);
  if (Number.isNaN(parsed)) {
    throw new InvalidArgumentError('must be epoch milliseconds, ISO-8601 time, or a relative duration');
  }
  return parsed;
}

function channelIdFromOptions(options: Record<string, unknown>): string {
  return String(options.channel ?? '').trim() || requireChannelId();
}

function senderDefaults(kind: string) {
  switch (kind) {
    case SenderKind.HUMAN:
      return { sender_type: 'human', sender_id: 'cli', sender_name: 'CLI' };
    case SenderKind.SYSTEM:
      return { sender_type: 'system', sender_id: 'system:cli', sender_name: 'system' };
    case SenderKind.EXTERNAL:
      return { sender_type: 'external', sender_id: 'external:cli', sender_name: 'external' };
    case SenderKind.AGENT:
    default: {
      const agentName = String(process.env.COAGENT_AGENT_NAME ?? 'channel-agent').trim() || 'channel-agent';
      return { sender_type: 'channel_agent', sender_id: agentName, sender_name: agentName };
    }
  }
}

function deriveSummary(markdown: string, docPath: string): string {
  const first = markdown
    .split('\n')
    .map((line) => line.trim().replace(/^#+\s*/, ''))
    .find(Boolean);
  return (first || path.basename(docPath)).slice(0, 200);
}

async function rpc<T>(method: string, params: Record<string, unknown>): Promise<T> {
  return callDaemonRpc<T>(method, params, configureDaemonRpcEnv());
}

export function registerProtocolCommands(program: Command): void {
  program.command('emit')
    .description('Emit an envelope message into the channel engine')
    .option('--channel <channelId>', 'channel ID')
    .requiredOption('--payload-type <type>', 'payload.type')
    .option('--payload <json>', 'payload body JSON', parseJsonOption, {})
    .option('--sender-kind <kind>', 'sender kind', SenderKind.AGENT)
    .option('--sender-id <id>', 'sender id')
    .option('--sender-name <name>', 'sender display name')
    .option('--sender-type <type>', 'legacy sender_type')
    .option('--audience <items>', 'comma-separated audience values', parseCsv)
    .option('--mentions <items>', 'comma-separated mention IDs', parseCsv)
    .option('--origin <origin>', 'message origin')
    .option('--parent-id <id>', 'parent message id')
    .option('--correlation-id <id>', 'correlation id')
    .option('--task-id <id>', 'task id')
    .option('--thread-id <id>', 'thread id')
    .option('--not-before <time>', 'epoch ms, ISO time, or relative duration', parseTimeMs)
    .option('--expires-at <time>', 'epoch ms, ISO time, or relative duration', parseTimeMs)
    .option('--message-id <id>', 'explicit envelope id')
    .option('--text <text>', 'content text; defaults to payload.body.text')
    .option('--text-file <path>', 'read content text from a file')
    .action(async (options) => {
      rejectBothOptions(options, 'text', 'textFile', '--text', '--text-file');
      const senderKind = String(options.senderKind ?? SenderKind.AGENT);
      const defaults = senderDefaults(senderKind);
      const payloadBody = options.payload as Record<string, unknown>;
      const content = options.textFile != null
        ? readExplicitTextFile(String(options.textFile), '--text-file')
        : options.text != null
          ? String(options.text)
          : String(payloadBody?.text ?? JSON.stringify(payloadBody));
      writeSuccess(await rpc('message.emit', {
        channel_id: channelIdFromOptions(options),
        message_id: options.messageId,
        sender_kind: senderKind,
        sender_type: options.senderType ?? defaults.sender_type,
        sender_id: options.senderId ?? defaults.sender_id,
        sender_name: options.senderName ?? defaults.sender_name,
        message_type: options.payloadType,
        payload_type: options.payloadType,
        payload_body: payloadBody,
        content,
        audience: options.audience ?? ['channel'],
        mentions: options.mentions,
        origin: options.origin,
        parent_id: options.parentId,
        correlation_id: options.correlationId,
        task_id: options.taskId,
        thread_id: options.threadId,
        not_before: options.notBefore,
        expires_at: options.expiresAt,
      }));
    });

  program.command('query')
    .description('Query channel messages by envelope and payload fields')
    .option('--channel <channelId>', 'channel ID')
    .option('--correlation-id <id>', 'filter by correlation_id')
    .option('--task-id <id>', 'filter by task_id')
    .option('--payload-type <type>', 'filter by payload.type')
    .option('--sender-kind <kind>', 'filter by sender.kind')
    .option('--sender-id <id>', 'filter by sender.id')
    .option('--not-before <time>', 'filter messages with not_before <= time', parseTimeMs)
    .option('--text <text>', 'text search over content/payload body')
    .option('--tag <tag>', 'filter self.memo tag')
    .option('--status <status>', 'filter self.memo status')
    .option('--unread', 'filter rows without delivered_at')
    .option('--include-future', 'include unread messages scheduled for the future')
    .option('--limit <number>', 'maximum number of messages', parsePositiveInteger, DEFAULT_LIMIT)
    .action(async (options) => {
      writeSuccess(await rpc('message.query', {
        channel_id: channelIdFromOptions(options),
        correlation_id: options.correlationId,
        task_id: options.taskId,
        payload_type: options.payloadType,
        sender_kind: options.senderKind,
        sender_id: options.senderId,
        not_before_lte: options.notBefore,
        text: options.text,
        tag: options.tag,
        status: options.status,
        unread: options.unread === true,
        ...(options.includeFuture === true ? { include_future: true } : {}),
        limit: options.limit,
      }));
    });

  program.command('schedule')
    .description('Schedule a self D message by writing not_before into the message engine')
    .option('--channel <channelId>', 'channel ID')
    .requiredOption('--not-before <time>', 'epoch ms, ISO time, or relative duration', parseTimeMs)
    .requiredOption('--payload <json>', 'payload body JSON', parseJsonOption)
    .option('--payload-type <type>', 'payload.type', PayloadType.DISPATCH_SELF_CHECK_DUE)
    .option('--audience <items>', 'comma-separated audience values', parseCsv)
    .option('--correlation-id <id>', 'correlation id')
    .option('--task-id <id>', 'task id')
    .option('--in-task <taskId>', 'task id to attach to')
    .action(async (options) => {
      writeSuccess(await rpc('message.schedule', {
        channel_id: channelIdFromOptions(options),
        not_before: options.notBefore,
        payload_type: options.payloadType,
        payload_body: options.payload,
        audience: options.audience,
        correlation_id: options.correlationId,
        task_id: options.inTask ?? options.taskId,
      }));
    });

  const dispatch = program.command('dispatch').description('Dispatch promise-chain helpers');

  dispatch.command('start')
    .requiredOption('--target <target>', 'self, external:X, or agent:X')
    .requiredOption('--type <type>', 'business dispatch type')
    .requiredOption('--params <json>', 'dispatch params JSON', parseJsonOption)
    .requiredOption('--check-after <duration>', 'self-check delay', parseDurationMs)
    .option('--channel <channelId>', 'channel ID')
    .option('--in-task <taskId>', 'task id to attach to')
    .action(async (options) => {
      writeSuccess(await rpc('dispatch.start', {
        channel_id: channelIdFromOptions(options),
        target: options.target,
        type: options.type,
        params: options.params,
        in_task: options.inTask,
        check_after_ms: options.checkAfter,
      }));
    });

  dispatch.command('check')
    .requiredOption('--correlation-id <id>', 'correlation id')
    .option('--channel <channelId>', 'channel ID')
    .action(async (options) => {
      writeSuccess(await rpc('dispatch.check', {
        channel_id: channelIdFromOptions(options),
        correlation_id: options.correlationId,
      }));
    });

  dispatch.command('renew')
    .requiredOption('--correlation-id <id>', 'correlation id')
    .requiredOption('--check-after <duration>', 'self-check delay', parseDurationMs)
    .option('--channel <channelId>', 'channel ID')
    .action(async (options) => {
      writeSuccess(await rpc('dispatch.renew', {
        channel_id: channelIdFromOptions(options),
        correlation_id: options.correlationId,
        check_after_ms: options.checkAfter,
      }));
    });

  dispatch.command('ls')
    .option('--channel <channelId>', 'channel ID')
    .option('--task-id <taskId>', 'filter by task id')
    .option('--status <status>', 'pending or terminal')
    .action(async (options) => {
      writeSuccess(await rpc('dispatch.list', {
        channel_id: channelIdFromOptions(options),
        task_id: options.taskId,
        status: options.status,
      }));
    });

  program.command('memo')
    .description('Write a self.memo index message')
    .requiredOption('--tag <tag>', 'memo tag')
    .option('--channel <channelId>', 'channel ID')
    .option('--scope <scope>', 'channel or forever', 'channel')
    .option('--doc <path>', 'doc_ref path')
    .option('--correlation-id <id>', 'correlation id')
    .option('--in-task <taskId>', 'task id to attach to')
    .argument('<summary...>', 'self-contained memo summary')
    .action(async (summaryParts: string[], options: any) => {
      const summary = summaryParts.join(' ').trim();
      if (!summary) throw new CliError('invalid_arguments', 'summary is required', 2);
      writeSuccess(await rpc('memo.create', {
        channel_id: channelIdFromOptions(options),
        tag: options.tag,
        scope: options.scope,
        doc: options.doc,
        correlation_id: options.correlationId,
        task_id: options.inTask,
        summary,
      }));
    });

  program.command('recall')
    .description('Recall self.memo entries')
    .requiredOption('--tag <tag>', 'memo tag')
    .option('--channel <channelId>', 'channel ID')
    .option('--limit <number>', 'maximum number of memos', parsePositiveInteger, DEFAULT_LIMIT)
    .option('--status <status>', 'active or all', 'active')
    .action(async (options) => {
      writeSuccess(await rpc('memo.recall', {
        channel_id: channelIdFromOptions(options),
        tag: options.tag,
        limit: options.limit,
        status: options.status,
      }));
    });

  program.command('memo-write')
    .description('Write a markdown doc in the channel workdir and emit a self.memo')
    .requiredOption('--doc <path>', 'doc path relative to channel workdir')
    .option('--content <markdown>', 'markdown content')
    .option('--content-file <path>', 'read markdown content from a file')
    .option('--channel <channelId>', 'channel ID')
    .option('--tag <tag>', 'memo tag', 'pending_action')
    .option('--scope <scope>', 'channel or forever', 'channel')
    .option('--summary <summary>', 'memo summary; defaults to first markdown heading')
    .option('--correlation-id <id>', 'correlation id')
    .option('--in-task <taskId>', 'task id to attach to')
    .action(async (options) => {
      const docPath = String(options.doc ?? '').trim();
      if (!docPath || path.isAbsolute(docPath) || docPath.split(path.sep).includes('..')) {
        throw new CliError('invalid_arguments', 'doc must be a relative path inside the channel workdir', 2);
      }
      rejectBothOptions(options, 'content', 'contentFile', '--content', '--content-file');
      if (options.content == null && options.contentFile == null) {
        throw new CliError('invalid_arguments', '--content or --content-file is required', 2);
      }
      const content = options.contentFile != null
        ? readExplicitTextFile(String(options.contentFile), '--content-file')
        : String(options.content);
      const absoluteDocPath = path.join(resolveWorkdir(), docPath);
      mkdirSync(path.dirname(absoluteDocPath), { recursive: true });
      writeFileSync(absoluteDocPath, content, 'utf8');
      writeSuccess(await rpc('memo.create', {
        channel_id: channelIdFromOptions(options),
        tag: options.tag,
        scope: options.scope,
        doc: docPath,
        correlation_id: options.correlationId,
        task_id: options.inTask,
        summary: options.summary ?? deriveSummary(content, docPath),
      }));
    });
}
