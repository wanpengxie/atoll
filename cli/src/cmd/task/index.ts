import { existsSync } from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { Command, Option } from 'commander';
import { configureDaemonRpcEnv } from '../../lib/coagent-env.js';
import { requireChannelId, resolveWorkdir } from '../../lib/channel-fs.js';
import { CliError } from '../../lib/errors.js';
import { callDaemonRpc } from '../../lib/rpc.js';
import { writeSuccess } from '../../lib/output.js';

function channelIdFromOptions(options: Record<string, unknown>): string {
  return String(options.channel ?? '').trim() || requireChannelId();
}

async function rpc<T>(method: string, params: Record<string, unknown>): Promise<T> {
  return callDaemonRpc<T>(method, params, configureDaemonRpcEnv());
}

function localDateStamp(date = new Date()): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function slugify(value: string): string {
  const slug = value
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 60)
    .replace(/-+$/g, '');
  return slug || 'task';
}

function validateRelativeDocRef(value: string): string {
  const docRef = String(value ?? '').trim().replace(/\\/g, '/');
  if (!docRef || path.isAbsolute(docRef) || docRef.split('/').includes('..')) {
    throw new CliError('invalid_arguments', 'doc must be a relative path inside the channel workdir', 2);
  }
  return docRef;
}

function defaultDocRef(title: string): string {
  return `notes/tasks/${localDateStamp()}-${slugify(title)}.md`;
}

function docRefExists(docRef: string): boolean {
  return existsSync(path.join(resolveWorkdir(), validateRelativeDocRef(docRef)));
}

function defaultDocRefForNewTask(title: string, taskId: string): string {
  const base = defaultDocRef(title);
  if (!docRefExists(base)) return base;
  const parsed = path.posix.parse(base);
  return path.posix.join(parsed.dir, `${parsed.name}-${taskId.slice(0, 8)}${parsed.ext}`);
}

export function registerTaskCommands(program: Command): void {
  const task = program.command('task').description('Open and inspect task entities');

  task.command('open')
    .requiredOption('--type <type>', 'task type')
    .requiredOption('--title <title>', 'task title')
    .option('--channel <channelId>', 'channel ID')
    .option('--parent <taskId>', 'parent task id')
    .option('--doc <path>', 'doc path relative to channel workdir')
    .option('--rationale <markdown>', 'opening rationale')
    .action(async (options: Record<string, unknown>) => {
      const channelId = channelIdFromOptions(options);
      const title = String(options.title ?? '').trim();
      const type = String(options.type ?? '').trim();
      if (!title) throw new CliError('invalid_arguments', 'title is required', 2);
      if (!type) throw new CliError('invalid_arguments', 'type is required', 2);
      const taskId = randomUUID();
      const docRef = validateRelativeDocRef(String(options.doc ?? defaultDocRefForNewTask(title, taskId)));

      const opened = await rpc<Record<string, unknown>>('task.open', {
        channel_id: channelId,
        task_id: taskId,
        type,
        title,
        parent_task_id: options.parent,
        doc_ref: docRef,
        rationale: options.rationale,
      });
      const openedTaskId = String(opened.task_id ?? taskId);
      const openedDocRef = validateRelativeDocRef(String(opened.doc_ref ?? docRef));

      writeSuccess({
        task_id: openedTaskId,
        doc_ref: openedDocRef,
      });
    });

  task.command('close')
    .argument('<task_id>', 'task id')
    .addOption(new Option('--status <status>', 'completed, failed, or abandoned')
      .choices(['completed', 'failed', 'abandoned'])
      .makeOptionMandatory())
    .option('--channel <channelId>', 'channel ID')
    .option('--summary <markdown>', 'closing summary')
    .option('--result-ref <ref>', 'result reference')
    .action(async (taskId: string, options: Record<string, unknown>) => {
      const channelId = channelIdFromOptions(options);
      const status = String(options.status ?? '').trim();
      const summary = options.summary ? String(options.summary) : undefined;
      const resultRef = options.resultRef ? String(options.resultRef) : undefined;
      const closed = await rpc<Record<string, unknown>>('task.close', {
        channel_id: channelId,
        task_id: taskId,
        status,
        summary,
        result_ref: resultRef,
      });
      const closedTask = closed.task && typeof closed.task === 'object'
        ? (closed.task as Record<string, unknown>)
        : {};
      const docRef = String(closed.doc_ref ?? closedTask.doc_ref ?? '').trim();
      writeSuccess({
        task_id: String(closed.task_id ?? taskId),
        ...(docRef ? { doc_ref: validateRelativeDocRef(docRef) } : {}),
        status: String(closed.status ?? status),
      });
    });

  task.command('append')
    .argument('<task_id>', 'task id')
    .argument('<event_summary...>', 'event summary')
    .option('--channel <channelId>', 'channel ID')
    .action(async (taskId: string, summaryParts: string[], options: Record<string, unknown>) => {
      const summary = summaryParts.join(' ').trim();
      if (!summary) throw new CliError('invalid_arguments', 'event summary is required', 2);
      const channelId = channelIdFromOptions(options);
      const appended = await rpc<Record<string, unknown>>('task.append', {
        channel_id: channelId,
        task_id: taskId,
        summary,
      });
      const docRef = String(appended.doc_ref ?? '').trim();
      writeSuccess({
        task_id: String(appended.task_id ?? taskId),
        ...(docRef ? { doc_ref: validateRelativeDocRef(docRef) } : {}),
      });
    });

  task.command('ls')
    .option('--channel <channelId>', 'channel ID')
    .option('--status <status>', 'task status filter')
    .option('--mine', 'only tasks initiated by this channel agent')
    .option('--parent <taskId>', 'parent task id')
    .action(async (options: Record<string, unknown>) => {
      writeSuccess(await rpc('task.list', {
        channel_id: channelIdFromOptions(options),
        status: options.status,
        mine: options.mine === true,
        parent_task_id: options.parent,
      }));
    });

  task.command('show')
    .argument('<task_id>', 'task id')
    .option('--channel <channelId>', 'channel ID')
    .action(async (taskId: string, options: Record<string, unknown>) => {
      writeSuccess(await rpc('task.show', {
        channel_id: channelIdFromOptions(options),
        task_id: taskId,
      }));
    });

  task.command('tree')
    .option('--channel <channelId>', 'channel ID')
    .option('--root <taskId>', 'root task id')
    .action(async (options: Record<string, unknown>) => {
      writeSuccess(await rpc('task.tree', {
        channel_id: channelIdFromOptions(options),
        root_task_id: options.root,
      }));
    });
}
