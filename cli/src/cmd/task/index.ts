import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { Command } from 'commander';
import { configureDaemonRpcEnv } from '../../lib/coagent-env.js';
import { requireChannelId, resolveWorkdir } from '../../lib/channel-fs.js';
import { CliError } from '../../lib/errors.js';
import { callDaemonRpc } from '../../lib/rpc.js';
import { writeSuccess } from '../../lib/output.js';

interface TaskView {
  task?: {
    task_id?: string;
    doc_ref?: string;
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

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

function nowIso(): string {
  return new Date().toISOString();
}

function taskTemplate({ taskId, type, title, parent, rationale }: {
  taskId: string;
  type: string;
  title: string;
  parent?: string;
  rationale?: string;
}): string {
  const timestamp = nowIso();
  return [
    `# ${title}`,
    '',
    '## Brief',
    '',
    rationale || 'TBD',
    '',
    '## Stakeholders',
    '',
    '- Initiator: channel-agent',
    '',
    '## Decisions',
    '',
    '- TBD',
    '',
    '## Constraints',
    '',
    '- TBD',
    '',
    '## Refs',
    '',
    `- Task ID: ${taskId}`,
    `- Type: ${type}`,
    ...(parent ? [`- Parent Task: ${parent}`] : []),
    '',
    '## Timeline',
    '',
    `- ${timestamp} - Task opened.`,
    '',
    '## Status',
    '',
    'Status: opened',
    '',
  ].join('\n');
}

function writeTaskDoc(docRef: string, content: string): void {
  const absoluteDocPath = path.join(resolveWorkdir(), validateRelativeDocRef(docRef));
  mkdirSync(path.dirname(absoluteDocPath), { recursive: true });
  if (!existsSync(absoluteDocPath)) {
    writeFileSync(absoluteDocPath, content, 'utf8');
  }
}

function readTaskDoc(docRef: string): string {
  const absoluteDocPath = path.join(resolveWorkdir(), validateRelativeDocRef(docRef));
  return existsSync(absoluteDocPath) ? readFileSync(absoluteDocPath, 'utf8') : '';
}

function overwriteTaskDoc(docRef: string, content: string): void {
  const absoluteDocPath = path.join(resolveWorkdir(), validateRelativeDocRef(docRef));
  mkdirSync(path.dirname(absoluteDocPath), { recursive: true });
  writeFileSync(absoluteDocPath, content, 'utf8');
}

function appendTimeline(content: string, summary: string): string {
  const line = `- ${nowIso()} - ${summary}`;
  const statusMarker = '\n## Status';
  const statusIndex = content.lastIndexOf(statusMarker);
  if (statusIndex >= 0) {
    return `${content.slice(0, statusIndex).trimEnd()}\n${line}\n${content.slice(statusIndex)}`
      .replace(/\n?$/, '\n');
  }
  return `${content.trimEnd()}\n\n## Timeline\n\n${line}\n`;
}

function appendClosedStatus(content: string, status: string, summary?: string, resultRef?: string): string {
  const lines = [
    '## Status',
    '',
    ...(summary ? [`Summary: ${summary}`] : []),
    ...(resultRef ? [`Result ref: ${resultRef}`] : []),
    `Status: ${status}`,
  ];
  return `${content.trimEnd()}\n\n${lines.join('\n')}\n`;
}

async function loadTaskView(channelId: string, taskId: string): Promise<TaskView> {
  return rpc<TaskView>('task.show', { channel_id: channelId, task_id: taskId });
}

function docRefFromTaskView(view: TaskView, taskId: string): string {
  const docRef = String(view.task?.doc_ref ?? '').trim();
  if (!docRef) {
    throw new CliError('task_doc_missing', `task ${taskId} does not have a doc_ref`, 1);
  }
  return validateRelativeDocRef(docRef);
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
      writeTaskDoc(docRef, taskTemplate({
        taskId,
        type,
        title,
        parent: options.parent ? String(options.parent) : undefined,
        rationale: options.rationale ? String(options.rationale) : undefined,
      }));

      await rpc<Record<string, unknown>>('task.open', {
        channel_id: channelId,
        task_id: taskId,
        type,
        title,
        parent_task_id: options.parent,
        doc_ref: docRef,
        rationale: options.rationale,
      });

      writeSuccess({
        task_id: taskId,
        doc_ref: docRef,
      });
    });

  task.command('close')
    .argument('<task_id>', 'task id')
    .requiredOption('--status <status>', 'completed, failed, or abandoned')
    .option('--channel <channelId>', 'channel ID')
    .option('--summary <markdown>', 'closing summary')
    .option('--result-ref <ref>', 'result reference')
    .action(async (taskId: string, options: Record<string, unknown>) => {
      const channelId = channelIdFromOptions(options);
      const view = await loadTaskView(channelId, taskId);
      const docRef = docRefFromTaskView(view, taskId);
      const status = String(options.status ?? '').trim();
      const summary = options.summary ? String(options.summary) : undefined;
      const resultRef = options.resultRef ? String(options.resultRef) : undefined;
      await rpc<Record<string, unknown>>('task.close', {
        channel_id: channelId,
        task_id: taskId,
        status,
        summary,
        result_ref: resultRef,
      });
      overwriteTaskDoc(docRef, appendClosedStatus(readTaskDoc(docRef), status, summary, resultRef));
      writeSuccess({ task_id: taskId, doc_ref: docRef, status });
    });

  task.command('append')
    .argument('<task_id>', 'task id')
    .argument('<event_summary...>', 'event summary')
    .option('--channel <channelId>', 'channel ID')
    .action(async (taskId: string, summaryParts: string[], options: Record<string, unknown>) => {
      const summary = summaryParts.join(' ').trim();
      if (!summary) throw new CliError('invalid_arguments', 'event summary is required', 2);
      const channelId = channelIdFromOptions(options);
      const view = await loadTaskView(channelId, taskId);
      const docRef = docRefFromTaskView(view, taskId);
      await rpc<Record<string, unknown>>('task.append', {
        channel_id: channelId,
        task_id: taskId,
        summary,
      });
      overwriteTaskDoc(docRef, appendTimeline(readTaskDoc(docRef), summary));
      writeSuccess({ task_id: taskId, doc_ref: docRef });
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
