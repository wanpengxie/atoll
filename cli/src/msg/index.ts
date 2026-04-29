import { existsSync, readFileSync } from 'node:fs';
import { Command, CommanderError } from 'commander';
import { parseCsv, parseIsoTimestamp, parsePositiveInteger } from '../lib/arg-parsers.js';
import { readMessages, requireChannelId, resolveWorkdir } from '../lib/channel-fs.js';
import { CliError, toCliError } from '../lib/errors.js';
import { writeFailure, writeSuccess } from '../lib/output.js';
import { callDaemonRpc } from '../lib/rpc.js';

const DEFAULT_LIMIT = 20;

function sortByCreatedAt<T extends { createdAt?: string; created_at?: string }>(items: T[]): T[] {
  return [...items].sort((left, right) => {
    const a = new Date(left.createdAt ?? left.created_at ?? 0).getTime();
    const b = new Date(right.createdAt ?? right.created_at ?? 0).getTime();
    return a - b;
  });
}

function resolveContent(input: string): string {
  if (existsSync(input)) {
    return readFileSync(input, 'utf8');
  }
  return input;
}

async function main(): Promise<void> {
  const program = new Command();

  program
    .name('coagent-msg')
    .description('Coagent channel messaging CLI')
    .showHelpAfterError(false)
    .configureOutput({
      writeErr: () => {},
    })
    .exitOverride();

  program
    .command('send')
    .requiredOption('--content <textOrPath>', 'message content or a path to a text file')
    .option('--attachments <paths>', 'comma-separated attachment paths', parseCsv, [])
    .action(async (options) => {
      const result = await callDaemonRpc<Record<string, unknown>>('message.send', {
        channel_id: requireChannelId(),
        content: resolveContent(options.content),
        attachments: options.attachments,
      });
      writeSuccess(result);
    });

  program
    .command('check')
    .option('--since <iso8601>', 'only include messages at or after this timestamp', parseIsoTimestamp)
    .option('--limit <number>', 'maximum number of messages', parsePositiveInteger, DEFAULT_LIMIT)
    .action((options) => {
      const sinceTs = options.since ? new Date(options.since).getTime() : null;
      const messages = sortByCreatedAt(readMessages(resolveWorkdir()))
        .filter((message) => {
          if (sinceTs == null) return true;
          const createdAt = new Date(String(message.createdAt ?? message.created_at ?? 0)).getTime();
          return !Number.isNaN(createdAt) && createdAt >= sinceTs;
        })
        .slice(-options.limit)
        .reverse();

      writeSuccess({ messages });
    });

  program
    .command('search')
    .requiredOption('--keyword <keyword>', 'search keyword')
    .option('--limit <number>', 'maximum number of messages', parsePositiveInteger, DEFAULT_LIMIT)
    .action((options) => {
      const needle = String(options.keyword).trim().toLowerCase();
      if (!needle) {
        throw new CliError('invalid_arguments', 'keyword is required', 2);
      }

      const messages = sortByCreatedAt(readMessages(resolveWorkdir()))
        .filter((message) => String(message.content ?? '').toLowerCase().includes(needle))
        .slice(-options.limit)
        .reverse();

      writeSuccess({ messages });
    });

  if (process.argv.length <= 2) {
    program.outputHelp();
    return;
  }

  await program.parseAsync(process.argv);
}

main().catch((error: unknown) => {
  if (error instanceof CommanderError) {
    if (error.code === 'commander.helpDisplayed') {
      process.exitCode = 0;
      return;
    }

    writeFailure(new CliError('invalid_arguments', error.message, error.exitCode || 2));
    return;
  }

  writeFailure(toCliError(error));
});
