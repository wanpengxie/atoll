import { existsSync, readFileSync } from 'node:fs';
import { Command } from 'commander';
import { parseCsv, parsePositiveInteger } from '../../lib/arg-parsers.js';
import { configureDaemonRpcEnv } from '../../lib/coagent-env.js';
import { callDaemonRpc } from '../../lib/rpc.js';
import { writeSuccess } from '../../lib/output.js';

const DEFAULT_LIMIT = 20;

function resolveText(input: string): string {
  return existsSync(input) ? readFileSync(input, 'utf8') : input;
}

async function rpc<T>(method: string, params: Record<string, unknown>): Promise<T> {
  return callDaemonRpc<T>(method, params, configureDaemonRpcEnv());
}

export function registerMessageCommands(program: Command): void {
  const message = program.command('message').description('Send and inspect channel messages');

  message.command('send')
    .requiredOption('--channel <channelId>', 'channel ID')
    .requiredOption('--text <textOrPath>', 'message text or a path to a text file')
    .option('--attachments <paths>', 'comma-separated attachment paths', parseCsv, [])
    .action(async (options) => {
      writeSuccess(await rpc('message.send', {
        channel_id: options.channel,
        content: resolveText(options.text),
        attachments: options.attachments,
        sender_type: 'human',
        sender_id: 'cli',
        sender_name: 'CLI',
      }));
    });

  message.command('history')
    .requiredOption('--channel <channelId>', 'channel ID')
    .option('--limit <number>', 'maximum number of messages', parsePositiveInteger, DEFAULT_LIMIT)
    .action(async (options) => {
      writeSuccess(await rpc('message.list', { channel_id: options.channel, limit: options.limit }));
    });

  message.command('search')
    .requiredOption('--channel <channelId>', 'channel ID')
    .requiredOption('--query <query>', 'search query')
    .option('--limit <number>', 'maximum number of messages', parsePositiveInteger, DEFAULT_LIMIT)
    .action(async (options) => {
      writeSuccess(await rpc('message.search', {
        channel_id: options.channel,
        query: options.query,
        limit: options.limit,
      }));
    });
}
