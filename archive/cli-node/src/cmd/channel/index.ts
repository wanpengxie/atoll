import { Command } from 'commander';
import { configureDaemonRpcEnv } from '../../lib/coagent-env.js';
import { callDaemonRpc } from '../../lib/rpc.js';
import { writeSuccess } from '../../lib/output.js';

async function rpc<T>(method: string, params: Record<string, unknown>): Promise<T> {
  return callDaemonRpc<T>(method, params, configureDaemonRpcEnv());
}

export function registerChannelCommands(program: Command): void {
  const channel = program.command('channel').description('Manage coagent channels');

  channel.command('ls').description('List local daemon channels').action(async () => {
    writeSuccess(await rpc('channel.list', {}));
  });

  channel.command('show').description('Show a channel').argument('<channelId>').action(async (channelId) => {
    writeSuccess(await rpc('channel.info', { channel_id: channelId }));
  });

  channel.command('start').description('Start a channel').argument('<channelId>').action(async (channelId) => {
    writeSuccess(await rpc('channel.start', { channel_id: channelId }));
  });

  channel.command('restart').description('Restart a channel').argument('<channelId>').action(async (channelId) => {
    writeSuccess(await rpc('channel.restart', { channel_id: channelId }));
  });

  channel.command('stop').description('Stop a channel').argument('<channelId>').action(async (channelId) => {
    writeSuccess(await rpc('channel.stop', { channel_id: channelId }));
  });

  channel.command('archive').description('Archive a channel').argument('<channelId>').action(async (channelId) => {
    writeSuccess(await rpc('channel.archive', { channel_id: channelId }));
  });
}
