import { Command } from 'commander';
import { configureDaemonRpcEnv } from '../../lib/coagent-env.js';
import { callDaemonRpc } from '../../lib/rpc.js';
import { writeSuccess } from '../../lib/output.js';

async function rpc<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
  return callDaemonRpc<T>(method, params, configureDaemonRpcEnv());
}

export function registerAdminCommands(program: Command): void {
  const admin = program.command('admin').description('Inspect the local daemon');

  admin.command('status').description('Show daemon status').action(async () => {
    writeSuccess(await rpc('admin.status'));
  });

  admin.command('machines').description('Show daemon machine view').action(async () => {
    writeSuccess(await rpc('admin.machines'));
  });
}
