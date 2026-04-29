import { Command, CommanderError } from 'commander';
import { parseJsonOption } from '../lib/arg-parsers.js';
import { readChannelMeta, readSchedules, requireChannelId, resolveWorkdir } from '../lib/channel-fs.js';
import { CliError, toCliError } from '../lib/errors.js';
import { writeFailure, writeSuccess } from '../lib/output.js';
import { callDaemonRpc } from '../lib/rpc.js';

async function main(): Promise<void> {
  const program = new Command();

  program
    .name('coagent-kernel')
    .description('Coagent kernel CLI')
    .showHelpAfterError(false)
    .configureOutput({
      writeErr: () => {},
    })
    .exitOverride();

  program
    .command('schedule-cron')
    .requiredOption('--cron <expr>', 'cron expression')
    .requiredOption('--reason <reason>', 'schedule reason')
    .option('--payload <json>', 'JSON payload', parseJsonOption, {})
    .action(async (options) => {
      const result = await callDaemonRpc<Record<string, unknown>>('schedule.cron', {
        channel_id: requireChannelId(),
        cron: options.cron,
        reason: options.reason,
        payload: options.payload,
      });
      writeSuccess({
        schedule_id: String(result.schedule_id ?? result.id ?? ''),
        schedule: result,
      });
    });

  program
    .command('schedule-at')
    .requiredOption('--at <iso8601>', 'scheduled timestamp')
    .requiredOption('--reason <reason>', 'schedule reason')
    .option('--payload <json>', 'JSON payload', parseJsonOption, {})
    .action(async (options) => {
      const result = await callDaemonRpc<Record<string, unknown>>('schedule.at', {
        channel_id: requireChannelId(),
        at: options.at,
        reason: options.reason,
        payload: options.payload,
      });
      writeSuccess({
        schedule_id: String(result.schedule_id ?? result.id ?? ''),
        schedule: result,
      });
    });

  program
    .command('list-schedules')
    .action(() => {
      writeSuccess({ schedules: readSchedules(resolveWorkdir()) });
    });

  program
    .command('cancel-schedule')
    .requiredOption('--id <scheduleId>', 'schedule ID')
    .action(async (options) => {
      const result = await callDaemonRpc<Record<string, unknown>>('schedule.cancel', {
        channel_id: requireChannelId(),
        id: options.id,
      });
      writeSuccess({
        schedule_id: String(result.schedule_id ?? options.id),
        canceled: Boolean(result.canceled),
      });
    });

  program
    .command('channel-info')
    .action(() => {
      const meta = readChannelMeta(resolveWorkdir());
      writeSuccess({
        id: meta.channel_id,
        name: meta.name,
        type: meta.type,
        status: meta.status,
        capability_set: meta.capability_set,
        members_count: meta.members.length,
        created_at: meta.created_at,
        archived_at: meta.archived_at,
      });
    });

  program
    .command('member-list')
    .action(() => {
      const meta = readChannelMeta(resolveWorkdir());
      writeSuccess({
        members: meta.members.map((member) => ({
          member_type: member.member_type,
          member_id: member.member_id,
          display_name: member.display_name,
          joined_at: member.joined_at,
        })),
      });
    });

  program
    .command('capability-list')
    .action(() => {
      const meta = readChannelMeta(resolveWorkdir());
      writeSuccess({
        cli_binaries: meta.capability_set.cli_binaries,
      });
    });

  program
    .command('sub-agent-spawn')
    .requiredOption('--name <name>', 'sub-agent name')
    .requiredOption('--kind <kind>', 'sub-agent kind')
    .action(() => {
      throw new CliError('not_implemented', 'sub-agent spawn is not implemented in M1.0', 1);
    });

  program
    .command('worker-spawn')
    .action(() => {
      throw new CliError('not_implemented', 'worker spawn is not implemented in M1.0', 1);
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
