import { Command, CommanderError } from 'commander';
import { registerAdminCommands } from './admin/index.js';
import { registerChannelCommands } from './channel/index.js';
import { registerMessageCommands } from './message/index.js';
import { CliError, toCliError } from '../lib/errors.js';
import { writeFailure } from '../lib/output.js';

async function routeXhsIfNeeded(): Promise<boolean> {
  const args = process.argv.slice(2);
  if (args[0] !== 'xhs') return false;
  process.argv = [process.argv[0] ?? 'node', 'xhs', ...args.slice(1)];
  await import('../index.js');
  return true;
}

async function main(): Promise<void> {
  if (await routeXhsIfNeeded()) return;

  const program = new Command();
  program
    .name('coagent')
    .description('Coagent business CLI')
    .showHelpAfterError(false)
    .configureOutput({ writeErr: () => {} })
    .exitOverride();

  registerChannelCommands(program);
  registerMessageCommands(program);
  registerAdminCommands(program);
  program.command('xhs').description('Run Xiaohongshu business commands');

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
