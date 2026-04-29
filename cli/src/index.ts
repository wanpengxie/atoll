import { Command, CommanderError } from 'commander';
import { resolveBackend } from './lib/backends/index.js';
import { parseCsv, parsePositiveInteger } from './lib/arg-parsers.js';
import { CliError, toCliError } from './lib/errors.js';
import { writeFailure, writeSuccess } from './lib/output.js';

const DEFAULT_LIMIT = 10;

async function main(): Promise<void> {
  const backend = resolveBackend();
  const program = new Command();

  program
    .name('xhs')
    .description('Coagent XHS CLI')
    .showHelpAfterError(false)
    .configureOutput({
      writeErr: () => {
        // Errors are rendered as JSON envelopes in the top-level catch block.
      },
    })
    .exitOverride();

  program
    .command('publish')
    .description('Publish an XHS note')
    .requiredOption('--title <title>', 'note title')
    .requiredOption('--content <path>', 'path to markdown content')
    .requiredOption('--images <paths>', 'comma-separated image paths', parseCsv)
    .option('--tags <tags>', 'comma-separated tags', parseCsv, [])
    .action(async (options) => {
      const data = await backend.publish({
        title: options.title,
        contentPath: options.content,
        images: options.images,
        tags: options.tags,
      });
      writeSuccess(data);
    });

  program
    .command('search')
    .description('Search XHS notes by keyword')
    .argument('<keyword>', 'search keyword')
    .option('--limit <number>', 'maximum number of results', parsePositiveInteger, DEFAULT_LIMIT)
    .action(async (keyword, options) => {
      const data = await backend.search({
        keyword,
        limit: options.limit,
      });
      writeSuccess(data);
    });

  program
    .command('get-my-recent')
    .description('List recent XHS notes')
    .option('--limit <number>', 'maximum number of notes', parsePositiveInteger, DEFAULT_LIMIT)
    .action(async (options) => {
      const data = await backend.getMyRecent({
        limit: options.limit,
      });
      writeSuccess(data);
    });

  program
    .command('get-note')
    .description('Get an XHS note by note_id')
    .requiredOption('--note-id <id>', 'note ID')
    .action(async (options) => {
      const data = await backend.getNote({
        noteId: options.noteId,
      });
      writeSuccess(data);
    });

  program
    .command('publish-status')
    .description('Get publish status by note_id')
    .requiredOption('--note-id <id>', 'note ID')
    .action(async (options) => {
      const data = await backend.getPublishStatus({
        noteId: options.noteId,
      });
      writeSuccess(data);
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
