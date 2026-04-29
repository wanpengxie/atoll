import type { CliError } from './errors.js';

export function writeSuccess<T>(data: T): void {
  process.stdout.write(`${JSON.stringify({ ok: true, data })}\n`);
}

export function writeFailure(error: CliError): void {
  process.stdout.write(
    `${JSON.stringify({
      ok: false,
      error: {
        code: error.code,
        message: error.message,
      },
    })}\n`,
  );
  process.exitCode = error.exitCode;
}
