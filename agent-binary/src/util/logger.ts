function formatMessage(level: string, message: string, fields: Record<string, unknown> = {}): string {
  const suffix = Object.keys(fields).length > 0 ? ` ${JSON.stringify(fields)}` : '';
  return `[agent-binary][${level}] ${message}${suffix}`;
}

export function logInfo(message: string, fields?: Record<string, unknown>): void {
  process.stderr.write(`${formatMessage('info', message, fields)}\n`);
}

export function logWarn(message: string, fields?: Record<string, unknown>): void {
  process.stderr.write(`${formatMessage('warn', message, fields)}\n`);
}

export function logError(message: string, fields?: Record<string, unknown>): void {
  process.stderr.write(`${formatMessage('error', message, fields)}\n`);
}
