import { InvalidArgumentError } from 'commander';

export function parsePositiveInteger(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new InvalidArgumentError('must be a positive integer');
  }
  return parsed;
}

export function parseCsv(value: string): string[] {
  const items = value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);

  if (items.length === 0) {
    throw new InvalidArgumentError('must contain at least one comma-separated value');
  }

  return items;
}

export function parseJsonOption(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch (error) {
    throw new InvalidArgumentError(
      error instanceof Error ? `must be valid JSON: ${error.message}` : 'must be valid JSON',
    );
  }
}

export function parseIsoTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new InvalidArgumentError('must be a valid ISO-8601 timestamp');
  }
  return date.toISOString();
}
