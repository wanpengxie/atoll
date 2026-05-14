import readline from 'node:readline';
import type { StdinEnvelope } from '../types/ipc.js';

export function startStdinReader(onEnvelope: (envelope: StdinEnvelope) => void): void {
  const rl = readline.createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
  });

  rl.on('line', (line) => {
    const trimmed = line.trim();
    if (!trimmed) return;

    try {
      onEnvelope(JSON.parse(trimmed) as StdinEnvelope);
    } catch {}
  });
}
