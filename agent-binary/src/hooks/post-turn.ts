import { writeFileSync } from 'node:fs';
import path from 'node:path';
import { archiveLargeArtifacts } from './artifact-archive.js';

export interface PostTurnResult {
  archivedArtifacts: string[];
  workingStatePath: string;
}

export function runPostTurn(
  workdir: string,
  agentName: string,
  summary: string,
): PostTurnResult {
  const workingStatePath = path.join(workdir, 'agents', agentName, 'working-state', 'current.md');
  writeFileSync(workingStatePath, `${summary.trim()}\n`, 'utf8');
  return {
    archivedArtifacts: archiveLargeArtifacts(workdir),
    workingStatePath,
  };
}
