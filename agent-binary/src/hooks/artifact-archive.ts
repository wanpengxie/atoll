import {
  copyFileSync,
  mkdirSync,
  readdirSync,
  rmSync,
  statSync,
} from 'node:fs';
import path from 'node:path';

const INLINE_LIMIT_BYTES = 1024;
const SKIP_TOP_LEVEL = new Set(['messages', 'artifacts', 'schedules', 'agents']);
const SQLITE_FILE_PATTERN = /\.(sqlite|sqlite3|db)(?:-(?:wal|shm))?$/i;

function shouldSkip(relPath: string): boolean {
  const [topLevel] = relPath.split(path.sep);
  return SKIP_TOP_LEVEL.has(topLevel) || SQLITE_FILE_PATTERN.test(path.basename(relPath));
}

function collectCandidates(rootDir: string, currentDir: string, files: string[]): void {
  for (const entry of readdirSync(currentDir, { withFileTypes: true })) {
    const fullPath = path.join(currentDir, entry.name);
    const relPath = path.relative(rootDir, fullPath);
    if (!relPath || shouldSkip(relPath)) continue;

    if (entry.isDirectory()) {
      collectCandidates(rootDir, fullPath, files);
      continue;
    }

    files.push(fullPath);
  }
}

export function archiveLargeArtifacts(workdir: string): string[] {
  const candidates: string[] = [];
  collectCandidates(workdir, workdir, candidates);

  const artifactsDir = path.join(workdir, 'artifacts');
  mkdirSync(artifactsDir, { recursive: true });

  const archived: string[] = [];
  for (const candidate of candidates) {
    const stats = statSync(candidate);
    if (!stats.isFile() || stats.size < INLINE_LIMIT_BYTES) continue;

    const archivedName = `${Date.now()}-${path.basename(candidate)}`;
    const archivedPath = path.join(artifactsDir, archivedName);
    copyFileSync(candidate, archivedPath);
    rmSync(candidate, { force: true });
    archived.push(path.relative(workdir, archivedPath));
  }

  return archived;
}
