import { existsSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

function missingBuildArtifactError({ packageName, entry, buildCommand }) {
  const err = new Error(
    `Missing build artifact for ${packageName}: ${entry}\n`
    + `Run "${buildCommand}" or "pnpm -r build" before using this command.`,
  );
  err.code = 'missing_build_artifact';
  return err;
}

export function resolveDistEntry({ binDir, relativeDistPath, packageName, buildCommand }) {
  const entry = path.join(binDir, relativeDistPath);
  if (!existsSync(entry)) {
    throw missingBuildArtifactError({ packageName, entry, buildCommand });
  }
  return entry;
}

export async function importDistEntry(options) {
  try {
    const entry = resolveDistEntry(options);
    return await import(pathToFileURL(entry).href);
  } catch (err) {
    if (err?.code !== 'missing_build_artifact') {
      throw err;
    }
    process.stderr.write(`${err.message}\n`);
    process.exitCode = 1;
    return false;
  }
}
