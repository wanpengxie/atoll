import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { parse } from 'yaml';
import type { NoteData, RecentData, SearchData } from './backend.js';
import { CliError } from './errors.js';

function fixturePath(filename: string): string {
  return fileURLToPath(new URL(`../../src/mock/fixtures/${filename}`, import.meta.url));
}

function readYamlFixture<T>(filename: string): T {
  const path = fixturePath(filename);
  if (!existsSync(path)) {
    throw new CliError('fixture_not_found', `Mock fixture '${filename}' not found.`, 1);
  }

  return parse(readFileSync(path, 'utf8')) as T;
}

export class MockFixtureRepository {
  loadSearch(keyword: string): SearchData {
    const path = fixturePath(`mock-search-${keyword}.yaml`);
    if (!existsSync(path)) {
      return { results: [] };
    }

    return parse(readFileSync(path, 'utf8')) as SearchData;
  }

  loadRecent(): RecentData {
    return readYamlFixture<RecentData>('mock-recent.yaml');
  }

  loadNote(noteId: string): NoteData | null {
    const path = fixturePath(`mock-note-${noteId}.yaml`);
    if (!existsSync(path)) {
      return null;
    }

    return parse(readFileSync(path, 'utf8')) as NoteData;
  }
}
