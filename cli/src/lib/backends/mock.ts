import { ulid } from 'ulid';
import type {
  NoteData,
  NoteInput,
  PublishData,
  PublishInput,
  PublishStatusData,
  RecentData,
  RecentInput,
  SearchData,
  SearchInput,
  XhsBackend,
} from '../backend.js';
import { CliError } from '../errors.js';
import { MockFixtureRepository } from '../mock-fixtures.js';
import { nowIso } from '../time.js';

export class MockXhsBackend implements XhsBackend {
  private readonly fixtures: MockFixtureRepository;

  constructor(fixtures = new MockFixtureRepository()) {
    this.fixtures = fixtures;
  }

  async publish(_input: PublishInput): Promise<PublishData> {
    const noteId = ulid();
    return {
      note_id: noteId,
      url: `https://xhs.com/explore/${noteId}`,
      published_at: nowIso(),
    };
  }

  async search(input: SearchInput): Promise<SearchData> {
    const fixture = this.fixtures.loadSearch(input.keyword);
    return {
      results: fixture.results.slice(0, input.limit),
    };
  }

  async getMyRecent(input: RecentInput): Promise<RecentData> {
    const fixture = this.fixtures.loadRecent();
    return {
      notes: fixture.notes.slice(0, input.limit),
    };
  }

  async getNote(input: NoteInput): Promise<NoteData> {
    const fixture = this.fixtures.loadNote(input.noteId);
    if (!fixture) {
      throw new CliError('note_not_found', `Mock note '${input.noteId}' not found.`, 1);
    }

    return fixture;
  }

  async getPublishStatus(input: NoteInput): Promise<PublishStatusData> {
    return {
      status: 'published',
      url: `https://xhs.com/explore/${input.noteId}`,
    };
  }
}
