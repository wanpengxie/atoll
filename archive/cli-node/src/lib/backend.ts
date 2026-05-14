export interface PublishInput {
  title: string;
  contentPath: string;
  images: string[];
  tags: string[];
}

export interface SearchInput {
  keyword: string;
  limit: number;
}

export interface RecentInput {
  limit: number;
}

export interface NoteInput {
  noteId: string;
}

export interface PublishData {
  note_id: string;
  url: string;
  published_at: string;
}

export interface SearchItem {
  note_id: string;
  title: string;
  author: string;
  likes: number;
}

export interface SearchData {
  results: SearchItem[];
}

export interface RecentItem {
  note_id: string;
  title: string;
  url: string;
  published_at: string;
}

export interface RecentData {
  notes: RecentItem[];
}

export interface NoteData {
  note: {
    title: string;
    content: string;
    metrics: {
      likes: number;
      comments: number;
      collects: number;
    };
  };
}

export interface PublishStatusData {
  status: 'published' | 'pending' | 'failed';
  url: string;
}

export interface XhsBackend {
  publish(input: PublishInput): Promise<PublishData>;
  search(input: SearchInput): Promise<SearchData>;
  getMyRecent(input: RecentInput): Promise<RecentData>;
  getNote(input: NoteInput): Promise<NoteData>;
  getPublishStatus(input: NoteInput): Promise<PublishStatusData>;
}
