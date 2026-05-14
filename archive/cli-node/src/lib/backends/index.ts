import type { XhsBackend } from '../backend.js';
import { MockXhsBackend } from './mock.js';

export function resolveBackend(): XhsBackend {
  return new MockXhsBackend();
}
