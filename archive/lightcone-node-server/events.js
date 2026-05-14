import { nowIso } from './time.js';

export function emitJsonEvent(event, fields = {}) {
  console.log(JSON.stringify({
    ts: nowIso(),
    event,
    ...fields,
  }));
}
