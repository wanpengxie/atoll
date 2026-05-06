const LOCAL_OFFSET_MINUTES = 8 * 60;
const LOCAL_OFFSET_SUFFIX = '+08:00';

export function formatLocalIso(value = Date.now()) {
  const date = value instanceof Date ? value : new Date(value);
  const shifted = new Date(date.getTime() + LOCAL_OFFSET_MINUTES * 60_000);
  return shifted.toISOString().replace('Z', LOCAL_OFFSET_SUFFIX);
}

export function nowIso() {
  return formatLocalIso();
}
