export function emitJsonEvent(event, fields = {}) {
  console.log(JSON.stringify({
    ts: new Date().toISOString(),
    event,
    ...fields,
  }));
}
