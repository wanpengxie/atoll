// agentId → Message[]
const agentInboxes = new Map();

export function pushToInbox(agentId, message) {
  if (!agentInboxes.has(agentId)) agentInboxes.set(agentId, []);
  agentInboxes.get(agentId).push(message);
}

export function popInbox(agentId) {
  const msgs = agentInboxes.get(agentId) ?? [];
  agentInboxes.delete(agentId);
  return msgs;
}

export function peekInbox(agentId) {
  return agentInboxes.get(agentId) ?? [];
}

// daemon 重连后批量补发
export function flushInbox(agentId, deliverFn) {
  const msgs = popInbox(agentId);
  for (const msg of msgs) deliverFn(msg);
}
