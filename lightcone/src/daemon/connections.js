// machineId → WebSocket 实例
const daemonConnections = new Map();

export function registerDaemon(machineId, ws) {
  daemonConnections.set(machineId, ws);
}

export function unregisterDaemon(machineId) {
  daemonConnections.delete(machineId);
}

export function getDaemonWs(machineId) {
  return daemonConnections.get(machineId) ?? null;
}

export function isMachineOnline(machineId) {
  const ws = daemonConnections.get(machineId);
  return ws != null && ws.readyState === 1; // OPEN
}

export function sendToDaemon(machineId, msg) {
  const ws = daemonConnections.get(machineId);
  if (!ws || ws.readyState !== 1) {
    console.warn(`[Daemon] Cannot send to machine ${machineId}: not connected`);
    return false;
  }
  ws.send(JSON.stringify(msg));
  return true;
}

export function getAllOnlineMachineIds() {
  return [...daemonConnections.keys()];
}
