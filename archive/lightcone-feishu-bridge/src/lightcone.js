import { io } from 'socket.io-client';

export async function lightconePost(url, token, teamId, content, { senderId, senderName } = {}) {
  const body = { teamId, content };
  if (senderId) body.senderId = senderId;
  if (senderName) body.senderName = senderName;
  const res = await fetch(`${url}/api/messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`lightconePost failed ${res.status}: ${text}`);
  }
  return res.json();
}

export function lightconeSubscribe(url, teamId, onAgentMessage, token) {
  const socket = io(url, {
    auth: { token: token ?? process.env.LIGHTCONE_TOKEN ?? 'demo-token', serverId: 'server-001' },
    transports: ['websocket'],
  });

  socket.on('connect', () => {
    console.log('[lightcone] Socket.IO connected');
    socket.emit('team:join', teamId);
  });

  socket.on('disconnect', () => {
    console.log('[lightcone] Socket.IO disconnected');
  });

  socket.on('message:new', (msg) => {
    if (msg.senderType === 'agent') onAgentMessage(msg);
  });

  socket.on('ping', () => socket.emit('pong'));

  return socket;
}
