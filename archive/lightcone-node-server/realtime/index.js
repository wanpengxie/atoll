import { Server as SocketIOServer } from 'socket.io';
import { setIo } from './broadcast.js';
import { getDb, getMessagesSince, maxSeq } from '../db/index.js';
import { formatMessage } from '../internal/index.js';
import { emitJsonEvent } from '../events.js';

export function setupSocketIO(httpServer) {
  const io = new SocketIOServer(httpServer, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    transports: ['websocket', 'polling'],
  });

  setIo(io);

  io.on('connection', (socket) => {
    const { serverId } = socket.handshake.auth ?? {};
    console.error(`[SocketIO] Client connected: ${socket.id}, server=${serverId}`);
    emitJsonEvent('socket.connection', { socket_id: socket.id, server_id: serverId ?? null });
    if (serverId) socket.join(`server:${serverId}`);

    socket.on('team:join',  (teamId) => { socket.join(`team:${teamId}`); });
    socket.on('team:leave', (teamId) => { socket.leave(`team:${teamId}`); });
    socket.on('channel:join',  (channelId) => { socket.join(`channel:${channelId}`); });
    socket.on('channel:leave', (channelId) => { socket.leave(`channel:${channelId}`); });

    socket.on('sync:resume', async ({ since, teamId } = {}) => {
      const db = getDb();
      const msgs = await getMessagesSince(db, since ?? 0, teamId);
      socket.emit('sync:messages', {
        messages: msgs.map(formatMessage),
        currentSeq: await maxSeq(db),
      });
    });

    socket.on('disconnect', () => {
      console.error(`[SocketIO] Client disconnected: ${socket.id}`);
      emitJsonEvent('socket.disconnect', { socket_id: socket.id, server_id: serverId ?? null });
    });
    socket.on('ping', () => socket.emit('pong'));
  });

  setInterval(() => { io.emit('ping'); }, 25_000);
  console.error('[SocketIO] Ready');
  return io;
}
