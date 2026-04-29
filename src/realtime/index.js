import { Server as SocketIOServer } from 'socket.io';
import { setIo } from './broadcast.js';
import { getDb, getMessagesSince, maxSeq } from '../db/index.js';
import { formatMessage } from '../internal/index.js';

export function setupSocketIO(httpServer) {
  const io = new SocketIOServer(httpServer, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    transports: ['websocket', 'polling'],
  });

  setIo(io);

  io.on('connection', (socket) => {
    const { serverId } = socket.handshake.auth ?? {};
    console.log(`[SocketIO] Client connected: ${socket.id}, server=${serverId}`);
    if (serverId) socket.join(`server:${serverId}`);

    socket.on('team:join',  (teamId) => { socket.join(`team:${teamId}`); });
    socket.on('team:leave', (teamId) => { socket.leave(`team:${teamId}`); });

    socket.on('sync:resume', async ({ since, teamId } = {}) => {
      const db = getDb();
      const msgs = await getMessagesSince(db, since ?? 0, teamId);
      socket.emit('sync:messages', {
        messages: msgs.map(formatMessage),
        currentSeq: await maxSeq(db),
      });
    });

    socket.on('disconnect', () => {
      console.log(`[SocketIO] Client disconnected: ${socket.id}`);
    });
    socket.on('ping', () => socket.emit('pong'));
  });

  setInterval(() => { io.emit('ping'); }, 25_000);
  console.log('[SocketIO] Ready');
  return io;
}
