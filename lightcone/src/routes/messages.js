import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import {
  getDb, getMessages, insertMessage, getMessageById, updateMessage,
  getMessagesSince, maxSeq, getTeamById, deleteMessage,
} from '../db/index.js';
import { broadcast } from '../realtime/broadcast.js';
import { deliverMessageToAgents } from '../scheduler/deliver.js';
import { formatMessage, parseMentions } from '../internal/index.js';

const router = Router();
const DEFAULT_USER_ID   = process.env.DEFAULT_USER_ID   ?? 'user-001';
const DEFAULT_USER_NAME = process.env.DEFAULT_USER_NAME ?? 'Admin';

router.get('/team/:teamId', async (req, res) => {
  const { limit = 50, before, after } = req.query;
  const msgs = await getMessages(getDb(), req.params.teamId, {
    limit: Number(limit),
    before: before != null ? Number(before) : undefined,
    after:  after  != null ? Number(after)  : undefined,
  });
  res.json({ messages: msgs.map(formatMessage), hasMore: msgs.length === Number(limit) });
});

const GUEST_MESSAGE_LIMIT = 20;

router.post('/', async (req, res) => {
  const { teamId, content, threadId } = req.body;
  if (!teamId || !content) return res.status(400).json({ error: 'teamId and content required' });
  const db = getDb();
  const ch = await getTeamById(db, teamId);
  if (!ch) return res.status(404).json({ error: 'Team not found' });

  // Guest message limit check
  if (req.user?.is_guest) {
    const [[{ cnt }]] = await db.execute(
      `SELECT COUNT(*) AS cnt FROM messages WHERE sender_id = ? AND sender_type = 'user'`,
      [req.user.id]
    );
    if (cnt >= GUEST_MESSAGE_LIMIT) {
      return res.status(403).json({
        error: 'trial_limit',
        message: '试用消息已用完，注册解锁更多功能',
        current: cnt, limit: GUEST_MESSAGE_LIMIT,
      });
    }
  }

  // Service callers (feishu-bridge etc.) pass sender identity in body
  const senderId   = (req.isService && req.body.senderId)   ? req.body.senderId   : (req.user?.id   ?? DEFAULT_USER_ID);
  const senderName = (req.isService && req.body.senderName) ? req.body.senderName : (req.user?.name ?? DEFAULT_USER_NAME);
  const mentions = await parseMentions(db, content, teamId);
  const msg = await insertMessage(db, {
    id: uuidv4(), teamId,
    senderType: 'user', senderId, senderName,
    messageType: 'chat', content, threadId: threadId ?? null,
    mentions: mentions !== null ? JSON.stringify(mentions) : null,
  });

  broadcast.message(teamId, formatMessage(msg));
  await deliverMessageToAgents(teamId, msg);
  res.json(formatMessage(msg));
});

router.patch('/:id', async (req, res) => {
  const { content } = req.body;
  if (!content) return res.status(400).json({ error: 'content required' });
  const msg = await updateMessage(getDb(), req.params.id, { content });
  if (!msg) return res.status(404).json({ error: 'Message not found' });
  broadcast.messageUpdated(msg.team_id, formatMessage(msg));
  res.json(formatMessage(msg));
});

router.delete('/:id', async (req, res) => {
  const db = getDb();
  const msg = await getMessageById(db, req.params.id);
  if (!msg) return res.status(404).json({ error: 'Message not found' });
  await deleteMessage(db, req.params.id);
  res.json({ ok: true });
});

router.get('/sync', async (req, res) => {
  const db = getDb();
  const { since, teamId } = req.query;
  const msgs = await getMessagesSince(db, since != null ? Number(since) : 0, teamId);
  res.json({ messages: msgs.map(formatMessage), currentSeq: await maxSeq(db) });
});

export default router;
