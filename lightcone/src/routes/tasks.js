import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import {
  getDb, getTasksByTeam, insertMessage, getMessageById, updateMessage,
  nextTaskNumber, getTeamById, deleteMessage,
} from '../db/index.js';
import { broadcast } from '../realtime/broadcast.js';
import { formatMessage } from '../internal/index.js';

const router = Router();
const DEFAULT_USER_ID   = process.env.DEFAULT_USER_ID   ?? 'user-001';
const DEFAULT_USER_NAME = process.env.DEFAULT_USER_NAME ?? 'Admin';

router.get('/team/:teamId', async (req, res) => {
  const tasks = await getTasksByTeam(getDb(), req.params.teamId, req.query.status);
  res.json({ tasks: tasks.map(formatMessage) });
});

router.post('/', async (req, res) => {
  const { teamId, content } = req.body;
  if (!teamId || !content) return res.status(400).json({ error: 'teamId and content required' });
  const db = getDb();
  const ch = await getTeamById(db, teamId);
  if (!ch) return res.status(404).json({ error: 'Team not found' });

  const taskNumber = await nextTaskNumber(db, teamId);
  const task = await insertMessage(db, {
    id: uuidv4(), teamId,
    senderType: 'user', senderId: req.user?.id ?? DEFAULT_USER_ID, senderName: req.user?.name ?? DEFAULT_USER_NAME,
    messageType: 'task', content, taskStatus: 'open', taskNumber,
  });
  broadcast.taskCreated(teamId, formatMessage(task));
  res.json(formatMessage(task));
});

router.patch('/:id', async (req, res) => {
  const db = getDb();
  const task = await getMessageById(db, req.params.id);
  if (!task || task.task_status == null) return res.status(404).json({ error: 'Task not found' });
  const { content } = req.body;
  if (!content) return res.status(400).json({ error: 'content required' });
  const updated = await updateMessage(db, req.params.id, { content });
  broadcast.taskUpdated(updated.team_id, formatMessage(updated));
  res.json(formatMessage(updated));
});

router.delete('/:id', async (req, res) => {
  const db = getDb();
  const task = await getMessageById(db, req.params.id);
  if (!task) return res.status(404).json({ error: 'Task not found' });
  await deleteMessage(db, req.params.id);
  broadcast.taskDeleted(task.team_id, req.params.id);
  res.json({ ok: true });
});

router.post('/:id/claim', async (req, res) => {
  const db = getDb();
  const task = await getMessageById(db, req.params.id);
  if (!task || task.task_status == null) return res.status(404).json({ error: 'Task not found' });
  const userName = req.user?.name ?? DEFAULT_USER_NAME;
  await updateMessage(db, task.id, {
    task_assignee_type: 'user', task_assignee_id: req.user?.id ?? DEFAULT_USER_ID,
    task_claimed_at: new Date().toISOString().slice(0,19).replace('T',' '), task_status: 'in_progress',
  });
  const updated = await getMessageById(db, task.id);
  broadcast.taskUpdated(updated.team_id, formatMessage(updated));
  const sysMsg = await insertMessage(db, {
    id: uuidv4(), teamId: updated.team_id,
    senderType: 'user', senderId: 'system', senderName: 'System', messageType: 'system',
    content: `📌 ${userName} 认领了 #${task.task_number} "${task.content.slice(0, 40)}"`,
  });
  broadcast.message(updated.team_id, formatMessage(sysMsg));
  res.json(formatMessage(updated));
});

router.post('/:id/unclaim', async (req, res) => {
  const db = getDb();
  const task = await getMessageById(db, req.params.id);
  if (!task || task.task_status == null) return res.status(404).json({ error: 'Task not found' });
  const updated = await updateMessage(db, task.id, {
    task_assignee_type: null, task_assignee_id: null,
    task_claimed_at: null, task_status: 'open',
  });
  broadcast.taskUpdated(updated.team_id, formatMessage(updated));
  res.json(formatMessage(updated));
});

router.post('/:id/status', async (req, res) => {
  const VALID = ['open', 'todo', 'in_progress', 'in_review', 'done', 'cancelled'];
  const { status } = req.body;
  if (!VALID.includes(status)) return res.status(400).json({ error: `status must be one of: ${VALID.join(', ')}` });
  const db = getDb();
  const task = await getMessageById(db, req.params.id);
  if (!task || task.task_status == null) return res.status(404).json({ error: 'Task not found' });
  const fields = { task_status: status };
  if (status === 'done') fields.task_completed_at = new Date().toISOString().slice(0,19).replace('T',' ');
  const updated = await updateMessage(db, task.id, fields);
  broadcast.taskUpdated(updated.team_id, formatMessage(updated));
  const statusLabels = { open: '待办', todo: '待办', in_progress: '进行中', in_review: '审核中', done: '已完成', cancelled: '已取消' };
  const statusIcons = { done: '✅', in_review: '👀', cancelled: '🚫' };
  const icon = statusIcons[status] || '🔄';
  const userName = req.user?.name ?? DEFAULT_USER_NAME;
  const sysMsg = await insertMessage(db, {
    id: uuidv4(), teamId: updated.team_id,
    senderType: 'user', senderId: 'system', senderName: 'System', messageType: 'system',
    content: `${icon} ${userName} 将 #${task.task_number} "${task.content.slice(0, 40)}" 设为${statusLabels[status]}`,
  });
  broadcast.message(updated.team_id, formatMessage(sysMsg));
  res.json(formatMessage(updated));
});

router.post('/convert-message', async (req, res) => {
  const { messageId } = req.body;
  if (!messageId) return res.status(400).json({ error: 'messageId required' });
  const db = getDb();
  const msg = await getMessageById(db, messageId);
  if (!msg) return res.status(404).json({ error: 'Message not found' });
  if (msg.task_status != null) return res.status(400).json({ error: 'Already a task' });
  const taskNumber = await nextTaskNumber(db, msg.team_id);
  const updated = await updateMessage(db, messageId, { message_type: 'task', task_status: 'open', task_number: taskNumber });
  broadcast.taskCreated(updated.team_id, formatMessage(updated));
  res.json(formatMessage(updated));
});

export default router;
