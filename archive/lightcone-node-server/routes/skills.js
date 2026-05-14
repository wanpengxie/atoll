import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import {
  getDb, getSkillById, getSkillsByOwner, insertSkill, updateSkill,
  insertSkillBinding, deleteSkillBinding, getSkillBindings,
} from '../db/index.js';
import { requireAuth } from '../middleware/auth.js';
import { nowMysqlDatetime } from '../time.js';

const router = Router();

// All routes require auth
router.use(requireAuth);

// ── List skills (user's + platform) ─────────────────────────────────────────

router.get('/', async (req, res) => {
  const { type, search } = req.query;
  const skills = await getSkillsByOwner(getDb(), req.user.id, { type, search });
  res.json(skills.map(fmtSkill));
});

// ── Create user skill ───────────────────────────────────────────────────────

router.post('/', async (req, res) => {
  const { name, description, content, tags } = req.body;
  if (!name) return res.status(400).json({ error: 'name is required' });
  const skill = await insertSkill(getDb(), {
    id: uuidv4(),
    ownerId: req.user.id,
    type: 'user',
    name,
    description: description ?? '',
    content: content ?? '',
    tags: tags ?? [],
  });
  res.status(201).json(fmtSkill(skill));
});

// ── Get skill detail ────────────────────────────────────────────────────────

router.get('/:id', async (req, res) => {
  const skill = await getSkillById(getDb(), req.params.id);
  if (!skill) return res.status(404).json({ error: 'Skill not found' });
  if (skill.type === 'user' && skill.owner_id !== req.user.id)
    return res.status(403).json({ error: 'Not your skill' });
  res.json(fmtSkill(skill));
});

// ── Update skill ────────────────────────────────────────────────────────────

router.patch('/:id', async (req, res) => {
  const skill = await getSkillById(getDb(), req.params.id);
  if (!skill) return res.status(404).json({ error: 'Skill not found' });
  if (skill.type === 'platform') return res.status(403).json({ error: 'Cannot modify platform skills' });
  if (skill.owner_id !== req.user.id) return res.status(403).json({ error: 'Not your skill' });

  const { name, description, content, tags } = req.body;
  const fields = {};
  if (name != null) fields.name = name;
  if (description != null) fields.description = description;
  if (content != null) fields.content = content;
  if (tags != null) fields.tags = tags;

  const updated = await updateSkill(getDb(), req.params.id, fields);
  res.json(fmtSkill(updated));
});

// ── Delete skill (soft) ─────────────────────────────────────────────────────

router.delete('/:id', async (req, res) => {
  const skill = await getSkillById(getDb(), req.params.id);
  if (!skill) return res.status(404).json({ error: 'Skill not found' });
  if (skill.type === 'platform') return res.status(403).json({ error: 'Cannot delete platform skills' });
  if (skill.owner_id !== req.user.id) return res.status(403).json({ error: 'Not your skill' });

  await updateSkill(getDb(), req.params.id, { is_del: 1, deleted_at: nowMysqlDatetime() });
  res.json({ ok: true });
});

// ── Bindings ────────────────────────────────────────────────────────────────

router.get('/:id/bindings', async (req, res) => {
  const skill = await getSkillById(getDb(), req.params.id);
  if (!skill) return res.status(404).json({ error: 'Skill not found' });
  if (skill.type === 'user' && skill.owner_id !== req.user.id)
    return res.status(403).json({ error: 'Not your skill' });
  const bindings = await getSkillBindings(getDb(), req.params.id);
  res.json(bindings);
});

router.post('/:id/bindings', async (req, res) => {
  const { targetType, targetId } = req.body;
  if (!targetType || !targetId) return res.status(400).json({ error: 'targetType and targetId required' });
  if (!['team', 'agent'].includes(targetType)) return res.status(400).json({ error: 'targetType must be team or agent' });

  const skill = await getSkillById(getDb(), req.params.id);
  if (!skill) return res.status(404).json({ error: 'Skill not found' });
  if (skill.type === 'user' && skill.owner_id !== req.user.id)
    return res.status(403).json({ error: 'Not your skill' });

  await insertSkillBinding(getDb(), { id: uuidv4(), skillId: req.params.id, targetType, targetId });
  res.json({ ok: true });
});

router.delete('/:id/bindings', async (req, res) => {
  const { targetType, targetId } = req.body;
  if (!targetType || !targetId) return res.status(400).json({ error: 'targetType and targetId required' });

  const skill = await getSkillById(getDb(), req.params.id);
  if (!skill) return res.status(404).json({ error: 'Skill not found' });
  if (skill.type === 'user' && skill.owner_id !== req.user.id)
    return res.status(403).json({ error: 'Not your skill' });

  await deleteSkillBinding(getDb(), req.params.id, targetType, targetId);
  res.json({ ok: true });
});

// ── helpers ─────────────────────────────────────────────────────────────────

function fmtSkill(s) {
  return {
    id: s.id,
    ownerId: s.owner_id,
    type: s.type,
    name: s.name,
    description: s.description,
    content: s.content,
    tags: typeof s.tags === 'string' ? JSON.parse(s.tags) : (s.tags ?? []),
    mcpConfig: s.mcp_config ? JSON.parse(s.mcp_config) : null,
    createdByAgentId: s.created_by_agent_id,
    createdAt: s.created_at,
    updatedAt: s.updated_at,
  };
}

export default router;
