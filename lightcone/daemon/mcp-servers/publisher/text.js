function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export function normalizeTags(tags = []) {
  const seen = new Set();
  const normalized = [];
  for (const raw of tags) {
    const tag = String(raw ?? '').trim().replace(/^[#＃]+/, '').trim();
    if (!tag || seen.has(tag)) continue;
    seen.add(tag);
    normalized.push(tag);
  }
  return normalized;
}

export function formatTextWithTags(text, tags = []) {
  const base = String(text ?? '').trimEnd();
  const missingTags = normalizeTags(tags).filter(tag => {
    const pattern = new RegExp(`(^|[\\s,，。！？!?:：;；、])(?:#|＃)${escapeRegExp(tag)}(?=$|[\\s,，。！？!?:：;；、])`);
    return !pattern.test(base);
  });

  if (missingTags.length === 0) return base;
  const suffix = missingTags.map(tag => `#${tag}`).join(' ');
  return base ? `${base}\n${suffix}` : suffix;
}
