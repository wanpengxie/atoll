// media.js — inline rendering of envelope doc_refs.
//
// Authoritative spec: .dalek/pm/v4-layer3-spec.md §4 (Inline 媒体渲染).
//
// Triggered when a message envelope carries doc_refs whose path extensions
// match the §4.1 inline-media table. Renderer hands us:
//   - one container element (the bubble) where attachments should be
//     appended below the text body
//   - the envelope's doc_refs array (already filtered to non-empty strings)
//   - channelID — needed to build the GET /api/channels/<id>/files/<path>
//     proxy URL per spec §4.2
//
// The proxy endpoint is the server's responsibility (M1.6 demo server
// does NOT yet expose it — see ui/README §Manual e2e for the known gap).
// When the URL 404s the <img> / <video> falls back to the file card so
// the UI degrades gracefully without breaking the rest of the bubble.

import { INLINE_MEDIA_KIND, classifyDocRef } from './protocol.js';

/**
 * Build the GET URL for a workdir-relative path.
 * Exported so tests / debug consoles can build the same URL.
 */
export function fileURL(channelID, path) {
  // Encode each path segment individually so "/" stays as separator but
  // Chinese filenames + spaces survive.
  const safe = path
    .split('/')
    .map((seg) => encodeURIComponent(seg))
    .join('/');
  return `/api/channels/${encodeURIComponent(channelID)}/files/${safe}`;
}

/**
 * Render every doc_refs path of an envelope as inline DOM nodes appended
 * to `container`. Skips empty / non-string entries silently.
 *
 * @param {HTMLElement} container — the message bubble DOM node
 * @param {string} channelID
 * @param {string[]} docRefs — envelope.doc_refs (already non-nil)
 */
export function appendInlineMedia(container, channelID, docRefs) {
  if (!container || !Array.isArray(docRefs)) return;
  for (const path of docRefs) {
    if (typeof path !== 'string' || path.length === 0) continue;
    const kind = classifyDocRef(path);
    const url = fileURL(channelID, path);
    const node = buildMediaNode(kind, path, url);
    if (node) container.appendChild(node);
  }
}

function buildMediaNode(kind, path, url) {
  const wrap = document.createElement('div');
  wrap.className = `media media-${kind}`;
  switch (kind) {
    case INLINE_MEDIA_KIND.IMAGE: {
      const img = document.createElement('img');
      img.src = url;
      img.alt = path;
      img.loading = 'lazy';
      img.addEventListener('click', () => window.open(url, '_blank'));
      img.addEventListener('error', () => {
        wrap.replaceChildren(buildFileCard(path, url));
      });
      wrap.appendChild(img);
      return wrap;
    }
    case INLINE_MEDIA_KIND.VIDEO: {
      const vid = document.createElement('video');
      vid.src = url;
      vid.controls = true;
      vid.preload = 'metadata';
      vid.addEventListener('error', () => {
        wrap.replaceChildren(buildFileCard(path, url));
      });
      wrap.appendChild(vid);
      return wrap;
    }
    case INLINE_MEDIA_KIND.MARKDOWN: {
      // Folded markdown — clicking the summary fetches the file and dumps
      // it as preformatted text. We deliberately don't pull a heavy
      // markdown parser into the demo console; cmd/server can render later.
      const details = document.createElement('details');
      details.className = 'media-md';
      const summary = document.createElement('summary');
      summary.textContent = `📄 ${path}`;
      details.appendChild(summary);
      const pre = document.createElement('pre');
      pre.className = 'media-md-body';
      pre.textContent = '…';
      details.appendChild(pre);
      details.addEventListener('toggle', async () => {
        if (!details.open || pre.dataset.loaded === '1') return;
        try {
          const res = await fetch(url, { credentials: 'include' });
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          pre.textContent = await res.text();
          pre.dataset.loaded = '1';
        } catch (err) {
          pre.textContent = `(load failed: ${err.message})`;
        }
      });
      wrap.appendChild(details);
      return wrap;
    }
    case INLINE_MEDIA_KIND.PDF:
    case INLINE_MEDIA_KIND.FILE:
    default:
      wrap.appendChild(buildFileCard(path, url));
      return wrap;
  }
}

function buildFileCard(path, url) {
  const card = document.createElement('a');
  card.className = 'media-file-card';
  card.href = url;
  card.target = '_blank';
  card.rel = 'noopener';
  card.textContent = `📎 ${displayName(path)}`;
  return card;
}

function displayName(path) {
  const slash = path.lastIndexOf('/');
  return slash < 0 ? path : path.slice(slash + 1);
}
