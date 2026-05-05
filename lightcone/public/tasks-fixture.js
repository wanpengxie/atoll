(function initTaskFixture(global) {
  const fixtureTasks = [
    {
      task_id: 'task-campaign',
      channel_id: 'channel-demo',
      parent_task_id: null,
      type: 'topic.series',
      title: 'Q2 creator campaign plan',
      initiator_kind: 'agent',
      initiator_id: 'channel-agent',
      status: 'active',
      opened_at: 1778025600000,
      last_event_at: 1778036400000,
      closed_at: null,
      doc_ref: 'notes/tasks/2026-05-06-q2-creator-campaign-plan.md',
      primary_correlation: 'corr-campaign',
      doc: '# Q2 creator campaign plan\n\n## Brief\nCoordinate launch content and publish checkpoints.\n\n## Decisions\n- Use creator proof points as the spine.\n\n## Timeline\n- Campaign task opened.\n- Draft outline approved.\n\n## Status\nStatus: active',
      messages: [
        { id: 'msg-open-campaign', payload_type: 'task.opened', summary: 'Campaign task opened.', ts_received: 1778025600000 },
        { id: 'msg-outline-campaign', payload_type: 'agent.text', summary: 'Draft outline approved.', ts_received: 1778036400000 },
      ],
    },
    {
      task_id: 'task-research',
      channel_id: 'channel-demo',
      parent_task_id: 'task-campaign',
      type: 'research',
      title: 'Collect creator references',
      initiator_kind: 'agent',
      initiator_id: 'strategy-agent',
      status: 'blocked',
      opened_at: 1778027400000,
      last_event_at: 1778031000000,
      closed_at: null,
      doc_ref: 'notes/tasks/2026-05-06-collect-creator-references.md',
      primary_correlation: 'corr-research',
      doc: '# Collect creator references\n\n## Brief\nFind three comparable launch examples.\n\n## Constraints\n- Wait for workspace access.\n\n## Timeline\n- Research started.\n- Blocked on reference folder access.\n\n## Status\nStatus: blocked',
      messages: [
        { id: 'msg-open-research', payload_type: 'task.opened', summary: 'Research started.', ts_received: 1778027400000 },
        { id: 'msg-blocked-research', payload_type: 'self.memo', summary: 'Blocked on reference folder access.', ts_received: 1778031000000 },
      ],
    },
    {
      task_id: 'task-script',
      channel_id: 'channel-demo',
      parent_task_id: 'task-campaign',
      type: 'note.publish',
      title: 'Draft launch note',
      initiator_kind: 'agent',
      initiator_id: 'channel-agent',
      status: 'completed',
      opened_at: 1778029200000,
      last_event_at: 1778038200000,
      closed_at: 1778038200000,
      doc_ref: 'notes/tasks/2026-05-06-draft-launch-note.md',
      primary_correlation: 'corr-script',
      doc: '# Draft launch note\n\n## Brief\nPrepare the first launch post.\n\n## Decisions\n- Keep the first version short.\n\n## Timeline\n- Draft task opened.\n- Dispatch completed with preview URL.\n\n## Status\nStatus: completed',
      messages: [
        { id: 'msg-open-script', payload_type: 'task.opened', summary: 'Draft task opened.', ts_received: 1778029200000 },
        { id: 'msg-dispatch-script', payload_type: 'dispatch.completed', summary: 'Dispatch completed with preview URL.', ts_received: 1778038200000 },
      ],
    },
    {
      task_id: 'task-publish',
      channel_id: 'channel-demo',
      parent_task_id: 'task-campaign',
      type: 'dispatch.review',
      title: 'Review publish checklist',
      initiator_kind: 'agent',
      initiator_id: 'channel-agent',
      status: 'opened',
      opened_at: 1778032800000,
      last_event_at: 1778032800000,
      closed_at: null,
      doc_ref: 'notes/tasks/2026-05-06-review-publish-checklist.md',
      primary_correlation: 'corr-publish',
      doc: '# Review publish checklist\n\n## Brief\nConfirm images, copy, and timing before publishing.\n\n## Timeline\n- Review task opened.\n\n## Status\nStatus: opened',
      messages: [
        { id: 'msg-open-publish', payload_type: 'task.opened', summary: 'Review task opened.', ts_received: 1778032800000 },
      ],
    },
  ];

  const statusLabels = {
    all: 'All',
    active: 'Active',
    opened: 'Opened',
    blocked: 'Blocked',
    completed: 'Completed',
    failed: 'Failed',
    abandoned: 'Abandoned',
    archived: 'Archived',
  };

  function esc(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function isActiveTask(task) {
    return ['opened', 'active', 'blocked'].includes(String(task.status));
  }

  function taskMatches(task, state) {
    const status = state.status || 'all';
    const statusMatch = status === 'all'
      || (status === 'active' ? isActiveTask(task) : task.status === status);
    const mineMatch = !state.mineOnly || task.initiator_id === 'channel-agent';
    return statusMatch && mineMatch;
  }

  function buildTree(tasks) {
    const byParent = new Map();
    tasks.forEach((task) => {
      const parent = task.parent_task_id || null;
      const items = byParent.get(parent) || [];
      items.push(task);
      byParent.set(parent, items);
    });
    const nest = (task) => ({ ...task, children: (byParent.get(task.task_id) || []).map(nest) });
    return (byParent.get(null) || []).map(nest);
  }

  function flattenTree(nodes, depth = 0, rows = []) {
    nodes.forEach((node) => {
      rows.push({ task: node, depth });
      flattenTree(node.children || [], depth + 1, rows);
    });
    return rows;
  }

  function visibleRows(tasks, state) {
    return flattenTree(buildTree(tasks)).filter(({ task }) => taskMatches(task, state));
  }

  function statusCounts(tasks) {
    const counts = { all: tasks.length, active: 0, opened: 0, blocked: 0, completed: 0, failed: 0, abandoned: 0, archived: 0 };
    tasks.forEach((task) => {
      if (isActiveTask(task)) counts.active += 1;
      if (task.status !== 'active' && counts[task.status] != null) counts[task.status] += 1;
    });
    return counts;
  }

  function markdownToHtml(markdown) {
    const lines = String(markdown || '').split('\n');
    const html = [];
    let inList = false;
    const closeList = () => {
      if (inList) {
        html.push('</ul>');
        inList = false;
      }
    };
    lines.forEach((line) => {
      const text = line.trim();
      if (!text) {
        closeList();
        return;
      }
      if (text.startsWith('# ')) {
        closeList();
        html.push(`<h3>${esc(text.slice(2))}</h3>`);
      } else if (text.startsWith('## ')) {
        closeList();
        html.push(`<h4>${esc(text.slice(3))}</h4>`);
      } else if (text.startsWith('- ')) {
        if (!inList) {
          html.push('<ul>');
          inList = true;
        }
        html.push(`<li>${esc(text.slice(2))}</li>`);
      } else {
        closeList();
        html.push(`<p>${esc(text)}</p>`);
      }
    });
    closeList();
    return html.join('');
  }

  function renderFilters(tasks, state) {
    const counts = statusCounts(tasks);
    const filters = ['all', 'active', 'opened', 'blocked', 'completed', 'failed', 'abandoned'];
    return `<div class="task-entity-toolbar">
      <div class="task-filter-bar">${filters.map((key) =>
        `<button class="task-filter-pill${(state.status || 'all') === key ? ' active' : ''}" onclick="setTaskFilter('${key}')">${statusLabels[key]}<span class="pill-count">${counts[key] || 0}</span></button>`
      ).join('')}</div>
      <button class="task-filter-pill task-mine-toggle${state.mineOnly ? ' active' : ''}" onclick="toggleTaskMineFilter()">Mine</button>
    </div>`;
  }

  function renderTaskRows(rows, selectedTaskId) {
    if (!rows.length) {
      return '<div class="task-empty">No tasks</div>';
    }
    return rows.map(({ task, depth }) => `<button class="task-tree-row${task.task_id === selectedTaskId ? ' selected' : ''}" data-task-id="${esc(task.task_id)}" onclick="selectMockTask('${esc(task.task_id)}')" style="--task-depth:${depth}">
      <span class="task-status status-${esc(task.status)}">${esc(statusLabels[task.status] || task.status)}</span>
      <span class="task-row-main">
        <span class="task-title">${esc(task.title)}</span>
        <span class="task-meta">${esc(task.type)} &middot; ${esc(task.initiator_id)} &middot; ${new Date(task.last_event_at).toLocaleDateString()}</span>
      </span>
    </button>`).join('');
  }

  function renderDetail(task, tasks, state) {
    if (!task) {
      return '<section class="task-detail"><div class="task-empty">No task selected</div></section>';
    }
    const children = tasks.filter((item) => item.parent_task_id === task.task_id && taskMatches(item, state));
    const timeline = (task.messages || []).map((message) => `<div class="task-timeline-item">
      <span class="task-timeline-type">${esc(message.payload_type)}</span>
      <span class="task-timeline-summary">${esc(message.summary)}</span>
    </div>`).join('');
    return `<section class="task-detail">
      <div class="task-detail-header">
        <div>
          <div class="task-detail-title">${esc(task.title)}</div>
          <div class="task-detail-meta">${esc(task.task_id)} &middot; ${esc(task.type)} &middot; ${esc(task.doc_ref)}</div>
        </div>
        <span class="task-status status-${esc(task.status)}">${esc(statusLabels[task.status] || task.status)}</span>
      </div>
      <div class="task-detail-grid">
        <div><span>Opened</span><b>${new Date(task.opened_at).toLocaleString()}</b></div>
        <div><span>Last event</span><b>${new Date(task.last_event_at).toLocaleString()}</b></div>
        <div><span>Initiator</span><b>${esc(task.initiator_kind)}:${esc(task.initiator_id)}</b></div>
      </div>
      <div class="task-doc">${markdownToHtml(task.doc)}</div>
      <div class="task-section-title">Timeline</div>
      <div class="task-timeline">${timeline || '<div class="task-empty">No messages</div>'}</div>
      <div class="task-section-title">Child Tasks</div>
      <div class="task-child-list">${children.length ? children.map((child) =>
        `<button class="task-child-link" onclick="selectMockTask('${esc(child.task_id)}')">${esc(child.title)}<span>${esc(statusLabels[child.status] || child.status)}</span></button>`
      ).join('') : '<div class="task-empty">No child tasks</div>'}</div>
    </section>`;
  }

  function renderTasksPanelHtml({ tasks = fixtureTasks, state = {} } = {}) {
    const normalizedState = { status: 'all', mineOnly: false, selectedTaskId: null, ...state };
    const rows = visibleRows(tasks, normalizedState);
    const selectedTaskId = normalizedState.selectedTaskId && rows.some(({ task }) => task.task_id === normalizedState.selectedTaskId)
      ? normalizedState.selectedTaskId
      : rows[0]?.task.task_id;
    const selectedTask = tasks.find((task) => task.task_id === selectedTaskId) || null;
    return `${renderFilters(tasks, normalizedState)}
      <div class="task-entity-layout">
        <div class="task-tree" role="tree">${renderTaskRows(rows, selectedTaskId)}</div>
        ${renderDetail(selectedTask, tasks, normalizedState)}
      </div>`;
  }

  function activeCount(tasks = fixtureTasks) {
    return tasks.filter(isActiveTask).length;
  }

  global.LIGHTCONE_TASK_FIXTURES = fixtureTasks;
  global.LightconeTaskPanel = {
    fixtureTasks,
    renderTasksPanelHtml,
    activeCount,
    statusCounts,
  };
})(typeof window !== 'undefined' ? window : globalThis);
