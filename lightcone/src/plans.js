// ─── Plan configuration & quota enforcement ─────────────────────────────────

export const PLAN_CONFIG = {
  free: {
    displayName: 'Hobby',
    maxMachines: 2,
    maxAgents: 5,
    maxTeams: 5,
    maxHostedAgents: 1,
    messageHistoryDays: 30,
    price: 0,
  },
  pro: {
    displayName: 'Team',
    maxMachines: 8,
    maxAgents: 40,
    maxTeams: 20,
    maxHostedAgents: 5,
    messageHistoryDays: -1,
    price: 2000,
  },
  team: {
    displayName: 'Business',
    maxMachines: 40,
    maxAgents: 200,
    maxTeams: -1,
    maxHostedAgents: 20,
    messageHistoryDays: -1,
    price: 20000,
  },
};

export function getPlanLimits(planName) {
  return PLAN_CONFIG[planName] ?? PLAN_CONFIG.free;
}

/**
 * Check whether a resource can be created under the server's plan limits.
 * @param {object} db - MySQL pool
 * @param {string} serverId
 * @param {'agents'|'machines'|'teams'|'hostedAgents'} resource
 * @param {string} [ownerId] - when provided, agents/hostedAgents are counted per-user
 * @returns {{ allowed: boolean, current: number, limit: number }}
 */
export async function checkQuota(db, serverId, resource, ownerId) {
  const [serverRows] = await db.execute(`SELECT plan FROM servers WHERE id = ?`, [serverId]);
  const planName = serverRows[0]?.plan ?? 'free';
  const limits = getPlanLimits(planName);

  let current = 0;
  let limit = 0;

  switch (resource) {
    case 'agents': {
      const [rows] = ownerId
        ? await db.execute(
            `SELECT COUNT(*) AS cnt FROM agents WHERE server_id = ? AND owner_id = ? AND is_del = 0 AND deleted_at IS NULL`,
            [serverId, ownerId]
          )
        : await db.execute(
            `SELECT COUNT(*) AS cnt FROM agents WHERE server_id = ? AND is_del = 0 AND deleted_at IS NULL`,
            [serverId]
          );
      current = rows[0].cnt;
      limit = limits.maxAgents;
      break;
    }
    case 'machines': {
      const [rows] = await db.execute(
        `SELECT COUNT(*) AS cnt FROM machines WHERE server_id = ? AND is_platform = 0 AND is_del = 0`,
        [serverId]
      );
      current = rows[0].cnt;
      limit = limits.maxMachines;
      break;
    }
    case 'teams': {
      const [rows] = ownerId
        ? await db.execute(
            `SELECT COUNT(*) AS cnt FROM teams WHERE server_id = ? AND owner_id = ? AND type = 'team' AND is_del = 0 AND deleted_at IS NULL`,
            [serverId, ownerId]
          )
        : await db.execute(
            `SELECT COUNT(*) AS cnt FROM teams WHERE server_id = ? AND type = 'team' AND is_del = 0 AND deleted_at IS NULL`,
            [serverId]
          );
      current = rows[0].cnt;
      limit = limits.maxTeams;
      break;
    }
    case 'hostedAgents': {
      const [rows] = ownerId
        ? await db.execute(
            `SELECT COUNT(*) AS cnt FROM agents WHERE server_id = ? AND owner_id = ? AND hosted = 1 AND is_del = 0 AND deleted_at IS NULL`,
            [serverId, ownerId]
          )
        : await db.execute(
            `SELECT COUNT(*) AS cnt FROM agents WHERE server_id = ? AND hosted = 1 AND is_del = 0 AND deleted_at IS NULL`,
            [serverId]
          );
      current = rows[0].cnt;
      limit = limits.maxHostedAgents;
      break;
    }
  }

  // -1 means unlimited
  const allowed = limit === -1 || current < limit;
  return { allowed, current, limit };
}

/**
 * Full usage summary for a server (used by the plan API endpoint).
 */
export async function getUsageSummary(db, serverId) {
  const [serverRows] = await db.execute(`SELECT plan FROM servers WHERE id = ?`, [serverId]);
  const planName = serverRows[0]?.plan ?? 'free';
  const limits = getPlanLimits(planName);

  const [[agentRow], [machineRow], [teamRow], [hostedRow]] = await Promise.all([
    db.execute(`SELECT COUNT(*) AS cnt FROM agents WHERE server_id = ? AND is_del = 0 AND deleted_at IS NULL`, [serverId]),
    db.execute(`SELECT COUNT(*) AS cnt FROM machines WHERE server_id = ? AND is_platform = 0 AND is_del = 0`, [serverId]),
    db.execute(`SELECT COUNT(*) AS cnt FROM teams WHERE server_id = ? AND type = 'team' AND is_del = 0 AND deleted_at IS NULL`, [serverId]),
    db.execute(`SELECT COUNT(*) AS cnt FROM agents WHERE server_id = ? AND hosted = 1 AND is_del = 0 AND deleted_at IS NULL`, [serverId]),
  ]);

  return {
    plan: planName,
    displayName: limits.displayName,
    limits: {
      maxMachines: limits.maxMachines,
      maxAgents: limits.maxAgents,
      maxTeams: limits.maxTeams,
      maxHostedAgents: limits.maxHostedAgents,
      messageHistoryDays: limits.messageHistoryDays,
    },
    usage: {
      machines: machineRow[0].cnt,
      agents: agentRow[0].cnt,
      teams: teamRow[0].cnt,
      hostedAgents: hostedRow[0].cnt,
    },
  };
}
