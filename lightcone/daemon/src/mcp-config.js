import { profileDir } from './browser-login.js';

function resolveSkillArg(arg, config) {
  if (arg === '{mysql_mcp_path}')
    return new URL('../../mcp-servers/mysql/index.js', import.meta.url).pathname;
  if (arg === '{workspace_migrate_mcp_path}')
    return new URL('../../mcp-servers/workspace-migrate/index.js', import.meta.url).pathname;
  if (arg === '{platform_mcp_path}')
    return new URL('../../mcp-servers/platform/index.js', import.meta.url).pathname;
  if (arg === '{publisher_mcp_path}')
    return new URL('../mcp-servers/publisher/index.js', import.meta.url).pathname;
  if (arg === '{xhs_profile_dir}')
    return profileDir('xhs', config.userId ?? 'default');
  if (arg === '{douyin_profile_dir}')
    return profileDir('douyin', config.userId ?? 'default');
  if (arg === '{kuaishou_profile_dir}')
    return profileDir('kuaishou', config.userId ?? 'default');
  return arg;
}

function baseEnvForServer(serverKey, { serverUrl, authToken, agentId, teamId, workspaceDir }) {
  if (serverKey === 'workspace-migrate') {
    return { SERVER_URL: serverUrl, MACHINE_API_KEY: authToken, AGENT_ID: agentId };
  }
  if (serverKey === 'publisher' || serverKey === 'platform') {
    return {
      SERVER_URL: serverUrl,
      MACHINE_API_KEY: authToken,
      AGENT_ID: agentId,
      TEAM_ID: teamId ?? '',
      WORKSPACE_DIR: workspaceDir,
    };
  }
  return {};
}

export function buildSkillMcpServers({
  skills,
  credentialGrants,
  config,
  agentId,
  teamId,
  workspaceDir,
  serverUrl,
  authToken,
}) {
  const mcpServers = {};
  const agentEnv = config.envVars ?? {};

  for (const skill of (skills ?? [])) {
    if (!skill.mcpConfig) continue;
    const mc = skill.mcpConfig;
    if (mcpServers[mc.server]) continue;

    const resolvedArgs = (mc.args ?? []).map(arg => resolveSkillArg(arg, config));
    const resolvedEnv = {};
    for (const envKey of (mc.env ?? [])) {
      resolvedEnv[envKey] = agentEnv[envKey] ?? process.env[envKey] ?? '';
    }

    mcpServers[mc.server] = {
      command: mc.command,
      args: resolvedArgs,
      env: {
        ...resolvedEnv,
        ...baseEnvForServer(mc.server, { serverUrl, authToken, agentId, teamId, workspaceDir }),
      },
    };
  }

  for (const skill of (skills ?? [])) {
    if (!skill.mcpConfig?.platform) continue;
    const grant = (credentialGrants ?? []).find(g => g.platform === skill.mcpConfig.platform);
    if (!grant) continue;
    const serverKey = skill.mcpConfig.server;
    if (!mcpServers[serverKey]) continue;
    mcpServers[serverKey].env = { ...(mcpServers[serverKey].env ?? {}), ...(grant.envVars ?? {}) };
  }

  return mcpServers;
}
