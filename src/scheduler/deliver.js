import { getDb, getTeamAgentMembers } from '../db/index.js';
import { isMachineOnline, sendToDaemon } from '../daemon/connections.js';
import { formatMessageForDaemon } from '../daemon/index.js';
import { pushToInbox } from './inbox.js';

export async function deliverMessageToAgents(teamId, message) {
  const db = getDb();
  const agents = await getTeamAgentMembers(db, teamId);

  // 解析 mentions：如果消息 @ 了特定 agent，只投递给被 @ 的
  const mentionIds = message.mentions ? JSON.parse(message.mentions) : null;

  for (const agent of agents) {
    if (agent.id === message.sender_id) continue;
    if (!agent.machine_id) continue;
    if (mentionIds && !mentionIds.includes(agent.id)) continue;

    const daemonMsg = {
      type: 'agent:deliver',
      agentId: agent.id,
      teamId: message.team_id,
      seq: message.seq,
      message: await formatMessageForDaemon(message),
    };

    if (isMachineOnline(agent.machine_id)) {
      sendToDaemon(agent.machine_id, daemonMsg);
      console.log(`[Scheduler] Delivered msg seq=${message.seq} to agent ${agent.name}`);
    } else {
      pushToInbox(agent.id, message);
      console.log(`[Scheduler] Machine offline, queued msg seq=${message.seq} for agent ${agent.name}`);
    }
  }
}
