export interface CapabilitySet {
  cli_binaries: string[];
}

export interface AgentEnv {
  channelId: string;
  channelName: string;
  channelType: string;
  workspaceId: string;
  workdir: string;
  agentName: string;
  sessionId: string;
  sessionIdPath: string;
  daemonSocket: string;
  daemonHttp: string;
  daemonToken: string;
  capabilitySet: CapabilitySet;
}
