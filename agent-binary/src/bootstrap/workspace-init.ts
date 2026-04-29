import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import type { AgentEnv } from '../types/env.js';
import { CliError } from '../util/simple-error.js';

const REQUIRED_DIRS = ['messages', 'artifacts', 'schedules'];

export function initializeWorkspace(env: AgentEnv): void {
  const cwd = path.resolve(process.cwd());
  if (cwd !== env.workdir) {
    throw new CliError('invalid_workdir', `Expected cwd ${env.workdir} but process started in ${cwd}`);
  }

  for (const dirName of REQUIRED_DIRS) {
    mkdirSync(path.join(env.workdir, dirName), { recursive: true });
  }

  const agentRoot = path.join(env.workdir, 'agents', env.agentName);
  mkdirSync(path.join(agentRoot, 'trace'), { recursive: true });
  mkdirSync(path.join(agentRoot, 'working-state'), { recursive: true });

  const workingStatePath = path.join(agentRoot, 'working-state', 'current.md');
  if (!existsSync(workingStatePath)) {
    writeFileSync(workingStatePath, '# Current State\n', 'utf8');
  }
}
