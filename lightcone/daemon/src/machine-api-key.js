import { readMachineKeyFile } from './paths.js';

export function resolveMachineApiKey({
  cliApiKey = '',
  env = process.env,
  projectKey,
  readMachineKeyFileImpl = readMachineKeyFile,
} = {}) {
  if (cliApiKey) return { value: cliApiKey, source: 'cli' };
  if (env.MACHINE_API_KEY) return { value: env.MACHINE_API_KEY, source: 'env' };

  const fileKey = readMachineKeyFileImpl(projectKey);
  if (fileKey) return { value: fileKey, source: 'machine.key' };

  return { value: '', source: 'missing' };
}

export function missingMachineApiKeyErrorLines(machineKeyPath) {
  return [
    'Error: API key is required.',
    `Checked CLI --api-key, MACHINE_API_KEY, and ${machineKeyPath}.`,
    'Run "make register" to create the machine key file.',
  ];
}
