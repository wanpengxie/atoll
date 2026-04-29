import { cpSync, existsSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const rootDir = path.dirname(fileURLToPath(new URL('../package.json', import.meta.url)));
const sourceTemplates = path.join(rootDir, 'src', 'prompt', 'templates');
const distTemplates = path.join(rootDir, 'dist', 'prompt', 'templates');

if (existsSync(sourceTemplates)) {
  mkdirSync(path.dirname(distTemplates), { recursive: true });
  cpSync(sourceTemplates, distTemplates, { recursive: true });
}
