import { cpSync, existsSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const rootDir = path.dirname(fileURLToPath(new URL('../package.json', import.meta.url)));
const assetDirs = ['templates', 'channel-types'];

for (const assetDir of assetDirs) {
  const source = path.join(rootDir, 'src', 'prompt', assetDir);
  const destination = path.join(rootDir, 'dist', 'prompt', assetDir);
  if (existsSync(source)) {
    mkdirSync(path.dirname(destination), { recursive: true });
    cpSync(source, destination, { recursive: true });
  }
}
