// wxt-helpers.ts — pure functions consumed by wxt.config.ts.
//
// Pulled out of wxt.config.ts so vitest can test them in isolation
// without booting the wxt config loader. wxt.config.ts is the only
// production consumer; everything here is side-effect-free except for
// readFileSync (gated by existsSync first).

import { readFileSync, existsSync } from 'fs';

export const DEFAULT_EXTERNAL_ORIGINS = ['http://localhost:*/*', 'http://127.0.0.1:*/*'];

export interface ExternalOriginEnv {
  COAGENT_WEB_ORIGINS?: string;
  COAGENT_WEB_DOMAIN?: string;
}

/**
 * resolveExternallyConnectableMatches — compute the manifest
 * externally_connectable.matches list from build-time env.
 *
 * Precedence:
 *   1. COAGENT_WEB_ORIGINS (full comma-separated chrome match patterns)
 *      — wins outright when set.
 *   2. COAGENT_WEB_DOMAIN — single canonical https host expanded to
 *      `https://${domain}/*`, merged with the dev defaults so smoke
 *      tests against localhost still work against the prod artefact.
 *   3. Neither set — dev defaults (localhost + 127.0.0.1).
 */
export function resolveExternallyConnectableMatches(env: ExternalOriginEnv): string[] {
  const rawOrigins = (env.COAGENT_WEB_ORIGINS ?? '').trim();
  if (rawOrigins) {
    const patterns = rawOrigins
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
    if (patterns.length > 0) return patterns;
  }

  const domain = (env.COAGENT_WEB_DOMAIN ?? '').trim();
  if (domain) {
    return [...DEFAULT_EXTERNAL_ORIGINS, `https://${domain}/*`];
  }

  return [...DEFAULT_EXTERNAL_ORIGINS];
}

export interface ExtensionKeyEnv {
  COAGENT_EXTENSION_KEY?: string;
  COAGENT_EXTENSION_KEY_FILE?: string;
}

/**
 * resolveManifestKey — return the base64 RSA public key for the
 * manifest `key` field, or undefined when no env knob is set.
 *
 * Precedence:
 *   1. COAGENT_EXTENSION_KEY env var (inline base64) — CI / headless.
 *   2. COAGENT_EXTENSION_KEY_FILE env var (path to file containing the
 *      base64 string) — local dev where committing the key bytes
 *      verbatim feels intrusive.
 *   3. Neither — return undefined (manifest ships without `key`; Chrome
 *      generates a random ID per load, legacy behaviour).
 */
export function resolveManifestKey(env: ExtensionKeyEnv): string | undefined {
  const inline = (env.COAGENT_EXTENSION_KEY ?? '').trim();
  if (inline) return inline;

  const keyFile = (env.COAGENT_EXTENSION_KEY_FILE ?? '').trim();
  if (keyFile && existsSync(keyFile)) {
    const fileContents = readFileSync(keyFile, 'utf-8').trim();
    if (fileContents) return fileContents;
  }

  return undefined;
}
