// wxt-helpers.test.ts — vitest coverage for the build-time env
// resolvers consumed by wxt.config.ts. Acceptance:
//   - M1.6-T7 phase-3 C1: $COAGENT_WEB_DOMAIN=test.example.com injects
//     `https://test.example.com/*` into externally_connectable.matches.
//   - C3: manifest `key` field surfaces from COAGENT_EXTENSION_KEY.

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { writeFileSync, mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import {
  DEFAULT_EXTERNAL_ORIGINS,
  resolveExternallyConnectableMatches,
  resolveManifestKey,
} from './wxt-helpers';

describe('resolveExternallyConnectableMatches', () => {
  it('falls back to dev defaults when env is empty', () => {
    expect(resolveExternallyConnectableMatches({})).toEqual(DEFAULT_EXTERNAL_ORIGINS);
  });

  it('falls back to dev defaults when env is whitespace', () => {
    expect(
      resolveExternallyConnectableMatches({
        COAGENT_WEB_ORIGINS: '   ',
        COAGENT_WEB_DOMAIN: '\t',
      }),
    ).toEqual(DEFAULT_EXTERNAL_ORIGINS);
  });

  it('expands COAGENT_WEB_DOMAIN into https://${domain}/* + dev defaults', () => {
    const got = resolveExternallyConnectableMatches({
      COAGENT_WEB_DOMAIN: 'test.example.com',
    });
    // Acceptance C1: the prod domain MUST be in the list as
    // `https://test.example.com/*`.
    expect(got).toContain('https://test.example.com/*');
    // Dev defaults are merged so smoke tests against localhost still
    // work on the prod build.
    expect(got).toContain('http://localhost:*/*');
    expect(got).toContain('http://127.0.0.1:*/*');
  });

  it('COAGENT_WEB_ORIGINS wins outright over COAGENT_WEB_DOMAIN', () => {
    const got = resolveExternallyConnectableMatches({
      COAGENT_WEB_ORIGINS: 'https://only.example/*,http://localhost:*/*',
      COAGENT_WEB_DOMAIN: 'should-be-ignored.example',
    });
    expect(got).toEqual(['https://only.example/*', 'http://localhost:*/*']);
  });

  it('drops empty + whitespace patterns from comma list', () => {
    const got = resolveExternallyConnectableMatches({
      COAGENT_WEB_ORIGINS: 'https://a.example/*,, ,https://b.example/*',
    });
    expect(got).toEqual(['https://a.example/*', 'https://b.example/*']);
  });
});

describe('resolveManifestKey', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'wxt-helpers-test-'));
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns undefined when both env vars are unset', () => {
    expect(resolveManifestKey({})).toBeUndefined();
  });

  it('returns the COAGENT_EXTENSION_KEY value verbatim (trimmed)', () => {
    const k = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...';
    expect(
      resolveManifestKey({
        COAGENT_EXTENSION_KEY: `  ${k}  `,
      }),
    ).toBe(k);
  });

  it('falls back to reading COAGENT_EXTENSION_KEY_FILE when env value is blank', () => {
    const k = 'BASE64KEYCONTENTS';
    const path = join(tmpDir, 'ext.key');
    writeFileSync(path, `${k}\n`);
    expect(
      resolveManifestKey({
        COAGENT_EXTENSION_KEY_FILE: path,
      }),
    ).toBe(k);
  });

  it('returns undefined when the key file points at a missing path', () => {
    expect(
      resolveManifestKey({
        COAGENT_EXTENSION_KEY_FILE: join(tmpDir, 'does-not-exist'),
      }),
    ).toBeUndefined();
  });

  it('returns undefined when the key file is empty', () => {
    const path = join(tmpDir, 'empty.key');
    writeFileSync(path, '   \n');
    expect(
      resolveManifestKey({
        COAGENT_EXTENSION_KEY_FILE: path,
      }),
    ).toBeUndefined();
  });

  it('prefers COAGENT_EXTENSION_KEY over the file when both set', () => {
    const inlineKey = 'INLINE';
    const filePath = join(tmpDir, 'ext.key');
    writeFileSync(filePath, 'FROMFILE');
    expect(
      resolveManifestKey({
        COAGENT_EXTENSION_KEY: inlineKey,
        COAGENT_EXTENSION_KEY_FILE: filePath,
      }),
    ).toBe(inlineKey);
  });
});
