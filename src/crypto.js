import { createCipheriv, createDecipheriv, randomBytes } from 'crypto';

const ALGO = 'aes-256-gcm';

function getKey() {
  const hex = process.env.CREDENTIAL_KEY ?? '';
  if (hex.length === 64) return Buffer.from(hex, 'hex');
  // Fallback: derive a fixed key from a warning-level default (not secure, logs warning)
  if (!process.env.CREDENTIAL_KEY) {
    console.warn('[crypto] CREDENTIAL_KEY not set — using insecure default. Set a 64-char hex key in .env!');
  }
  return Buffer.alloc(32, 0);
}

export function encrypt(obj) {
  const key = getKey();
  const iv = randomBytes(12);
  const cipher = createCipheriv(ALGO, key, iv);
  const plain = JSON.stringify(obj);
  const encrypted = Buffer.concat([cipher.update(plain, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return {
    iv: iv.toString('hex'),
    data: Buffer.concat([encrypted, tag]).toString('hex'),
  };
}

export function decrypt(ivHex, dataHex) {
  const key = getKey();
  const iv = Buffer.from(ivHex, 'hex');
  const buf = Buffer.from(dataHex, 'hex');
  const tag = buf.subarray(buf.length - 16);
  const encrypted = buf.subarray(0, buf.length - 16);
  const decipher = createDecipheriv(ALGO, key, iv);
  decipher.setAuthTag(tag);
  const plain = Buffer.concat([decipher.update(encrypted), decipher.final()]).toString('utf8');
  return JSON.parse(plain);
}
