import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { RpcServer } from '../src/rpc-server.js';

function socketRequest(socketPath, { headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      socketPath,
      path: '/rpc',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...headers,
      },
    }, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { raw += chunk; });
      res.on('end', () => {
        resolve({ statusCode: res.statusCode, body: JSON.parse(raw) });
      });
    });
    req.on('error', reject);
    req.write(JSON.stringify({ method: 'admin.status', params: {} }));
    req.end();
  });
}

test('socket /rpc requires the same bearer token as http /rpc', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'rpc-server-auth-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  const socketPath = path.join(tempDir, 'daemon.sock');
  const rpcServer = new RpcServer({
    socketPath,
    authToken: 'daemon-token',
    channelManager: {
      async rpcCall(method, params, context) {
        return { method, params, transport: context.transport };
      },
    },
  });

  await rpcServer.start();
  t.after(async () => {
    await rpcServer.stop();
  });

  const unauthorized = await socketRequest(socketPath);
  assert.equal(unauthorized.statusCode, 401);
  assert.equal(unauthorized.body.ok, false);
  assert.equal(unauthorized.body.error.code, 'unauthorized');

  const authorized = await socketRequest(socketPath, {
    headers: { Authorization: 'Bearer daemon-token' },
  });
  assert.equal(authorized.statusCode, 200);
  assert.equal(authorized.body.ok, true);
  assert.equal(authorized.body.result.transport, 'socket');
});
