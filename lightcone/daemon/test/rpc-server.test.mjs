import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { RpcServer } from '../src/rpc-server.js';

function socketRequest(socketPath, { method = 'admin.status', params = {}, headers = {} } = {}) {
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
    req.write(JSON.stringify({ method, params }));
    req.end();
  });
}

function httpRequest(port, { path = '/rpc', method = 'POST', payload = {}, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: '127.0.0.1',
      port,
      path,
      method,
      headers: {
        ...(payload ? { 'Content-Type': 'application/json' } : {}),
        ...headers,
      },
    }, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { raw += chunk; });
      res.on('end', () => {
        resolve({ statusCode: res.statusCode, body: raw ? JSON.parse(raw) : {} });
      });
    });
    req.on('error', reject);
    if (payload) req.write(JSON.stringify(payload));
    req.end();
  });
}

test('socket /rpc requires bearer token for admin and mutating methods', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'rpc-server-auth-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  const socketPath = path.join(tempDir, 'daemon.sock');
  const rpcServer = new RpcServer({
    socketPath,
    authToken: 'daemon-token',
    authTokens: ['machine-key'],
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

  for (const method of ['admin.machines', 'channel.start']) {
    const unauthorized = await socketRequest(socketPath, {
      method,
      params: method === 'channel.start' ? { channel_id: 'channel-a' } : {},
    });
    assert.equal(unauthorized.statusCode, 401);
    assert.equal(unauthorized.body.ok, false);
    assert.equal(unauthorized.body.error.code, 'unauthorized');

    const authorized = await socketRequest(socketPath, {
      method,
      params: method === 'channel.start' ? { channel_id: 'channel-a' } : {},
      headers: { Authorization: 'Bearer machine-key' },
    });
    assert.equal(authorized.statusCode, 200);
    assert.equal(authorized.body.ok, true);
    assert.equal(authorized.body.result.method, method);
    assert.equal(authorized.body.result.transport, 'socket');
  }
});

test('http /admin routes still require bearer token', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'rpc-server-http-auth-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  const rpcServer = new RpcServer({
    socketPath: path.join(tempDir, 'daemon.sock'),
    httpPort: 0,
    authToken: 'daemon-token',
    authTokens: ['machine-key'],
    channelManager: {
      async listMachines() {
        return { machines: [{ id: 'local' }] };
      },
    },
  });

  await rpcServer.start();
  t.after(async () => {
    await rpcServer.stop();
  });
  const { port } = rpcServer.httpServer.address();

  const unauthorized = await httpRequest(port, { path: '/admin/machines', method: 'GET', payload: null });
  assert.equal(unauthorized.statusCode, 401);
  assert.equal(unauthorized.body.error.code, 'unauthorized');

  const authorized = await httpRequest(port, {
    path: '/admin/machines',
    method: 'GET',
    payload: null,
    headers: { Authorization: 'Bearer machine-key' },
  });
  assert.equal(authorized.statusCode, 200);
  assert.deepEqual(authorized.body, { machines: [{ id: 'local' }] });
});
