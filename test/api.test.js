import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { after, before, describe, test } from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { VaultCluster } from '../src/core/vault-engine.js';

let rootDir;
let vault;
let server;
let baseUrl;

function s3Key(url) {
  const match = url.pathname.match(/^\/s3\/([^/]+)\/(.+)$/);
  if (!match) throw new Error('not found');
  return `${decodeURIComponent(match[1])}/${decodeURIComponent(match[2])}`;
}

before(async () => {
  rootDir = await mkdtemp(path.join(os.tmpdir(), 'vault-api-test-'));
  vault = await new VaultCluster({ rootDir }).init();
  server = createServer(async (request, response) => {
    const url = new URL(request.url, 'http://localhost');
    try {
      if (url.pathname.startsWith('/s3/')) {
        const key = s3Key(url);
        if (request.method === 'PUT') {
          const chunks = [];
          for await (const chunk of request) chunks.push(chunk);
          const record = await vault.putObject(key, Buffer.concat(chunks), { contentType: request.headers['content-type'] || 'application/octet-stream' });
          response.writeHead(200, { ETag: `"${record.checksum}"`, 'X-Vault-Version': String(record.version) });
          return response.end();
        }
        if (request.method === 'GET') {
          const { buffer, record } = await vault.getObject(key);
          response.writeHead(200, { 'Content-Type': record.contentType, ETag: `"${record.checksum}"` });
          return response.end(buffer);
        }
        if (request.method === 'DELETE') {
          await vault.deleteObject(key, 0);
          response.writeHead(204);
          return response.end();
        }
      }
      response.writeHead(404);
      response.end();
    } catch (error) {
      response.writeHead(error.status || 500, { 'Content-Type': 'application/json' });
      response.end(JSON.stringify({ error: error.message }));
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  baseUrl = `http://127.0.0.1:${server.address().port}`;
});

after(async () => {
  await vault?.close();
  await new Promise((resolve) => server.close(resolve));
  await rm(rootDir, { recursive: true, force: true });
});

describe('S3-compatible object route behavior', () => {
  test('stores, retrieves, and deletes raw object bytes', async () => {
    const content = 'raw s3-compatible body';
    const put = await fetch(`${baseUrl}/s3/demo-bucket/folder/object.txt`, { method: 'PUT', body: content, headers: { 'Content-Type': 'text/plain' } });
    assert.equal(put.status, 200);
    assert.ok(put.headers.get('etag'));

    const get = await fetch(`${baseUrl}/s3/demo-bucket/folder/object.txt`);
    assert.equal(get.status, 200);
    assert.equal(await get.text(), content);

    const deleted = await fetch(`${baseUrl}/s3/demo-bucket/folder/object.txt?retentionMs=0`, { method: 'DELETE' });
    assert.equal(deleted.status, 204);
    await vault.garbageCollect();

    const missing = await fetch(`${baseUrl}/s3/demo-bucket/folder/object.txt`);
    assert.equal(missing.status, 404);
  });
});
