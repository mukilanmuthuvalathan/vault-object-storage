import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { VaultCluster, VaultError } from './core/vault-engine.js';
import { GcsMirror } from './integrations/gcs-mirror.js';
import { SlidingWindowRateLimiter } from './gateway/rate-limiter.js';
import { hasValidApiKey } from './gateway/auth.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(here, '..');
const publicDir = path.join(projectRoot, 'public');
const dataDir = process.env.VAULT_DATA_DIR || path.join(projectRoot, 'data');
const port = Number(process.env.PORT || 8080);
const storageMode = process.env.VAULT_STORAGE_MODE || 'http';
const vault = await new VaultCluster({
  rootDir: dataDir,
  storageMode,
  manageStorageNodes: storageMode === 'http',
  storageBasePort: Number(process.env.VAULT_STORAGE_BASE_PORT || 9100),
}).init();
const mirror = new GcsMirror();
const rateLimiter = new SlidingWindowRateLimiter({ limit: Number(process.env.RATE_LIMIT_PER_MINUTE || 180) });
vault.startBackgroundRepair(Number(process.env.REPAIR_INTERVAL_MS || 15000));

const mimeTypes = {
  '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.svg': 'image/svg+xml', '.json': 'application/json; charset=utf-8', '.ico': 'image/x-icon',
};

const securityHeaders = {
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'Referrer-Policy': 'no-referrer',
  'Cross-Origin-Opener-Policy': 'same-origin',
  'Cross-Origin-Resource-Policy': 'same-origin',
  'Permissions-Policy': 'camera=(), microphone=(), geolocation=(), payment=(), usb=()',
  'Content-Security-Policy': "default-src 'self'; style-src 'self' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; script-src 'self'; connect-src 'self'; img-src 'self' data:",
};

function sendJson(response, status, body) {
  response.writeHead(status, { ...securityHeaders, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  response.end(JSON.stringify(body));
}

async function parseJson(request, maxBytes = 30 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > maxBytes) throw new VaultError('Request exceeds 30 MB demo limit', 413, 'PAYLOAD_TOO_LARGE');
    chunks.push(chunk);
  }
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8')); }
  catch { throw new VaultError('Invalid JSON body', 400, 'INVALID_JSON'); }
}

async function readRaw(request, maxBytes = 300 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > maxBytes) throw new VaultError('Object exceeds 300 MB API limit', 413, 'PAYLOAD_TOO_LARGE');
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

const decodeKey = (segment) => {
  try { return decodeURIComponent(segment); }
  catch { throw new VaultError('Invalid object key', 400, 'INVALID_KEY'); }
};

function s3ObjectKey(url) {
  const match = url.pathname.match(/^\/s3\/([^/]+)\/(.+)$/);
  if (!match) throw new VaultError('S3 route not found', 404, 'NOT_FOUND');
  const bucket = decodeKey(match[1]);
  const key = decodeKey(match[2]);
  return `${bucket}/${key}`;
}

async function s3Api(request, response, url) {
  const key = s3ObjectKey(url);
  if (request.method === 'PUT') {
    const buffer = await readRaw(request);
    const storageClass = (request.headers['x-vault-storage-class'] || 'hot').toString().toLowerCase() === 'cold' ? 'cold' : 'hot';
    const record = await vault.putObject(key, buffer, { contentType: request.headers['content-type'] || 'application/octet-stream', storageClass });
    response.writeHead(200, {
      ...securityHeaders,
      ETag: `"${record.checksum}"`,
      'X-Vault-Version': String(record.version),
      'X-Vault-Storage-Class': record.storageClass,
      'Content-Length': '0',
    });
    return response.end();
  }
  if (request.method === 'GET' || request.method === 'HEAD') {
    const { buffer, record } = await vault.getObject(key);
    response.writeHead(200, {
      ...securityHeaders,
      'Content-Type': record.contentType,
      'Content-Length': buffer.length,
      ETag: `"${record.checksum}"`,
      'X-Vault-Version': String(record.version),
      'X-Vault-Storage-Class': record.storageClass,
    });
    return request.method === 'HEAD' ? response.end() : response.end(buffer);
  }
  if (request.method === 'DELETE') {
    await vault.deleteObject(key, Number(url.searchParams.get('retentionMs') ?? 86400000));
    response.writeHead(204, securityHeaders);
    return response.end();
  }
  throw new VaultError('S3 method not allowed', 405, 'METHOD_NOT_ALLOWED');
}

async function api(request, response, url) {
  if (request.method === 'GET' && url.pathname === '/api/health') {
    await vault.refreshNodeHealth();
    const status = vault.clusterStatus();
    const healthyStorageNodes = status.nodes.filter((node) => node.status === 'healthy').length;
    const healthyMetadataNodes = status.metadataControlPlane.healthyNodes;
    const ready = healthyStorageNodes >= status.policy.writeQuorum && healthyMetadataNodes >= status.metadataControlPlane.majority;
    return sendJson(response, ready ? 200 : 503, {
      ok: ready,
      status: ready ? 'ready' : 'degraded',
      storage: { mode: status.storage.mode, healthy: healthyStorageNodes, total: status.nodes.length },
      metadata: { healthy: healthyMetadataNodes, majority: status.metadataControlPlane.majority },
      revision: status.revision,
      checkedAt: new Date().toISOString(),
    });
  }
  if (request.method === 'GET' && url.pathname === '/api/status') {
    await vault.refreshNodeHealth();
    return sendJson(response, 200, { ...vault.clusterStatus(), googleCloud: mirror.status() });
  }
  if (request.method === 'GET' && url.pathname === '/api/objects') return sendJson(response, 200, { objects: vault.listObjects() });
  if (request.method === 'POST' && url.pathname === '/api/objects') {
    const body = await parseJson(request);
    const buffer = Buffer.from(body.data || '', 'base64');
    const record = await vault.putObject(body.key, buffer, { contentType: body.contentType, policy: body.policy, storageClass: body.storageClass });
    const cloudMirror = await mirror.put(body.key, buffer, body.contentType);
    return sendJson(response, 201, { object: record, cloudMirror });
  }
  if (request.method === 'GET' && url.pathname.startsWith('/api/objects/')) {
    const key = decodeKey(url.pathname.slice('/api/objects/'.length));
    const { buffer, record } = await vault.getObject(key);
    response.writeHead(200, {
      ...securityHeaders, 'Content-Type': record.contentType, 'Content-Length': buffer.length,
      'Content-Disposition': `attachment; filename="${path.basename(record.key).replace(/["\r\n]/g, '_')}"`,
      'X-Vault-Version': String(record.version), 'X-Vault-Checksum': record.checksum,
    });
    return response.end(buffer);
  }
  if (request.method === 'DELETE' && url.pathname.startsWith('/api/objects/')) {
    const key = decodeKey(url.pathname.slice('/api/objects/'.length));
    const retentionMs = Number(url.searchParams.get('retentionMs') ?? 86400000);
    return sendJson(response, 202, { tombstone: await vault.deleteObject(key, retentionMs) });
  }
  if (request.method === 'PATCH' && /^\/api\/nodes\/[^/]+$/.test(url.pathname)) {
    const nodeId = decodeKey(url.pathname.split('/').at(-1));
    const body = await parseJson(request);
    let node = vault.setNodeStatus(nodeId, body.status);
    if (body.status === 'healthy') node = await vault.waitForNodeHealthy(nodeId);
    return sendJson(response, 200, { node });
  }
  if (request.method === 'POST' && url.pathname === '/api/integrity-scan') return sendJson(response, 200, { report: await vault.integrityScan() });
  if (request.method === 'POST' && url.pathname === '/api/rebalance') return sendJson(response, 200, { report: await vault.rebalance() });
  if (request.method === 'POST' && url.pathname === '/api/garbage-collect') return sendJson(response, 200, { report: await vault.garbageCollect() });
  if (request.method === 'POST' && url.pathname === '/api/faults/corrupt') {
    const body = await parseJson(request);
    return sendJson(response, 200, { fault: await vault.corruptReplica(body.key, body.nodeId) });
  }
  if (request.method === 'POST' && url.pathname === '/api/faults/crash-node') {
    const body = await parseJson(request);
    return sendJson(response, 200, { node: vault.crashStorageNode(body.nodeId) });
  }
  if (request.method === 'POST' && url.pathname === '/api/demo/seed') {
    const samples = [
      ['research/aurora-model.bin', 'Vault demo model checkpoint · immutable sample payload', 'hot'],
      ['media/orbit-launch.mp4', 'Vault demo media segment · replicated sample payload', 'hot'],
      ['logs/telemetry-2026-09.jsonl', '{"service":"gateway","status":"healthy"}\n', 'hot'],
      ['backups/customer-ledger.snap', 'Vault demo encrypted database snapshot', 'cold'],
    ];
    for (const [key, value, storageClass] of samples) await vault.putObject(key, Buffer.from(value), { contentType: 'application/octet-stream', storageClass });
    return sendJson(response, 201, { seeded: samples.length });
  }
  throw new VaultError('API route not found', 404, 'NOT_FOUND');
}

async function serveStatic(response, url) {
  const requested = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
  const filePath = path.resolve(publicDir, requested);
  if (!filePath.startsWith(`${publicDir}${path.sep}`) && filePath !== path.join(publicDir, 'index.html')) throw new VaultError('Not found', 404, 'NOT_FOUND');
  try {
    const content = await readFile(filePath);
    response.writeHead(200, { ...securityHeaders, 'Content-Type': mimeTypes[path.extname(filePath)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    response.end(content);
  } catch (error) {
    if (error.code === 'ENOENT') throw new VaultError('Not found', 404, 'NOT_FOUND');
    throw error;
  }
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url, `http://${request.headers.host || 'localhost'}`);
  try {
    if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/s3/')) {
      const clientId = String(request.headers['x-forwarded-for'] || request.socket.remoteAddress || 'unknown').split(',')[0].trim();
      const rate = rateLimiter.consume(clientId);
      response.setHeader('X-RateLimit-Remaining', String(rate.remaining));
      if (!rate.allowed) {
        response.setHeader('Retry-After', String(Math.ceil(rate.retryAfterMs / 1000)));
        return sendJson(response, 429, { error: 'Rate limit exceeded', code: 'RATE_LIMITED' });
      }
      const expectedKey = process.env.VAULT_API_KEY;
      if (!hasValidApiKey(request.headers.authorization, expectedKey)) return sendJson(response, 401, { error: 'Authentication required', code: 'UNAUTHORIZED' });
      if (url.pathname.startsWith('/s3/')) await s3Api(request, response, url);
      else await api(request, response, url);
    }
    else await serveStatic(response, url);
  } catch (error) {
    const status = error instanceof VaultError ? error.status : 500;
    sendJson(response, status, { error: error.message || 'Internal server error', code: error.code || 'INTERNAL_ERROR' });
  }
});

server.listen(port, '0.0.0.0', () => console.log(`Vault listening on http://localhost:${port}`));

function shutdown() {
  vault.close().finally(() => server.close(() => process.exit(0)));
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
