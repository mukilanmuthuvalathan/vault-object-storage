import { createServer } from 'node:http';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, rename, rm, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';

const args = new Map(process.argv.slice(2).map((arg) => {
  const [key, value = ''] = arg.replace(/^--/, '').split('=');
  return [key, value];
}));

const nodeId = args.get('id') || process.env.STORAGE_NODE_ID || 'node-01';
const zone = args.get('zone') || process.env.STORAGE_NODE_ZONE || 'zone-1';
const rack = args.get('rack') || process.env.STORAGE_NODE_RACK || 'rack-1';
const rootDir = args.get('dir') || process.env.STORAGE_NODE_DIR || path.join(process.cwd(), 'data', 'nodes-v2', nodeId);
const port = Number(args.get('port') || process.env.STORAGE_NODE_PORT || 9101);

const artifactPattern = /^[A-Za-z0-9._-]{1,512}$/;
const sha256 = (buffer) => createHash('sha256').update(buffer).digest('hex');

function json(response, status, body) {
  response.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  response.end(JSON.stringify(body));
}

function artifactPath(artifactId) {
  if (!artifactPattern.test(artifactId) || artifactId.includes('..')) {
    const error = new Error('Invalid artifact id');
    error.status = 400;
    throw error;
  }
  return path.join(rootDir, artifactId);
}

async function readBody(request, maxBytes = 300 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > maxBytes) {
      const error = new Error('Artifact too large');
      error.status = 413;
      throw error;
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

await mkdir(rootDir, { recursive: true });

const server = createServer(async (request, response) => {
  const url = new URL(request.url, `http://${request.headers.host || 'localhost'}`);
  try {
    if (request.method === 'GET' && url.pathname === '/health') {
      return json(response, 200, { id: nodeId, zone, rack, status: 'healthy' });
    }

    const match = url.pathname.match(/^\/artifacts\/([^/]+)(?:\/quarantine)?$/);
    if (!match) return json(response, 404, { error: 'Not found' });

    const artifactId = decodeURIComponent(match[1]);
    const target = artifactPath(artifactId);

    if (request.method === 'PUT' && !url.pathname.endsWith('/quarantine')) {
      const body = await readBody(request);
      const temporary = `${target}.${randomUUID()}.tmp`;
      await writeFile(temporary, body);
      await rename(temporary, target);
      return json(response, 201, { id: artifactId, bytes: body.length, checksum: sha256(body) });
    }

    if (request.method === 'GET' && !url.pathname.endsWith('/quarantine')) {
      const content = await readFile(target);
      response.writeHead(200, {
        'Content-Type': 'application/octet-stream',
        'Content-Length': content.length,
        'X-Artifact-Checksum': sha256(content),
        'Cache-Control': 'no-store',
      });
      return response.end(content);
    }

    if (request.method === 'DELETE' && !url.pathname.endsWith('/quarantine')) {
      let removedBytes = 0;
      try { removedBytes = (await stat(target)).size; } catch {}
      await rm(target, { force: true });
      return json(response, 200, { id: artifactId, removedBytes });
    }

    if (request.method === 'POST' && url.pathname.endsWith('/quarantine')) {
      const quarantineDir = path.join(rootDir, 'quarantine');
      await mkdir(quarantineDir, { recursive: true });
      let removedBytes = 0;
      try { removedBytes = (await stat(target)).size; } catch {}
      await rename(target, path.join(quarantineDir, `${path.basename(target)}.${Date.now()}.bad`));
      return json(response, 200, { id: artifactId, removedBytes, quarantined: true });
    }

    return json(response, 405, { error: 'Method not allowed' });
  } catch (error) {
    const status = error.status || (error.code === 'ENOENT' ? 404 : 500);
    return json(response, status, { error: error.message || 'Storage node error' });
  }
});

server.listen(port, '127.0.0.1', () => {
  console.log(`${nodeId} listening on http://127.0.0.1:${port}`);
});

function shutdown() {
  server.close(() => process.exit(0));
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
