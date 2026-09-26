import { spawnSync } from 'node:child_process';

const files = [
  'src/server.js',
  'src/core/vault-engine.js',
  'src/control/raft-metadata.js',
  'src/data/erasure-codec.js',
  'src/data/read-coordinator.js',
  'src/background/repair-scheduler.js',
  'src/gateway/auth.js',
  'src/gateway/rate-limiter.js',
  'src/integrations/gcs-mirror.js',
  'src/storage/storage-client.js',
  'src/storage/storage-node-server.js',
  'public/app.js',
];

for (const file of files) {
  const result = spawnSync(process.execPath, ['--check', file], { encoding: 'utf8' });
  if (result.status !== 0) {
    process.stderr.write(result.stderr || result.stdout || `Syntax check failed: ${file}\n`);
    process.exit(result.status ?? 1);
  }
}

console.log(`Syntax valid: ${files.length} JavaScript files`);
