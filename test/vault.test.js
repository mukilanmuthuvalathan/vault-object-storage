import assert from 'node:assert/strict';
import { after, afterEach, beforeEach, describe, test } from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { VaultCluster } from '../src/core/vault-engine.js';

const directories = [];
let vault;

beforeEach(async () => {
  const rootDir = await mkdtemp(path.join(os.tmpdir(), 'vault-test-'));
  directories.push(rootDir);
  vault = await new VaultCluster({ rootDir }).init();
});

afterEach(async () => {
  await vault?.close();
});

after(async () => {
  await Promise.all(directories.map((directory) => rm(directory, { recursive: true, force: true })));
});

describe('VaultCluster', () => {
  test('stores an object across failure domains and retrieves identical bytes', async () => {
    const content = Buffer.from('mission critical payload');
    const record = await vault.putObject('missions/alpha.bin', content);
    assert.equal(record.replicas.length, 3);
    assert.equal(new Set(record.replicas.map((id) => vault.nodes.get(id).zone)).size, 3);
    const retrieved = await vault.getObject('missions/alpha.bin');
    assert.deepEqual(retrieved.buffer, content);
  });

  test('remains available after a replica node fails', async () => {
    const record = await vault.putObject('logs/audit.jsonl', Buffer.from('{"ok":true}\n'));
    vault.setNodeStatus(record.replicas[0], 'offline');
    const retrieved = await vault.getObject('logs/audit.jsonl');
    assert.equal(retrieved.buffer.toString(), '{"ok":true}\n');
    assert.equal(vault.listObjects()[0].health, 'degraded');
  });

  test('uses independent storage-node processes for replicated data', async () => {
    await vault.close();
    const rootDir = await mkdtemp(path.join(os.tmpdir(), 'vault-distributed-test-'));
    directories.push(rootDir);
    const storageBasePort = 13000 + Math.floor(Math.random() * 1000);
    vault = await new VaultCluster({ rootDir, nodes: 3, storageMode: 'http', manageStorageNodes: true, storageBasePort }).init();

    const content = Buffer.from('served by independent node processes');
    const record = await vault.putObject('distributed/process-proof.bin', content);
    const failedNode = record.replicas[0];
    const failedIndex = Number(failedNode.split('-').at(-1)) - 1;
    vault.storageNodeProcesses[failedIndex].kill('SIGTERM');
    await new Promise((resolve) => setTimeout(resolve, 250));
    await vault.refreshNodeHealth();

    assert.equal(vault.nodes.get(failedNode).status, 'partitioned');
    const retrieved = await vault.getObject(record.key);
    assert.deepEqual(retrieved.buffer, content);
  });

  test('recovers managed storage processes before failing a read', async () => {
    await vault.close();
    const rootDir = await mkdtemp(path.join(os.tmpdir(), 'vault-read-recovery-test-'));
    directories.push(rootDir);
    const storageBasePort = 15000 + Math.floor(Math.random() * 1000);
    vault = await new VaultCluster({ rootDir, nodes: 3, storageMode: 'http', manageStorageNodes: true, storageBasePort }).init();

    const content = Buffer.from('read recovery after upload');
    const record = await vault.putObject('uploads/recover-before-read.txt', content);
    for (const nodeId of record.replicas) vault.setNodeStatus(nodeId, 'offline');

    const retrieved = await vault.getObject(record.key);
    assert.deepEqual(retrieved.buffer, content);
    assert.equal(record.replicas.every((nodeId) => vault.nodes.get(nodeId).status === 'healthy'), true);
  });

  test('detects corruption and reconstructs the bad replica', async () => {
    const original = Buffer.from('checksum-protected-content');
    const record = await vault.putObject('backups/checkpoint.snap', original);
    await vault.corruptReplica(record.key, record.replicas[0]);
    const report = await vault.integrityScan();
    assert.equal(report.degraded, 1);
    assert.equal(report.repaired, 1);
    const retrieved = await vault.getObject(record.key);
    assert.deepEqual(retrieved.buffer, original);
  });

  test('serializes concurrent writes and preserves monotonic versions', async () => {
    await Promise.all(Array.from({ length: 12 }, (_, index) => vault.putObject('concurrent/key', Buffer.from(`version-${index}`))));
    const [record] = vault.listObjects();
    assert.equal(record.version, 12);
    assert.equal(record.replicas.length, 3);
  });

  test('rejects a quorum policy that cannot guarantee consistency', async () => {
    await assert.rejects(
      vault.putObject('unsafe/key', Buffer.from('unsafe'), { policy: { replicationFactor: 3, readQuorum: 1, writeQuorum: 2 } }),
      (error) => error.code === 'UNSAFE_POLICY',
    );
  });
});
