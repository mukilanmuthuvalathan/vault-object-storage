import assert from 'node:assert/strict';
import { after, beforeEach, describe, test } from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { ErasureCodec } from '../src/data/erasure-codec.js';
import { VaultCluster } from '../src/core/vault-engine.js';

const directories = [];
let vault;

beforeEach(async () => {
  const rootDir = await mkdtemp(path.join(os.tmpdir(), 'vault-ec-test-'));
  directories.push(rootDir);
  vault = await new VaultCluster({ rootDir }).init();
});

after(async () => {
  await Promise.all(directories.map((directory) => rm(directory, { recursive: true, force: true })));
});

describe('10+4 erasure storage', () => {
  test('reconstructs the original bytes from any ten fragments', () => {
    const codec = new ErasureCodec(10, 4);
    const original = Buffer.from('Reed-Solomon protects cold objects with storage-efficient parity.');
    const encoded = codec.encode(original);
    const missing = new Set([0, 3, 10, 13]);
    const available = encoded.shards.map((buffer, index) => ({ index, buffer })).filter(({ index }) => !missing.has(index));
    assert.deepEqual(codec.decode(available, original.length), original);
  });

  test('stores cold data as fourteen fragments and repairs corruption', async () => {
    const original = Buffer.from('cold archive payload that must survive four independent fragment losses');
    const record = await vault.putObject('archive/2026.snap', original, { storageClass: 'cold' });
    assert.equal(record.chunks[0].mode, 'erasure');
    assert.equal(record.chunks[0].fragments.length, 14);
    const damagedNode = record.chunks[0].fragments[0].nodeId;
    await vault.corruptReplica(record.key, damagedNode);
    const retrieved = await vault.getObject(record.key);
    assert.deepEqual(retrieved.buffer, original);
    const report = await vault.integrityScan();
    assert.ok(report.repaired >= 1 || vault.metrics.repairs >= 1);
  });

  test('uses tombstones before garbage collection removes data', async () => {
    await vault.putObject('expired/object', Buffer.from('retained until gc'));
    await vault.deleteObject('expired/object', 0);
    await assert.rejects(vault.getObject('expired/object'), (error) => error.code === 'NOT_FOUND');
    const report = await vault.garbageCollect();
    assert.equal(report.purged, 1);
    assert.equal(vault.metadata.objects['expired/object'], undefined);
  });
});
