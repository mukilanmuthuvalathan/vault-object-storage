import assert from 'node:assert/strict';
import { after, beforeEach, describe, test } from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { RaftMetadataControlPlane } from '../src/control/raft-metadata.js';

const directories = [];
let rootDir;

beforeEach(async () => {
  rootDir = await mkdtemp(path.join(os.tmpdir(), 'vault-raft-test-'));
  directories.push(rootDir);
});

after(async () => {
  await Promise.all(directories.map((directory) => rm(directory, { recursive: true, force: true })));
});

describe('RaftMetadataControlPlane', () => {
  test('commits catalog revisions to a five-member control plane and recovers them', async () => {
    const controlPlane = await new RaftMetadataControlPlane({ rootDir }).init();
    const catalog = { revision: 1, objects: { 'demo/key': { key: 'demo/key', version: 1 } } };
    await controlPlane.commit(catalog);
    assert.equal(controlPlane.status().healthyNodes, 5);
    assert.equal(controlPlane.status().majority, 3);
    assert.equal(controlPlane.status().commitIndex, 1);

    const recovered = await new RaftMetadataControlPlane({ rootDir }).init();
    assert.deepEqual(recovered.catalog, catalog);
    assert.equal(recovered.status().commitIndex, 1);
  });

  test('rejects metadata commits without a majority', async () => {
    const controlPlane = await new RaftMetadataControlPlane({ rootDir }).init();
    controlPlane.setNodeStatus('meta-03', 'offline');
    controlPlane.setNodeStatus('meta-04', 'offline');
    controlPlane.setNodeStatus('meta-05', 'offline');
    await assert.rejects(
      controlPlane.commit({ revision: 1, objects: {} }),
      (error) => error.code === 'METADATA_QUORUM_UNAVAILABLE',
    );
  });
});
