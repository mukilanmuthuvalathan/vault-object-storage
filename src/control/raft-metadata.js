import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

export class MetadataQuorumError extends Error {
  constructor(message = 'Metadata quorum unavailable') {
    super(message);
    this.code = 'METADATA_QUORUM_UNAVAILABLE';
    this.status = 503;
  }
}

export class RaftMetadataControlPlane {
  constructor({ rootDir, nodeCount = 5 } = {}) {
    this.rootDir = rootDir;
    this.nodeCount = nodeCount;
    this.majority = Math.floor(nodeCount / 2) + 1;
    this.nodes = [];
    this.leaderId = 'meta-01';
    this.term = 1;
    this.commitIndex = 0;
    this.catalog = { revision: 0, objects: {} };
  }

  async init() {
    await mkdir(this.rootDir, { recursive: true });
    for (let index = 0; index < this.nodeCount; index += 1) {
      const id = `meta-${String(index + 1).padStart(2, '0')}`;
      const node = { id, status: 'healthy', role: index === 0 ? 'leader' : 'follower', path: path.join(this.rootDir, id, 'snapshot.json') };
      await mkdir(path.dirname(node.path), { recursive: true });
      this.nodes.push(node);
    }

    const snapshots = [];
    for (const node of this.nodes) {
      try { snapshots.push(JSON.parse(await readFile(node.path, 'utf8'))); } catch {}
    }
    const newest = snapshots.sort((a, b) => (b.commitIndex ?? 0) - (a.commitIndex ?? 0))[0];
    if (newest) {
      this.term = newest.term ?? 1;
      this.commitIndex = newest.commitIndex ?? newest.catalog?.revision ?? 0;
      this.catalog = newest.catalog ?? this.catalog;
    } else {
      await this.#replicate(this.catalog);
    }
    return this;
  }

  async commit(catalog) {
    const healthy = this.nodes.filter((node) => node.status === 'healthy');
    if (healthy.length < this.majority) throw new MetadataQuorumError();
    const acknowledged = await this.#replicate(structuredClone(catalog));
    if (acknowledged < this.majority) throw new MetadataQuorumError('Metadata command failed to reach a Raft majority');
    this.catalog = structuredClone(catalog);
    this.commitIndex = catalog.revision;
    return this.status();
  }

  setNodeStatus(nodeId, status) {
    const node = this.nodes.find((item) => item.id === nodeId);
    if (!node) throw new Error('Metadata node not found');
    if (!['healthy', 'offline'].includes(status)) throw new Error('Invalid metadata node status');
    node.status = status;
    return node;
  }

  status() {
    return {
      algorithm: 'Raft',
      term: this.term,
      leaderId: this.leaderId,
      commitIndex: this.commitIndex,
      majority: this.majority,
      healthyNodes: this.nodes.filter((node) => node.status === 'healthy').length,
      nodes: this.nodes.map(({ id, status, role }) => ({ id, status, role })),
    };
  }

  async #replicate(catalog) {
    const snapshot = JSON.stringify({ term: this.term, commitIndex: catalog.revision, catalog }, null, 2);
    const results = await Promise.allSettled(this.nodes.filter((node) => node.status === 'healthy').map(async (node) => {
      const temporary = `${node.path}.${randomUUID()}.tmp`;
      await writeFile(temporary, snapshot);
      await rename(temporary, node.path);
    }));
    return results.filter((result) => result.status === 'fulfilled').length;
  }
}
