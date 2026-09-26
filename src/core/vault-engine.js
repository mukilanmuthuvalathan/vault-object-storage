import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdir, readFile, rename, rm, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { LockManager } from './lock-manager.js';
import { RaftMetadataControlPlane } from '../control/raft-metadata.js';
import { PlacementEngine } from '../data/placement-engine.js';
import { ReadCoordinator } from '../data/read-coordinator.js';
import { ErasureCodec } from '../data/erasure-codec.js';
import { HealthMonitor } from '../background/health-monitor.js';
import { RepairScheduler } from '../background/repair-scheduler.js';
import { StorageNodeClient } from '../storage/storage-client.js';

const sha256 = (buffer) => createHash('sha256').update(buffer).digest('hex');
const safeKey = (key) => Buffer.from(key).toString('base64url');
const now = () => new Date().toISOString();
const DEFAULT_CHUNK_SIZE = 64 * 1024 * 1024;

export class VaultError extends Error {
  constructor(message, status = 500, code = 'VAULT_ERROR') {
    super(message);
    this.name = 'VaultError';
    this.status = status;
    this.code = code;
  }
}

export class VaultCluster {
  constructor({ rootDir, nodes = 15, replicationFactor = 3, readQuorum = 2, writeQuorum = 2, chunkSize = DEFAULT_CHUNK_SIZE, storageMode = 'local', manageStorageNodes = false, storageBasePort = 9100 } = {}) {
    this.rootDir = rootDir;
    this.nodeCount = nodes;
    this.storageMode = storageMode;
    this.manageStorageNodes = manageStorageNodes;
    this.storageBasePort = storageBasePort;
    this.chunkSize = Math.min(256 * 1024 * 1024, Math.max(DEFAULT_CHUNK_SIZE, chunkSize));
    this.defaultPolicy = { replicationFactor, readQuorum, writeQuorum, dataShards: 10, parityShards: 4, ecWriteQuorum: 12 };
    this.nodes = new Map();
    this.metadata = { revision: 0, objects: {} };
    this.locks = new LockManager();
    this.placement = new PlacementEngine();
    this.readCoordinator = new ReadCoordinator({ checksum: sha256 });
    this.erasureCodec = new ErasureCodec(10, 4);
    this.storageClient = new StorageNodeClient();
    this.storageNodeProcesses = [];
    this.repairScheduler = new RepairScheduler({ concurrency: 2 });
    this.controlPlane = new RaftMetadataControlPlane({ rootDir: path.join(rootDir, 'control-plane-v2'), nodeCount: 5 });
    this.healthMonitor = new HealthMonitor({ scan: () => this.integrityScan() });
    this.events = [];
    this.metrics = { reads: 0, writes: 0, repairs: 0, failures: 0, bytesStored: 0, lastRepairMs: 0, hedgedReads: 0 };
  }

  async init() {
    await mkdir(this.rootDir, { recursive: true });
    for (let index = 0; index < this.nodeCount; index += 1) {
      const id = `node-${String(index + 1).padStart(2, '0')}`;
      const node = { id, zone: `zone-${(index % 3) + 1}`, rack: `rack-${Math.floor(index / 3) + 1}`, region: 'asia-south1', status: 'healthy', usedBytes: 0, latencyMs: 7 + (index % 5) * 5, path: path.join(this.rootDir, 'nodes-v2', id), endpoint: `http://127.0.0.1:${this.storageBasePort + index + 1}` };
      await mkdir(node.path, { recursive: true });
      this.nodes.set(id, node);
    }
    if (this.storageMode === 'http' && this.manageStorageNodes) await this.#startManagedStorageNodes();
    await this.controlPlane.init();
    this.metadata = this.controlPlane.catalog;
    await this.#recalculateUsage();
    this.#event('cluster', 'Vault fabric initialized', `${this.nodes.size} nodes · 3 zones · 5 metadata voters`);
    return this;
  }

  async close() {
    this.stopBackgroundRepair();
    for (const child of this.storageNodeProcesses) child.kill('SIGTERM');
    this.storageNodeProcesses = [];
  }

  async refreshNodeHealth() {
    if (this.storageMode !== 'http') return [...this.nodes.values()];
    await Promise.all([...this.nodes.values()].map(async (node) => {
      const reachable = await this.storageClient.health(node);
      if (node.status === 'offline') return;
      node.status = reachable ? 'healthy' : 'partitioned';
    }));
    return [...this.nodes.values()];
  }

  async waitForNodeHealthy(nodeId, attempts = 20) {
    const node = this.nodes.get(nodeId);
    if (!node || this.storageMode !== 'http') return node;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      if (await this.storageClient.health(node)) {
        node.status = 'healthy';
        return node;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    node.status = 'partitioned';
    return node;
  }

  startBackgroundRepair(intervalMs = 15000) {
    this.healthMonitor.intervalMs = intervalMs;
    this.healthMonitor.start();
  }

  stopBackgroundRepair() { this.healthMonitor.stop(); }

  async putObject(key, buffer, options = {}) {
    this.#validateObject(key, buffer);
    return this.locks.withKey(`object:${key}`, async () => {
      const policy = this.#policy(options.policy);
      const storageClass = options.storageClass === 'cold' ? 'cold' : 'hot';
      const previous = this.metadata.objects[key];
      const version = (previous?.version ?? 0) + 1;
      const chunks = [];
      const written = [];

      try {
        for (let offset = 0, index = 0; offset < buffer.length; offset += this.chunkSize, index += 1) {
          const content = buffer.subarray(offset, Math.min(buffer.length, offset + this.chunkSize));
          const chunk = storageClass === 'cold'
            ? await this.#writeErasureChunk(key, version, index, content, policy, written)
            : await this.#writeReplicatedChunk(key, version, index, content, policy, written);
          chunks.push(chunk);
        }

        const manifest = {
          key, version, manifestId: randomUUID(), storageClass, size: buffer.length, checksum: sha256(buffer),
          contentType: options.contentType || 'application/octet-stream',
          createdAt: previous?.createdAt ?? now(), updatedAt: now(), tombstone: false, policy, chunks,
        };
        manifest.replicas = this.#manifestNodeIds(manifest);

        await this.locks.withKey('metadata:commit', async () => {
          const priorRevision = this.metadata.revision;
          this.metadata.objects[key] = manifest;
          this.metadata.revision += 1;
          try { await this.#persistMetadata(); }
          catch (error) {
            if (previous) this.metadata.objects[key] = previous;
            else delete this.metadata.objects[key];
            this.metadata.revision = priorRevision;
            throw error;
          }
        });

        if (previous) await this.#removeManifestData(previous);
        this.metrics.writes += 1;
        this.metrics.bytesStored += buffer.length;
        this.#event('write', `Committed ${key}`, `v${version} · ${chunks.length} immutable chunk${chunks.length === 1 ? '' : 's'} · ${storageClass}`);
        return { ...manifest, durability: storageClass === 'cold' ? '10+4 erasure coded' : `${policy.replicationFactor} replicas` };
      } catch (error) {
        await Promise.allSettled(written.map((artifact) => this.#removeArtifact(artifact.node, artifact.path)));
        throw error;
      }
    });
  }

  async getObject(key) {
    const manifest = this.metadata.objects[key];
    if (!manifest || manifest.tombstone) throw new VaultError('Object not found', 404, 'NOT_FOUND');
    const buffers = [];
    for (const chunk of [...manifest.chunks].sort((a, b) => a.index - b.index)) {
      buffers.push(chunk.mode === 'erasure'
        ? await this.#readErasureChunk(manifest, chunk)
        : await this.#readReplicatedChunk(manifest, chunk));
    }
    const buffer = Buffer.concat(buffers).subarray(0, manifest.size);
    if (sha256(buffer) !== manifest.checksum) throw new VaultError('Object checksum mismatch', 503, 'DATA_UNAVAILABLE');
    this.metrics.reads += 1;
    this.#event('read', `Retrieved ${key}`, `v${manifest.version} verified from ${manifest.storageClass} storage`);
    return { buffer, record: manifest };
  }

  listObjects() {
    return Object.values(this.metadata.objects).filter((manifest) => !manifest.tombstone)
      .map((manifest) => ({ ...manifest, replicas: this.#manifestNodeIds(manifest), health: this.#objectHealth(manifest) }))
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
  }

  clusterStatus() {
    const objects = this.listObjects();
    const nodes = [...this.nodes.values()].map((node) => ({
      id: node.id, zone: node.zone, rack: node.rack, region: node.region, status: node.status,
      usedBytes: node.usedBytes, latencyMs: node.latencyMs,
      replicas: objects.filter((manifest) => manifest.replicas.includes(node.id)).length,
    }));
    return {
      revision: this.metadata.revision,
      health: nodes.filter((node) => node.status === 'healthy').length >= this.defaultPolicy.writeQuorum ? 'operational' : 'degraded',
      storage: { mode: this.storageMode, managedProcesses: this.manageStorageNodes, processCount: this.storageNodeProcesses.length },
      policy: this.defaultPolicy, metadataControlPlane: this.controlPlane.status(), repairScheduler: this.repairScheduler.status(),
      nodes, objectCount: objects.length, healthyObjects: objects.filter((item) => item.health === 'healthy').length,
      degradedObjects: objects.filter((item) => item.health !== 'healthy').length,
      logicalBytes: objects.reduce((sum, item) => sum + item.size, 0), physicalBytes: nodes.reduce((sum, node) => sum + node.usedBytes, 0),
      metrics: this.metrics, events: this.events.slice(0, 20),
    };
  }

  setNodeStatus(nodeId, status) {
    const node = this.nodes.get(nodeId);
    if (!node) throw new VaultError('Node not found', 404, 'NODE_NOT_FOUND');
    if (!['healthy', 'offline', 'partitioned'].includes(status)) throw new VaultError('Invalid node status', 400, 'INVALID_STATUS');
    if (status === 'healthy' && this.storageMode === 'http' && this.manageStorageNodes) this.#spawnStorageNode(node);
    if (status !== 'healthy' && this.storageMode === 'http' && this.manageStorageNodes) this.#killStorageNode(node);
    node.status = status;
    this.#event(status === 'healthy' ? 'recovery' : 'failure', `${nodeId} is ${status}`, `${node.zone} · ${node.rack}`);
    if (status === 'healthy') this.integrityScan().catch(() => {});
    return node;
  }

  crashStorageNode(nodeId) {
    const node = this.nodes.get(nodeId);
    if (!node) throw new VaultError('Node not found', 404, 'NODE_NOT_FOUND');
    this.#killStorageNode(node);
    node.status = 'partitioned';
    this.#event('failure', `${nodeId} process crashed`, `${node.zone} · HTTP storage endpoint unavailable`);
    return node;
  }

  async corruptReplica(key, nodeId) {
    const manifest = this.metadata.objects[key];
    const node = this.nodes.get(nodeId);
    if (!manifest || !node) throw new VaultError('Replica not found', 404, 'REPLICA_NOT_FOUND');
    for (const chunk of manifest.chunks) {
      const fragment = chunk.mode === 'erasure' ? chunk.fragments.find((item) => item.nodeId === nodeId) : null;
      if (fragment) {
        await this.#overwriteArtifact(node, this.#artifactPath(node, key, manifest.version, chunk.index, fragment.index), Buffer.from('CORRUPTED_FRAGMENT'));
        this.#event('corruption', 'Fragment checksum mismatch injected', `${key} · chunk ${chunk.index} · ${nodeId}`);
        return { key, nodeId, chunk: chunk.index };
      }
      if (chunk.replicas?.includes(nodeId)) {
        await this.#overwriteArtifact(node, this.#artifactPath(node, key, manifest.version, chunk.index), Buffer.from('CORRUPTED_REPLICA'));
        this.#event('corruption', 'Replica checksum mismatch injected', `${key} · chunk ${chunk.index} · ${nodeId}`);
        return { key, nodeId, chunk: chunk.index };
      }
    }
    throw new VaultError('Replica not found on selected node', 404, 'REPLICA_NOT_FOUND');
  }

  async deleteObject(key, retentionMs = 24 * 60 * 60 * 1000) {
    const manifest = this.metadata.objects[key];
    if (!manifest || manifest.tombstone) throw new VaultError('Object not found', 404, 'NOT_FOUND');
    await this.locks.withKey('metadata:commit', async () => {
      manifest.tombstone = true;
      manifest.deletedAt = now();
      manifest.purgeAfter = new Date(Date.now() + Math.max(0, retentionMs)).toISOString();
      this.metadata.revision += 1;
      await this.#persistMetadata();
    });
    this.#event('delete', `Tombstoned ${key}`, `Retention until ${manifest.purgeAfter}`);
    return { key, purgeAfter: manifest.purgeAfter };
  }

  async garbageCollect() {
    const due = Object.values(this.metadata.objects).filter((manifest) => manifest.tombstone && new Date(manifest.purgeAfter).getTime() <= Date.now());
    for (const manifest of due) {
      await this.#removeManifestData(manifest);
      delete this.metadata.objects[manifest.key];
    }
    if (due.length) {
      this.metadata.revision += 1;
      await this.#persistMetadata();
    }
    this.#event('delete', 'Garbage collection completed', `${due.length} tombstoned object${due.length === 1 ? '' : 's'} purged`);
    return { purged: due.length };
  }

  async integrityScan() {
    const started = performance.now();
    const report = { checked: 0, repaired: 0, degraded: 0, unavailable: 0 };
    for (const manifest of Object.values(this.metadata.objects).filter((item) => !item.tombstone)) {
      for (const chunk of manifest.chunks) {
        report.checked += 1;
        const result = chunk.mode === 'erasure'
          ? await this.#inspectErasureChunk(manifest, chunk)
          : await this.#inspectReplicatedChunk(manifest, chunk);
        if (result.unavailable) { report.unavailable += 1; continue; }
        if (result.degraded) {
          report.degraded += 1;
          report.repaired += await this.repairScheduler.schedule(result.repair, { priority: result.deficit, label: `${manifest.key}:${chunk.index}` });
        }
      }
    }
    this.metrics.lastRepairMs = Math.round(performance.now() - started);
    this.#event('scan', 'Integrity scrub completed', `${report.checked} chunks checked · ${report.repaired} fragments repaired`);
    return { ...report, durationMs: this.metrics.lastRepairMs };
  }

  async rebalance(maxMoves = 8) {
    let moved = 0;
    const manifests = Object.values(this.metadata.objects).filter((item) => !item.tombstone).sort((a, b) => b.size - a.size);
    for (const manifest of manifests) {
      for (const chunk of manifest.chunks) {
        if (moved >= maxMoves || chunk.mode === 'erasure') continue;
        const valid = await this.#validReplicas(manifest, chunk);
        if (!valid.length) continue;
        const desired = this.placement.select(this.#writableNodes(), manifest.policy.replicationFactor).map((node) => node.id);
        for (const nodeId of desired.filter((id) => !chunk.replicas.includes(id))) {
          if (moved >= maxMoves) break;
          const node = this.nodes.get(nodeId);
          await this.#writeArtifact(node, this.#artifactPath(node, manifest.key, manifest.version, chunk.index), valid[0].buffer);
          const verification = await readFile(this.#artifactPath(node, manifest.key, manifest.version, chunk.index));
          if (sha256(verification) !== chunk.hash) throw new VaultError('Rebalance verification failed', 500, 'VERIFY_FAILED');
          chunk.replicas.push(nodeId);
          const removable = chunk.replicas.find((id) => !desired.includes(id) && id !== nodeId);
          if (removable) {
            await this.#removeArtifact(this.nodes.get(removable), this.#artifactPath(this.nodes.get(removable), manifest.key, manifest.version, chunk.index));
            chunk.replicas = chunk.replicas.filter((id) => id !== removable);
          }
          moved += 1;
        }
      }
      manifest.replicas = this.#manifestNodeIds(manifest);
    }
    if (moved) { this.metadata.revision += 1; await this.#persistMetadata(); }
    this.#event('rebalance', 'Rate-limited rebalance completed', `${moved}/${maxMoves} permitted moves completed`);
    return { moved, limit: maxMoves };
  }

  async #writeReplicatedChunk(key, version, index, content, policy, written) {
    const targets = this.placement.select(this.#writableNodes(), policy.replicationFactor);
    if (targets.length < policy.writeQuorum) throw new VaultError('Write quorum unavailable', 503, 'QUORUM_UNAVAILABLE');
    const successes = [];
    await Promise.all(targets.map(async (node) => {
      const artifactPath = this.#artifactPath(node, key, version, index);
      try { await this.#writeArtifact(node, artifactPath, content); successes.push(node.id); written.push({ node, path: artifactPath }); }
      catch { this.metrics.failures += 1; }
    }));
    if (successes.length < policy.writeQuorum) throw new VaultError('Chunk write quorum failed', 503, 'WRITE_QUORUM_FAILED');
    const zones = new Set(successes.map((id) => this.nodes.get(id).zone));
    if (zones.size < Math.min(3, policy.replicationFactor)) throw new VaultError('Durability requires independent failure zones', 503, 'ZONE_DURABILITY_FAILED');
    return { id: `${version}:${index}`, index, offset: index * this.chunkSize, size: content.length, hash: sha256(content), mode: 'replicated', replicas: successes };
  }

  async #writeErasureChunk(key, version, index, content, policy, written) {
    const targets = this.placement.select(this.#writableNodes(), policy.dataShards + policy.parityShards);
    if (targets.length < policy.dataShards + policy.parityShards) throw new VaultError('10+4 erasure coding requires 14 healthy nodes', 503, 'EC_PLACEMENT_FAILED');
    const encoded = this.erasureCodec.encode(content);
    const fragments = [];
    await Promise.all(encoded.shards.map(async (buffer, fragmentIndex) => {
      const node = targets[fragmentIndex];
      const artifactPath = this.#artifactPath(node, key, version, index, fragmentIndex);
      try {
        await this.#writeArtifact(node, artifactPath, buffer);
        fragments.push({ index: fragmentIndex, nodeId: node.id, hash: sha256(buffer), size: buffer.length });
        written.push({ node, path: artifactPath });
      } catch { this.metrics.failures += 1; }
    }));
    if (fragments.length < policy.ecWriteQuorum) throw new VaultError('Erasure durability quorum failed', 503, 'EC_QUORUM_FAILED');
    if (new Set(fragments.map((item) => this.nodes.get(item.nodeId).zone)).size < 3) throw new VaultError('Erasure fragments must span three zones', 503, 'ZONE_DURABILITY_FAILED');
    return { id: `${version}:${index}`, index, offset: index * this.chunkSize, size: content.length, hash: sha256(content), mode: 'erasure', dataShards: policy.dataShards, parityShards: policy.parityShards, shardSize: encoded.shardSize, fragments: fragments.sort((a, b) => a.index - b.index) };
  }

  async #readReplicatedChunk(manifest, chunk) {
    let read = await this.#readReplicatedChunkOnce(manifest, chunk);
    if ((!read.quorumAvailable || !read.valid.length) && this.storageMode === 'http' && this.manageStorageNodes) {
      await Promise.all(chunk.replicas.map((nodeId) => this.#recoverManagedNode(nodeId)));
      read = await this.#readReplicatedChunkOnce(manifest, chunk);
    }
    if (!read.quorumAvailable || !read.valid.length) throw new VaultError('No valid chunk replica available', 503, 'DATA_UNAVAILABLE');
    if (read.damaged.length || read.valid.length < manifest.policy.replicationFactor) {
      const invalidIds = read.damaged.map((entry) => entry.node.id);
      this.repairScheduler.schedule(() => this.#repairReplicatedChunk(manifest, chunk, read.valid[0].buffer, invalidIds), { priority: manifest.policy.replicationFactor - read.valid.length, label: `${manifest.key}:${chunk.index}` }).catch(() => {});
    }
    return read.valid.sort((a, b) => a.node.latencyMs - b.node.latencyMs)[0].buffer;
  }

  async #readReplicatedChunkOnce(manifest, chunk) {
    return this.readCoordinator.read({
      record: { ...manifest, checksum: chunk.hash, replicas: chunk.replicas, policy: { ...manifest.policy, readQuorum: 1 } },
      nodes: this.nodes,
      replicaPath: (node) => this.#artifactPath(node, manifest.key, manifest.version, chunk.index),
      readReplica: (node) => this.#readArtifact(node, this.#artifactPath(node, manifest.key, manifest.version, chunk.index)),
    });
  }

  async #readErasureChunk(manifest, chunk) {
    let available = await this.#validFragments(manifest, chunk);
    if (available.length < chunk.dataShards && this.storageMode === 'http' && this.manageStorageNodes) {
      await Promise.all(chunk.fragments.map((fragment) => this.#recoverManagedNode(fragment.nodeId)));
      available = await this.#validFragments(manifest, chunk);
    }
    if (available.length < chunk.dataShards) throw new VaultError('Not enough valid erasure fragments', 503, 'DATA_UNAVAILABLE');
    const content = this.erasureCodec.decode(available, chunk.size);
    if (sha256(content) !== chunk.hash) throw new VaultError('Decoded chunk checksum mismatch', 503, 'DATA_UNAVAILABLE');
    if (available.length < chunk.dataShards + chunk.parityShards) {
      this.repairScheduler.schedule(() => this.#repairErasureChunk(manifest, chunk, content, available), { priority: chunk.dataShards + chunk.parityShards - available.length, label: `${manifest.key}:${chunk.index}` }).catch(() => {});
    }
    return content;
  }

  async #inspectReplicatedChunk(manifest, chunk) {
    const valid = await this.#validReplicas(manifest, chunk);
    if (!valid.length) return { unavailable: true };
    const validIds = valid.map((item) => item.node.id);
    const invalidIds = chunk.replicas.filter((id) => this.nodes.get(id)?.status === 'healthy' && !validIds.includes(id));
    const deficit = Math.max(0, manifest.policy.replicationFactor - valid.length);
    return { unavailable: false, degraded: deficit > 0 || invalidIds.length > 0, deficit: deficit + invalidIds.length, repair: () => this.#repairReplicatedChunk(manifest, chunk, valid[0].buffer, invalidIds) };
  }

  async #inspectErasureChunk(manifest, chunk) {
    const valid = await this.#validFragments(manifest, chunk);
    if (valid.length < chunk.dataShards) return { unavailable: true };
    const target = chunk.dataShards + chunk.parityShards;
    return { unavailable: false, degraded: valid.length < target, deficit: target - valid.length, repair: async () => this.#repairErasureChunk(manifest, chunk, this.erasureCodec.decode(valid, chunk.size), valid) };
  }

  async #validReplicas(manifest, chunk) {
    const results = await Promise.all(chunk.replicas.map(async (id) => {
      const node = this.nodes.get(id);
      if (!node || node.status !== 'healthy') return null;
      try {
        const buffer = await this.#readArtifact(node, this.#artifactPath(node, manifest.key, manifest.version, chunk.index));
        return sha256(buffer) === chunk.hash ? { node, buffer } : null;
      } catch { return null; }
    }));
    return results.filter(Boolean);
  }

  async #validFragments(manifest, chunk) {
    const results = await Promise.all(chunk.fragments.map(async (fragment) => {
      const node = this.nodes.get(fragment.nodeId);
      if (!node || node.status !== 'healthy') return null;
      try {
        const buffer = await this.#readArtifact(node, this.#artifactPath(node, manifest.key, manifest.version, chunk.index, fragment.index));
        return sha256(buffer) === fragment.hash ? { index: fragment.index, node, buffer, fragment } : null;
      } catch { return null; }
    }));
    return results.filter(Boolean).sort((a, b) => a.node.latencyMs - b.node.latencyMs);
  }

  async #repairReplicatedChunk(manifest, chunk, source, invalidIds = []) {
    for (const id of invalidIds) await this.#quarantine(this.nodes.get(id), this.#artifactPath(this.nodes.get(id), manifest.key, manifest.version, chunk.index));
    const valid = await this.#validReplicas(manifest, chunk);
    const validIds = valid.map((item) => item.node.id);
    const targets = this.placement.select(this.#writableNodes().filter((node) => !validIds.includes(node.id)), Math.max(0, manifest.policy.replicationFactor - validIds.length));
    for (const node of targets) await this.#writeArtifact(node, this.#artifactPath(node, manifest.key, manifest.version, chunk.index), source);
    chunk.replicas = [...new Set([...validIds, ...targets.map((node) => node.id)])];
    manifest.replicas = this.#manifestNodeIds(manifest);
    const repaired = targets.length;
    if (repaired || invalidIds.length) await this.#commitRepair(manifest, repaired, `chunk ${chunk.index}`);
    return repaired;
  }

  async #repairErasureChunk(manifest, chunk, source, valid) {
    const encoded = this.erasureCodec.encode(source);
    const validIndices = new Set(valid.map((item) => item.index));
    const usedNodes = new Set(valid.map((item) => item.node.id));
    const missing = Array.from({ length: chunk.dataShards + chunk.parityShards }, (_, index) => index).filter((index) => !validIndices.has(index));
    for (const fragment of chunk.fragments.filter((item) => missing.includes(item.index))) {
      const node = this.nodes.get(fragment.nodeId);
      await this.#quarantine(node, this.#artifactPath(node, manifest.key, manifest.version, chunk.index, fragment.index));
    }
    const targets = this.placement.select(this.#writableNodes().filter((node) => !usedNodes.has(node.id)), missing.length);
    for (let index = 0; index < Math.min(missing.length, targets.length); index += 1) {
      const fragmentIndex = missing[index];
      const node = targets[index];
      const buffer = encoded.shards[fragmentIndex];
      await this.#writeArtifact(node, this.#artifactPath(node, manifest.key, manifest.version, chunk.index, fragmentIndex), buffer);
      chunk.fragments = chunk.fragments.filter((fragment) => fragment.index !== fragmentIndex);
      chunk.fragments.push({ index: fragmentIndex, nodeId: node.id, hash: sha256(buffer), size: buffer.length });
    }
    chunk.fragments.sort((a, b) => a.index - b.index);
    manifest.replicas = this.#manifestNodeIds(manifest);
    const repaired = Math.min(missing.length, targets.length);
    if (repaired) await this.#commitRepair(manifest, repaired, `EC chunk ${chunk.index}`);
    return repaired;
  }

  async #commitRepair(manifest, repaired, detail) {
    await this.locks.withKey('metadata:commit', async () => {
      this.metadata.revision += 1;
      await this.#persistMetadata();
    });
    this.metrics.repairs += repaired;
    this.#event('repair', `Repaired ${manifest.key}`, `${detail} · ${repaired} artifact${repaired === 1 ? '' : 's'} restored`);
  }

  async #removeManifestData(manifest) {
    const tasks = [];
    for (const chunk of manifest.chunks ?? []) {
      if (chunk.mode === 'erasure') for (const fragment of chunk.fragments) {
        const node = this.nodes.get(fragment.nodeId);
        if (node) tasks.push(this.#removeArtifact(node, this.#artifactPath(node, manifest.key, manifest.version, chunk.index, fragment.index)));
      }
      else for (const id of chunk.replicas) {
        const node = this.nodes.get(id);
        if (node) tasks.push(this.#removeArtifact(node, this.#artifactPath(node, manifest.key, manifest.version, chunk.index)));
      }
    }
    await Promise.allSettled(tasks);
  }

  async #writeArtifact(node, finalPath, buffer) {
    if (!node || node.status !== 'healthy') throw new Error('Node unavailable');
    if (this.storageMode === 'http') {
      const result = await this.storageClient.put(node, this.#artifactId(finalPath), buffer);
      node.usedBytes += result.bytes ?? buffer.length;
      return;
    }
    const temporary = `${finalPath}.${randomUUID()}.tmp`;
    await writeFile(temporary, buffer);
    await rename(temporary, finalPath);
    node.usedBytes += buffer.length;
  }

  async #overwriteArtifact(node, finalPath, buffer) {
    if (this.storageMode === 'http') {
      await this.storageClient.put(node, this.#artifactId(finalPath), buffer);
      return;
    }
    await writeFile(finalPath, buffer);
  }

  async #readArtifact(node, artifactPath) {
    if (this.storageMode === 'http') return this.storageClient.get(node, this.#artifactId(artifactPath));
    return readFile(artifactPath);
  }

  async #removeArtifact(node, artifactPath) {
    if (!node) return;
    if (this.storageMode === 'http') {
      try {
        const result = await this.storageClient.delete(node, this.#artifactId(artifactPath));
        node.usedBytes = Math.max(0, node.usedBytes - (result.removedBytes ?? 0));
      } catch {}
      return;
    }
    try {
      const info = await stat(artifactPath);
      await rm(artifactPath, { force: true });
      node.usedBytes = Math.max(0, node.usedBytes - info.size);
    } catch {}
  }

  async #quarantine(node, artifactPath) {
    if (!node) return;
    if (this.storageMode === 'http') {
      try {
        const result = await this.storageClient.quarantine(node, this.#artifactId(artifactPath));
        node.usedBytes = Math.max(0, node.usedBytes - (result.removedBytes ?? 0));
      } catch {}
      return;
    }
    try {
      const quarantineDir = path.join(node.path, 'quarantine');
      await mkdir(quarantineDir, { recursive: true });
      try { node.usedBytes = Math.max(0, node.usedBytes - (await stat(artifactPath)).size); } catch {}
      await rename(artifactPath, path.join(quarantineDir, `${path.basename(artifactPath)}.${Date.now()}.bad`));
    } catch {}
  }

  async #startManagedStorageNodes() {
    for (const node of this.nodes.values()) {
      this.#spawnStorageNode(node);
    }
    for (let attempt = 0; attempt < 25; attempt += 1) {
      const results = await Promise.all([...this.nodes.values()].map((node) => this.storageClient.health(node)));
      if (results.every(Boolean)) return;
      await new Promise((resolve) => setTimeout(resolve, 120));
    }
    throw new VaultError('Managed storage nodes failed to start', 503, 'STORAGE_BOOT_FAILED');
  }

  #spawnStorageNode(node) {
    const index = Number(node.id.split('-').at(-1)) - 1;
    const existing = this.storageNodeProcesses[index];
    if (existing?.exitCode === null && !existing.killed) return existing;
    const storageServer = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'storage', 'storage-node-server.js');
    const port = Number(new URL(node.endpoint).port);
    const child = spawn(process.execPath, [storageServer, `--id=${node.id}`, `--port=${port}`, `--zone=${node.zone}`, `--rack=${node.rack}`, `--dir=${node.path}`], {
      stdio: 'ignore',
      windowsHide: true,
    });
    this.storageNodeProcesses[index] = child;
    return child;
  }

  #killStorageNode(node) {
    const index = Number(node.id.split('-').at(-1)) - 1;
    const child = this.storageNodeProcesses[index];
    if (child?.exitCode === null && !child.killed) child.kill('SIGTERM');
  }

  async #recoverManagedNode(nodeId) {
    const node = this.nodes.get(nodeId);
    if (!node || this.storageMode !== 'http' || !this.manageStorageNodes) return node;
    if (node.status !== 'healthy') {
      this.#spawnStorageNode(node);
      await this.waitForNodeHealthy(nodeId, 12);
    }
    return node;
  }

  #artifactId(artifactPath) { return path.basename(artifactPath); }

  #artifactPath(node, key, version, chunkIndex, fragmentIndex = null) {
    const suffix = fragmentIndex === null ? 'replica' : `fragment-${fragmentIndex}`;
    return path.join(node.path, `${safeKey(key)}.v${version}.c${chunkIndex}.${suffix}.blob`);
  }

  #manifestNodeIds(manifest) {
    return [...new Set(manifest.chunks.flatMap((chunk) => chunk.mode === 'erasure' ? chunk.fragments.map((item) => item.nodeId) : chunk.replicas))];
  }

  #objectHealth(manifest) {
    let degraded = false;
    for (const chunk of manifest.chunks) {
      const available = chunk.mode === 'erasure'
        ? chunk.fragments.filter((item) => this.nodes.get(item.nodeId)?.status === 'healthy').length
        : chunk.replicas.filter((id) => this.nodes.get(id)?.status === 'healthy').length;
      const minimum = chunk.mode === 'erasure' ? chunk.dataShards : 1;
      const target = chunk.mode === 'erasure' ? chunk.dataShards + chunk.parityShards : manifest.policy.replicationFactor;
      if (available < minimum) return 'unavailable';
      if (available < target) degraded = true;
    }
    return degraded ? 'degraded' : 'healthy';
  }

  #policy(input = {}) {
    const policy = { ...this.defaultPolicy, ...input };
    for (const key of ['replicationFactor', 'readQuorum', 'writeQuorum', 'dataShards', 'parityShards', 'ecWriteQuorum']) policy[key] = Number(policy[key]);
    if (policy.replicationFactor < 1 || policy.replicationFactor > this.nodes.size) throw new VaultError('Invalid replication factor', 400, 'INVALID_POLICY');
    if (policy.readQuorum < 1 || policy.writeQuorum < 1 || policy.readQuorum > policy.replicationFactor || policy.writeQuorum > policy.replicationFactor) throw new VaultError('Invalid quorum policy', 400, 'INVALID_POLICY');
    if (policy.readQuorum + policy.writeQuorum <= policy.replicationFactor) throw new VaultError('R + W must exceed replication factor', 400, 'UNSAFE_POLICY');
    return policy;
  }

  #validateObject(key, buffer) {
    if (!key || typeof key !== 'string' || key.length > 240) throw new VaultError('Object key must be 1-240 characters', 400, 'INVALID_KEY');
    if (!Buffer.isBuffer(buffer) || buffer.length === 0) throw new VaultError('Object content cannot be empty', 400, 'EMPTY_OBJECT');
  }

  #writableNodes() { return [...this.nodes.values()].filter((node) => node.status === 'healthy'); }
  async #persistMetadata() { await this.controlPlane.commit(this.metadata); }

  async #recalculateUsage() {
    for (const node of this.nodes.values()) node.usedBytes = 0;
    for (const manifest of Object.values(this.metadata.objects)) for (const chunk of manifest.chunks ?? []) {
      const artifacts = chunk.mode === 'erasure' ? chunk.fragments.map((item) => [item.nodeId, item.index]) : chunk.replicas.map((id) => [id, null]);
      for (const [id, fragmentIndex] of artifacts) {
        const node = this.nodes.get(id);
        if (!node) continue;
        try { node.usedBytes += (await stat(this.#artifactPath(node, manifest.key, manifest.version, chunk.index, fragmentIndex))).size; } catch {}
      }
    }
  }

  #event(type, title, detail) {
    this.events.unshift({ id: randomUUID(), type, title, detail, at: now() });
    this.events = this.events.slice(0, 100);
  }
}
