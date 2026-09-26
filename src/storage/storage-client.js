export class StorageNodeClient {
  constructor({ timeoutMs = 2500 } = {}) {
    this.timeoutMs = timeoutMs;
  }

  async health(node) {
    try {
      const response = await fetch(`${node.endpoint}/health`, { signal: AbortSignal.timeout(this.timeoutMs) });
      return response.ok;
    } catch {
      return false;
    }
  }

  async put(node, artifactId, buffer) {
    const response = await fetch(`${node.endpoint}/artifacts/${encodeURIComponent(artifactId)}`, {
      method: 'PUT',
      body: buffer,
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    return this.#json(response);
  }

  async get(node, artifactId) {
    const response = await fetch(`${node.endpoint}/artifacts/${encodeURIComponent(artifactId)}`, {
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    if (!response.ok) throw new Error(`Storage read failed: ${response.status}`);
    return Buffer.from(await response.arrayBuffer());
  }

  async delete(node, artifactId) {
    const response = await fetch(`${node.endpoint}/artifacts/${encodeURIComponent(artifactId)}`, {
      method: 'DELETE',
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    return this.#json(response);
  }

  async quarantine(node, artifactId) {
    const response = await fetch(`${node.endpoint}/artifacts/${encodeURIComponent(artifactId)}/quarantine`, {
      method: 'POST',
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    return this.#json(response);
  }

  async #json(response) {
    let body = {};
    try { body = await response.json(); } catch {}
    if (!response.ok) throw new Error(body.error || `Storage request failed: ${response.status}`);
    return body;
  }
}
