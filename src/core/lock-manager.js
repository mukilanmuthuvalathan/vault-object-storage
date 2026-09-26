export class LockManager {
  #tails = new Map();

  async withKey(key, operation) {
    const previous = this.#tails.get(key) ?? Promise.resolve();
    let release;
    const current = new Promise((resolve) => { release = resolve; });
    this.#tails.set(key, current);
    await previous.catch(() => {});
    try {
      return await operation();
    } finally {
      release();
      if (this.#tails.get(key) === current) this.#tails.delete(key);
    }
  }
}
