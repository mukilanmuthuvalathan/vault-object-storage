export class RepairScheduler {
  constructor({ concurrency = 2 } = {}) {
    this.concurrency = concurrency;
    this.active = 0;
    this.queue = [];
    this.completed = 0;
  }

  schedule(operation, { priority = 0, label = 'repair' } = {}) {
    return new Promise((resolve, reject) => {
      this.queue.push({ operation, priority, label, resolve, reject, queuedAt: Date.now() });
      this.queue.sort((left, right) => right.priority - left.priority || left.queuedAt - right.queuedAt);
      this.#drain();
    });
  }

  status() {
    return { queued: this.queue.length, active: this.active, completed: this.completed, concurrency: this.concurrency };
  }

  #drain() {
    while (this.active < this.concurrency && this.queue.length) {
      const job = this.queue.shift();
      this.active += 1;
      Promise.resolve().then(job.operation).then((result) => {
        this.completed += 1;
        job.resolve(result);
      }, job.reject).finally(() => {
        this.active -= 1;
        this.#drain();
      });
    }
  }
}
