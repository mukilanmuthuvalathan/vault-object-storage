export class SlidingWindowRateLimiter {
  constructor({ limit = 180, windowMs = 60_000 } = {}) {
    this.limit = limit;
    this.windowMs = windowMs;
    this.clients = new Map();
  }

  consume(clientId) {
    const current = Date.now();
    const cutoff = current - this.windowMs;
    const requests = (this.clients.get(clientId) ?? []).filter((timestamp) => timestamp > cutoff);
    if (requests.length >= this.limit) return { allowed: false, remaining: 0, retryAfterMs: requests[0] + this.windowMs - current };
    requests.push(current);
    this.clients.set(clientId, requests);
    return { allowed: true, remaining: this.limit - requests.length, retryAfterMs: 0 };
  }
}
