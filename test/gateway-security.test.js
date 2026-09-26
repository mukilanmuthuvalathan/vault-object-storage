import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { hasValidApiKey } from '../src/gateway/auth.js';
import { SlidingWindowRateLimiter } from '../src/gateway/rate-limiter.js';

describe('gateway security controls', () => {
  test('requires an exact bearer token when API-key protection is enabled', () => {
    assert.equal(hasValidApiKey(undefined, undefined), true);
    assert.equal(hasValidApiKey(undefined, 'secret'), false);
    assert.equal(hasValidApiKey('Basic secret', 'secret'), false);
    assert.equal(hasValidApiKey('Bearer wrong', 'secret'), false);
    assert.equal(hasValidApiKey('Bearer secret', 'secret'), true);
  });

  test('enforces a sliding-window request limit independently per client', () => {
    const limiter = new SlidingWindowRateLimiter({ limit: 2, windowMs: 60_000 });
    assert.equal(limiter.consume('client-a').allowed, true);
    assert.equal(limiter.consume('client-a').allowed, true);
    const blocked = limiter.consume('client-a');
    assert.equal(blocked.allowed, false);
    assert.equal(blocked.remaining, 0);
    assert.ok(blocked.retryAfterMs > 0);
    assert.equal(limiter.consume('client-b').allowed, true);
  });
});
