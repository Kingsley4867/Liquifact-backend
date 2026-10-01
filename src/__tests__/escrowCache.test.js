'use strict';

const {
  RedisEscrowSummaryCache,
} = require('../cache/redis');
const { CircuitBreaker, CircuitBreakerState } = require('../utils/circuitBreaker');

/**
 * Minimal in-memory Redis stub used for integration-style cache tests.
 */
class FakeRedisClient {
  constructor() {
    this.map = new Map();
  }

  async get(key) {
    return this.map.get(key) || null;
  }

  async set(key, value, _mode, _ttl) {
    this.map.set(key, value);
    return 'OK';
  }

  async del(key) {
    this.map.delete(key);
    return 1;
  }

  async eval(script, numberOfKeys, ...values) {
    const keys = values.slice(0, numberOfKeys);
    const args = values.slice(numberOfKeys);
    if (script.includes('redis-cache:read')) {
      return [this.map.get(keys[0]) ?? null, this.map.get(keys[1]) ?? '0'];
    }
    if (script.includes('redis-cache:write')) {
      this.map.set(keys[0], args[1]);
      return 1;
    }
    if (script.includes('redis-cache:compare-delete')) {
      if (this.map.get(keys[0]) === args[0]) {
        this.map.delete(keys[0]);
        return 1;
      }
      return 0;
    }
    if (script.includes('redis-cache:invalidate')) {
      const generation = Number(this.map.get(keys[0]) ?? '0') + 1;
      this.map.set(keys[0], String(generation));
      this.map.delete(keys[1]);
      return generation;
    }
    throw new Error('Unsupported Redis script');
  }
}

describe('Escrow Cache Integration', () => {
  it('serves cached response on second request for same invoiceId', async () => {
    const client = new FakeRedisClient();
    const cache = new RedisEscrowSummaryCache({ client, ttlSeconds: 60 });

    // First call — miss.
    const miss = await cache.getSummary('inv_100');
    expect(miss.hit).toBe(false);
    expect(miss.reason).toBe('miss');

    // Populate the cache.
    const summary = { invoiceId: 'inv_100', status: 'funded', fundedAmount: 500 };
    await cache.setSummary('inv_100', summary, 200);

    // Second call — hit.
    const hit = await cache.getSummary('inv_100', 201);
    expect(hit.hit).toBe(true);
    expect(hit.value).toEqual(summary);
  });

  it('caches different invoiceIds independently', async () => {
    const client = new FakeRedisClient();
    const cache = new RedisEscrowSummaryCache({ client, ttlSeconds: 60 });

    await cache.setSummary('inv_200', { invoiceId: 'inv_200', status: 'a' }, 100);
    await cache.setSummary('inv_300', { invoiceId: 'inv_300', status: 'b' }, 100);

    const r1 = await cache.getSummary('inv_200', 101);
    expect(r1.hit).toBe(true);
    expect(r1.value.invoiceId).toBe('inv_200');

    const r2 = await cache.getSummary('inv_300', 101);
    expect(r2.hit).toBe(true);
    expect(r2.value.invoiceId).toBe('inv_300');
  });

  it('fails open when a summary cannot be serialized', async () => {
    const client = new FakeRedisClient();
    const cache = new RedisEscrowSummaryCache({ client });
    const summary = {};
    summary.self = summary;

    await expect(cache.setSummary('inv_circular', summary)).resolves.toBe(false);
    await expect(cache.setSummary('inv_missing', undefined)).resolves.toBe(false);
    expect(client.map.size).toBe(0);
  });

  it('fails open for a cached entry without a summary field', async () => {
    const client = new FakeRedisClient();
    const cache = new RedisEscrowSummaryCache({ client });
    client.map.set('escrow:summary:inv_malformed', JSON.stringify({ cachedLedger: 10 }));

    await expect(cache.getSummary('inv_malformed')).resolves.toEqual({
      hit: false,
      reason: 'fail_open',
    });
  });

  it('simulated Redis timeout fails open and falls through', async () => {
    const slowClient = {
      get: () => new Promise((resolve) => setTimeout(() => resolve('data'), 5000)),
      set: () => new Promise((resolve) => setTimeout(() => resolve('OK'), 5000)),
      del: () => Promise.resolve(1),
      eval: (script) => new Promise((resolve) => setTimeout(() => {
        resolve(script.includes('redis-cache:read') ? [null, '0'] : 1);
      }, 5000)),
    };

    const cache = new RedisEscrowSummaryCache({
      client: slowClient,
      ttlSeconds: 30,
      timeoutMs: 50,
    });

    // getSummary should not throw — it should return a miss.
    const getResult = await cache.getSummary('inv_timeout');
    expect(getResult.hit).toBe(false);
    expect(getResult.reason).toBe('fail_open');

    // setSummary should not throw — it should return false.
    const setResult = await cache.setSummary('inv_timeout', { status: 'funded' });
    expect(setResult).toBe(false);
  });

  it('falls through when circuit breaker trips open', async () => {
    const client = new FakeRedisClient();
    const breaker = new CircuitBreaker({
      failureThreshold: 1,
      recoveryTimeout: 60000,
      fallbackLogic: () => null,
    });

    // Force breaker to OPEN state.
    breaker.state = CircuitBreakerState.OPEN;
    breaker.nextAttemptTime = Date.now() + 60000;

    const cache = new RedisEscrowSummaryCache({
      client,
      ttlSeconds: 30,
      circuitBreaker: breaker,
    });

    // Even though the underlying client is healthy, the breaker is open
    // so the cache should degrade silently.
    const result = await cache.getSummary('inv_breaker');
    expect(result.hit).toBe(false);

    const setResult = await cache.setSummary('inv_breaker', { status: 'funded' });
    expect(setResult).toBe(false);
  });
});
