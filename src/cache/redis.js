'use strict';

const { randomUUID } = require('crypto');
const { CircuitBreaker } = require('../utils/circuitBreaker');
const { redisCacheFailOpenTotal } = require('../metrics');

const DEFAULT_TTL_SECONDS = 30;
const MIN_TTL_SECONDS = 5;
const MAX_TTL_SECONDS = 300;

const DEFAULT_LEDGER_GAP_THRESHOLD = 3;
const MAX_LEDGER_GAP_THRESHOLD = 1000;

let redis;
try {
  redis = require('redis');
} catch (_e) {
  redis = null;
}

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
let redisClient = null;
let isRedisConnected = false;

if (redis && (process.env.NODE_ENV !== 'test' || process.env.USE_REDIS_TEST === 'true')) {
  redisClient = redis.createClient({ url: REDIS_URL });

  redisClient.on('connect', () => {
    isRedisConnected = true;
    console.log('Redis client linked securely.');
  });

  redisClient.on('error', (err) => {
    isRedisConnected = false;
    console.warn('Redis connection degraded or broken:', err.message);
  });

  redisClient.connect().catch((err) => {
    console.warn('Initial Redis connection handshake failed:', err.message);
  });
}

/**
 * Returns the active Redis client context along with its real-time health availability flag.
 *
 * Used by [`src/middleware/rateLimit.js`](../middleware/rateLimit.js) to share
 * the cache-layer Redis client for distributed counters when the operator has
 * not passed an explicit `redisClient` to createRateLimiter(...).
 *
 * @returns {{client: object|null, isAvailable: boolean}} Active client + liveness.
 */
function getRedisClient() {
  return { client: redisClient, isAvailable: isRedisConnected };
}
const DEFAULT_TIMEOUT_MS = 500;
const MIN_TIMEOUT_MS = 50;
const MAX_TIMEOUT_MS = 5000;

const READ_CACHE_SCRIPT = `-- redis-cache:read
local generation = redis.call('GET', KEYS[2])
if not generation then
  generation = ARGV[1]
  redis.call('SET', KEYS[2], generation, 'PX', ARGV[2])
else
  redis.call('PEXPIRE', KEYS[2], ARGV[2])
end
return { redis.call('GET', KEYS[1]) or false, generation }
`;
const WRITE_CACHE_SCRIPT = `-- redis-cache:write
local generation = redis.call('GET', KEYS[2]) or '0'
if ARGV[1] ~= '*' and generation ~= ARGV[1] then return 0 end
local existing = redis.call('GET', KEYS[1])
local incomingLedger = tonumber(ARGV[4])
if existing and incomingLedger then
  local ok, decoded = pcall(cjson.decode, existing)
  if ok and type(decoded) == 'table' and type(decoded.cachedLedger) == 'number'
    and decoded.cachedLedger > incomingLedger then
    return 0
  end
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
return 1
`;
const COMPARE_AND_DELETE_SCRIPT = `-- redis-cache:compare-delete
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;
const INVALIDATE_CACHE_SCRIPT = `-- redis-cache:invalidate
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('DEL', KEYS[2])
return 1
`;


/**
 * Parses a raw value into a positive integer within a specified range.
 * @param {any} rawValue The value to parse.
 * @param {number} fallback The fallback value if parsing fails.
 * @param {number} min The minimum allowed value.
 * @param {number} max The maximum allowed value.
 * @returns {number} The parsed integer or fallback.
 */
function parsePositiveInt(rawValue, fallback, min, max) {
  const parsed = Number.parseInt(String(rawValue || ''), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.min(Math.max(parsed, min), max);
}

/**
 * Parses Redis escrow cache configuration from environment variables.
 * @param {Object} env The environment variables object.
 * @returns {Object} The parsed configuration object.
 */
function parseRedisEscrowCacheConfig(env = process.env) {
  const enabled = String(env.REDIS_ESCROW_CACHE_ENABLED || '').toLowerCase() === 'true';
  const redisUrl = env.REDIS_URL || '';

  return {
    enabled: enabled && Boolean(redisUrl),
    redisUrl,
    ttlSeconds: parsePositiveInt(
      env.REDIS_ESCROW_CACHE_TTL_SECONDS,
      DEFAULT_TTL_SECONDS,
      MIN_TTL_SECONDS,
      MAX_TTL_SECONDS
    ),
    ledgerGapThreshold: parsePositiveInt(
      env.REDIS_ESCROW_LEDGER_GAP_THRESHOLD,
      DEFAULT_LEDGER_GAP_THRESHOLD,
      1,
      MAX_LEDGER_GAP_THRESHOLD
    ),
    timeoutMs: parsePositiveInt(
      env.REDIS_ESCROW_CACHE_TIMEOUT_MS,
      DEFAULT_TIMEOUT_MS,
      MIN_TIMEOUT_MS,
      MAX_TIMEOUT_MS
    ),
  };
}

/**
 * Creates a Redis client based on the provided configuration.
 * @param {Object} config The configuration object.
 * @param {Function} [RedisCtor] Optional Redis constructor for testing.
 * @returns {Object|null} The Redis client or null if not enabled.
 */
function createRedisClient(config = parseRedisEscrowCacheConfig(), RedisCtor) {
  if (!config.enabled) {
    return null;
  }

  const Redis = RedisCtor || require('ioredis');
  return new Redis(config.redisUrl, {
    lazyConnect: true,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
  });
}

/**
 * Validates an invoice ID.
 * @param {string} invoiceId The invoice ID to validate.
 * @returns {boolean} True if the invoice ID is valid.
 */
function isValidInvoiceId(invoiceId) {
  return typeof invoiceId === 'string' && /^[a-zA-Z0-9:_-]{1,128}$/.test(invoiceId);
}

/**
 * Races a promise against a timeout. Rejects with a timeout error if the
 * promise does not settle within `ms` milliseconds.
 * @param {Promise<any>} promise The promise to race.
 * @param {number} ms Timeout in milliseconds.
 * @returns {Promise<any>} The result of the promise or a timeout rejection.
 */
function withTimeout(promise, ms) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error('Redis operation timed out'));
    }, ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

class RedisEscrowSummaryCache {
  /**
   * Initializes the RedisEscrowSummaryCache.
   * @param {Object} root0 Configuration object.
   * @param {Object} root0.client The Redis client.
   * @param {number} [root0.ttlSeconds] Time-to-live in seconds.
   * @param {number} [root0.ledgerGapThreshold] Maximum allowed ledger gap.
   * @param {string} [root0.keyPrefix] Prefix for Redis keys.
   * @param {number} [root0.timeoutMs] Per-operation timeout in milliseconds.
   * @param {Object} [root0.circuitBreaker] Optional CircuitBreaker instance for DI.
   */
  constructor({
    client,
    ttlSeconds = DEFAULT_TTL_SECONDS,
    ledgerGapThreshold = DEFAULT_LEDGER_GAP_THRESHOLD,
    keyPrefix = 'escrow:summary',
    timeoutMs = DEFAULT_TIMEOUT_MS,
    circuitBreaker,
  }) {
    this.client = client;
    this.ttlSeconds = ttlSeconds;
    this.ledgerGapThreshold = ledgerGapThreshold;
    this.keyPrefix = keyPrefix;
    this.timeoutMs = timeoutMs;

    /** @type {CircuitBreaker} Shared breaker — falls back to null so callers never see throws. */
    this.circuitBreaker = circuitBreaker || new CircuitBreaker({
      failureThreshold: 5,
      recoveryTimeout: 10000,
      fallbackLogic: () => null,
    });
  }

  /**
   * Generates a Redis key for a given invoice ID.
   * @param {string} invoiceId The invoice ID.
   * @returns {string} The Redis key.
   */
  key(invoiceId) {
    return `${this.keyPrefix}:${invoiceId}`;
  }

  /**
   * Uses a non-user-addressable suffix so generation metadata cannot overlap
   * another valid invoice cache key.
   * @param {string} invoiceId The invoice ID.
   * @returns {string} The Redis generation key.
   */
  generationKey(invoiceId) {
    return `${this.key(invoiceId)}\u0000generation`;
  }

  /**
   * Retrieves an escrow summary from the cache.
   * Wraps the Redis GET in a bounded timeout and circuit breaker.
   * On any Redis/timeout/CB failure, fails open by returning a cache miss
   * so the caller falls through to the DB/RPC layer.
   * @param {string} invoiceId The invoice ID.
   * @param {number} [currentLedger] The current ledger sequence.
   * @returns {Promise<Object>} The cache result including hit status and value.
   */
  async getSummary(invoiceId, currentLedger) {
    if (!this.client || !isValidInvoiceId(invoiceId)) {
      return { hit: false, reason: 'invalid_input' };
    }

    const key = this.key(invoiceId);
    let generation;

    try {
      const result = await this.circuitBreaker.execute(() =>
        withTimeout(
          this.client.eval(
            READ_CACHE_SCRIPT,
            2,
            key,
            this.generationKey(invoiceId),
            randomUUID(),
            String(Math.max(1, this.ttlSeconds * 1000))
          ),
          this.timeoutMs
        )
      );

      // Circuit breaker fallback returns null — treat as fail-open miss.
      if (result === null) {
        return { hit: false, reason: 'miss' };
      }

      if (!Array.isArray(result) || result.length < 2) {
        throw new Error('Invalid Redis escrow summary response');
      }
      const [raw, generationValue] = result;
      generation = String(generationValue);
      const miss = (reason) => ({ hit: false, reason, generation });
      if (raw === null || raw === false) {
        return miss('miss');
      }

      const entry = JSON.parse(raw);
      if (
        entry === null ||
        typeof entry !== 'object' ||
        !Object.prototype.hasOwnProperty.call(entry, 'summary')
      ) {
        throw new Error('Invalid Redis escrow summary entry');
      }
      if (
        Number.isFinite(currentLedger) &&
        Number.isFinite(entry.cachedLedger) &&
        Math.abs(currentLedger - entry.cachedLedger) > this.ledgerGapThreshold
      ) {
        // Best-effort eviction — failures here are non-critical.
        try {
          await withTimeout(
            this.client.eval(COMPARE_AND_DELETE_SCRIPT, 1, key, raw),
            this.timeoutMs
          );
        } catch {
          // Ignore eviction errors; the TTL will handle cleanup.
        }
        return miss('ledger_gap');
      }

      return { hit: true, value: entry.summary, generation };
    } catch {
      // Redis error, timeout, or circuit breaker exception — fail open.
      redisCacheFailOpenTotal.inc();
      return {
        hit: false,
        reason: 'fail_open',
        ...(generation === undefined ? {} : { generation }),
      };
    }
  }

  /**
   * Sets an escrow summary in the cache.
   * Wraps the Redis SET in a bounded timeout and circuit breaker.
   * On any failure, fails open by returning false so the caller
   * proceeds without caching. Never throws.
   * @param {string} invoiceId The invoice ID.
   * @param {Object} summary The summary object to cache.
   * @param {number} [currentLedger] The current ledger sequence.
   * @param {string} [expectedGeneration] Generation returned by getSummary;
   *   stale fetches are rejected after invalidation. Omitted for legacy callers.
   * @returns {Promise<boolean>} True if the summary was successfully cached.
   */
  async setSummary(invoiceId, summary, currentLedger, expectedGeneration) {
    if (!this.client || !isValidInvoiceId(invoiceId) || summary === undefined) {
      return false;
    }

    const hasExpectedGeneration = arguments.length >= 4;
    if (hasExpectedGeneration && typeof expectedGeneration !== 'string') {
      return false;
    }

    const key = this.key(invoiceId);
    try {
      const payload = JSON.stringify({
        summary,
        cachedLedger: Number.isFinite(currentLedger) ? currentLedger : null,
        cachedAt: new Date().toISOString(),
      });
      const result = await this.circuitBreaker.execute(() =>
        withTimeout(
          this.client.eval(
            WRITE_CACHE_SCRIPT,
            2,
            key,
            this.generationKey(invoiceId),
            hasExpectedGeneration ? expectedGeneration : '*',
            payload,
            String(this.ttlSeconds),
            Number.isFinite(currentLedger) ? String(currentLedger) : ''
          ),
          this.timeoutMs
        )
      );
      // Circuit breaker fallback returns null on trip.
      return result === 1 || result === '1';
    } catch {
      // Redis error, timeout, or circuit breaker exception — fail open.
      redisCacheFailOpenTotal.inc();
      return false;
    }
  }

  /**
   * Deletes an invoice summary after a successful escrow write.
   * Failures are non-fatal because callers can still invalidate their local cache.
   * @param {string} invoiceId The invoice ID.
   * @returns {Promise<boolean>} Whether Redis accepted the deletion.
   */
  async deleteSummary(invoiceId) {
    if (!this.client || !isValidInvoiceId(invoiceId)) {
      return false;
    }
    try {
      const result = await this.circuitBreaker.execute(() =>
        withTimeout(
          this.client.eval(
            INVALIDATE_CACHE_SCRIPT,
            2,
            this.generationKey(invoiceId),
            this.key(invoiceId),
            randomUUID(),
            String(Math.max(1, this.ttlSeconds * 1000))
          ),
          this.timeoutMs
        )
      );
      return result !== null && result !== undefined;
    } catch {
      redisCacheFailOpenTotal.inc();
      return false;
    }
  }
}

/**
 * Factory function to create a RedisEscrowSummaryCache instance.
 * @param {Object} [root0] Configuration object.
 * @param {Object} [root0.env] Environment variables.
 * @param {Object} [root0.client] Optional Redis client.
 * @param {Function} [root0.RedisCtor] Optional Redis constructor.
 * @returns {RedisEscrowSummaryCache|null} The cache instance or null.
 */
function createRedisEscrowSummaryCache({ env = process.env, client, RedisCtor } = {}) {
  const config = parseRedisEscrowCacheConfig(env);
  const redisClient = client || createRedisClient(config, RedisCtor);

  if (!redisClient) {
    return null;
  }

  return new RedisEscrowSummaryCache({
    client: redisClient,
    ttlSeconds: config.ttlSeconds,
    ledgerGapThreshold: config.ledgerGapThreshold,
    timeoutMs: config.timeoutMs,
  });
}

module.exports = {
  // Primary public surface — kept at top of exports so existing callers that
  // imported the cache module from an older snapshot continue to work.
  getRedisClient,
  // Cache layer API.
  RedisEscrowSummaryCache,
  createRedisClient,
  createRedisEscrowSummaryCache,
  isValidInvoiceId,
  parseRedisEscrowCacheConfig,
};
