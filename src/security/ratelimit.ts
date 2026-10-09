/**
 * Two-layer rate limiting — per member/device + endpoint class.
 *
 * Layer 1 — token bucket (Redis Lua, atomic): burst 120, refill 2/sec per
 *   device identity. Continuous — no window boundaries, so the fixed-window
 *   boundary double-burst (60 in the last second of window N + 60 in the
 *   first second of window N+1) is impossible.
 * Layer 2 — fixed window (same Redis EVAL, atomic): per-class sustained
 *   rate guard, kept alongside the bucket.
 *
 * Limits (v1.2 — 2026-10-09 429 flood fix):
 *   /v1/tmdb/*   : 200 req/min per member
 *   /v1/stream/* : 120 req/min per member (was 60 — too tight; normal
 *                   browsing + player re-resolve hits it)
 *   /v1/auth/*   :  10 req/min per IP (brute-force protection)
 *   default      : 120 req/min per member
 *
 * NOTE (v1.2): stream/tmdb/default limits raised to match the token-bucket
 * burst headroom (120 burst + 2/s refill). Fixed-window was the effective
 * cap and blocked legitimate users.
 *
 * NOTE: in-memory per instance (serverless). The Redis combined check is
 * tried FIRST (shared across api1/api2); the in-memory bucket is the
 * fallback when Redis is unavailable (degrades to best-effort).
 */
import { redisEnabled, redisRateCheck, redisCommand } from "./redis.js";

/** Device-wide burst bucket (P1 2026-10-09). */
const TB_CAPACITY = 120;
const TB_REFILL_PER_SEC = 2;

interface Bucket {
  tokens: number;
  lastRefill: number;
}

const buckets = new Map<string, Bucket>();

function bucketTake(
  key: string,
  capacity: number,
  refillPerSec: number
): { ok: boolean; remaining: number; resetSec: number; retryAfterSec: number } {
  const now = Date.now();
  let b = buckets.get(key);
  if (!b) {
    b = { tokens: capacity, lastRefill: now };
    buckets.set(key, b);
  }
  // refill
  const elapsed = (now - b.lastRefill) / 1000;
  b.tokens = Math.min(capacity, b.tokens + elapsed * refillPerSec);
  b.lastRefill = now;
  if (b.tokens < 1) {
    return {
      ok: false,
      remaining: 0,
      resetSec: (capacity - b.tokens) / refillPerSec,
      retryAfterSec: (1 - b.tokens) / refillPerSec,
    };
  }
  b.tokens -= 1;
  return {
    ok: true,
    remaining: b.tokens,
    resetSec: (capacity - b.tokens) / refillPerSec,
    retryAfterSec: 0,
  };
}

// periodic cleanup to bound memory
let lastCleanup = Date.now();
function maybeCleanup(): void {
  const now = Date.now();
  if (now - lastCleanup < 60_000) return;
  lastCleanup = now;
  for (const [k, b] of buckets) {
    if (now - b.lastRefill > 120_000) buckets.delete(k);
  }
  if (buckets.size > 20000) {
    // emergency trim: drop oldest-ish (Map preserves insertion order)
    const drop = buckets.size - 20000;
    let i = 0;
    for (const k of buckets.keys()) {
      if (i++ >= drop) break;
      buckets.delete(k);
    }
  }
}

export interface RateLimit {
  allowed: boolean;
  retryAfterSec?: number;
  /** RateLimit-Limit / Remaining / Reset header values. */
  limit?: number;
  remaining?: number;
  resetSec?: number;
}

/**
 * Global backstop (P1-7, 2026-10-09): per-identity buckets stop one bad
 * actor, but a leaked shared key (or a botnet) spreads across identities —
 * the per-identity limits never see the aggregate. This is the
 * cluster-wide circuit breaker: ~3000 requests/minute across ALL
 * identities on the cluster (Redis-shared, api1+api2 each have their own
 * window key — a "cluster" here is one Vercel project). Trips fail-closed
 * with 429 for everyone except nothing (health checks are public and
 * unaffected — this sits inside checkRateLimit, which only runs on gated
 * routes). Fail-open when Redis is unavailable.
 */
const BACKSTOP_LIMIT_PER_MIN = 3000;

export async function checkGlobalBackstop(): Promise<{ tripped: boolean; count: number }> {
  if (!redisEnabled()) return { tripped: false, count: 0 };
  const window = Math.floor(Date.now() / 60_000);
  const key = `pf:backstop:${window}`;
  try {
    const n = await redisCommand(["INCR", key]);
    if (n === 1 || n === "1") await redisCommand(["EXPIRE", key, 90]).catch(() => null);
    const count = Number(n);
    return { tripped: Number.isFinite(count) && count > BACKSTOP_LIMIT_PER_MIN, count: Number.isFinite(count) ? count : 0 };
  } catch {
    return { tripped: false, count: 0 };
  }
}

/** Current backstop window state for /health (read-only — never INCRs). */
export async function globalBackstopState(): Promise<{ limit: number; count: number; tripped: boolean }> {
  if (!redisEnabled()) return { limit: BACKSTOP_LIMIT_PER_MIN, count: 0, tripped: false };
  try {
    const window = Math.floor(Date.now() / 60_000);
    const n = Number(await redisCommand(["GET", `pf:backstop:${window}`]).catch(() => null));
    const count = Number.isFinite(n) ? n : 0;
    return { limit: BACKSTOP_LIMIT_PER_MIN, count, tripped: count > BACKSTOP_LIMIT_PER_MIN };
  } catch {
    return { limit: BACKSTOP_LIMIT_PER_MIN, count: 0, tripped: false };
  }
}

/** Seconds until the current fixed window rolls over. */
function windowRetryAfterSec(): number {
  const nowS = Math.floor(Date.now() / 1000);
  return Math.max(1, 60 - (nowS % 60));
}

export async function checkRateLimit(pathname: string, identity: string): Promise<RateLimit> {
  // P1-7: global backstop FIRST — a leaked shared key spreads across
  // identities, so per-identity buckets alone can't see the aggregate.
  const backstop = await checkGlobalBackstop();
  if (backstop.tripped) {
    return {
      allowed: false,
      retryAfterSec: windowRetryAfterSec(),
      limit: BACKSTOP_LIMIT_PER_MIN,
      remaining: 0,
      resetSec: windowRetryAfterSec(),
    };
  }
  let capacity: number;
  let cls: string;
  if (pathname.startsWith("/v1/stream/")) {
    capacity = 120;
    cls = "stream";
  } else if (pathname.startsWith("/v1/tmdb/")) {
    capacity = 200;
    cls = "tmdb";
  } else if (pathname.startsWith("/v1/auth/")) {
    capacity = 10;
    cls = "auth";
  } else {
    capacity = 120;
    cls = "default";
  }
  // Redis: token bucket + fixed window in ONE atomic EVAL (shared across
  // instances). Preferred path.
  if (redisEnabled()) {
    const window = Math.floor(Date.now() / 60_000);
    const rc = await redisRateCheck(
      `pf:tb:${identity}`,
      TB_CAPACITY,
      TB_REFILL_PER_SEC,
      `pf:rl:${cls}:${identity}:${window}`,
      capacity,
      120
    );
    if (rc) {
      const base = {
        limit: TB_CAPACITY,
        remaining: Math.max(0, Math.floor(rc.tbRemaining)),
        resetSec: Math.max(0, Math.ceil(rc.tbResetSec)),
      };
      if (!rc.tbAllowed) {
        return {
          allowed: false,
          retryAfterSec: Math.max(1, Math.ceil(rc.tbRetrySec)),
          ...base,
          remaining: 0,
        };
      }
      if (!rc.fwAllowed) {
        return { allowed: false, retryAfterSec: windowRetryAfterSec(), ...base, remaining: 0 };
      }
      return { allowed: true, ...base };
    }
    // null = Redis failed → fall through to in-memory bucket.
  }
  // In-memory token bucket (per-instance fallback).
  maybeCleanup();
  const key = `${pathname.split("/").slice(0, 4).join("/")}:${identity}`;
  const b = bucketTake(key, capacity, capacity / 60);
  if (!b.ok) {
    return {
      allowed: false,
      retryAfterSec: Math.max(1, Math.ceil(b.retryAfterSec)),
      limit: capacity,
      remaining: 0,
      resetSec: Math.max(0, Math.ceil(b.resetSec)),
    };
  }
  return {
    allowed: true,
    limit: capacity,
    remaining: Math.floor(b.remaining),
    resetSec: Math.max(0, Math.ceil(b.resetSec)),
  };
}

export function rateLimitStats(): { buckets: number } {
  return { buckets: buckets.size };
}
