/**
 * Token-bucket rate limiting — per member + endpoint class.
 *
 * Limits (v1.1 — 2026-10-08 429 fix):
 *   /v1/tmdb/*   : 100 req/min per member
 *   /v1/stream/* :  60 req/min per member (was 20 — too low; home load fires
 *                   20-30 checkPlayable calls via /v1/stream/movie/*)
 *   /v1/auth/*   :  10 req/min per IP (brute-force protection)
 *   default      :  60 req/min per member
 *
 * NOTE: in-memory per instance (serverless). Redis-backed fixed-window
 * counters are tried FIRST (shared across api1/api2); the in-memory bucket
 * is the fallback when Redis is unavailable (degrades to best-effort).
 */
import { redisEnabled, redisFixedWindow } from "./redis.js";

interface Bucket {
  tokens: number;
  lastRefill: number;
}

const buckets = new Map<string, Bucket>();

function bucketFor(key: string, capacity: number, refillPerSec: number): boolean {
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
  if (b.tokens < 1) return false; // rate limited
  b.tokens -= 1;
  return true;
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
}

export async function checkRateLimit(pathname: string, identity: string): Promise<RateLimit> {
  let capacity: number;
  let perMin: number;
  let cls: string;
  if (pathname.startsWith("/v1/stream/")) {
    capacity = 60;
    perMin = 60;
    cls = "stream";
  } else if (pathname.startsWith("/v1/tmdb/")) {
    capacity = 100;
    perMin = 100;
    cls = "tmdb";
  } else if (pathname.startsWith("/v1/auth/")) {
    capacity = 10;
    perMin = 10;
    cls = "auth";
  } else {
    capacity = 60;
    perMin = 60;
    cls = "default";
  }
  // Redis fixed-window (shared across instances) — preferred path.
  if (redisEnabled()) {
    const window = Math.floor(Date.now() / 60_000);
    const key = `pf:rl:${cls}:${identity}:${window}`;
    const allowed = await redisFixedWindow(key, capacity, 120);
    if (allowed === true) return { allowed: true };
    if (allowed === false) {
      return { allowed: false, retryAfterSec: Math.ceil(60 / perMin) };
    }
    // null = Redis failed → fall through to in-memory bucket.
  }
  // In-memory token bucket (per-instance fallback).
  maybeCleanup();
  const key = `${pathname.split("/").slice(0, 4).join("/")}:${identity}`;
  const ok = bucketFor(key, capacity, perMin / 60);
  if (!ok) {
    return { allowed: false, retryAfterSec: Math.ceil(60 / perMin) };
  }
  return { allowed: true };
}

export function rateLimitStats(): { buckets: number } {
  return { buckets: buckets.size };
}
