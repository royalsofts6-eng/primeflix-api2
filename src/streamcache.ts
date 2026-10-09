/**
 * Stream resolution cache (P1 — 2026-10-09): Redis-shared,
 * stale-while-revalidate, request coalescing.
 *
 * Every successful resolveStream() result is cached under
 *   pf:stream:{type}:{tmdbId}:{season}:{episode}:{audio}
 * with provider-specific fresh TTLs:
 *   VidLink 2h · VidZee 30min · FZMovies 10h (existing) · default 30min
 * plus a 15-minute stale-while-revalidate window.
 *
 * Read path: Redis (shared api1+api2) → in-memory (per-instance) → live.
 *
 * Stale hit: served instantly + fire-and-forget background revalidate.
 *   Best-effort on serverless — the invocation may freeze after the
 *   response ends, so a revalidate is not guaranteed to complete. The
 *   bounded 15min stale window guarantees a sync re-resolve at worst, and
 *   the nightly /v1/cron/stream-warm keeps hot titles fresh.
 *
 * Coalescing: concurrent in-flight resolutions for the same key collapse
 * into one upstream resolve — in-memory promise map per instance, plus a
 * Redis lock with bounded polling cross-instance (thundering herd → 1
 * upstream resolve; waiters share the winner's result).
 *
 * Only successes are cached. Errors ("all providers failed",
 * "hindi dubbed not available") are NEVER cached.
 */
import { redisEnabled, redisCacheGet, redisCacheSet, redisCommand } from "./security/redis.js";
import { cacheGet, cacheSet } from "./cache.js";
import type { ChainResult } from "./chain.js";

// Provider-specific fresh TTLs (seconds). Ordered by the CEO 2026-10-09.
const TTL_BY_PROVIDER: Record<string, number> = {
  vidlink: 2 * 3600,
  vidzee: 30 * 60,
  fzmovies: 10 * 3600,
  vaplayer: 3600,
};
const DEFAULT_TTL_S = 30 * 60;
/** Stale-while-revalidate window (seconds) — bounded. */
const STALE_WINDOW_S = 15 * 60;
/** How long a waiter polls Redis for another instance's result. */
const COALESCE_POLL_MS = 8000;
const COALESCE_POLL_INTERVAL_MS = 400;
/** Redis single-flight lock TTL (must exceed the ~8s provider race). */
const LOCK_TTL_S = 25;

// Release the single-flight lock only if we still own it.
const RELEASE_LOCK_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

export interface StreamArgs {
  type: "movie" | "tv";
  tmdbId: string;
  season?: number;
  episode?: number;
  audio?: string;
}

interface StreamCacheEnvelope {
  v: 1;
  result: ChainResult;
  freshUntil: number; // epoch ms
  staleUntil: number; // epoch ms
}

const stats = { hits: 0, misses: 0, stale: 0, coalesced: 0, sets: 0 };

/** Stats for /health. */
export function streamCacheStats(): Record<string, number> {
  return { ...stats, inflight: inflight.size };
}

export function streamCacheKey(
  type: "movie" | "tv",
  tmdbId: string,
  season?: number,
  episode?: number,
  audio?: string
): string {
  return `${type}:${tmdbId}:${season ?? 0}:${episode ?? 0}:${audio || "def"}`;
}

const redisKeyFor = (key: string): string => `pf:stream:${key}`;

// ── Envelope read/write ─────────────────────────────────────────────────────

async function readEnvelope(key: string, rkey: string): Promise<StreamCacheEnvelope | null> {
  // Redis first (shared api1+api2 — one warm benefits both clusters),
  // then per-instance memory.
  if (redisEnabled()) {
    const rhit = await redisCacheGet<StreamCacheEnvelope>(rkey);
    if (rhit && rhit.v === 1 && rhit.result?.qualities?.length) {
      // Backfill local memory with the remaining lifetime.
      const now = Date.now();
      const freshMs = Math.max(0, rhit.freshUntil - now);
      if (freshMs > 0 || now < rhit.staleUntil) {
        cacheSet(key, rhit, freshMs, Math.max(0, rhit.staleUntil - rhit.freshUntil));
      }
      return rhit;
    }
  }
  const hit = cacheGet<StreamCacheEnvelope>(key);
  if (hit && hit.value.v === 1 && hit.value.result?.qualities?.length) return hit.value;
  return null;
}

async function writeEnvelope(key: string, rkey: string, result: ChainResult): Promise<void> {
  if (!result?.qualities?.length) return; // never cache empty/failed results
  const ttlS = TTL_BY_PROVIDER[result.provider] ?? DEFAULT_TTL_S;
  const now = Date.now();
  const env: StreamCacheEnvelope = {
    v: 1,
    result,
    freshUntil: now + ttlS * 1000,
    staleUntil: now + (ttlS + STALE_WINDOW_S) * 1000,
  };
  cacheSet(key, env, ttlS * 1000, STALE_WINDOW_S * 1000);
  if (redisEnabled()) {
    await redisCacheSet(rkey, env, ttlS + STALE_WINDOW_S);
  }
  stats.sets++;
}

/** True when a FRESH (non-stale) entry exists — used by the pre-warm cron. */
export async function isStreamCachedFresh(
  type: "movie" | "tv",
  tmdbId: string,
  season?: number,
  episode?: number,
  audio?: string
): Promise<boolean> {
  const key = streamCacheKey(type, tmdbId, season, episode, audio);
  const env = await readEnvelope(key, redisKeyFor(key));
  return !!env && Date.now() < env.freshUntil;
}

// ── Request coalescing (single-flight) ──────────────────────────────────────

// In-flight live resolutions, keyed by cache key (per-instance dedup).
const inflight = new Map<string, Promise<ChainResult>>();

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Wait (bounded) for another instance's in-flight resolution to land in Redis. */
async function pollForResult(rkey: string): Promise<ChainResult | null> {
  const start = Date.now();
  while (Date.now() - start < COALESCE_POLL_MS) {
    await sleep(COALESCE_POLL_INTERVAL_MS);
    const env = await redisCacheGet<StreamCacheEnvelope>(rkey);
    if (env && env.v === 1 && env.result?.qualities?.length && Date.now() < env.freshUntil) {
      return env.result;
    }
  }
  return null;
}

async function resolveLiveSingleflight(
  key: string,
  rkey: string,
  live: () => Promise<ChainResult>
): Promise<ChainResult> {
  // Same-instance: share the in-flight promise.
  const ongoing = inflight.get(key);
  if (ongoing) {
    stats.coalesced++;
    return ongoing;
  }

  // Cross-instance: Redis lock. Whoever holds it resolves; the rest poll
  // for the cached result instead of stampeding the providers.
  const lockKey = `pf:sflock:${key}`;
  let lockToken: string | null = null;
  if (redisEnabled()) {
    lockToken = `${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`;
    const got = await redisCommand(["SET", lockKey, lockToken, "NX", "EX", LOCK_TTL_S]);
    if (got !== "OK") {
      lockToken = null;
      const hit = await pollForResult(rkey);
      if (hit) {
        stats.coalesced++;
        return hit;
      }
      // Timed out waiting — fail open and resolve ourselves (still caches,
      // so the next waiter gets a hit). Rare; bounded by the poll above.
    }
  }

  const p = (async (): Promise<ChainResult> => {
    try {
      const r = await live();
      await writeEnvelope(key, rkey, r);
      return r;
    } finally {
      if (lockToken) {
        await redisCommand(["EVAL", RELEASE_LOCK_SCRIPT, 1, lockKey, lockToken]);
      }
    }
  })();
  inflight.set(key, p);
  try {
    return await p;
  } finally {
    inflight.delete(key);
  }
}

/** Fire-and-forget revalidation for stale hits (never throws). */
function revalidateSoon(
  key: string,
  rkey: string,
  live: () => Promise<ChainResult>
): void {
  void (async (): Promise<void> => {
    try {
      // Goes through the same single-flight path, so concurrent stale
      // requests trigger exactly one background re-resolve per instance.
      await resolveLiveSingleflight(key, rkey, live);
    } catch {
      // Keep serving stale — the bounded stale window guarantees a sync
      // re-resolve at worst, and the nightly cron refreshes hot titles.
    }
  })();
}

// ── Public entry point ──────────────────────────────────────────────────────

/**
 * Cached stream resolution. `live` is the real provider chain — called at
 * most once per cache key per TTL window (coalesced), never on cache hits.
 */
export async function resolveStreamCached(
  args: StreamArgs,
  live: () => Promise<ChainResult>
): Promise<ChainResult> {
  const key = streamCacheKey(args.type, args.tmdbId, args.season, args.episode, args.audio);
  const rkey = redisKeyFor(key);
  const env = await readEnvelope(key, rkey);
  const now = Date.now();
  if (env) {
    if (now < env.freshUntil) {
      stats.hits++;
      return env.result;
    }
    if (now < env.staleUntil) {
      stats.stale++;
      revalidateSoon(key, rkey, live);
      return env.result;
    }
  }
  stats.misses++;
  return resolveLiveSingleflight(key, rkey, live);
}
