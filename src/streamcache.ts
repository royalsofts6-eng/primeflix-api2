/**
 * Stream resolution cache (P1 — 2026-10-09, envelope v2 2026-10-09): Redis-shared,
 * stale-while-revalidate, request coalescing.
 *
 * Every successful resolveStream() result is cached under
 *   pf:stream:{type}:{tmdbId}:{season}:{episode}:{audio}
 * as envelope v2: { v: 2, result, alternates[<=3], freshUntil, staleUntil }
 * (1 key, 1 read — the app fails over client-side across alternates with
 * zero new backend round-trip).
 *
 * Provider-specific fresh TTLs (seconds):
 *   VidZee 2h (token lifetime 180min verified — the old 30m re-resolved 4x
 *     too often) · VidLink 2h · FZMovies 10h · MovieBox wrapper 1h
 *     (conservative, expiry unverified) · default 30m
 * plus a 15-minute stale-while-revalidate window.
 *
 * Read path: Redis (shared api1+api2) → in-memory (per-instance) → live.
 *
 * Stale hit: served instantly + background revalidate via waitUntil when
 * available (best-effort on serverless — falls back to fire-and-forget).
 *
 * Coalescing: concurrent in-flight resolutions for the same key collapse
 * into one upstream resolve — in-memory promise map per instance, plus a
 * Redis lock with bounded polling cross-instance.
 *
 * Only successes are cached. Errors ("all providers failed",
 * "hindi dubbed not available") are NEVER cached.
 */
import { redisEnabled, redisCacheGet, redisCacheSet, redisCommand } from "./security/redis.js";
import { cacheGet, cacheSet, cacheDel } from "./cache.js";
import { background } from "./revalidate.js";
import { NotAvailableError } from "./providers/types.js";
import type { ChainResult } from "./chain.js";
import type { RankedAlternate } from "./race.js";

// Provider-specific fresh TTLs (seconds). Ordered by the CEO 2026-10-09.
const TTL_BY_PROVIDER: Record<string, number> = {
  vidlink: 2 * 3600,
  vidzee: 2 * 3600,
  fzmovies: 10 * 3600,
  vaplayer: 3600,
  moviebox: 3600,
  "moviebox-hi": 3600,
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
  v: 2;
  /** Winner (alternates stripped — they live in `alternates`). */
  result: ChainResult;
  /** Ranked runner-ups, best-first (max 3) — client-side failover. */
  alternates: RankedAlternate[];
  freshUntil: number; // epoch ms
  staleUntil: number; // epoch ms
}

const stats = { hits: 0, misses: 0, stale: 0, coalesced: 0, sets: 0, neg: 0 };

/** Win #2 (2026-10-09): negative cache — a recent all-sources-miss for this
 * key short-circuits BEFORE the provider race (incl. the MovieBox wrapper
 * leg with its ~15s pacer wait). TTL 30min: a newly added dub is invisible
 * for at most 30min, vs. re-running the full race on every request.
 * Only CONTENT misses are recorded here — transient failures (network,
 * 429, 5xx, deadline) are never negative-cached (outage confusion).
 */
const NEG_TTL_S = 30 * 60;
const NEG_TTL_MS = NEG_TTL_S * 1000;

interface NegEntry {
  v: 1;
  reason: string;
  at: number;
}

const negRedisKey = (key: string): string => `pf:neg:${key}`;
const negMemKey = (key: string): string => `pf:negmem:${key}`;

/** True when the error is a content miss (safe to negative-cache). Never throws. */
function isContentMissError(e: unknown): boolean {
  if (e instanceof NotAvailableError) return true;
  // ChainContentMissError (chain.ts) — duck-typed to avoid a module cycle
  // (chain.ts imports this file at runtime).
  return !!(e && typeof e === "object" && (e as { contentMiss?: unknown }).contentMiss === true);
}

async function negWrite(key: string, reason: string): Promise<void> {
  const entry: NegEntry = { v: 1, reason: reason.slice(0, 200), at: Date.now() };
  cacheSet(negMemKey(key), entry, NEG_TTL_MS, 0);
  if (!redisEnabled()) return;
  try {
    await redisCacheSet(negRedisKey(key), entry, NEG_TTL_S);
  } catch {
    /* best-effort */
  }
}

async function negClear(key: string): Promise<void> {
  cacheDel(negMemKey(key));
  if (!redisEnabled()) return;
  try {
    await redisCommand(["DEL", negRedisKey(key)]);
  } catch {
    /* best-effort */
  }
}

async function negRead(key: string): Promise<NegEntry | null> {
  const mem = cacheGet<NegEntry>(negMemKey(key));
  if (mem && mem.value.v === 1) return mem.value;
  if (!redisEnabled()) return null;
  try {
    const r = await redisCacheGet<NegEntry>(negRedisKey(key));
    if (r && r.v === 1) {
      // Backfill instance memory with the remaining TTL.
      cacheSet(negMemKey(key), r, NEG_TTL_MS, 0);
      return r;
    }
  } catch {
    /* fail-open */
  }
  return null;
}

/**
 * True when a recent attempt already proved this title+audio has no stream
 * (content miss). Used by the prefetch writer to skip dead titles. Never throws.
 */
export async function isNegativelyCached(args: StreamArgs): Promise<boolean> {
  try {
    return (await negRead(streamCacheKey(args.type, args.tmdbId, args.season, args.episode, args.audio))) !== null;
  } catch {
    return false;
  }
}

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
  // then per-instance memory. v1 envelopes are ignored (graceful: they
  // simply re-resolve and are rewritten as v2 — no migration needed).
  if (redisEnabled()) {
    const rhit = await redisCacheGet<StreamCacheEnvelope>(rkey);
    if (rhit && rhit.v === 2 && rhit.result?.qualities?.length) {
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
  if (hit && hit.value.v === 2 && hit.value.result?.qualities?.length) return hit.value;
  return null;
}

async function writeEnvelope(key: string, rkey: string, result: ChainResult): Promise<void> {
  if (!result?.qualities?.length) return; // never cache empty/failed results
  // P1-3 (2026-10-09): the envelope used to inherit the WINNER's TTL — a
  // MovieBox alternate (signed URL, ~1h life) inside a VidZee-winner
  // envelope (2h) could be served expired, and the client-side failover
  // would land on a dead URL. Clamp to the SHORTEST provider lifetime in
  // the envelope (winner + every alternate), so no alternate outlives its
  // URL's real expiry.
  const ttlOf = (p?: string): number => (p && TTL_BY_PROVIDER[p]) || DEFAULT_TTL_S;
  const ttlS = Math.min(
    ttlOf(result.provider),
    ...((result.alternates ?? []).map((a) => ttlOf(a.provider)))
  );
  const now = Date.now();
  const { alternates, ...rest } = result;
  const env: StreamCacheEnvelope = {
    v: 2,
    result: { ...rest, alternates: [] },
    alternates: (alternates ?? []).slice(0, 3),
    freshUntil: now + ttlS * 1000,
    staleUntil: now + (ttlS + STALE_WINDOW_S) * 1000,
  };
  cacheSet(key, env, ttlS * 1000, STALE_WINDOW_S * 1000);
  if (redisEnabled()) {
    await redisCacheSet(rkey, env, ttlS + STALE_WINDOW_S);
  }
  stats.sets++;
  // A success proves the title is servable — drop any stale negativity
  // (e.g. a dub added after the negative entry was written).
  await negClear(key);
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

/**
 * Providers that won (or were ranked alternates in) any cached envelope for
 * a title — default and Hindi audio keys (P1 2026-10-09). Lets /languages
 * answer MovieBox-Hindi from chain state with zero wrapper calls. Never
 * throws (fail-open -> empty set).
 */
export async function cachedStreamProviders(
  type: "movie" | "tv",
  tmdbId: string,
  season?: number,
  episode?: number
): Promise<Set<string>> {
  const out = new Set<string>();
  for (const audio of [undefined, "hi"] as const) {
    try {
      const key = streamCacheKey(type, tmdbId, season, episode, audio);
      const env = await readEnvelope(key, redisKeyFor(key));
      if (!env) continue;
      if (env.result?.provider) out.add(env.result.provider);
      for (const a of env.alternates ?? []) {
        if (a?.provider) out.add(a.provider);
      }
    } catch {
      /* fail-open */
    }
  }
  return out;
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
    if (env && env.v === 2 && env.result?.qualities?.length && Date.now() < env.freshUntil) {
      return { ...env.result, alternates: env.alternates ?? [] };
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

/**
 * Background revalidation for stale hits (never throws). Uses Vercel
 * waitUntil when available so the revalidate actually completes after the
 * response ends; falls back to fire-and-forget otherwise.
 */
function revalidateSoon(
  key: string,
  rkey: string,
  live: () => Promise<ChainResult>
): void {
  background(
    (async (): Promise<void> => {
      try {
        // Goes through the same single-flight path, so concurrent stale
        // requests trigger exactly one background re-resolve per instance.
        await resolveLiveSingleflight(key, rkey, live);
      } catch {
        // Keep serving stale — the bounded stale window guarantees a sync
        // re-resolve at worst, and the cron refreshes hot titles.
      }
    })()
  );
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
  // Win #2: negative cache BEFORE the envelope read, the race, and the
  // pacer — a recent content miss answers honestly in <1s with ONE Redis
  // read instead of re-running the full provider chain.
  const neg = await negRead(key);
  if (neg) {
    stats.neg++;
    const err = new NotAvailableError(`${neg.reason} (checked recently)`);
    (err as unknown as { negCacheHit: boolean }).negCacheHit = true;
    throw err;
  }
  const env = await readEnvelope(key, rkey);
  const now = Date.now();
  if (env) {
    if (now < env.freshUntil) {
      stats.hits++;
      return { ...env.result, alternates: env.alternates ?? [] };
    }
    if (now < env.staleUntil) {
      stats.stale++;
      revalidateSoon(key, rkey, live);
      return { ...env.result, alternates: env.alternates ?? [] };
    }
  }
  stats.misses++;
  try {
    return await resolveLiveSingleflight(key, rkey, live);
  } catch (e) {
    // Record content misses only — transient failures (network, 429, 5xx,
    // deadline) must NEVER look permanent.
    if (isContentMissError(e)) {
      await negWrite(key, e instanceof Error ? e.message : "not available");
    }
    throw e;
  }
}
