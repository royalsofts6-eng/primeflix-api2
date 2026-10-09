/**
 * TMDB proxy with aggressive caching.
 *
 * TMDB rate limit is PER-IP (40 req / 10s), shared across all Vercel
 * customers on the same egress IPs. Caching is the ONLY real mitigation.
 *
 * Two cache layers: in-memory L1 (per instance) + Redis L2 `pf:tmdb:*`
 * (shared api1+api2 — cold invocations read the shared cache instead of
 * stampeding TMDB; this was the warm-cron cold-start stampede).
 *
 * TTLs (from final plan v1.0):
 *   trending/home : 6h  (+ 7d stale)
 *   details       : 24h (+ 7d stale)
 *   search        : 1h  (+ 24h stale)
 *   recommendations ("more like this"): 24h (+ 7d stale)
 *
 * If TMDB fails and stale cache exists -> serve stale (never blank).
 */
import { cacheGet, cacheSet, ck } from "./cache.js";
import { redisEnabled, redisCacheGet, redisCacheSet, redisCommand } from "./security/redis.js";

const TMDB = "https://api.themoviedb.org/3";

function key(): string {
  const k = process.env.TMDB_API_KEY;
  if (!k) throw new Error("TMDB_API_KEY not configured");
  return k;
}

// ── TMDB 429 backoff (P1-6, 2026-10-09) ──────────────────────────────────────
// TMDB is 40 req/10s per egress IP, shared across ALL Vercel customers on
// the same IPs — a cold-instance miss storm used to stretch a 429 into a
// long outage because every instance kept retrying. Now:
//   (a) cluster-wide cooldown: the first 429 sets pf:cooldown:tmdb (60s) in
//       Redis — every instance (api1 + api2) skips live TMDB fetches and
//       serves stale instead, and
//   (b) per-instance exponential backoff: 5s, 10s, 20s, 40s, 60s cap —
//       reset on the first success. (Instances are short-lived, so this is
//       the local fast path; the Redis key is the cross-instance truth.)
// Fail-open: Redis down -> local backoff only.
const TMDB_COOLDOWN_KEY = "pf:cooldown:tmdb";
const TMDB_COOLDOWN_S = 60;
let tmdbBackoffStep = 0;
let tmdbBackoffUntil = 0;

/** True while any instance (or this one) is backing off TMDB. For /health. */
export async function tmdbCoolingDown(): Promise<boolean> {
  if (Date.now() < tmdbBackoffUntil) return true;
  if (!redisEnabled()) return false;
  try {
    return (await redisCommand(["GET", TMDB_COOLDOWN_KEY]).catch(() => null)) !== null;
  } catch {
    return false;
  }
}

async function tmdbNote429(): Promise<void> {
  tmdbBackoffStep = Math.min(tmdbBackoffStep + 1, 4);
  tmdbBackoffUntil = Date.now() + Math.min(60_000, 5_000 * 2 ** (tmdbBackoffStep - 1));
  if (redisEnabled()) {
    await redisCommand(["SET", TMDB_COOLDOWN_KEY, "1", "EX", TMDB_COOLDOWN_S]).catch(() => {});
  }
}

function tmdbNoteSuccess(): void {
  tmdbBackoffStep = 0;
  tmdbBackoffUntil = 0;
}

/**
 * Serve stale (never blank) — memory L1 first, then the shared Redis copy.
 * Returns the stale payload, or null when nothing stale exists.
 */
async function tmdbServeStale(cacheKey: string): Promise<unknown | null> {
  const stale = cacheGet<unknown>(cacheKey);
  if (stale) return stale.value;
  if (redisEnabled()) {
    // Phase D (2026-10-09): versioned Redis key pf:v3:tmdb:* (memory key stays unversioned — instance-local).
    const rhit = await redisCacheGet<unknown>(ck("tmdb", cacheKey)).catch(() => null);
    if (rhit) return rhit;
  }
  return null;
}

export interface TmdbFetchOpts {
  cacheKey: string;
  ttlMs: number;
  staleMs: number;
}

async function tmdbGet(path: string, params: Record<string, string>, opts: TmdbFetchOpts): Promise<unknown> {
  const cached = cacheGet<unknown>(opts.cacheKey);
  if (cached && !cached.stale) return cached.value;

  // Redis L2 (shared api1+api2): a cold instance reads the shared entry
  // instead of hitting TMDB. Backfills memory L1 on hit.
  const rkey = ck("tmdb", opts.cacheKey);
  if (redisEnabled()) {
    const rhit = await redisCacheGet<unknown>(rkey).catch(() => null);
    if (rhit) {
      cacheSet(opts.cacheKey, rhit, opts.ttlMs, opts.staleMs);
      return rhit;
    }
  }

  // P1-6 (2026-10-09): cluster-wide 429 cooldown — while ANY instance is
  // backing off, skip the live fetch entirely and serve stale (never
  // blank). This stops a cold-instance miss storm from stretching a 429
  // into a long outage.
  if (await tmdbCoolingDown()) {
    const stale = await tmdbServeStale(opts.cacheKey);
    if (stale !== null) return stale;
    throw new Error("TMDB rate limited (cooldown)");
  }

  // Single-flight: concurrent requests for the same cache key share ONE
  // upstream fetch. TMDB is 40 req/10s per IP shared across all Vercel
  // customers on the same egress IPs — a stampede after TTL expiry used to
  // multiply upstream calls N× and cause the 429s surfaced as "Server busy".
  const ongoing = inflight.get(opts.cacheKey);
  if (ongoing) {
    try {
      return await ongoing;
    } catch {
      // The owner will serve stale-or-throw below; fall through to our own fetch.
    }
  }

  const p = (async (): Promise<unknown> => {
    const url = new URL(TMDB + path);
    // NOTE (2026-10-08): the key MUST stay a ?api_key= query param. TMDB v3
    // API keys do not work as Authorization: Bearer (that needs a v4 read
    // access token) — Bearer was tried and returned HTTP 401 on every call.
    // Exposure is limited to TLS-encrypted server→TMDB transit.
    url.searchParams.set("api_key", key());
    for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);

    try {
      const res = await fetch(url.toString(), {
        headers: { "User-Agent": "PrimeFlix/1.0" },
        signal: AbortSignal.timeout(8000),
      });
      if (res.status === 429) {
        // P1-6: record the 429 BEFORE the stale fallback below — the first
        // 429 arms the cluster-wide cooldown so the next fetch (any
        // instance) skips upstream instead of re-hitting TMDB.
        await tmdbNote429();
        throw new Error("TMDB rate limited (HTTP 429)");
      }
      if (!res.ok) throw new Error(`TMDB HTTP ${res.status}`);
      const data = await res.json();
      tmdbNoteSuccess();
      cacheSet(opts.cacheKey, data, opts.ttlMs, opts.staleMs);
      // Fan out to Redis so every instance (and both clusters) shares it.
      if (redisEnabled()) {
        await redisCacheSet(rkey, data, Math.floor(opts.ttlMs / 1000)).catch(() => {});
      }
      return data;
    } catch (e) {
      // Serve stale on failure (C4: never blank screen) — memory first,
      // then the shared Redis copy.
      const stale = await tmdbServeStale(opts.cacheKey);
      if (stale !== null) return stale;
      throw e;
    } finally {
      inflight.delete(opts.cacheKey);
    }
  })();
  inflight.set(opts.cacheKey, p);
  return p;
}

// In-flight upstream fetches, keyed by cache key (single-flight dedup).
const inflight = new Map<string, Promise<unknown>>();

const H = 3600_000;
const D = 24 * H;

export const tmdb = {
  trendingMovie: (timeWindow = "day", page = "1") =>
    tmdbGet("/trending/movie/" + timeWindow, { language: "en-US", page }, {
      cacheKey: `tmdb:trending:movie:${timeWindow}:${page}`, ttlMs: 6 * H, staleMs: 7 * D,
    }),
  trendingTv: (timeWindow = "day", page = "1") =>
    tmdbGet("/trending/tv/" + timeWindow, { language: "en-US", page }, {
      cacheKey: `tmdb:trending:tv:${timeWindow}:${page}`, ttlMs: 6 * H, staleMs: 7 * D,
    }),
  movie: (id: string) =>
    tmdbGet(`/movie/${id}`, { language: "en-US", append_to_response: "credits,videos" }, {
      cacheKey: `tmdb:movie:${id}`, ttlMs: 24 * H, staleMs: 7 * D,
    }),
  tv: (id: string) =>
    tmdbGet(`/tv/${id}`, { language: "en-US", append_to_response: "credits,videos" }, {
      cacheKey: `tmdb:tv:${id}`, ttlMs: 24 * H, staleMs: 7 * D,
    }),
  tvSeason: (id: string, season: number) =>
    tmdbGet(`/tv/${id}/season/${season}`, { language: "en-US" }, {
      cacheKey: `tmdb:tv:${id}:s${season}`, ttlMs: 24 * H, staleMs: 7 * D,
    }),
  search: (query: string, page = "1") =>
    tmdbGet("/search/multi", { language: "en-US", query, page, include_adult: "false" }, {
      cacheKey: `tmdb:search:${query.toLowerCase()}:${page}`, ttlMs: 1 * H, staleMs: 1 * D,
    }),
  movieRecs: (id: string) =>
    tmdbGet(`/movie/${id}/recommendations`, { language: "en-US", page: "1" }, {
      cacheKey: `tmdb:movie:${id}:recs`, ttlMs: 24 * H, staleMs: 7 * D,
    }),
  tvRecs: (id: string) =>
    tmdbGet(`/tv/${id}/recommendations`, { language: "en-US", page: "1" }, {
      cacheKey: `tmdb:tv:${id}:recs`, ttlMs: 24 * H, staleMs: 7 * D,
    }),
  /**
   * Discover movies by genre/language/country (Ali 2026-10-08: new Home rails —
   * Hollywood, Bollywood, South Indian, Comedy, Adventure, Horror).
   * Params are allowlisted at the route level; cache key is sorted for stability.
   */
  discoverMovie: (params: Record<string, string>) => {
    const sorted = Object.keys(params).sort().map((k) => `${k}=${params[k]}`).join("&");
    return tmdbGet(
      "/discover/movie",
      { language: "en-US", include_adult: "false", sort_by: "popularity.desc", ...params },
      { cacheKey: `tmdb:discover:${sorted}`, ttlMs: 12 * H, staleMs: 7 * D }
    );
  },
  /** Upcoming theatrical releases (Ali 2026-10-08: "Upcoming" home rail). */
  upcomingMovies: (region = "US", page = "1") =>
    tmdbGet("/movie/upcoming", { language: "en-US", region, page }, {
      cacheKey: `tmdb:upcoming:${region}:${page}`, ttlMs: 12 * H, staleMs: 7 * D,
    }),
};

/** Edge cache headers matching the TTLs above (seconds). */
export function edgeCacheHeaders(ttlMs: number, staleMs: number): Record<string, string> {
  return {
    "Cache-Control": `public, s-maxage=${Math.floor(ttlMs / 1000)}, stale-while-revalidate=${Math.floor(staleMs / 1000)}`,
  };
}

export const TTL = { trending: 6 * H, details: 24 * H, search: 1 * H, discover: 12 * H, stale7d: 7 * D, stale1d: 1 * D };
