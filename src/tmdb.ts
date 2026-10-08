/**
 * TMDB proxy with aggressive caching.
 *
 * TMDB rate limit is PER-IP (40 req / 10s), shared across all Vercel
 * customers on the same egress IPs. Caching is the ONLY real mitigation.
 *
 * TTLs (from final plan v1.0):
 *   trending/home : 6h  (+ 7d stale)
 *   details       : 24h (+ 7d stale)
 *   search        : 1h  (+ 24h stale)
 *   recommendations ("more like this"): 24h (+ 7d stale)
 *
 * If TMDB fails and stale cache exists -> serve stale (never blank).
 */
import { cacheGet, cacheSet } from "./cache.js";

const TMDB = "https://api.themoviedb.org/3";

function key(): string {
  const k = process.env.TMDB_API_KEY;
  if (!k) throw new Error("TMDB_API_KEY not configured");
  return k;
}

export interface TmdbFetchOpts {
  cacheKey: string;
  ttlMs: number;
  staleMs: number;
}

async function tmdbGet(path: string, params: Record<string, string>, opts: TmdbFetchOpts): Promise<unknown> {
  const cached = cacheGet<unknown>(opts.cacheKey);
  if (cached && !cached.stale) return cached.value;

  const url = new URL(TMDB + path);
  url.searchParams.set("api_key", key());
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);

  try {
    const res = await fetch(url.toString(), {
      headers: { "User-Agent": "PrimeFlix/1.0" },
      signal: AbortSignal.timeout(8000),
    });
    if (res.status === 429) throw new Error("TMDB rate limited (HTTP 429)");
    if (!res.ok) throw new Error(`TMDB HTTP ${res.status}`);
    const data = await res.json();
    cacheSet(opts.cacheKey, data, opts.ttlMs, opts.staleMs);
    return data;
  } catch (e) {
    // Serve stale on failure (C4: never blank screen)
    if (cached) return cached.value;
    throw e;
  }
}

const H = 3600_000;
const D = 24 * H;

export const tmdb = {
  trendingMovie: (timeWindow = "day") =>
    tmdbGet("/trending/movie/" + timeWindow, { language: "en-US" }, {
      cacheKey: `tmdb:trending:movie:${timeWindow}`, ttlMs: 6 * H, staleMs: 7 * D,
    }),
  trendingTv: (timeWindow = "day") =>
    tmdbGet("/trending/tv/" + timeWindow, { language: "en-US" }, {
      cacheKey: `tmdb:trending:tv:${timeWindow}`, ttlMs: 6 * H, staleMs: 7 * D,
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
};

/** Edge cache headers matching the TTLs above (seconds). */
export function edgeCacheHeaders(ttlMs: number, staleMs: number): Record<string, string> {
  return {
    "Cache-Control": `public, s-maxage=${Math.floor(ttlMs / 1000)}, stale-while-revalidate=${Math.floor(staleMs / 1000)}`,
  };
}

export const TTL = { trending: 6 * H, details: 24 * H, search: 1 * H, stale7d: 7 * D, stale1d: 1 * D };
