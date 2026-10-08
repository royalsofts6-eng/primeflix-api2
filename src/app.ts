/**
 * PrimeFlix API — Cluster 1 (Vercel)
 * Uniform contract: { success: true, data } | { success: false, error, code }
 */
import { Hono } from "hono";
import { apiKeyAuth } from "./auth.js";
import {
  pfAuth,
  handleRegister,
  handleRefresh,
  handleRevoke,
  securityStats,
} from "./security/middleware.js";
import { tmdb, TTL, edgeCacheHeaders } from "./tmdb.js";
import { resolveStream, providerHealth } from "./chain.js";
import { cacheStats } from "./cache.js";
import { getChannels, refreshChannels, groupByCategory } from "./livetv.js";
import { getSeries, getEpisodes, getStreamUrl, NIAZI_TTL } from "./niazitv.js";

export const app = new Hono();

const VERSION = "1.1.0";
const CLUSTER = process.env.CLUSTER_NAME || "api1";

// ── Global middleware ───────────────────────────────────────────────────────
// pfAuth: day-1 X-API-Key (backward compat) + HMAC-SHA256 signed requests.
// (Legacy apiKeyAuth kept as import for reference; pfAuth supersedes it.)
app.use("*", pfAuth);
void apiKeyAuth;

// ── Helpers ─────────────────────────────────────────────────────────────────
type Handler = (c: any) => Promise<Response>;

function ok(data: unknown, cacheTtlMs?: number, cacheStaleMs?: number): (c: any) => Response {
  return (c: any) => {
    const headers: Record<string, string> = {};
    if (cacheTtlMs && cacheStaleMs) Object.assign(headers, edgeCacheHeaders(cacheTtlMs, cacheStaleMs));
    return c.json({ success: true, data }, 200, headers);
  };
}

function wrap(fn: Handler): Handler {
  return async (c: any) => {
    try {
      return await fn(c);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      const status = msg.includes("TMDB rate limited") ? 429 : msg.includes("all providers") ? 502 : 500;
      return c.json({ success: false, error: msg, code: status === 429 ? "TMDB_RATE_LIMIT" : "UPSTREAM_ERROR" }, status);
    }
  };
}

const num = (v: string | undefined, d: number): number => {
  const n = parseInt(v || "", 10);
  return Number.isFinite(n) && n > 0 ? n : d;
};

// ── Public ──────────────────────────────────────────────────────────────────
app.get("/", (c) =>
  c.json({
    name: "PrimeFlix API",
    cluster: CLUSTER,
    version: VERSION,
    endpoints: [
      "GET /health",
      "GET /v1/tmdb/trending/movie?time_window=day",
      "GET /v1/tmdb/trending/tv?time_window=day",
      "GET /v1/tmdb/movie/:id",
      "GET /v1/tmdb/tv/:id",
      "GET /v1/tmdb/tv/:id/season/:season",
      "GET /v1/tmdb/search/multi?query=&page=",
      "GET /v1/tmdb/movie/:id/recommendations",
      "GET /v1/tmdb/tv/:id/recommendations",
      "GET /v1/stream/movie/:tmdbId",
      "GET /v1/stream/tv/:tmdbId/:season/:episode",
      "GET /v1/livetv/channels",
      "GET /v1/niazi/series",
      "GET /v1/niazi/series/:id/episodes",
      "GET /v1/niazi/stream/:serieId/:episodeId",
      "POST /v1/auth/register",
      "POST /v1/auth/refresh",
      "POST /v1/auth/revoke",
    ],
  })
);

// ── Auth (HMAC-SHA256 + JWT 24h) ────────────────────────────────────────────
app.post("/v1/auth/register", handleRegister);
app.post("/v1/auth/refresh", handleRefresh);
app.post("/v1/auth/revoke", handleRevoke);

app.get("/health", (c) =>
  c.json({
    ok: true,
    cluster: CLUSTER,
    version: VERSION,
    tmdbKeyConfigured: !!process.env.TMDB_API_KEY,
    providers: providerHealth(),
    cache: cacheStats(),
    security: securityStats(),
  })
);

// ── TMDB proxy ──────────────────────────────────────────────────────────────
app.get("/v1/tmdb/trending/movie", wrap(async (c) => {
  const data = await tmdb.trendingMovie(c.req.query("time_window") || "day");
  return ok(data, TTL.trending, TTL.stale7d)(c);
}));

app.get("/v1/tmdb/trending/tv", wrap(async (c) => {
  const data = await tmdb.trendingTv(c.req.query("time_window") || "day");
  return ok(data, TTL.trending, TTL.stale7d)(c);
}));

app.get("/v1/tmdb/movie/:id", wrap(async (c) => {
  const data = await tmdb.movie(c.req.param("id"));
  return ok(data, TTL.details, TTL.stale7d)(c);
}));

app.get("/v1/tmdb/tv/:id", wrap(async (c) => {
  const data = await tmdb.tv(c.req.param("id"));
  return ok(data, TTL.details, TTL.stale7d)(c);
}));

app.get("/v1/tmdb/tv/:id/season/:season", wrap(async (c) => {
  const data = await tmdb.tvSeason(c.req.param("id"), num(c.req.param("season"), 1));
  return ok(data, TTL.details, TTL.stale7d)(c);
}));

app.get("/v1/tmdb/search/multi", wrap(async (c) => {
  const q = c.req.query("query") || "";
  if (q.length < 2) return c.json({ success: false, error: "query too short", code: "BAD_QUERY" }, 400);
  const data = await tmdb.search(q, c.req.query("page") || "1");
  return ok(data, TTL.search, TTL.stale1d)(c);
}));

app.get("/v1/tmdb/movie/:id/recommendations", wrap(async (c) => {
  const data = await tmdb.movieRecs(c.req.param("id"));
  return ok(data, TTL.details, TTL.stale7d)(c);
}));

app.get("/v1/tmdb/tv/:id/recommendations", wrap(async (c) => {
  const data = await tmdb.tvRecs(c.req.param("id"));
  return ok(data, TTL.details, TTL.stale7d)(c);
}));

// ── Stream resolution ───────────────────────────────────────────────────────
// NOTE: stream URLs are signed/time-limited — NEVER cache these responses.
app.get("/v1/stream/movie/:tmdbId", wrap(async (c) => {
  const data = await resolveStream(c.req.param("tmdbId"), "movie");
  return c.json({ success: true, data }, 200, { "Cache-Control": "no-store" });
}));

app.get("/v1/stream/tv/:tmdbId/:season/:episode", wrap(async (c) => {
  const data = await resolveStream(
    c.req.param("tmdbId"),
    "tv",
    num(c.req.param("season"), 1),
    num(c.req.param("episode"), 1)
  );
  return c.json({ success: true, data }, 200, { "Cache-Control": "no-store" });
}));

// ── Live TV ─────────────────────────────────────────────────────────────────
// Channel list is edge-cached 12h + 7d stale (auto-refreshes via cron/SWR).
app.get("/v1/livetv/channels", wrap(async (c) => {
  const result = await getChannels();
  return ok(
    {
      refreshedAt: result.refreshedAt,
      total: result.total,
      alive: result.alive,
      categories: groupByCategory(result.channels),
    },
    12 * 60 * 60 * 1000, // 12h edge cache
    7 * 24 * 60 * 60 * 1000 // 7d stale
  )(c);
}));

// ── Cron: Live TV refresh ───────────────────────────────────────────────────
// Protected by CRON_SECRET (Vercel cron sends it as Authorization header).
// Vercel Hobby only allows DAILY cron — the 12h TTL + SWR above keeps data
// fresh regardless of cron frequency.
app.get("/v1/cron/livetv-refresh", async (c) => {
  const secret = process.env.CRON_SECRET;
  const auth = c.req.header("Authorization") || "";
  if (!secret || auth !== `Bearer ${secret}`) {
    return c.json({ success: false, error: "unauthorized", code: "BAD_CRON_SECRET" }, 401);
  }
  try {
    const result = await refreshChannels();
    return c.json({
      success: true,
      data: { refreshedAt: result.refreshedAt, total: result.total, alive: result.alive },
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return c.json({ success: false, error: msg, code: "REFRESH_FAILED" }, 500);
  }
});

// ── NiaziTV Turkish dramas ──────────────────────────────────────────────────
// Series list + episodes are cached (site rarely changes).
// Stream URLs are signed/time-limited — NEVER cache these responses.
app.get("/v1/niazi/series", wrap(async (c) => {
  const data = await getSeries();
  return ok(data, NIAZI_TTL.series, NIAZI_TTL.staleSeries)(c);
}));

app.get("/v1/niazi/series/:id/episodes", wrap(async (c) => {
  const data = await getEpisodes(c.req.param("id"));
  return ok(data, NIAZI_TTL.episodes, NIAZI_TTL.staleEpisodes)(c);
}));

app.get("/v1/niazi/stream/:serieId/:episodeId", wrap(async (c) => {
  const data = await getStreamUrl(c.req.param("serieId"), c.req.param("episodeId"));
  return c.json({ success: true, data }, 200, { "Cache-Control": "no-store" });
}));

// ── 404 ─────────────────────────────────────────────────────────────────────
app.notFound((c) => c.json({ success: false, error: "not found", code: "NOT_FOUND" }, 404));
