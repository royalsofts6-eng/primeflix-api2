/**
 * PrimeFlix API — plain Vercel serverless function (no framework).
 * Uniform contract: { success: true, data } | { success: false, error, code }
 */
import { tmdb, TTL } from "../src/tmdb.js";
import { resolveStream, providerHealth } from "../src/chain.js";
import { cacheStats } from "../src/cache.js";
import { getSeries, getEpisodes, getStreamUrl, NIAZI_TTL } from "../src/niazitv.js";
import { getChannels, refreshChannels, groupByCategory } from "../src/livetv.js";
import { securityStats } from "../src/security/middleware.js";
import {
  authGatePlain,
  nodeHeaderGetter,
  readBody,
  registerPlain,
  refreshPlain,
  revokePlain,
} from "../src/security/plain.js";

const VERSION = "1.1.0";
const CLUSTER = process.env.CLUSTER_NAME || "api1";
const PUBLIC_PATHS = new Set(["/", "/health", "/api", "/api/health"]);

function send(res: any, status: number, body: unknown, headers: Record<string, string> = {}) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  for (const [k, v] of Object.entries(headers)) res.setHeader(k, v);
  res.end(JSON.stringify(body));
}

const ok = (data: unknown) => ({ success: true, data });
const fail = (error: string, code: string) => ({ success: false, error, code });

function edgeCache(ttlMs: number, staleMs: number): Record<string, string> {
  return {
    "Cache-Control": `public, s-maxage=${Math.floor(ttlMs / 1000)}, stale-while-revalidate=${Math.floor(staleMs / 1000)}`,
  };
}

const num = (v: string | undefined, d: number): number => {
  const n = parseInt(v || "", 10);
  return Number.isFinite(n) && n > 0 ? n : d;
};

export default async function handler(req: any, res: any) {
  try {
    const url = new URL(req.url || "/", "http://localhost");
    // strip /api prefix if present (Vercel serves api/index.ts at /api/*)
    let path = url.pathname.replace(/^\/api(?=\/|$)/, "") || "/";
    const q = url.searchParams;

    // ── Auth ──
    // Public paths skip. /v1/auth/* and /v1/cron/* handle their own auth.
    const header = nodeHeaderGetter(req);
    const clientIp =
      (header("x-forwarded-for") || "").split(",")[0].trim() ||
      header("x-real-ip") ||
      "unknown";
    const isAuthRoute = path.startsWith("/v1/auth/");
    const isCronRoute = path.startsWith("/v1/cron/");
    if (!PUBLIC_PATHS.has(url.pathname) && !PUBLIC_PATHS.has(path) && !isAuthRoute && !isCronRoute) {
      // Read body for HMAC signature verification on non-GET requests.
      let gateBody = "";
      if (req.method !== "GET" && req.method !== "HEAD") {
        gateBody = await readBody(req);
      }
      // Also accept ?api_key= query param for day-1 compat
      const qKey = q.get("api_key");
      const gate = await authGatePlain(req.method || "GET", path, (n) => {
        if (n === "X-API-Key") return header("X-API-Key") || qKey;
        return header(n);
      }, clientIp, gateBody);
      if (!gate.ok) {
        if (gate.retryAfter) res.setHeader("Retry-After", String(gate.retryAfter));
        return send(res, gate.status || 401, fail(gate.error || "unauthorized", gate.code || "UNAUTHORIZED"));
      }
    }

    // ── Auth routes (own auth handling) ──
    if (path === "/v1/auth/register" && req.method === "POST") {
      const body = await readBody(req);
      const r = await registerPlain(body, clientIp);
      if (r.status === 429) res.setHeader("Retry-After", "60");
      return send(res, r.status, r.json);
    }
    if (path === "/v1/auth/refresh" && req.method === "POST") {
      const body = await readBody(req);
      const r = await refreshPlain(body);
      return send(res, r.status, r.json);
    }
    if (path === "/v1/auth/revoke" && req.method === "POST") {
      const body = await readBody(req);
      const r = await revokePlain(body, header);
      return send(res, r.status, r.json);
    }

    // ── Routes ──
    if (path === "/") {
      return send(res, 200, {
        name: "PrimeFlix API", cluster: CLUSTER, version: VERSION,
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
          "POST /v1/auth/register",
          "POST /v1/auth/refresh",
          "POST /v1/auth/revoke",
          "GET /v1/niazi/series",
          "GET /v1/niazi/series/:id/episodes",
          "GET /v1/niazi/stream/:serieId/:episodeId",
          "GET /v1/livetv/channels",
          "GET /v1/cron/livetv-refresh",
        ],
      });
    }

    if (path === "/health") {
      return send(res, 200, {
        ok: true, cluster: CLUSTER, version: VERSION,
        tmdbKeyConfigured: !!process.env.TMDB_API_KEY,
        providers: providerHealth(), cache: cacheStats(),
        security: securityStats(),
      });
    }

    // TMDB proxy
    let m: RegExpMatchArray | null;
    if (path === "/v1/tmdb/trending/movie") {
      const data = await tmdb.trendingMovie(q.get("time_window") || "day");
      return send(res, 200, ok(data), edgeCache(TTL.trending, TTL.stale7d));
    }
    if (path === "/v1/tmdb/trending/tv") {
      const data = await tmdb.trendingTv(q.get("time_window") || "day");
      return send(res, 200, ok(data), edgeCache(TTL.trending, TTL.stale7d));
    }
    if ((m = path.match(/^\/v1\/tmdb\/movie\/([^/]+)$/))) {
      const data = await tmdb.movie(m[1]);
      return send(res, 200, ok(data), edgeCache(TTL.details, TTL.stale7d));
    }
    if ((m = path.match(/^\/v1\/tmdb\/movie\/([^/]+)\/recommendations$/))) {
      const data = await tmdb.movieRecs(m[1]);
      return send(res, 200, ok(data), edgeCache(TTL.details, TTL.stale7d));
    }
    if ((m = path.match(/^\/v1\/tmdb\/tv\/([^/]+)\/season\/([^/]+)$/))) {
      const data = await tmdb.tvSeason(m[1], num(m[2], 1));
      return send(res, 200, ok(data), edgeCache(TTL.details, TTL.stale7d));
    }
    if ((m = path.match(/^\/v1\/tmdb\/tv\/([^/]+)\/recommendations$/))) {
      const data = await tmdb.tvRecs(m[1]);
      return send(res, 200, ok(data), edgeCache(TTL.details, TTL.stale7d));
    }
    if ((m = path.match(/^\/v1\/tmdb\/tv\/([^/]+)$/))) {
      const data = await tmdb.tv(m[1]);
      return send(res, 200, ok(data), edgeCache(TTL.details, TTL.stale7d));
    }
    if (path === "/v1/tmdb/search/multi") {
      const query = q.get("query") || "";
      if (query.length < 2) return send(res, 400, fail("query too short", "BAD_QUERY"));
      const data = await tmdb.search(query, q.get("page") || "1");
      return send(res, 200, ok(data), edgeCache(TTL.search, TTL.stale1d));
    }

    // Stream resolution (NEVER cache — signed URLs expire)
    // ?audio=hi → Hindi-dubbed only (VidZee); ?audio=en → English/original only.
    if ((m = path.match(/^\/v1\/stream\/movie\/([^/]+)$/))) {
      const data = await resolveStream(m[1], "movie", undefined, undefined, q.get("audio") || undefined);
      return send(res, 200, ok(data), { "Cache-Control": "no-store" });
    }
    if ((m = path.match(/^\/v1\/stream\/tv\/([^/]+)\/([^/]+)\/([^/]+)$/))) {
      const data = await resolveStream(m[1], "tv", num(m[2], 1), num(m[3], 1), q.get("audio") || undefined);
      return send(res, 200, ok(data), { "Cache-Control": "no-store" });
    }

    // NiaziTV Turkish dramas (stream URLs NEVER cached — signed/expiring)
    if (path === "/v1/niazi/series") {
      const data = await getSeries();
      return send(res, 200, ok(data), edgeCache(NIAZI_TTL.series, NIAZI_TTL.staleSeries));
    }
    if ((m = path.match(/^\/v1\/niazi\/series\/([^/]+)\/episodes$/))) {
      const data = await getEpisodes(m[1]);
      return send(res, 200, ok(data), edgeCache(NIAZI_TTL.episodes, NIAZI_TTL.staleEpisodes));
    }
    if ((m = path.match(/^\/v1\/niazi\/stream\/([^/]+)\/([^/]+)$/))) {
      const data = await getStreamUrl(m[1], m[2]);
      return send(res, 200, ok(data), { "Cache-Control": "no-store" });
    }

    // Live TV channels (12h cache + 7d stale; cron refreshes in background)
    if (path === "/v1/livetv/channels") {
      const data = await getChannels();
      return send(res, 200, ok({
        refreshedAt: data.refreshedAt,
        total: data.total,
        alive: data.alive,
        groups: groupByCategory(data.channels),
      }), edgeCache(12 * 3600 * 1000, 7 * 24 * 3600 * 1000));
    }
    if (path === "/v1/cron/livetv-refresh") {
      const secret = q.get("secret") || header("x-cron-secret");
      if (!process.env.CRON_SECRET || secret !== process.env.CRON_SECRET) {
        return send(res, 401, fail("unauthorized", "BAD_CRON_SECRET"));
      }
      const data = await refreshChannels();
      return send(res, 200, ok({ refreshedAt: data.refreshedAt, total: data.total, alive: data.alive }));
    }

    return send(res, 404, fail("not found", "NOT_FOUND"));
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    let code = "UPSTREAM_ERROR";
    let status = 500;
    if (msg.includes("hindi dubbed not available")) {
      code = "HINDI_UNAVAILABLE";
      status = 502;
    } else if (msg.includes("TMDB rate limited")) {
      code = "TMDB_RATE_LIMIT";
      status = 429;
    } else if (msg.includes("all providers")) {
      code = "UPSTREAM_ERROR";
      status = 502;
    }
    return send(res, status, fail(msg, code));
  }
}
