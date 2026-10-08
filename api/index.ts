/**
 * PrimeFlix API — plain Vercel serverless function (no framework).
 * Uniform contract: { success: true, data } | { success: false, error, code }
 */
import { tmdb, TTL } from "../src/tmdb.js";
import { resolveStream, providerHealth, availableAudio } from "../src/chain.js";
import { warmFZMovies, fzStats } from "../src/providers/fzmovies.js";
import { cacheStats } from "../src/cache.js";
import { getSeries, getSeasons, getEpisodesForSerie, getStreamUrl, NIAZI_TTL } from "../src/niazitv.js";
import { getChannels, refreshChannels, groupByCategory, hideDead, pendingChannels } from "../src/livetv.js";
import { securityStats } from "../src/security/stats.js";
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

/**
 * Authenticated responses are NEVER edge-cached publicly. Vercel keys edge
 * cache on URL, not on X-API-Key — a `public, s-maxage` response could be
 * served to a caller with no key at all (auth bypass via CDN). Use
 * `private` so only the caller's browser caches, never the shared edge.
 */
function privateCache(ttlMs: number): Record<string, string> {
  return {
    "Cache-Control": `private, max-age=${Math.floor(ttlMs / 1000)}`,
  };
}

const num = (v: string | undefined, d: number): number => {
  const n = parseInt(v || "", 10);
  return Number.isFinite(n) && n > 0 ? n : d;
};

/** TMDB ids are numeric — reject anything else before it reaches upstream URLs. */
const tmdbId = (v: string | undefined): string | null =>
  v && /^\d+$/.test(v) ? v : null;

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
      // Auth is header-only: X-API-Key (day-1) or HMAC headers. The old
      // ?api_key= query fallback is gone — secrets in URLs land in logs.
      const gate = await authGatePlain(req.method || "GET", path, (n) => {
        if (n === "X-API-Key") return header("X-API-Key");
        return header(n);
      }, clientIp, gateBody, url.search.slice(1));
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
      const r = await refreshPlain(body, clientIp);
      if (r.status === 429) res.setHeader("Retry-After", "60");
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
          "GET /v1/tmdb/discover/movie?with_genres=&with_original_language=&with_origin_country=&sort_by=&page=&region=",
          "GET /v1/tmdb/movie/upcoming?region=&page=",
          "GET /v1/tmdb/movie/:id",
          "GET /v1/tmdb/tv/:id",
          "GET /v1/tmdb/tv/:id/season/:season",
          "GET /v1/tmdb/search/multi?query=&page=",
          "GET /v1/tmdb/movie/:id/recommendations",
          "GET /v1/tmdb/tv/:id/recommendations",
          "GET /v1/stream/movie/:tmdbId",
          "GET /v1/stream/movie/:tmdbId/languages",
          "GET /v1/stream/tv/:tmdbId/:season/:episode/languages",
          "GET /v1/stream/tv/:tmdbId/:season/:episode",
          "POST /v1/auth/register",
          "POST /v1/auth/refresh",
          "POST /v1/auth/revoke",
          "GET /v1/niazi/series",
          "GET /v1/niazi/series/:id/seasons",
          "GET /v1/niazi/series/:id/episodes",
          "GET /v1/niazi/stream/:seasonId/:episodeId",
          "GET /v1/livetv/channels",
          "GET /v1/cron/livetv-refresh",
          "GET /v1/cron/fz-warm?tmdbId=",
        ],
      });
    }

    if (path === "/health") {
      return send(res, 200, {
        ok: true, cluster: CLUSTER, version: VERSION,
        tmdbKeyConfigured: !!process.env.TMDB_API_KEY,
        providers: providerHealth(), cache: cacheStats(), fzmovies: fzStats(),
        security: securityStats(),
      });
    }

    // TMDB proxy
    let m: RegExpMatchArray | null;
    if (path === "/v1/tmdb/trending/movie") {
      const data = await tmdb.trendingMovie(q.get("time_window") || "day");
      return send(res, 200, ok(data), privateCache(TTL.trending));
    }
    if (path === "/v1/tmdb/trending/tv") {
      const data = await tmdb.trendingTv(q.get("time_window") || "day");
      return send(res, 200, ok(data), privateCache(TTL.trending));
    }
    if (path === "/v1/tmdb/discover/movie") {
      // Allowlisted params only (Ali 2026-10-08: Home rails — Hollywood, Bollywood,
      // South Indian, Comedy, Adventure, Horror).
      const allow = [
        "with_genres", "with_original_language", "with_origin_country", "sort_by",
        "page", "region", "vote_count.gte", "primary_release_date.gte",
        "primary_release_date.lte", "with_release_type",
      ];
      const params: Record<string, string> = {};
      for (const k of allow) {
        const v = q.get(k);
        if (v) params[k] = v;
      }
      const data = await tmdb.discoverMovie(params);
      return send(res, 200, ok(data), privateCache(TTL.discover));
    }
    if (path === "/v1/tmdb/movie/upcoming") {
      // Must sit BEFORE the /v1/tmdb/movie/:id regex below.
      const data = await tmdb.upcomingMovies(q.get("region") || "US", q.get("page") || "1");
      return send(res, 200, ok(data), privateCache(TTL.discover));
    }
    if ((m = path.match(/^\/v1\/tmdb\/movie\/([^/]+)$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const data = await tmdb.movie(id);
      return send(res, 200, ok(data), privateCache(TTL.details));
    }
    if ((m = path.match(/^\/v1\/tmdb\/movie\/([^/]+)\/recommendations$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const data = await tmdb.movieRecs(id);
      return send(res, 200, ok(data), privateCache(TTL.details));
    }
    if ((m = path.match(/^\/v1\/tmdb\/tv\/([^/]+)\/season\/([^/]+)$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const data = await tmdb.tvSeason(id, num(m[2], 1));
      return send(res, 200, ok(data), privateCache(TTL.details));
    }
    if ((m = path.match(/^\/v1\/tmdb\/tv\/([^/]+)\/recommendations$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const data = await tmdb.tvRecs(id);
      return send(res, 200, ok(data), privateCache(TTL.details));
    }
    if ((m = path.match(/^\/v1\/tmdb\/tv\/([^/]+)$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const data = await tmdb.tv(id);
      return send(res, 200, ok(data), privateCache(TTL.details));
    }
    if (path === "/v1/tmdb/search/multi") {
      const query = q.get("query") || "";
      if (query.length < 2) return send(res, 400, fail("query too short", "BAD_QUERY"));
      const data = await tmdb.search(query, q.get("page") || "1");
      return send(res, 200, ok(data), privateCache(TTL.search));
    }

    // Available audio languages (Ali 2026-10-08: dub button shows ONLY what
    // actually exists — TMDB original_language + VidZee/FZMovies Hindi check.
    // Returns { audio, original, playing, labels }; `audio` stays top-level
    // for backward compat with older apps.
    // MUST sit before the /v1/stream/movie/:tmdbId regex below.
    if ((m = path.match(/^\/v1\/stream\/movie\/([^/]+)\/languages$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const info = await availableAudio(id, "movie");
      return send(res, 200, ok({ audio: info.audio, original: info.original, playing: info.playing, labels: info.labels }), { "Cache-Control": "no-store" });
    }
    if ((m = path.match(/^\/v1\/stream\/tv\/([^/]+)\/([^/]+)\/([^/]+)\/languages$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const info = await availableAudio(id, "tv", num(m[2], 1), num(m[3], 1));
      return send(res, 200, ok({ audio: info.audio, original: info.original, playing: info.playing, labels: info.labels }), { "Cache-Control": "no-store" });
    }

    // Stream resolution (NEVER cache — signed URLs expire)
    // ?audio=hi → Hindi-dubbed only (VidZee); ?audio=en → English/original only.
    if ((m = path.match(/^\/v1\/stream\/movie\/([^/]+)$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const data = await resolveStream(id, "movie", undefined, undefined, q.get("audio") || undefined);
      return send(res, 200, ok(data), { "Cache-Control": "no-store" });
    }
    if ((m = path.match(/^\/v1\/stream\/tv\/([^/]+)\/([^/]+)\/([^/]+)$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const data = await resolveStream(id, "tv", num(m[2], 1), num(m[3], 1), q.get("audio") || undefined);
      return send(res, 200, ok(data), { "Cache-Control": "no-store" });
    }

    // NiaziTV Turkish dramas (stream URLs NEVER cached — signed/expiring)
    if (path === "/v1/niazi/series") {
      const data = await getSeries();
      return send(res, 200, ok(data), privateCache(NIAZI_TTL.series));
    }
    if ((m = path.match(/^\/v1\/niazi\/series\/([^/]+)\/seasons$/))) {
      const data = await getSeasons(m[1]);
      return send(res, 200, ok(data), privateCache(NIAZI_TTL.seasons));
    }
    if ((m = path.match(/^\/v1\/niazi\/series\/([^/]+)\/episodes$/))) {
      // Series id (NOT season id — aggregates every season page).
      const data = await getEpisodesForSerie(m[1]);
      return send(res, 200, ok(data), privateCache(NIAZI_TTL.episodes));
    }
    if ((m = path.match(/^\/v1\/niazi\/stream\/([^/]+)\/([^/]+)$/))) {
      const data = await getStreamUrl(m[1], m[2]);
      return send(res, 200, ok(data), { "Cache-Control": "no-store" });
    }

    // Live TV channels. Playable channels go in `groups` (unchanged
    // contract); curated channels with no playable URL are reported honestly
    // in `pending` instead of silently flickering in/out between instances.
    if (path === "/v1/livetv/channels") {
      const data = await getChannels();
      const playable = hideDead(data.channels);
      const pending = pendingChannels(data.channels).map((c) => ({
        id: c.id,
        name: c.name,
        category: c.category,
        country: c.country,
        logo: c.logo,
        reason: "awaiting-source",
      }));
      return send(res, 200, ok({
        refreshedAt: data.refreshedAt,
        total: data.total,
        alive: playable.length,
        groups: groupByCategory(playable),
        pending,
      }), privateCache(12 * 3600 * 1000));
    }
    if (path === "/v1/cron/livetv-refresh") {
      // CRON_SECRET is header-only — never in the query string (logged).
      const secret = header("x-cron-secret");
      if (!process.env.CRON_SECRET || secret !== process.env.CRON_SECRET) {
        return send(res, 401, fail("unauthorized", "BAD_CRON_SECRET"));
      }
      const data = await refreshChannels();
      return send(res, 200, ok({ refreshedAt: data.refreshedAt, total: data.total, alive: data.alive }));
    }
    // FZMovies cache warmer (CRON_SECRET protected, header-only).
    // Bounded: single-attempt scrape with a hard 52s deadline so the
    // invocation ALWAYS completes inside Vercel's 60s maxDuration.
    // Usage: GET /v1/cron/fz-warm?tmdbId=299536  (x-cron-secret header)
    if (path === "/v1/cron/fz-warm") {
      const secret = header("x-cron-secret");
      if (!process.env.CRON_SECRET || secret !== process.env.CRON_SECRET) {
        return send(res, 401, fail("unauthorized", "BAD_CRON_SECRET"));
      }
      const id = tmdbId(q.get("tmdbId") || undefined);
      if (!id) return send(res, 400, fail("tmdbId required (numeric)", "BAD_QUERY"));
      const warm = await warmFZMovies(id, "movie", true);
      return send(res, 200, ok({ tmdbId: id, warmed: warm.warmed, reason: warm.reason, qualities: warm.qualities || 0 }));
    }

    return send(res, 404, fail("not found", "NOT_FOUND"));
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    let code = "UPSTREAM_ERROR";
    let status = 500;
    if (msg.includes("request body too large")) {
      code = "PAYLOAD_TOO_LARGE";
      status = 413;
    } else if (msg.startsWith("invalid ") || msg.includes("invalid tmdb id")) {
      code = "BAD_QUERY";
      status = 400;
    } else if (msg.includes("hindi dubbed not available")) {
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
