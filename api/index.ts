/**
 * PrimeFlix API — plain Vercel serverless function (no framework).
 * Uniform contract: { success: true, data } | { success: false, error, code }
 */
import { tmdb, TTL, stripTmdbList, stripTmdbDetail, stripTmdbSeason } from "../src/tmdb.js";
import { createHash } from "crypto";
import {
  resolveStream,
  ChainDeadlineError,
  providerHealth,
  providerCooldowns,
  cachedAvailableAudio,
  prefetchStreamOnDetail,
  wrapperStatus,
  RACE_MODE,
  recordDeadReport,
  readDeadCounters,
  deadCanaryToday,
  DEAD_CANARY_THRESHOLD,
} from "../src/chain.js";
import { NotAvailableError } from "../src/providers/types.js";
import { raceStats } from "../src/race.js";
import { warmFZMovies, fzStats } from "../src/providers/fzmovies.js";
import { cacheStats, ck, CACHE_SCHEMA } from "../src/cache.js";
import { streamCacheStats, streamCacheKey } from "../src/streamcache.js";
import type { StreamCacheStat } from "../src/streamcache.js";
import { warmTrendingStreams, warmFZMoviesBatch, noteWatch } from "../src/warm.js";
import { redisCommand } from "../src/security/redis.js";
import { cacheDel } from "../src/cache.js";
import { mbhilangKey } from "../src/providers/moviebox.js";
import { getSeries, getSeasons, getEpisodesForSerie, getStreamUrl, NIAZI_TTL } from "../src/niazitv.js";
import { getChannels, refreshChannels, groupByCategory, hideDead, pendingChannels } from "../src/livetv.js";
import { securityStats } from "../src/security/stats.js";
import { redisHealth } from "../src/security/redis.js";
import {
  authGatePlain,
  nodeHeaderGetter,
  readBody,
  registerPlain,
  refreshPlain,
  revokePlain,
  apiKeyKilled,
  adminKeyConfigured,
} from "../src/security/plain.js";
import { globalBackstopState } from "../src/security/ratelimit.js";
import { tmdbCoolingDown } from "../src/tmdb.js";
import { backgroundWired } from "../src/revalidate.js";

const VERSION = "1.4.0";
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
    // Slow-net (2026-10-09): stale-while-revalidate added for the BROWSER
    // cache (slow networks serve stale instantly while revalidating).
    // s-maxage is deliberately NOT set: these routes sit behind the auth
    // gate and Vercel's edge cache keys on URL only — `public` would serve
    // authed data to keyless callers (auth bypass via CDN).
    "Cache-Control": `private, max-age=${Math.floor(ttlMs / 1000)}, stale-while-revalidate=86400`,
  };
}

/**
 * Slow-net (2026-10-09): ETag/304 on heavy catalog responses. The ETag is a
 * sha1 of the exact response bytes; a repeat call whose If-None-Match
 * carries it gets a 304 with no body. Safe with `private` caching — the
 * response is per-key, so the ETag never leaks across users.
 */
function sendCatalog(res: any, req: any, data: unknown, headers: Record<string, string> = {}) {
  const body = JSON.stringify(data);
  const etag = `"${createHash("sha1").update(body).digest("hex")}"`;
  const inm: unknown = req.headers?.["if-none-match"];
  const match =
    inm === "*" ||
    (typeof inm === "string" &&
      (inm === etag || inm.split(",").map((s) => s.trim()).includes(etag)));
  res.setHeader("ETag", etag);
  for (const [k, v] of Object.entries(headers)) res.setHeader(k, v);
  if (match) {
    res.statusCode = 304;
    return res.end();
  }
  res.statusCode = 200;
  res.setHeader("Content-Type", "application/json");
  res.end(body);
}

/**
 * Phase D (2026-10-09): X-Cache observability on every stream response —
 * HIT (fresh envelope), STALE (served + background revalidate), MISS
 * (live resolve), NEG (negative-cache fast-fail, set by the error path),
 * plus the winning provider. Signed URLs themselves are never edge-cached
 * (Cache-Control: no-store) — the shared Redis cache does the work.
 */
function streamCacheHeaders(stat: StreamCacheStat): Record<string, string> {
  const headers: Record<string, string> = { "Cache-Control": "no-store" };
  if (stat.status) headers["X-Cache"] = stat.status;
  if (stat.provider) headers["X-Cache-Provider"] = stat.provider;
  return headers;
}

/**
 * Strict positive-int parse for season/episode path params (P1-1 fix
 * 2026-10-08): the old num() silently defaulted garbage like "-3" to 1,
 * so /v1/stream/tv/1396/1/-3 returned a REAL stream for S1E1. null means
 * the route must 400 — never silently remap to a default.
 */
const posInt = (v: string | undefined): number | null => {
  if (!v || !/^\d+$/.test(v)) return null;
  const n = parseInt(v, 10);
  return Number.isSafeInteger(n) && n >= 1 ? n : null;
};

// Sane upper bounds for season/episode (P1-1).
const MAX_SEASON = 100;
const MAX_EPISODE = 500;

/**
 * ?audio= allowlist (P2-1 2026-10-08): unknown values 400 BAD_AUDIO, never
 * silently ignored. Absent/empty = default Hindi-first chain.
 * Returns null AFTER sending the 400 (caller must return).
 */
const checkAudio = (res: any, v: string | null): string | undefined | null => {
  if (v === null || v === "") return undefined;
  if (v === "hi" || v === "en") return v;
  send(res, 400, fail("invalid audio (expected hi or en)", "BAD_AUDIO"));
  return null;
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
      // P0 fix (2026-10-09): the gate consumed the request stream — stash
      // the buffered body on the request so downstream routes (e.g.
      // POST /v1/stream/report) reuse it instead of re-reading the stream.
      // Re-attaching data/end listeners to an already-consumed stream never
      // fires `end` again -> the second readBody() hangs until Vercel's 60s
      // maxDuration kills the invocation.
      req.__gateBody = gateBody;
      // Auth is header-only: X-API-Key (day-1) or HMAC headers. The old
      // ?api_key= query fallback is gone — secrets in URLs land in logs.
      const gate = await authGatePlain(req.method || "GET", path, (n) => {
        if (n === "X-API-Key") return header("X-API-Key");
        return header(n);
      }, clientIp, gateBody, url.search.slice(1));
      // Standard rate-limit headers on EVERY gated response (allowed AND
      // denied) — clients can back off before hitting 429.
      if (gate.rlHeaders) {
        for (const [k, v] of Object.entries(gate.rlHeaders)) res.setHeader(k, v);
      }
      if (!gate.ok) {
        if (gate.retryAfter) res.setHeader("Retry-After", String(gate.retryAfter));
        return send(res, gate.status || 401, fail(gate.error || "unauthorized", gate.code || "UNAUTHORIZED"));
      }
    }

    // ── Auth routes (own auth handling) ──
    if (path === "/v1/auth/register" && req.method === "POST") {
      const body = await readBody(req);
      const r = await registerPlain(body, clientIp);
      if (r.status === 429) {
        res.setHeader("Retry-After", "60");
        if (r.rlHeaders) for (const [k, v] of Object.entries(r.rlHeaders)) res.setHeader(k, v);
      }
      return send(res, r.status, r.json);
    }
    if (path === "/v1/auth/refresh" && req.method === "POST") {
      const body = await readBody(req);
      const r = await refreshPlain(body, clientIp);
      if (r.status === 429) {
        res.setHeader("Retry-After", "60");
        if (r.rlHeaders) for (const [k, v] of Object.entries(r.rlHeaders)) res.setHeader(k, v);
      }
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
          "POST /v1/stream/report",
          "GET /v1/cron/fz-warm-batch?limit=",
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
          "GET /v1/cron/stream-warm?limit=",
        ],
        // P2-2 (2026-10-08): canonical auth contract. X-PF-Timestamp MUST
        // be unix MILLISECONDS (Date.now()); seconds-epoch values are
        // rejected as STALE_TIMESTAMP by verifyHmacParts.
        auth: {
          apiKey: { header: "X-API-Key", note: "day-1 app key; ?api_key= query fallback removed" },
          hmac: {
            headers: ["X-PF-Device", "X-PF-Timestamp", "X-PF-Token", "X-PF-Signature"],
            timestamp: "unix MILLISECONDS (Date.now()); seconds-epoch values are rejected as STALE_TIMESTAMP",
            windowSeconds: 300,
          },
        },
      });
    }

    if (path === "/health") {
      const sc = streamCacheStats();
      const total = sc.hits + sc.misses;
      return send(res, 200, {
        ok: true, cluster: CLUSTER, version: VERSION,
        // Phase D (2026-10-09): 12h smart cache — versioned namespace.
        cacheSchema: CACHE_SCHEMA,
        tmdbKeyConfigured: !!process.env.TMDB_API_KEY,
        // 4-source parallel system (2026-10-09)
        raceMode: RACE_MODE,
        race: raceStats(),
        moviebox: await wrapperStatus(),
        streamCacheHitRate: total ? +(sc.hits / total).toFixed(3) : null,
        providers: providerHealth(), providerCooldowns: await providerCooldowns(),
        // P1-1 (2026-10-09): dead-URL counters are no longer write-only —
        // /health exposes per-provider daily counts, and providers at the
        // canary threshold (5+/day) are cooled cluster-wide (see
        // providerCooldowns "dead" class above).
        deadCounters: await readDeadCounters(),
        deadCanary: { threshold: DEAD_CANARY_THRESHOLD, providers: await deadCanaryToday() },
        // P1-6 (2026-10-09): TMDB cluster-wide cooldown state.
        tmdbCooldown: await tmdbCoolingDown(),
        // P1-7 (2026-10-09): abuse-surface state.
        apiKeyKilled: await apiKeyKilled(),
        adminKeyConfigured: adminKeyConfigured(),
        backstop: await globalBackstopState(),
        cache: cacheStats(), streamCache: sc, fzmovies: fzStats(),
        security: securityStats(), redis: await redisHealth(),
        // P1 FIX (2026-10-09, QA Phase F): post-response background work
        // (prefetch writer, watch-history, stale revalidate) must be wired
        // via waitUntil — false means the Phase F bug is present.
        backgroundWired: backgroundWired(),
      });
    }

    // TMDB proxy
    let m: RegExpMatchArray | null;
    if (path === "/v1/tmdb/trending/movie") {
      const data = await tmdb.trendingMovie(q.get("time_window") || "day");
      return sendCatalog(res, req, ok(stripTmdbList(data)), privateCache(TTL.trending));
    }
    if (path === "/v1/tmdb/trending/tv") {
      const data = await tmdb.trendingTv(q.get("time_window") || "day");
      return sendCatalog(res, req, ok(stripTmdbList(data)), privateCache(TTL.trending));
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
      return sendCatalog(res, req, ok(stripTmdbList(data)), privateCache(TTL.discover));
    }
    if (path === "/v1/tmdb/movie/upcoming") {
      // Must sit BEFORE the /v1/tmdb/movie/:id regex below.
      const data = await tmdb.upcomingMovies(q.get("region") || "US", q.get("page") || "1");
      return sendCatalog(res, req, ok(stripTmdbList(data)), privateCache(TTL.discover));
    }
    if ((m = path.match(/^\/v1\/tmdb\/movie\/([^/]+)$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const data = await tmdb.movie(id);
      return sendCatalog(res, req, ok(stripTmdbDetail(data)), privateCache(TTL.details));
    }
    if ((m = path.match(/^\/v1\/tmdb\/movie\/([^/]+)\/recommendations$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const data = await tmdb.movieRecs(id);
      return sendCatalog(res, req, ok(stripTmdbList(data)), privateCache(TTL.details));
    }
    if ((m = path.match(/^\/v1\/tmdb\/tv\/([^/]+)\/season\/([^/]+)$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const season = posInt(m[2]);
      if (season === null || season > MAX_SEASON) {
        return send(res, 400, fail("invalid season (expected 1-100)", "BAD_QUERY"));
      }
      const data = await tmdb.tvSeason(id, season);
      return sendCatalog(res, req, ok(stripTmdbSeason(data)), privateCache(TTL.details));
    }
    if ((m = path.match(/^\/v1\/tmdb\/tv\/([^/]+)\/recommendations$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const data = await tmdb.tvRecs(id);
      return sendCatalog(res, req, ok(stripTmdbList(data)), privateCache(TTL.details));
    }
    if ((m = path.match(/^\/v1\/tmdb\/tv\/([^/]+)$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const data = await tmdb.tv(id);
      return sendCatalog(res, req, ok(stripTmdbDetail(data)), privateCache(TTL.details));
    }
    if (path === "/v1/tmdb/search/multi") {
      const query = q.get("query") || "";
      if (query.length < 2) return send(res, 400, fail("query too short", "BAD_QUERY"));
      // P1-2 (2026-10-08): a 10KB query made TMDB return 414, which we then
      // mapped to 500. Cap server-side — never proxy garbage upstream.
      if (query.length > 200) return send(res, 400, fail("query too long (max 200 chars)", "BAD_QUERY"));
      const data = await tmdb.search(query, q.get("page") || "1");
      return sendCatalog(res, req, ok(stripTmdbList(data)), privateCache(TTL.search));
    }

    // Available audio languages (Ali 2026-10-08: dub button shows ONLY what
    // actually exists — TMDB original_language + VidZee/FZMovies Hindi check.
    // Returns { audio, original, playing, labels }; `audio` stays top-level
    // for backward compat with older apps.
    // MUST sit before the /v1/stream/movie/:tmdbId regex below.
    // Available audio languages — cached 12h (pf:v3:lang:*, Phase D) instead
    // of a live VidZee check on every call (2026-10-09: was the biggest
    // per-request waste). MUST sit before the /v1/stream/movie/:tmdbId
    // regex below.
    if ((m = path.match(/^\/v1\/stream\/movie\/([^/]+)\/languages$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const info = await cachedAvailableAudio(id, "movie");
      // Win #3 (2026-10-09): the dub button fires on every detail-screen
      // open — fire-and-forget warm the stream cache so Play is instant.
      // Never blocks this response.
      prefetchStreamOnDetail("movie", id, undefined, undefined, info.playing);
      return send(res, 200, ok({ audio: info.audio, original: info.original, playing: info.playing, labels: info.labels }), { "Cache-Control": "no-store" });
    }
    if ((m = path.match(/^\/v1\/stream\/tv\/([^/]+)\/([^/]+)\/([^/]+)\/languages$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const season = posInt(m[2]);
      const episode = posInt(m[3]);
      if (season === null || episode === null || season > MAX_SEASON || episode > MAX_EPISODE) {
        return send(res, 400, fail("invalid season/episode (expected season 1-100, episode 1-500)", "BAD_QUERY"));
      }
      const info = await cachedAvailableAudio(id, "tv", season, episode);
      // Win #3 (2026-10-09): same prefetch writer for series episodes.
      prefetchStreamOnDetail("tv", id, season, episode, info.playing);
      return send(res, 200, ok({ audio: info.audio, original: info.original, playing: info.playing, labels: info.labels }), { "Cache-Control": "no-store" });
    }

    // Stream resolution — shared 12h smart cache (Phase D 2026-10-09):
    // the envelope carries X-Cache: HIT|STALE|MISS + X-Cache-Provider so
    // hit rates are observable per response. Signed-URL expiry is enforced
    // by the provider-capped fresh TTLs in streamcache.ts, not here.
    // ?audio=hi → Hindi-dubbed only (VidZee); ?audio=en → English/original only.
    if ((m = path.match(/^\/v1\/stream\/movie\/([^/]+)$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const audio = checkAudio(res, q.get("audio"));
      if (audio === null) return;
      const stat: StreamCacheStat = {};
      const data = await resolveStream(id, "movie", undefined, undefined, audio, stat);
      // Phase D: every successful Play feeds the watch-history ZSET — the
      // pre-warm cron warms THESE titles first (not generic trending).
      noteWatch("movie", id);
      return send(res, 200, ok(data), streamCacheHeaders(stat));
    }
    if ((m = path.match(/^\/v1\/stream\/tv\/([^/]+)\/([^/]+)\/([^/]+)$/))) {
      const id = tmdbId(m[1]);
      if (!id) return send(res, 400, fail("invalid tmdb id", "BAD_QUERY"));
      const season = posInt(m[2]);
      const episode = posInt(m[3]);
      if (season === null || episode === null || season > MAX_SEASON || episode > MAX_EPISODE) {
        return send(res, 400, fail("invalid season/episode (expected season 1-100, episode 1-500)", "BAD_QUERY"));
      }
      const audio = checkAudio(res, q.get("audio"));
      if (audio === null) return;
      const stat: StreamCacheStat = {};
      const data = await resolveStream(id, "tv", season, episode, audio, stat);
      // Phase D: every successful Play feeds the watch-history ZSET.
      noteWatch("tv", id);
      return send(res, 200, ok(data), streamCacheHeaders(stat));
    }

    // Dead-URL report (2026-10-09): the app calls this when every quality
    // AND every alternate failed playback. Guarded 1/hour/key (abuse +
    // duplicate storms), evicts the whole envelope (+ pf:v3:lang when the dub
    // itself is gone), logs to pf:v3:deadlog, and counts per-provider dead
    // signals for canaries.
    // Member-auth via the standard gate (not a public path, not /v1/cron/*).
    if (path === "/v1/stream/report" && req.method === "POST") {
      // P0 fix (2026-10-09): reuse the auth gate's already-read body — the
      // gate runs on every request that reaches this route (report is not
      // public/auth/cron), so __gateBody is guaranteed present. Calling
      // readBody(req) here again would hang forever (consumed stream).
      const raw = typeof req.__gateBody === "string" ? req.__gateBody : "";
      let b: any;
      try {
        b = JSON.parse(raw || "{}");
      } catch {
        return send(res, 400, fail("invalid JSON body", "BAD_QUERY"));
      }
      const type = b.type === "tv" ? "tv" : "movie";
      const id = tmdbId(String(b.tmdbId || ""));
      if (!id) return send(res, 400, fail("tmdbId required (numeric)", "BAD_QUERY"));
      const audio = b.audio === "hi" || b.audio === "en" ? b.audio : undefined;
      const season = type === "tv" ? posInt(String(b.season || "")) || undefined : undefined;
      const episode = type === "tv" ? posInt(String(b.episode || "")) || undefined : undefined;
      // Phase D (2026-10-09): versioned stream key pf:v3:stream:*. The
      // 1/hour guard stays unversioned (operational state, not cache).
      const rkey = ck("stream", streamCacheKey(type, id, season, episode, audio));
      const guardKey = `pf:report:${type}:${id}:${season ?? 0}:${episode ?? 0}:${audio || "def"}`;
      // Per-provider dead counter FIRST (2026-10-09): the 1/hour guard is
      // per title+audio, but a second provider dying for the same title
      // within the hour is still a canary signal worth counting. Provider
      // names are allowlisted into the key (no injection).
      // P1-1 (2026-10-09): counters are no longer write-only — recordDeadReport
      // INCRs, returns today's count, and cools the provider cluster-wide
      // for 1h when the canary threshold (5/day) is reached.
      const providerName =
        typeof b.provider === "string" && /^[a-z0-9-]{1,32}$/i.test(b.provider)
          ? b.provider.toLowerCase()
          : null;
      let deadCount = 0;
      if (providerName) {
        deadCount = await recordDeadReport(providerName);
      }
      const guard = await redisCommand(["SET", guardKey, "1", "NX", "EX", 3600]).catch(() => null);
      if (guard !== "OK") {
        return send(res, 200, ok({ evicted: false, reason: "already-reported-this-hour" }));
      }
      // Phase D (2026-10-09, design §4a): dead-URL log — LPUSH +
      // LTRIM 100 + 7d TTL so the team can see WHICH provider's URLs are
      // dying (signature death vs. one-off blips).
      const deadlogKey = ck("deadlog");
      await redisCommand([
        "LPUSH",
        deadlogKey,
        JSON.stringify({
          provider: providerName,
          type,
          tmdbId: id,
          season: season ?? 0,
          episode: episode ?? 0,
          audio: audio || "def",
          reason: typeof b.reason === "string" ? b.reason.slice(0, 64) : null,
          at: Date.now(),
        }),
      ]).catch(() => null);
      await redisCommand(["LTRIM", deadlogKey, "0", "99"]).catch(() => null);
      await redisCommand(["EXPIRE", deadlogKey, 7 * 24 * 3600]).catch(() => null);
      await redisCommand(["DEL", rkey]).catch(() => null);
      // Drop this instance's in-memory stream envelope too — otherwise it
      // keeps serving the dead envelope from memory for up to its TTL even
      // though Redis was evicted (2026-10-09).
      cacheDel(streamCacheKey(type, id, season, episode, audio));
      // P1-2 (2026-10-09): the `hindi_no_longer_available` reason branch
      // was dead code — the app never sends that reason (it only sends
      // segment_403/404/timeout / playback_error). Backend now INFERS it:
      // a dead-URL report against a Hindi-lane provider (vidzee, fzmovies,
      // moviebox-hi) on the Hindi/default chain means the Hindi dub itself
      // is gone — evict pf:lang + pf:mbhilang so the dub button re-probes
      // instead of lying for 6h/24h. The explicit reason still works if the
      // app ever sends it.
      const HINDI_LANE = new Set(["vidzee", "fzmovies", "moviebox-hi"]);
      const hindiGone =
        b.reason === "hindi_no_longer_available" ||
        (providerName !== null &&
          HINDI_LANE.has(providerName) &&
          (audio === undefined || audio === "hi"));
      // Phase D (2026-10-09, design §4a): a 403/404 is signature death
      // (not a network blip) — the lang envelope for this title must be
      // re-probed immediately instead of lying for 12h.
      const sigDeath = b.reason === "segment_403" || b.reason === "segment_404";
      if (hindiGone || sigDeath) {
        const langKey = ck("lang", type, id, season ?? 0, episode ?? 0);
        await redisCommand(["DEL", langKey]).catch(() => null);
        // Same memory-drop for the lang entry (chain.ts caches pf:v3:lang:*
        // in-memory under the identical key).
        cacheDel(langKey);
        // The MovieBox-Hindi verdict may be stale now too (the dub is
        // reported gone) — drop it so the next /languages re-probes
        // instead of trusting yesterday's "1" for 24h.
        await redisCommand(["DEL", mbhilangKey(id, type === "tv" ? "tv" : "movie")]).catch(() => null);
      }
      return send(res, 200, ok({ evicted: true, deadReportsToday: deadCount }));
    }

    // NiaziTV Turkish dramas (stream URLs NEVER cached — signed/expiring)
    if (path === "/v1/niazi/series") {
      const data = await getSeries();
      return sendCatalog(res, req, ok(data), privateCache(NIAZI_TTL.series));
    }
    if ((m = path.match(/^\/v1\/niazi\/series\/([^/]+)\/seasons$/))) {
      const data = await getSeasons(m[1]);
      return sendCatalog(res, req, ok(data), privateCache(NIAZI_TTL.seasons));
    }
    if ((m = path.match(/^\/v1\/niazi\/series\/([^/]+)\/episodes$/))) {
      // Series id (NOT season id — aggregates every season page).
      const data = await getEpisodesForSerie(m[1]);
      return sendCatalog(res, req, ok(data), privateCache(NIAZI_TTL.episodes));
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
        // P1-4 (2026-10-08): youtube-type channels are never playable
        // in-app — say so honestly instead of "awaiting-source".
        reason: c.type === "youtube" ? "youtube-only" : "awaiting-source",
      }));
      return sendCatalog(res, req, ok({
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
    // Stream pre-warm (P1 2026-10-09): resolves the top ~200 TMDB trending
    // titles into the shared stream cache. CRON_SECRET protected,
    // header-only (same pattern as /v1/cron/fz-warm).
    // Bounded: a Redis cursor cycles through the list — `limit` titles per
    // run (default 40, max 60), 4-way concurrency, hard 50s deadline — so the
    // invocation ALWAYS completes inside Vercel's 60s maxDuration.
    // Usage: GET /v1/cron/stream-warm?limit=40  (x-cron-secret header)
    if (path === "/v1/cron/stream-warm") {
      const secret = header("x-cron-secret");
      if (!process.env.CRON_SECRET || secret !== process.env.CRON_SECRET) {
        return send(res, 401, fail("unauthorized", "BAD_CRON_SECRET"));
      }
      const rawLimit = parseInt(q.get("limit") || "40", 10);
      const limit = Math.min(Math.max(Number.isFinite(rawLimit) ? rawLimit : 40, 1), 60);
      const report = await warmTrendingStreams(limit);
      return send(res, 200, ok(report));
    }

    // FZMovies batch warm (2026-10-09): cursor-based top-200 movie cycle for
    // the cache-only FZMovies Hindi tier. CRON_SECRET protected, header-only.
    // Called every 6h from a Hatch cron (api1 only — Redis is shared;
    // Phase D 2026-10-09: 4h → 6h, 30 titles/run per the command budget).
    // P1-4 (2026-10-09): throughput raised — 6 workers, 5–10s gaps only after
    // real scrapes (fresh entries skip in ~1 Redis read).
    // Usage: GET /v1/cron/fz-warm-batch?limit=30  (x-cron-secret header)
    if (path === "/v1/cron/fz-warm-batch") {
      const secret = header("x-cron-secret");
      if (!process.env.CRON_SECRET || secret !== process.env.CRON_SECRET) {
        return send(res, 401, fail("unauthorized", "BAD_CRON_SECRET"));
      }
      const rawLimit = parseInt(q.get("limit") || "16", 10);
      const limit = Math.min(Math.max(Number.isFinite(rawLimit) ? rawLimit : 16, 1), 30);
      const report = await warmFZMoviesBatch(limit);
      return send(res, 200, ok(report));
    }

    return send(res, 404, fail("not found", "NOT_FOUND"));
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    let code = "UPSTREAM_ERROR";
    let status = 500;
    let error = msg; // P2-2-style: some errors get a plain-language message
    const headers: Record<string, string> = {};
    if (e instanceof NotAvailableError) {
      // Win #2 (2026-10-09): honest fast-fail — a recent attempt already
      // proved no source has this title. 404 (not 502) so the app's retry
      // interceptor doesn't re-fire a known-dead title.
      code = "NOT_AVAILABLE";
      status = 404;
      error = "This title is not available on any source right now";
      if ((e as unknown as { negCacheHit?: boolean }).negCacheHit) {
        headers["X-Cache"] = "NEG";
      }
    } else if (e instanceof ChainDeadlineError) {
      // P0-1 (2026-10-09): the 45s chain deadline fired — honest fast
      // failure instead of a hung connection (Vercel would hard-kill at
      // 60s with no response at all). The client should retry; a stale
      // cache entry or the warm cron will usually serve next time.
      code = "RESOLVE_TIMEOUT";
      status = 504;
      error = "Stream search timed out — please try again";
    } else if (msg.includes("request body too large")) {
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
    } else if (msg.startsWith("series not found")) {
      // P1-3 (2026-10-08): valid-format but unknown Niazi series id is a
      // client error, not a server error.
      code = "SERIES_NOT_FOUND";
      status = 404;
    } else if (/^TMDB HTTP \d+$/.test(msg)) {
      // P1-2 (2026-10-08): upstream TMDB errors are never OUR 500.
      // 4xx passes through as-is (400/404/...); 5xx becomes 502
      // (upstream failed, not us).
      const upstream = parseInt(msg.slice("TMDB HTTP ".length), 10);
      code = "UPSTREAM_ERROR";
      status = upstream >= 400 && upstream < 500 ? upstream : 502;
    }
    return send(res, status, fail(error, code), headers);
  }
}
