/**
 * 5-provider chain with PARALLEL racing + circuit breaker + health tracking.
 *
 * CRITICAL: Vercel maxDuration is 60s (verified 2026-10-08 on both clusters).
 * Sequentially trying 5 providers would exceed it. We race the top-3
 * healthiest providers in parallel with an 8s overall budget and take the
 * first success.
 *
 * Chain (final plan v1.0 — NHD dead, 111Movies unverified, both removed):
 *   1. VidLink  ✅ verified 2026-10-08
 *   2. VaPlayer  ❌ DEAD — verified live 2026-10-09: streamdata.vaplayer.ru/api.php
 *      404s, vaplayer.ru pivoted to a "PlayBox" video-upload site, embed host
 *      unreachable. NOT added to the race (honest report, not forced).
 *      Stub kept so the chain degrades gracefully if it ever comes back.
 *   3. VidRock   (stub — Phase 1b)
 *   4. VidSrc    (stub — Phase 1b)
 *   5. ScreenScape (stub — Phase 1b)
 *
 * Circuit breaker: 5 consecutive fails -> 5 min cooldown.
 */
import { vidlink } from "./providers/vidlink.js";
import { vaplayer } from "./providers/vaplayer.js";
import { vidzee } from "./providers/vidzee.js";
import { fzmovies } from "./providers/fzmovies.js";
import { tmdb } from "./tmdb.js";
import type { ProviderFn, ProviderResult } from "./providers/types.js";
import { NotAvailableError } from "./providers/types.js";
import { toProviderFailure, type FailureClass } from "./providers/failures.js";
import { redisCommand } from "./security/redis.js";
import { resolveStreamCached } from "./streamcache.js";

// ── Stubs (Phase 1b — return null so chain skips them) ───────────────────────
const notYet = (_name: string): ProviderFn => async () => {
  return null;
};

/** VaPlayer needs IMDb ID — resolve from TMDB (cached 24h). */
const vaplayerWithImdb: ProviderFn = async (tmdbId, type, season, episode) => {
  try {
    const details = (await (type === "movie" ? tmdb.movie(tmdbId) : tmdb.tv(tmdbId))) as any;
    const imdbId: string | undefined = details?.imdb_id;
    if (!imdbId) return null;
    return vaplayer(tmdbId, type, season, episode, imdbId);
  } catch {
    return null;
  }
};

interface ProviderEntry {
  name: string;
  fn: ProviderFn;
  /** Unimplemented providers: "no result" is not a failure — don't trip circuits. */
  stub?: boolean;
}

// ── Health tracking ─────────────────────────────────────────────────────────
interface Health {
  success: number;
  fail: number;
  consecutiveFails: number;
  lastFailAt: number;
  totalLatencyMs: number;
  samples: number;
  /** Classification of the most recent classified failure (null if none). */
  lastClass: FailureClass | null;
  /** Successful NETWORK/SERVER second-attempt retries. */
  retries: number;
}

const health = new Map<string, Health>();
function h(name: string): Health {
  let x = health.get(name);
  if (!x) {
    x = { success: 0, fail: 0, consecutiveFails: 0, lastFailAt: 0, totalLatencyMs: 0, samples: 0, lastClass: null, retries: 0 };
    health.set(name, x);
  }
  return x;
}

const CIRCUIT_FAILS = 5;
const CIRCUIT_COOLDOWN_MS = 5 * 60_000;

function circuitOpen(name: string): boolean {
  const x = h(name);
  return x.consecutiveFails >= CIRCUIT_FAILS && Date.now() - x.lastFailAt < CIRCUIT_COOLDOWN_MS;
}

function recordSuccess(name: string, latencyMs: number): void {
  const x = h(name);
  x.success++;
  x.consecutiveFails = 0;
  x.totalLatencyMs += latencyMs;
  x.samples++;
}

/**
 * Record a failure. `cls` is the failure classification when the failure
 * came from a classified ProviderFailure; omitted for plain null-misses
 * (existing behavior preserved). When this failure just opens the
 * in-memory circuit, the cooldown is mirrored to Redis so EVERY instance
 * (api1 + api2) skips the provider — the cross-instance truth.
 */
function recordFail(name: string, cls?: FailureClass): void {
  const x = h(name);
  const wasOpen = circuitOpen(name);
  x.fail++;
  x.consecutiveFails++;
  x.lastFailAt = Date.now();
  if (cls) x.lastClass = cls;
  if (!wasOpen && circuitOpen(name) && cls && cls !== "content_miss") {
    // Fail-open: Redis errors are swallowed inside setProviderCooldown.
    void setProviderCooldown(name, cls).catch(() => {});
  }
}

export function providerHealth(): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [name, x] of health) {
    const total = x.success + x.fail;
    out[name] = {
      successRate: total ? +(x.success / total).toFixed(3) : null,
      avgLatencyMs: x.samples ? Math.round(x.totalLatencyMs / x.samples) : null,
      consecutiveFails: x.consecutiveFails,
      circuitOpen: circuitOpen(name),
      lastFailureClass: x.lastClass,
      retries: x.retries,
    };
  }
  return out;
}

// ── Redis provider cooldowns (shared across api1/api2) ─────────────────────
// Key: pf:cooldown:{provider}:{class}, 5-min TTL. The in-memory circuit
// breaker above is the fast path; these keys are the cross-instance truth —
// when one instance cools a provider (429, or 5 consecutive failures),
// every instance skips it. Fail-open: Redis down -> in-memory only.
const COOLDOWN_TTL_SEC = 300;
const COOLDOWN_CLASSES = ["network", "not_found", "forbidden", "rate_limited", "server"];

/** Set a provider cooldown. Never throws (fail-open). */
export async function setProviderCooldown(
  provider: string,
  cls: FailureClass,
  retryAfterMs?: number
): Promise<void> {
  if (cls === "content_miss") return; // never cooled
  // RATE_LIMITED honors Retry-After as a LOWER bound: the provider asked for
  // e.g. 120s, so we cool at least that long (min 5 min per the key design).
  const ttl =
    cls === "rate_limited" && retryAfterMs
      ? Math.max(COOLDOWN_TTL_SEC, Math.ceil(retryAfterMs / 1000))
      : COOLDOWN_TTL_SEC;
  const value = retryAfterMs ? String(retryAfterMs) : "1";
  await redisCommand(["SET", `pf:cooldown:${provider}:${cls}`, value, "EX", ttl]).catch(() => null);
}

const COOLDOWN_CHECK_SCRIPT = `
local out = {}
for i = 1, #KEYS do
  local p = KEYS[i]
  for _, c in ipairs(ARGV) do
    if redis.call('EXISTS', 'pf:cooldown:' .. p .. ':' .. c) == 1 then
      out[#out + 1] = p
      break
    end
  end
end
return out
`;

/**
 * Providers currently under a Redis cooldown (any class). ONE Lua EVAL for
 * all names. Fail-open: returns an empty set when Redis is unavailable.
 */
export async function redisProviderCooldowns(names: string[]): Promise<Set<string>> {
  if (names.length === 0) return new Set();
  try {
    const r = await redisCommand(["EVAL", COOLDOWN_CHECK_SCRIPT, names.length, ...names, ...COOLDOWN_CLASSES]);
    if (!Array.isArray(r)) return new Set();
    return new Set(r.map(String));
  } catch {
    return new Set();
  }
}

/** All provider names known to the chain (for cooldown checks + /health). */
export const PROVIDER_NAMES = [
  "vidlink",
  "vaplayer",
  "vidrock",
  "vidsrc",
  "screenscape",
  "vidzee",
  "fzmovies",
];

/** Live cooldown list for /health (fail-open). */
export async function providerCooldowns(): Promise<string[]> {
  return [...(await redisProviderCooldowns(PROVIDER_NAMES))];
}

/**
 * True when a provider must be skipped: in-memory circuit open OR under a
 * Redis cooldown set by any instance. Fail-open on Redis errors.
 */
export async function providerBlocked(name: string): Promise<boolean> {
  if (circuitOpen(name)) return true;
  return (await redisProviderCooldowns([name])).has(name);
}

// ── Chain ───────────────────────────────────────────────────────────────────
const PROVIDERS: ProviderEntry[] = [
  { name: "vidlink", fn: vidlink },
  { name: "vaplayer", fn: vaplayerWithImdb, stub: true },
  { name: "vidrock", fn: notYet("vidrock"), stub: true },
  { name: "vidsrc", fn: notYet("vidsrc"), stub: true },
  { name: "screenscape", fn: notYet("screenscape"), stub: true },
];

async function rankProviders(): Promise<ProviderEntry[]> {
  // Cross-instance truth: skip providers under a Redis cooldown set by ANY
  // instance (fail-open — Redis down means the in-memory circuit only).
  const cooled = await redisProviderCooldowns(PROVIDERS.map((p) => p.name));
  return PROVIDERS.filter((p) => !circuitOpen(p.name) && !cooled.has(p.name)).sort((a, b) => {
    const ha = h(a.name);
    const hb = h(b.name);
    const ra = ha.success + ha.fail ? ha.success / (ha.success + ha.fail) : 0.5;
    const rb = hb.success + hb.fail ? hb.success / (hb.success + hb.fail) : 0.5;
    // success rate desc, then avg latency asc
    if (rb !== ra) return rb - ra;
    const la = ha.samples ? ha.totalLatencyMs / ha.samples : Infinity;
    const lb = hb.samples ? hb.totalLatencyMs / hb.samples : Infinity;
    return la - lb;
  });
}

const RACE_COUNT = 3;
const OVERALL_BUDGET_MS = 8000;

async function tryProvider(
  p: ProviderEntry,
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number,
  /** When false, skip health recording (race losers that finished after
   *  the winner was returned must not pollute provider stats). */
  recordStats: () => boolean = () => true
): Promise<ProviderResult | null> {
  const t0 = Date.now();
  const ok = (r: ProviderResult | null): ProviderResult | null => {
    if (recordStats()) {
      if (r && r.qualities.length > 0) recordSuccess(p.name, Date.now() - t0);
      else if (!p.stub) recordFail(p.name);
    }
    return r && r.qualities.length > 0 ? r : null;
  };
  try {
    return ok(await p.fn(tmdbId, type, season, episode));
  } catch (e) {
    // "Not available" (e.g. VidZee 404/502 = no Hindi for this title) is a
    // correct provider response, NOT a failure. Don't trip the circuit
    // breaker — otherwise 5x "no Hindi" would block Hindi for everyone.
    if (e instanceof NotAvailableError) return null;
    const pf = toProviderFailure(p.name, e);
    if (pf.cls === "content_miss") return null;
    // Race losers and stubs: don't count, don't retry, don't cool down.
    if (!recordStats() || p.stub) return null;
    switch (pf.cls) {
      // NOT_FOUND / FORBIDDEN: retry is pointless -> next provider now.
      case "not_found":
      case "forbidden":
        recordFail(p.name, pf.cls);
        return null;
      // RATE_LIMITED: no in-request retry. Cool the provider cluster-wide
      // NOW, honoring Retry-After (min 5 min), so every instance backs off.
      case "rate_limited":
        recordFail(p.name, pf.cls);
        await setProviderCooldown(p.name, "rate_limited", pf.retryAfterMs).catch(() => {});
        return null;
      default: {
        // NETWORK / SERVER: exactly ONE retry, then next provider.
        // The stats gate is re-checked after the retry: if the race settled
        // while we retried, this provider lost — don't pollute /health.
        try {
          const r2 = await p.fn(tmdbId, type, season, episode);
          if (r2 && r2.qualities.length > 0) {
            if (recordStats()) {
              h(p.name).retries++;
              recordSuccess(p.name, Date.now() - t0);
            }
            return r2;
          }
          if (recordStats()) recordFail(p.name, pf.cls);
          return null;
        } catch (err2) {
          const pf2 = toProviderFailure(p.name, err2);
          if (recordStats()) {
            recordFail(p.name, pf2.cls);
            if (pf2.cls === "rate_limited") {
              await setProviderCooldown(p.name, "rate_limited", pf2.retryAfterMs).catch(() => {});
            }
          }
          return null;
        }
      }
    }
  }
}

export interface ChainResult extends ProviderResult {
  resolvedBy: string;
  latencyMs: number;
}

/**
 * Resolve a stream. Races the top-3 healthiest providers in parallel,
 * returns the first success within the overall budget.
 *
 * Hindi chain (Ali 2026-10-08): VidZee → FZMovies → (VidLink English).
 * FZMovies is a PROPER Hindi tier, not a backup. Its 4-hop scrape takes
 * 35-60s, so the request path only reads its cache; warming happens ONLY via
 * /v1/cron/fz-warm (bounded to the 60s maxDuration). Fire-and-forget
 * "background" warming was removed — it cannot complete on serverless.
 *
 * @param audio "hi" = Hindi (VidZee → FZMovies cache → VidLink when the
 *   original language itself is Hindi, e.g. Bollywood titles; throws
 *   HINDI_UNAVAILABLE when missing — no silent fallback so the app can show
 *   an honest message).
 *   "en" = English/original only (skips Hindi tiers). Omitted = Hindi-first:
 *   VidZee → FZMovies cache → English race chain.
 */
export async function resolveStreamLive(
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number,
  audio?: string
): Promise<ChainResult> {
  const t0 = Date.now();

  // Explicit Hindi request (language switcher): VidZee → FZMovies cache.
  if (audio === "hi") {
    const hindiEntry: ProviderEntry = { name: "vidzee", fn: vidzee };
    if (!(await providerBlocked(hindiEntry.name))) {
      const hindi = await tryProvider(hindiEntry, tmdbId, type, season, episode);
      if (hindi) {
        return { ...hindi, resolvedBy: hindiEntry.name, latencyMs: Date.now() - t0 };
      }
    }
    // FZMovies Hindi tier (cache-only in request path — never blocks).
    const fzEntry: ProviderEntry = { name: "fzmovies", fn: fzmovies };
    const fz = await tryProvider(fzEntry, tmdbId, type, season, episode);
    if (fz) {
      return { ...fz, resolvedBy: fzEntry.name, latencyMs: Date.now() - t0 };
    }
    // Bollywood/Hindi-original titles: VidLink serves the Hindi ORIGINAL, so
    // ?audio=hi is satisfiable even with no dubbed copy (Ali 2026-10-08:
    // Drishyam showed "English" — the original IS Hindi).
    if ((await originalLanguage(tmdbId, type)) === "hi" && !(await providerBlocked("vidlink"))) {
      const vl = await tryProvider({ name: "vidlink", fn: vidlink }, tmdbId, type, season, episode);
      if (vl) {
        return { ...vl, resolvedBy: "vidlink", latencyMs: Date.now() - t0 };
      }
    }
    // Miss: honest error now. The title can be warmed via /v1/cron/fz-warm
    // (request-path background warming cannot complete on serverless).
    throw new Error("hindi dubbed not available");
  }

  const skipHindi = audio === "en";
  const hindiEntry: ProviderEntry = { name: "vidzee", fn: vidzee };
  if (!skipHindi && !(await providerBlocked(hindiEntry.name))) {
    // Hindi-first (Ali 2026-10-08): try VidZee Hindi-dubbed before the English chain.
    // VidZee has a short internal fetch timeout (7s) so the 60s budget stays
    // safe; on miss we fall through to FZMovies cache, then chain.
    const hindi = await tryProvider(hindiEntry, tmdbId, type, season, episode);
    if (hindi) {
      return { ...hindi, resolvedBy: hindiEntry.name, latencyMs: Date.now() - t0 };
    }
    // FZMovies Hindi tier (cache-only — instant).
    const fzEntry: ProviderEntry = { name: "fzmovies", fn: fzmovies };
    const fz = await tryProvider(fzEntry, tmdbId, type, season, episode);
    if (fz) {
      return { ...fz, resolvedBy: fzEntry.name, latencyMs: Date.now() - t0 };
    }
    // Cache miss: the English chain serves now; Hindi warms via cron only.
  }

  const ranked = await rankProviders();
  if (ranked.length === 0) throw new Error("all providers in cooldown");

  const racers = ranked.slice(0, RACE_COUNT);
  const rest = ranked.slice(RACE_COUNT);

  // Stats gate: providers that finish AFTER the race settled (timeout or a
  // faster winner) must not record success/fail — their numbers describe a
  // request the user never saw and skew /health rankings.
  const raceBox = { settled: false };
  const gate = () => !raceBox.settled;

  const attempt = async (list: ProviderEntry[]): Promise<ProviderResult | null> => {
    // True race: resolve on the FIRST success, not when the slowest racer
    // finishes. Losers keep running but their stats are dropped by the gate.
    return new Promise((resolve) => {
      let pending = list.length;
      let done = false;
      for (const p of list) {
        tryProvider(p, tmdbId, type, season, episode, gate).then((r) => {
          if (done) return;
          if (r) {
            done = true;
            resolve(r);
          } else if (--pending === 0) {
            done = true;
            resolve(null);
          }
        });
      }
      if (list.length === 0) resolve(null);
    });
  };

  // Round 1: race top-3 in parallel with overall budget
  const winner = await Promise.race([
    attempt(racers).then((r) => {
      raceBox.settled = true;
      return r;
    }),
    new Promise<null>((res) =>
      setTimeout(() => {
        raceBox.settled = true;
        res(null);
      }, OVERALL_BUDGET_MS)
    ),
  ]);

  let result = winner;
  // Round 2: if round 1 failed, try the rest sequentially. Each provider is
  // capped by the REMAINING overall budget — the old code only checked the
  // budget *between* providers, so one slow provider could blow past it.
  if (!result) {
    for (const p of rest) {
      const remaining = OVERALL_BUDGET_MS + 1500 - (Date.now() - t0);
      if (remaining <= 0) break;
      result = await Promise.race([
        tryProvider(p, tmdbId, type, season, episode),
        new Promise<null>((res) => setTimeout(() => res(null), Math.min(remaining, 9000))),
      ]);
      if (result) break;
    }
  }

  if (!result) throw new Error("all providers failed");
  return { ...result, resolvedBy: result.provider, latencyMs: Date.now() - t0 };
}

/**
 * Cached stream resolution (P1 2026-10-09) — the entry point every caller
 * uses. Successful resolutions are served from the Redis-shared stream
 * cache (provider-specific TTLs, stale-while-revalidate) and concurrent
 * in-flight resolutions for the same title collapse into one upstream
 * resolve. Only successes are cached — errors always go live.
 */
export async function resolveStream(
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number,
  audio?: string
): Promise<ChainResult> {
  return resolveStreamCached({ tmdbId, type, season, episode, audio }, () =>
    resolveStreamLive(tmdbId, type, season, episode, audio)
  );
}

/**
 * Display names for ISO 639-1 language codes (Ali 2026-10-08: dub button must
 * show the movie's REAL original language, e.g. Hindi for Bollywood titles —
 * not a hardcoded "English").
 */
export const LANG_LABELS: Record<string, string> = {
  hi: "Hindi", en: "English", ur: "Urdu", pa: "Punjabi",
  te: "Telugu", ta: "Tamil", ml: "Malayalam", kn: "Kannada",
  bn: "Bengali", mr: "Marathi", gu: "Gujarati",
  ko: "Korean", ja: "Japanese", zh: "Chinese",
  es: "Spanish", fr: "French", de: "German", it: "Italian",
  pt: "Portuguese", ru: "Russian", ar: "Arabic", fa: "Persian",
  tr: "Turkish", id: "Indonesian", ms: "Malay", th: "Thai", vi: "Vietnamese",
};

/** TMDB original_language for a title (cached 24h by tmdb.movie/tv). */
export async function originalLanguage(
  tmdbId: string,
  type: "movie" | "tv"
): Promise<string> {
  try {
    const details = (await (type === "movie" ? tmdb.movie(tmdbId) : tmdb.tv(tmdbId))) as any;
    const code = details?.original_language;
    return typeof code === "string" && code.length >= 2 ? code : "en";
  } catch {
    return "en";
  }
}

export interface AudioInfo {
  /** TMDB original_language, e.g. "hi" for Drishyam, "en" for Avengers. */
  original: string;
  /** Available audio codes, Hindi-first. The original language is ALWAYS
   *  included (VidLink serves the original track). */
  audio: string[];
  /** Code of the track that plays by default ("hi" when Hindi is available). */
  playing: string;
  /** Display names for every code in `audio`. */
  labels: Record<string, string>;
}

function labelFor(code: string): string {
  return LANG_LABELS[code] || code.toUpperCase();
}

/**
 * Available audio languages for a title (Ali 2026-10-08: dub button must show
 * ONLY languages that are actually available, not a hardcoded list — AND it
 * must show the movie's REAL original language, e.g. Hindi for Bollywood).
 *
 * - The original language is ALWAYS available (VidLink serves the original).
 * - "hi" is added when the original is not Hindi AND VidZee has Hindi-dubbed
 *   (fast check, 3.5s internal timeout) OR the FZMovies cache has it (instant
 *   memory lookup).
 * - When the original IS Hindi (e.g. Drishyam), "hi" covers both the original
 *   and any dub — no duplicate entry, no fake "English".
 *
 * Budget: ~8s max — fits the Vercel 60s window with room to spare.
 * Returns e.g. { original: "hi", audio: ["hi"], playing: "hi" } for Drishyam,
 * or { original: "en", audio: ["hi", "en"], playing: "hi" } for Avengers.
 */
export async function availableAudio(
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number
): Promise<AudioInfo> {
  const original = await originalLanguage(tmdbId, type);

  // Hindi-dub check only matters when the original is not already Hindi.
  let hindiDub = original === "hi";
  if (!hindiDub) {
    const hindiEntry: ProviderEntry = { name: "vidzee", fn: vidzee };
    const fzEntry: ProviderEntry = { name: "fzmovies", fn: fzmovies };

    // Parallel: VidZee live check + FZMovies cache check. Either hit = Hindi.
    const checks: Promise<boolean>[] = [];
    if (!(await providerBlocked(hindiEntry.name))) {
      checks.push(
        (async () => {
          try {
            const r = await vidzee(tmdbId, type, season, episode);
            return !!(r && r.qualities.length > 0);
          } catch {
            return false;
          }
        })()
      );
    }
    // FZMovies cache-only check is instant and never throws meaningfully.
    checks.push(
      (async () => {
        try {
          const r = await fzmovies(tmdbId, type);
          return !!(r && r.qualities.length > 0);
        } catch {
          return false;
        }
      })()
    );

    const results = await Promise.race([
      Promise.all(checks),
      new Promise<boolean[]>((res) => setTimeout(() => res([false]), 8000)),
    ]);
    hindiDub = results.some(Boolean);
  }

  // Hindi-first (Ali's standing rule), then the original. Deduped.
  const audio: string[] = [];
  if (hindiDub && !audio.includes("hi")) audio.push("hi");
  if (!audio.includes(original)) audio.push(original);

  const labels: Record<string, string> = {};
  for (const code of audio) labels[code] = labelFor(code);

  return { original, audio, playing: hindiDub ? "hi" : original, labels };
}
