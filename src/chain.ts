/**
 * 4-source parallel chain with LANGUAGE LANES + circuit breaker + health.
 *
 * Architecture (Ali 2026-10-09 — 4-source parallel system):
 *   L0 stream cache (streamcache.ts, unchanged)
 *   miss -> L1 HINDI LANE (8s): vidzee + fzmovies — PARALLEL, first wins
 *   miss -> MovieBox-hi SEQUENTIAL tier (cyber rule: wrapper leg NEVER in a
 *           race — paced 1 req/5s, max 1 concurrent, kill-switched)
 *   miss -> L2 ENGLISH LANE (8s): vidlink
 *   miss -> MovieBox-en SEQUENTIAL tier
 *   miss -> honest error (never cached)
 *
 * Why lanes, not one flat race: a flat race lets VidLink English (~1-2s)
 * beat VidZee Hindi (~3s) on titles that HAVE Hindi — Hindi-first breaks.
 * Lanes = language protection; INSIDE a lane every source is equal and
 * parallel (no backup hierarchy).
 *
 * ?audio=hi -> L1 only (+ Bollywood-original VidLink check, then honest
 *   "hindi dubbed not available" — no silent English fallback).
 * ?audio=en -> L2 only. Omitted -> Hindi-first: L1 -> MB-hi -> L2 -> MB-en.
 *
 * Circuit breaker: 5 consecutive fails -> 5 min cooldown (in-memory fast
 * path + Redis cross-instance mirror). 429s are RateLimitedError — Redis
 * cooldown only, NEVER a circuit failure.
 */
import { vidlink } from "./providers/vidlink.js";
import { VidLinkKeyDeadError } from "./providers/vidlink.js";
import { vidzee } from "./providers/vidzee.js";
import { fzmovies } from "./providers/fzmovies.js";
import { moviebox, movieboxHindi, movieboxHindiKnownMissing, movieboxHindiVerdict, movieboxLive, noteMovieboxHindi, wrapperStatus } from "./providers/moviebox.js";
import { tmdb } from "./tmdb.js";
import type { ProviderFn, ProviderResult } from "./providers/types.js";
import { NotAvailableError } from "./providers/types.js";
import {
  toProviderFailure,
  RateLimitedError,
  type FailureClass,
} from "./providers/failures.js";
import { redisEnabled, redisCacheGet, redisCacheSet, redisCommand } from "./security/redis.js";
import { cacheGet, cacheSet } from "./cache.js";
import { cachedStreamProviders, resolveStreamCached } from "./streamcache.js";
import {
  raceTier,
  rankAlternates,
  type RaceMode,
  type RaceOpts,
  type RankedAlternate,
  type TierEntry,
  type TierError,
  type TierResult,
} from "./race.js";
import { background } from "./revalidate.js";

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
 * came from a classified ProviderFailure; omitted for plain null-misses.
 * When this failure just opens the in-memory circuit, the cooldown is
 * mirrored to Redis so EVERY instance (api1 + api2) skips the provider.
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

// ── VidLink key-death corroboration (P0-2, refined live 2026-10-09) ─────────
// A single 200+null body is NOT key-death (live-proven: valid key nulls
// individual titles it can't serve, e.g. Interstellar/157336, while
// Matrix/603 streams fine). A DEAD key nulls EVERYTHING — so we count
// DISTINCT tmdbIds nulling inside a fixed 10-minute bucket and only call
// it key-death at 3+. An isolated null stays a silent miss (the old,
// correct behavior). Fail-safe: without Redis we can't corroborate, so we
// never cool (a lone instance must not take down the English lane).
const VLNULL_BUCKET_MS = 10 * 60_000;
const VLNULL_DISTINCT_THRESHOLD = 3;

/**
 * Record a VidLink null-body for `tmdbId`. Returns true when 3+ distinct
 * titles nulled inside the current 10-minute bucket — i.e. the key is
 * (almost certainly) dead. Never throws (fail-safe -> false).
 */
async function vidlinkNullCorroborated(tmdbId: string): Promise<boolean> {
  if (!redisEnabled()) return false;
  try {
    const bucket = Math.floor(Date.now() / VLNULL_BUCKET_MS);
    const k = `pf:vlnull:${bucket}`;
    await redisCommand(["SADD", k, String(tmdbId)]);
    await redisCommand(["EXPIRE", k, 1200]);
    const n = await redisCommand(["SCARD", k]);
    return Number(n) >= VLNULL_DISTINCT_THRESHOLD;
  } catch {
    return false;
  }
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
  "moviebox",
  "moviebox-hi",
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

// ── Lanes ───────────────────────────────────────────────────────────────────
/**
 * P0-1 (2026-10-09): hard global deadline for the whole stream-resolution
 * path (lanes + wrapper tiers). Vercel kills invocations at 60s (maxDuration)
 * with NO response at all — the app sees a hung connection. The chain throws
 * ChainDeadlineError at 45s and the route maps it to an honest 504, NEVER a
 * hang. Best-effort caching is the answer for speed: resolveStreamCached
 * serves fresh/stale hits BEFORE live resolution, so only genuinely
 * uncached titles ever run the live path this deadline guards.
 */
export const CHAIN_DEADLINE_MS = Math.max(
  5000,
  parseInt(process.env.CHAIN_DEADLINE_MS || "45000", 10) || 45000
);

export class ChainDeadlineError extends Error {
  readonly elapsedMs: number;
  constructor(elapsedMs: number) {
    super(
      `stream resolution exceeded the ${CHAIN_DEADLINE_MS / 1000}s deadline (${elapsedMs}ms)`
    );
    this.name = "ChainDeadlineError";
    this.elapsedMs = elapsedMs;
  }
}

/** Race mode: "speed" (first success wins) or "quality" (best of the lane). */
export const RACE_MODE: RaceMode =
  process.env.RACE_MODE === "quality" ? "quality" : "speed";

const HINDI_LANE_BUDGET_MS = 8000;
const ENGLISH_LANE_BUDGET_MS = 8000;

/** L1 — Hindi lane: every source EQUAL, parallel, first success wins. */
const HINDI_LANE: TierEntry[] = [
  { name: "vidzee", fn: vidzee, audio: "hi" },
  { name: "fzmovies", fn: fzmovies, audio: "hi" }, // cache-only — race me "free"
];

/** L2 — English lane. */
const ENGLISH_LANE: TierEntry[] = [{ name: "vidlink", fn: vidlink }];

/**
 * One provider attempt inside a lane (race.ts's TryOneFn).
 *
 * Abort fix (2026-10-09): a loser aborted by the lane settles as a SILENT
 * null — no fail count, no retry, no circuit. The retry path carries the
 * race signal (the old code forgot it, so aborted losers retried).
 * Miss (null) is not a failure. 429 (RateLimitedError) cools the provider
 * cluster-wide but never touches the circuit.
 */
export async function tryProvider(
  p: TierEntry,
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number,
  recordStats: () => boolean = () => true,
  raceOpts?: RaceOpts
): Promise<ProviderResult | null> {
  const t0 = Date.now();
  const signal = raceOpts?.signal;
  const onError = (reason: string): void => {
    if (raceOpts?.onError) raceOpts.onError(p.name, reason);
  };
  const fail = (reason: string): null => {
    onError(reason);
    if (recordStats() && !p.stub) recordFail(p.name);
    return null;
  };
  /** 429 handling shared by the first attempt and the retry path. */
  const rateLimited = async (e: RateLimitedError): Promise<null> => {
    // Cluster-wide backoff via Redis cooldown. NEVER a circuit failure —
    // the provider is healthy, just asking us to slow down. lastClass is
    // still recorded for /health observability.
    if (recordStats()) h(p.name).lastClass = "rate_limited";
    await setProviderCooldown(p.name, "rate_limited", e.retryAfterMs).catch(() => {});
    onError(`http 429${e.retryAfterMs ? ` (retry-after ${e.retryAfterMs}ms)` : ""}`);
    return null;
  };
  try {
    const r = await p.fn(tmdbId, type, season, episode, { signal });
    if (signal?.aborted) return null;
    if (r && r.qualities.length > 0) {
      if (recordStats()) recordSuccess(p.name, Date.now() - t0);
      return r;
    }
    onError("miss: no qualities");
    return null; // miss is NOT a failure
  } catch (e) {
    // Loser abort: expected, invisible. (raceTier's ctrl.abort() makes
    // in-flight fetches reject with AbortError.)
    if (signal?.aborted || (e as Error)?.name === "AbortError") {
      onError("aborted (lost race)");
      return null;
    }
    if (e instanceof NotAvailableError) {
      onError("not-available");
      return null; // circuit does NOT trip (types.ts)
    }
    if (e instanceof RateLimitedError) return rateLimited(e);
    // P0-2 (2026-10-09, refined live): VidLink 200+null is the documented
    // dead-KEY shape — but a valid key also nulls individual titles it
    // can't serve (Interstellar nulled 3/3 while Matrix streamed on the
    // same key). So a lone null is a silent miss; only 3+ DISTINCT titles
    // nulling within 10 min corroborates key-death and cools the provider
    // cluster-wide (+ circuit count + daily dead-key counter for the
    // canary). This can never false-positive a whole lane on one title.
    if (e instanceof VidLinkKeyDeadError) {
      onError(`null body for ${e.tmdbId} (unconfirmed)`);
      if (!signal?.aborted && recordStats() && !p.stub) {
        const dead = await vidlinkNullCorroborated(e.tmdbId);
        if (dead) {
          onError("KEY-DEATH corroborated: 3+ distinct titles nulled in 10 min");
          recordFail(p.name, "server");
          await setProviderCooldown(p.name, "server").catch(() => {});
          const day = new Date().toISOString().slice(0, 10);
          const kkey = `pf:keydeath:${p.name}:${day}`;
          await redisCommand(["INCR", kkey]).catch(() => {});
          await redisCommand(["EXPIRE", kkey, 7 * 24 * 3600]).catch(() => {});
        }
      }
      return null;
    }
    const msg = e instanceof Error ? e.message : String(e);
    // Pacer congestion: the wrapper tier is serialized — treat as a miss,
    // not a failure (retrying would just queue behind the same congestion).
    if (/^pacer: timeout/.test(msg)) {
      onError("pacer timeout");
      return null;
    }
    const pf = toProviderFailure(p.name, e);
    if (pf.cls === "content_miss") {
      onError("content-miss");
      return null;
    }
    if (!recordStats() || p.stub) return null;
    switch (pf.cls) {
      // NOT_FOUND / FORBIDDEN: retry is pointless -> next provider now.
      case "not_found":
      case "forbidden":
        return fail(`http ${pf.status ?? pf.cls}`);
      // RATE_LIMITED: no in-request retry, no circuit. Cool cluster-wide.
      case "rate_limited":
        return rateLimited(
          new RateLimitedError(p.name, pf.message, pf.retryAfterMs)
        );
      default: {
        // NETWORK / SERVER: exactly ONE retry — WITH the race signal
        // (the old code forgot it), and never when the lane settled.
        if (signal?.aborted) return null;
        try {
          const r2 = await p.fn(tmdbId, type, season, episode, { signal });
          if (signal?.aborted) return null;
          if (r2 && r2.qualities.length > 0) {
            if (recordStats()) {
              h(p.name).retries++;
              recordSuccess(p.name, Date.now() - t0);
            }
            return r2;
          }
          return fail("retry: no qualities");
        } catch (err2) {
          if (signal?.aborted || (err2 as Error)?.name === "AbortError") return null;
          if (err2 instanceof RateLimitedError) return rateLimited(err2);
          const pf2 = toProviderFailure(p.name, err2);
          if (pf2.cls === "content_miss") {
            onError("content-miss");
            return null;
          }
          if (pf2.cls === "rate_limited") {
            return rateLimited(
              new RateLimitedError(p.name, pf2.message, pf2.retryAfterMs)
            );
          }
          if (recordStats()) {
            recordFail(p.name, pf2.cls);
            // (rate_limited handled above — never reaches recordFail)
          }
          onError(pf2.message);
          return null;
        }
      }
    }
  }
}

// ── Chain ───────────────────────────────────────────────────────────────────
export interface ChainResult extends ProviderResult {
  resolvedBy: string;
  latencyMs: number;
  /** Runner-up results for client-side failover (envelope v2). */
  alternates: RankedAlternate[];
}

/**
 * Sequential MovieBox wrapper tier — CYBER RULE: the wrapper leg is NEVER
 * in a race. The provider itself paces (1 req/5s, max 1 concurrent) and
 * humanizes (800–2500ms gaps); the kill switch bypasses instantly.
 */
async function wrapperTier(
  name: "moviebox-hi" | "moviebox",
  fn: ProviderFn,
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number,
  /** P0-1: the chain's 45s deadline — aborts a congested wrapper tier. */
  signal?: AbortSignal
): Promise<ProviderResult | null> {
  if (signal?.aborted) return null;
  if (await providerBlocked(name)) return null;
  return tryProvider({ name, fn }, tmdbId, type, season, episode, () => true, { signal });
}

/**
 * Resolve a stream through the language lanes.
 *
 * ?audio=hi -> L1 Hindi race -> MovieBox-hi tier -> (Bollywood-original
 *   VidLink) -> honest "hindi dubbed not available" (no silent fallback).
 * ?audio=en -> L2 English race -> MovieBox-en tier -> aggregated error.
 * omitted   -> Hindi-first: L1 -> MB-hi -> L2 -> MB-en -> aggregated error.
 *
 * P0-1 (2026-10-09): a hard 45s deadline guards the whole path (lanes +
 * wrapper tiers). The deadline signal aborts lanes/tiers mid-flight and the
 * chain throws ChainDeadlineError between tiers — the route maps it to an
 * honest 504. Worst case can never reach Vercel's 60s hard-kill again.
 */
export async function resolveStreamLive(
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number,
  audio?: string
): Promise<ChainResult> {
  const t0 = Date.now();
  const alternates: RankedAlternate[] = [];
  const laneErrors: TierError[] = [];

  // P0-1: global 45s deadline. Firing it aborts the current lane/tier via
  // the signal; the between-tier checks below turn it into the throw.
  const deadline = new AbortController();
  const deadlineTimer = setTimeout(() => deadline.abort(), CHAIN_DEADLINE_MS);
  const dsignal = deadline.signal;
  const throwIfDeadline = (): void => {
    if (dsignal.aborted) throw new ChainDeadlineError(Date.now() - t0);
  };

  /** Run one lane; collect runner-ups for the envelope's alternates. */
  const runLane = async (
    lane: TierEntry[],
    budgetMs: number,
    signal: AbortSignal
  ): Promise<ProviderResult | null> => {
    const tr: TierResult = await raceTier(
      lane,
      tmdbId,
      type,
      season,
      episode,
      tryProvider,
      providerBlocked,
      { budgetMs, mode: RACE_MODE, parentSignal: signal }
    );
    laneErrors.push(...tr.errors);
    if (tr.result) {
      for (const s of tr.successes) {
        if (s.result === tr.result) continue; // the winner isn't its own alternate
        alternates.push({
          provider: s.result.provider,
          audio: s.entry.audio || "en",
          qualities: s.result.qualities,
          subtitles: s.result.subtitles,
          cookie: s.result.cookie,
          latencyMs: s.latencyMs,
        });
      }
    }
    return tr.result;
  };

  const win = (r: ProviderResult): ChainResult => {
    // P1 (2026-10-09): MovieBox-Hindi language verdict — a moviebox-hi win
    // (or alternate) proves Hindi exists for this title, so /languages can
    // later answer from chain state instead of live-probing the wrapper.
    // Fire-and-forget, best-effort.
    if (
      type === "movie" &&
      (r.provider === "moviebox-hi" || alternates.some((a) => a.provider === "moviebox-hi"))
    ) {
      void noteMovieboxHindi(tmdbId, true);
    }
    return {
      ...r,
      resolvedBy: r.provider,
      latencyMs: Date.now() - t0,
      alternates: rankAlternates(alternates),
    };
  };

  const describeErrors = (): string =>
    laneErrors.map((x) => `${x.provider}: ${x.reason}`).join(", ");

  // Explicit Hindi request: L1 only.
  // Explicit English request: L2 only.
  // Omitted: Hindi-first across all four sources.
  // P0-1: the whole dispatch runs under the 45s deadline timer (cleared in
  // the finally below, whichever lane path wins or throws).
  try {
    if (audio === "hi") {
      const h1 = await runLane(HINDI_LANE, HINDI_LANE_BUDGET_MS, dsignal);
      if (h1) return win(h1);
      throwIfDeadline();
      const mb = await wrapperTier("moviebox-hi", movieboxHindi, tmdbId, type, season, episode, dsignal);
      if (mb) return win(mb);
      throwIfDeadline();
      // Bollywood/Hindi-original titles: VidLink serves the Hindi ORIGINAL, so
      // ?audio=hi is satisfiable even with no dubbed copy (Ali 2026-10-08:
      // Drishyam showed "English" — the original IS Hindi).
      if ((await originalLanguage(tmdbId, type)) === "hi" && !(await providerBlocked("vidlink"))) {
        const vl = await tryProvider({ name: "vidlink", fn: vidlink }, tmdbId, type, season, episode, () => true, { signal: dsignal });
        if (vl) return win(vl);
      }
      throwIfDeadline();
      throw new Error("hindi dubbed not available");
    }

    if (audio === "en") {
      const e1 = await runLane(ENGLISH_LANE, ENGLISH_LANE_BUDGET_MS, dsignal);
      if (e1) return win(e1);
      throwIfDeadline();
      const mb = await wrapperTier("moviebox", moviebox, tmdbId, type, season, episode, dsignal);
      if (mb) return win(mb);
      throwIfDeadline();
      throw new Error(`all providers failed (${describeErrors() || "no reason recorded"})`);
    }

    // Default: Hindi-first across all four sources.
    const d1 = await runLane(HINDI_LANE, HINDI_LANE_BUDGET_MS, dsignal);
    if (d1) return win(d1);
    throwIfDeadline();
    const dmb = await wrapperTier("moviebox-hi", movieboxHindi, tmdbId, type, season, episode, dsignal);
    if (dmb) return win(dmb);
    throwIfDeadline();
    const d2 = await runLane(ENGLISH_LANE, ENGLISH_LANE_BUDGET_MS, dsignal);
    if (d2) return win(d2);
    throwIfDeadline();
    const dmb2 = await wrapperTier("moviebox", moviebox, tmdbId, type, season, episode, dsignal);
    if (dmb2) return win(dmb2);
    throwIfDeadline();
    throw new Error(`all providers failed (${describeErrors() || "no reason recorded"})`);
  } finally {
    clearTimeout(deadlineTimer);
  }
}

/**
 * Cached stream resolution — the entry point every caller uses.
 * Successful resolutions are served from the Redis-shared stream cache
 * (envelope v2: winner + alternates, provider-specific TTLs,
 * stale-while-revalidate) and concurrent in-flight resolutions for the same
 * title collapse into one upstream resolve. Only successes are cached —
 * errors always go live.
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
 * MovieBox-Hindi availability for the dub button (P1 2026-10-09).
 *
 * Pacer-safe by construction: the shared verdict cache (24h) + the 24h
 * negative catalog-gap cache + cached chain state (stream envelopes) answer
 * with ZERO wrapper calls in the common case. The live probe is the LAST
 * resort and runs the real `movieboxHindi` leg — paced (1 req/5s, max 1
 * concurrent) and humanized exactly like a chain call. Pacer congestion
 * (acquire timeout) degrades to "not available": never over-claims, never
 * stampedes. Skipped entirely when the wrapper is killed/disabled or the
 * provider is blocked, so the kill switch keeps /languages honest too.
 * Movies only — the wrapper leg never serves series.
 */
async function movieboxHindiAvailable(tmdbId: string): Promise<boolean> {
  try {
    if (!(await movieboxLive())) return false;
    if (await providerBlocked("moviebox-hi")) return false;
    // 1. Shared verdict cache (written by chain wins and past probes).
    const verdict = await movieboxHindiVerdict(tmdbId);
    if (verdict !== null) return verdict;
    // 2. 24h negative cache written by chain runs (catalog gaps).
    if (await movieboxHindiKnownMissing(tmdbId)) {
      await noteMovieboxHindi(tmdbId, false);
      return false;
    }
    // 3. Chain state: any cached envelope (default/Hindi audio keys) whose
    //    winner or alternates include moviebox-hi proves Hindi played.
    const providers = await cachedStreamProviders("movie", tmdbId);
    if (providers.has("moviebox-hi")) {
      await noteMovieboxHindi(tmdbId, true);
      return true;
    }
    // 4. Last resort: ONE paced live probe; the verdict is cached 24h.
    const r = await movieboxHindi(tmdbId, "movie");
    const found = !!(r && r.qualities.length > 0);
    await noteMovieboxHindi(tmdbId, found);
    return found;
  } catch {
    return false;
  }
}

/**
 * Available audio languages for a title (Ali 2026-10-08: dub button must show
 * ONLY languages that are actually available, not a hardcoded list — AND it
 * must show the movie's REAL original language, e.g. Hindi for Bollywood).
 *
 * - The original language is ALWAYS available (VidLink serves the original).
 * - "hi" is added when the original is not Hindi AND VidZee has Hindi-dubbed
 *   (fast check, 7s internal timeout) OR the FZMovies cache has it (instant)
 *   OR MovieBox has Hindi (P1 2026-10-09: verdict cache + chain state first,
 *   one paced live probe as the last resort — movies only).
 * - When the original IS Hindi (e.g. Drishyam), "hi" covers both the original
 *   and any dub — no duplicate entry, no fake "English".
 *
 * Budget: ~8s max — fits the Vercel 60s window with room to spare.
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
    const hindiEntry: TierEntry = { name: "vidzee", fn: vidzee };
    const fzEntry: TierEntry = { name: "fzmovies", fn: fzmovies };

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
    // MovieBox-Hindi (P1 2026-10-09): the wrapper leg is movies-only, so
    // series skip it. Verdict-cache-first + paced live probe as the last
    // resort — /languages stays honest without raising wrapper call volume.
    if (type === "movie") {
      checks.push(movieboxHindiAvailable(tmdbId));
    }

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

// ── /languages cache: pf:lang:{type}:{tmdbId}:{s}:{e} (2026-10-09) ──────────
// The dub button hits /languages on every info-screen open; the old code ran
// a LIVE VidZee check (~7s) on every call with zero caching. Now: 6h fresh +
// 1h stale, memory L1 + Redis L2 (shared api1+api2).
const LANG_FRESH_MS = 6 * 3600_000;
const LANG_STALE_MS = 1 * 3600_000;

interface LangEnvelope {
  v: 1;
  info: AudioInfo;
  freshUntil: number;
  staleUntil: number;
}

const langKey = (
  type: "movie" | "tv",
  tmdbId: string,
  season?: number,
  episode?: number
): string => `pf:lang:${type}:${tmdbId}:${season ?? 0}:${episode ?? 0}`;

/** Cached wrapper around availableAudio() — what /languages routes call. */
export async function cachedAvailableAudio(
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number
): Promise<AudioInfo> {
  const key = langKey(type, tmdbId, season, episode);
  const now = Date.now();

  const read = async (): Promise<LangEnvelope | null> => {
    if (redisEnabled()) {
      const rhit = await redisCacheGet<LangEnvelope>(key).catch(() => null);
      if (rhit && rhit.v === 1 && rhit.info) {
        cacheSet(key, rhit, Math.max(0, rhit.freshUntil - now), Math.max(0, rhit.staleUntil - rhit.freshUntil));
        return rhit;
      }
    }
    const hit = cacheGet<LangEnvelope>(key);
    return hit ? hit.value : null;
  };

  const write = async (info: AudioInfo): Promise<void> => {
    const env: LangEnvelope = {
      v: 1,
      info,
      freshUntil: now + LANG_FRESH_MS,
      staleUntil: now + LANG_FRESH_MS + LANG_STALE_MS,
    };
    cacheSet(key, env, LANG_FRESH_MS, LANG_STALE_MS);
    if (redisEnabled()) {
      await redisCacheSet(key, env, Math.floor((LANG_FRESH_MS + LANG_STALE_MS) / 1000)).catch(() => {});
    }
  };

  const env = await read();
  if (env) {
    if (now < env.freshUntil) return env.info;
    if (now < env.staleUntil) {
      // Stale: serve now, revalidate in the background (best-effort).
      background(
        (async () => {
          try {
            await write(await availableAudio(tmdbId, type, season, episode));
          } catch {
            /* keep serving stale */
          }
        })()
      );
      return env.info;
    }
  }
  const info = await availableAudio(tmdbId, type, season, episode);
  await write(info);
  return info;
}

// Re-export for /health (single import surface).
export { wrapperStatus };
