/**
 * 5-provider chain with PARALLEL racing + circuit breaker + health tracking.
 *
 * CRITICAL: Vercel Hobby has a 10s function timeout. Sequentially trying
 * 5 providers would exceed it. We race the top-3 healthiest providers in
 * parallel with an 8s overall budget and take the first success.
 *
 * Chain (final plan v1.0 — NHD dead, 111Movies unverified, both removed):
 *   1. VidLink  ✅ verified 2026-10-08
 *   2. VaPlayer  (stub — Phase 1b)
 *   3. VidRock   (stub — Phase 1b)
 *   4. VidSrc    (stub — Phase 1b)
 *   5. ScreenScape (stub — Phase 1b)
 *
 * Circuit breaker: 5 consecutive fails -> 5 min cooldown.
 */
import { vidlink } from "./providers/vidlink.js";
import { vaplayer } from "./providers/vaplayer.js";
import { vidzee } from "./providers/vidzee.js";
import { fzmovies, warmFZMovies } from "./providers/fzmovies.js";
import { tmdb } from "./tmdb.js";
import type { ProviderFn, ProviderResult } from "./providers/types.js";

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
}

// ── Health tracking ─────────────────────────────────────────────────────────
interface Health {
  success: number;
  fail: number;
  consecutiveFails: number;
  lastFailAt: number;
  totalLatencyMs: number;
  samples: number;
}

const health = new Map<string, Health>();
function h(name: string): Health {
  let x = health.get(name);
  if (!x) {
    x = { success: 0, fail: 0, consecutiveFails: 0, lastFailAt: 0, totalLatencyMs: 0, samples: 0 };
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

function recordFail(name: string): void {
  const x = h(name);
  x.fail++;
  x.consecutiveFails++;
  x.lastFailAt = Date.now();
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
    };
  }
  return out;
}

// ── Chain ───────────────────────────────────────────────────────────────────
const PROVIDERS: ProviderEntry[] = [
  { name: "vidlink", fn: vidlink },
  { name: "vaplayer", fn: vaplayerWithImdb },
  { name: "vidrock", fn: notYet("vidrock") },
  { name: "vidsrc", fn: notYet("vidsrc") },
  { name: "screenscape", fn: notYet("screenscape") },
];

function rankProviders(): ProviderEntry[] {
  return [...PROVIDERS]
    .filter((p) => !circuitOpen(p.name))
    .sort((a, b) => {
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
  episode?: number
): Promise<ProviderResult | null> {
  const t0 = Date.now();
  try {
    const r = await p.fn(tmdbId, type, season, episode);
    if (r && r.qualities.length > 0) {
      recordSuccess(p.name, Date.now() - t0);
      return r;
    }
    recordFail(p.name);
    return null;
  } catch {
    recordFail(p.name);
    return null;
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
 * 35-60s (Vercel Hobby = 10s), so the request path only reads its cache;
 * misses trigger a best-effort background warm for next time.
 *
 * @param audio "hi" = Hindi-dubbed only (VidZee → FZMovies cache; throws
 *   HINDI_UNAVAILABLE when missing — no silent fallback so the app can show
 *   an honest message).
 *   "en" = English/original only (skips Hindi tiers). Omitted = Hindi-first:
 *   VidZee → FZMovies cache → English race chain.
 */
export async function resolveStream(
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
    if (!circuitOpen(hindiEntry.name)) {
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
    // Miss: warm in background for next time, honest error now.
    void warmFZMovies(tmdbId, type).catch(() => {});
    throw new Error("hindi dubbed not available");
  }

  const skipHindi = audio === "en";
  const hindiEntry: ProviderEntry = { name: "vidzee", fn: vidzee };
  if (!skipHindi && !circuitOpen(hindiEntry.name)) {
    // Hindi-first (Ali 2026-10-08): try VidZee Hindi-dubbed before the English chain.
    // VidZee has a short internal fetch timeout (3.5s) so the Vercel Hobby 10s
    // budget stays safe; on miss we fall through to FZMovies cache, then chain.
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
    // Cache miss: warm in background while the English chain serves now.
    void warmFZMovies(tmdbId, type).catch(() => {});
  }

  const ranked = rankProviders();
  if (ranked.length === 0) throw new Error("all providers in cooldown");

  const racers = ranked.slice(0, RACE_COUNT);
  const rest = ranked.slice(RACE_COUNT);

  const attempt = async (list: ProviderEntry[]): Promise<ProviderResult | null> => {
    const results = await Promise.all(list.map((p) => tryProvider(p, tmdbId, type, season, episode)));
    return results.find((r) => r !== null) ?? null;
  };

  // Round 1: race top-3 in parallel with overall budget
  const winner = await Promise.race([
    attempt(racers),
    new Promise<null>((res) => setTimeout(() => res(null), OVERALL_BUDGET_MS)),
  ]);

  let result = winner;
  // Round 2: if round 1 failed, try the rest sequentially (fast fail each)
  if (!result) {
    for (const p of rest) {
      result = await tryProvider(p, tmdbId, type, season, episode);
      if (result) break;
      if (Date.now() - t0 > OVERALL_BUDGET_MS + 1500) break;
    }
  }

  if (!result) throw new Error("all providers failed");
  return { ...result, resolvedBy: result.provider, latencyMs: Date.now() - t0 };
}
