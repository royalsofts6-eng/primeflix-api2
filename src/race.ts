/**
 * Parallel first-success-wins racing for provider lanes (2026-10-09).
 *
 * Why not Promise.race: it stops at the first SETTLEMENT (resolve OR
 * reject) — one provider's throw would kill a race another provider could
 * still win. Why not Promise.any: ProviderFn returns null on a miss
 * (providers/types.ts), and Promise.any treats null as success.
 * Neither aborts the losers (wasted upstream load + Vercel wall-clock).
 *
 * Custom design: first NON-NULL result wins; all-miss resolves null;
 * errors are aggregated; losers are cancelled via a lane-shared
 * AbortController (loser abort is silent — never a failure, never a retry).
 */
import type {
  ProviderFn,
  ProviderResult,
  StreamQuality,
  Subtitle,
} from "./providers/types.js";

export interface RaceOpts {
  /** Lane-shared abort signal — set when the lane settles. */
  signal?: AbortSignal;
  /** Per-provider miss/failure reason, for the aggregated error. */
  onError?: (provider: string, reason: string) => void;
  /** P1-12: pacer priority, threaded to the provider call. */
  priority?: "play" | "background";
}

export interface TierError {
  provider: string;
  reason: string;
}

export interface TierSuccess {
  entry: TierEntry;
  result: ProviderResult;
  latencyMs: number;
}

export interface TierResult {
  result: ProviderResult | null;
  errors: TierError[];
  /** Every non-null success collected before the lane settled (winner
   *  included). Used to build the cache envelope's alternates. */
  successes: TierSuccess[];
}

export interface TierEntry {
  name: string;
  fn: ProviderFn;
  /** "hi" for Hindi-only entries (vidzee, fzmovies, moviebox-hi). */
  audio?: string;
  stub?: boolean;
}

export type RaceMode = "speed" | "quality";

export interface RaceConfig {
  /** Lane backstop, e.g. 8000. */
  budgetMs: number;
  /** "speed": first success wins. "quality": collect all, pick best. */
  mode: RaceMode;
  /**
   * Global chain deadline signal (P0-1 2026-10-09: 45s). When it fires the
   * lane settles immediately as a miss — the chain then throws the deadline
   * error between tiers, so a hung provider can never stretch a request past
   * the deadline (Vercel hard-kills at 60s with no response at all).
   */
  parentSignal?: AbortSignal;
  /** P1-12: pacer priority for the lane's provider calls. */
  priority?: "play" | "background";
}

/** Runner for one provider inside a lane (chain.ts's tryProvider). */
export type TryOneFn = (
  p: TierEntry,
  tmdbId: string,
  type: "movie" | "tv",
  season: number | undefined,
  episode: number | undefined,
  recordStats: () => boolean,
  opts?: RaceOpts
) => Promise<ProviderResult | null>;

/** A ranked runner-up cached inside the stream envelope (client-side
 *  failover, zero new backend round-trip). */
export interface RankedAlternate {
  provider: string;
  /** Audio of this alternate ("hi"/"en") — tells the app its tier. */
  audio: string;
  qualities: StreamQuality[];
  subtitles?: Subtitle[];
  /** Passthrough (e.g. MovieBox Edge-Cache-Cookie) when present. */
  cookie?: string;
  /** Passthrough extra CDN request headers (e.g. Referer for MP4, 2026-10-10). */
  headers?: Record<string, string>;
  latencyMs: number;
}

/** "1080p" -> 1080, "720p" -> 720, unknown -> 0. */
export function qualityRank(q: string): number {
  const m = /^(\d{3,4})/.exec(q.trim());
  return m ? parseInt(m[1], 10) : 0;
}

function bestQualityOf(r: ProviderResult): number {
  return Math.max(0, ...r.qualities.map((x) => qualityRank(x.quality)));
}

/**
 * Pick the winner from collected successes. Highest best-quality wins;
 * ties go to the earliest arrival (stable, no flapping).
 */
function pickBest(successes: TierSuccess[]): ProviderResult | null {
  if (successes.length === 0) return null;
  let best = successes[0];
  for (const s of successes.slice(1)) {
    if (bestQualityOf(s.result) > bestQualityOf(best.result)) best = s;
  }
  return best.result;
}

/**
 * Rank alternates for the envelope: winner's tier (Hindi) first, then
 * fastest arrival, then most qualities. Max 3.
 */
export function rankAlternates(alts: RankedAlternate[]): RankedAlternate[] {
  return [...alts]
    .sort((a, b) => {
      const ta = a.audio === "hi" ? 0 : 1;
      const tb = b.audio === "hi" ? 0 : 1;
      if (ta !== tb) return ta - tb;
      if (a.latencyMs !== b.latencyMs) return a.latencyMs - b.latencyMs;
      return b.qualities.length - a.qualities.length;
    })
    .slice(0, 3);
}

// ── Race stats (for /health) ────────────────────────────────────────────────
const rstats = {
  races: 0,
  wins: {} as Record<string, number>,
  aborts: 0,
  lastWinner: null as string | null,
};

export function raceStats(): Record<string, unknown> {
  return { ...rstats, wins: { ...rstats.wins } };
}

function recordRaceWin(provider: string): void {
  rstats.races++;
  rstats.wins[provider] = (rstats.wins[provider] || 0) + 1;
  rstats.lastWinner = provider;
}

/**
 * Race one language lane. First non-null success wins (speed) or the best
 * quality among all successes wins (quality). Losers are aborted.
 */
export async function raceTier(
  tier: TierEntry[],
  tmdbId: string,
  type: "movie" | "tv",
  season: number | undefined,
  episode: number | undefined,
  tryOne: TryOneFn,
  /** Skip cooled-down / circuit-open providers BEFORE the race. */
  isBlocked: (name: string) => boolean | Promise<boolean>,
  cfg: RaceConfig
): Promise<TierResult> {
  const ctrl = new AbortController();
  const errors: TierError[] = [];
  const successes: TierSuccess[] = [];
  const racers: TierEntry[] = [];
  for (const p of tier) {
    if (await isBlocked(p.name)) {
      errors.push({ provider: p.name, reason: "blocked (cooldown/circuit)" });
      continue;
    }
    racers.push(p);
  }

  // All blocked pre-race: settle immediately (before the Promise/timer
  // below exist — finish() references the timer).
  if (racers.length === 0) {
    return { result: null, errors, successes };
  }

  return new Promise((resolve) => {
    let remaining = racers.length;
    let done = false;
    /** Stats gate: entries settling after the lane settled must not
     *  record success/fail — same pattern as the old chain.ts raceBox. */
    const box = { settled: false };

    const finish = (r: ProviderResult | null) => {
      if (done) return;
      done = true;
      box.settled = true;
      clearTimeout(timer);
      ctrl.abort(); // slow losers cancelled
      cfg.parentSignal?.removeEventListener("abort", onParentAbort);
      rstats.aborts++;
      if (r) recordRaceWin(r.provider);
      else rstats.races++;
      resolve({ result: r, errors, successes });
    };

    const timer = setTimeout(() => {
      // Budget backstop: speed and quality converge — take the best
      // of whatever arrived (null when nothing did).
      finish(pickBest(successes));
    }, cfg.budgetMs);

    // P0-1: the chain's 45s deadline settles the lane at once (miss), so the
    // chain can throw the deadline error between tiers instead of burning
    // wall-clock here.
    const onParentAbort = (): void => finish(null);
    if (cfg.parentSignal?.aborted) {
      onParentAbort();
      return;
    }
    cfg.parentSignal?.addEventListener("abort", onParentAbort, { once: true });

    for (const p of racers) {
      const t0 = Date.now();
      tryOne(p, tmdbId, type, season, episode, () => !box.settled, {
        signal: ctrl.signal,
        priority: cfg.priority, // P1-12: lane legs inherit the chain's pacer priority
        onError: (name, reason) => errors.push({ provider: name, reason }),
      } as RaceOpts).then((r) => {
        if (done) return;
        remaining--;
        if (r) {
          successes.push({ entry: p, result: r, latencyMs: Date.now() - t0 });
          if (cfg.mode === "speed") {
            finish(r); // FIRST non-null success WINS
            return;
          }
          // quality mode: keep collecting until budget/all settle
        }
        if (remaining === 0) finish(pickBest(successes));
      });
    }
  });
}
