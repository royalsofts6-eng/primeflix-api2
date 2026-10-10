/**
 * Nightly stream pre-warm (P1 — 2026-10-09, watch-history-first Phase D).
 *
 * Warms what members ACTUALLY watch first (pf:v3:watched ZSET, top-50 by
 * recency in the last 24h — recorded on every successful /v1/stream Play),
 * then the top ~200 TMDB trending titles (100 movies + 100 TV, day window)
 * into the shared stream cache, so members hit cache instead of the provider
 * race. Each title warms all three audio variants (default + hi + en, P1-5)
 * so dub-button taps never pay the live race. Triggered via
 * GET /v1/cron/stream-warm (CRON_SECRET header, same pattern as /v1/cron/fz-warm).
 *
 * Bounded for Vercel's 60s maxDuration: a Redis cursor cycles through the
 * title list (`limit` titles per run, default 40, 4-way concurrency, hard
 * 50s deadline AND a 120-command/run budget, whichever stops first). TV
 * titles warm S1E1 (the usual entry point). Titles that are already
 * fresh-cached are skipped, not re-resolved. Warmed entries land in Redis,
 * so BOTH clusters serve them.
 */
import { tmdb } from "./tmdb.js";
import { resolveStream } from "./chain.js";
import { isStreamCachedFresh } from "./streamcache.js";
import { warmFZMovies } from "./providers/fzmovies.js";
import { redisEnabled, redisCommand } from "./security/redis.js";
import { background } from "./revalidate.js";
import { ck } from "./cache.js";
import { warmTitleGapMs, warmFZGapMs } from "./humanize.js";

// Phase D (2026-10-09): versioned keys (functions, not module constants —
// ck() reads CACHE_SCHEMA_OVERRIDE hot, so a schema bump via env works
// without a redeploy).
const cursorKey = (): string => ck("warm", "cursor");
const watchedKey = (): string => ck("watched");
const DEADLINE_MS = 50_000;
const CONCURRENCY = 4;
const MAX_TITLES = 200;
const PAGES_PER_LIST = 5; // 5 pages × 20 = 100 per list
/** Per-run Redis command budget (Phase D design §9: 120 commands/run). */
const CMD_BUDGET = 120;
/** Watch-history window warmed each run (Phase D design §9: top-50/24h). */
const WATCHED_WARM_LIMIT = 50;
const WATCHED_WINDOW_MS = 24 * 3600_000;
const WATCHED_TTL_S = 30 * 24 * 3600; // 30d

interface WarmTitle {
  type: "movie" | "tv";
  id: string;
}

/** Top ~200 trending titles (100 movies + 100 TV). TMDB-cached 6h, cheap. */
async function trendingTitles(): Promise<WarmTitle[]> {
  const out: WarmTitle[] = [];
  for (let page = 1; page <= PAGES_PER_LIST; page++) {
    const [m, t] = await Promise.all([
      tmdb.trendingMovie("day", String(page)),
      tmdb.trendingTv("day", String(page)),
    ]);
    for (const r of ((m as any)?.results || []) as any[]) {
      if (r?.id) out.push({ type: "movie", id: String(r.id) });
    }
    for (const r of ((t as any)?.results || []) as any[]) {
      if (r?.id) out.push({ type: "tv", id: String(r.id) });
    }
    if (out.length >= MAX_TITLES) break;
  }
  return out.slice(0, MAX_TITLES);
}

/**
 * Phase D (2026-10-09): record a member watch for watch-history pre-warm.
 * Called on every successful /v1/stream Play (movie + TV routes) — NOT from
 * the warm cron or the prefetch writer (those are not real watches).
 * ZADD pf:v3:watched {now} {type}:{tmdbId} — 1 command, via background()
 * (waitUntil when available) so the Play response is never delayed.
 * Re-watches bump the score (recency); frequently watched titles keep fresh
 * scores. Best-effort, never throws.
 */
export function noteWatch(type: "movie" | "tv", tmdbId: string): void {
  if (!redisEnabled()) return;
  try {
    background(
      redisCommand(["ZADD", watchedKey(), Date.now(), `${type}:${tmdbId}`]).then(
        () => undefined
      )
    );
  } catch {
    /* best-effort */
  }
}

/** Top-N most recently watched titles in the last 24h. Never throws. */
async function readWatched(limit: number): Promise<WarmTitle[]> {
  const out: WarmTitle[] = [];
  try {
    if (!redisEnabled()) return out;
    const now = Date.now();
    const r = await redisCommand([
      "ZREVRANGEBYSCORE",
      watchedKey(),
      String(now),
      String(now - WATCHED_WINDOW_MS),
      "LIMIT",
      "0",
      String(limit),
    ]);
    if (Array.isArray(r)) {
      for (const m of r) {
        if (typeof m !== "string") continue;
        const [type, id] = m.split(":");
        if ((type === "movie" || type === "tv") && id && /^\d+$/.test(id)) {
          out.push({ type, id });
        }
      }
    }
  } catch {
    /* fail-open: no watch history, warm trending only */
  }
  return out;
}

export interface WarmReport {
  total: number;
  cursor: number;
  processed: number;
  warmed: number;
  skipped: number;
  failed: number;
  deadlineHit: boolean;
  /** Phase D: true when the 120-command/run budget stopped the run early. */
  budgetHit: boolean;
  /** Phase D: how many watch-history titles were warmed this run. */
  watchedWarmed: number;
}

export async function warmTrendingStreams(limit: number): Promise<WarmReport> {
  const deadline = Date.now() + DEADLINE_MS;
  // Phase D (2026-10-09): per-run command budget — the 50s deadline AND the
  // 120-command budget stop the run, whichever comes first (design §9).
  // Cost model: skip-if-fresh ≈ 1 GET, a real resolve ≈ ~8 commands.
  let cmdBudget = CMD_BUDGET;
  let budgetHit = false;

  // Phase D: warm what members ACTUALLY watched first (top-50 by recency
  // in the last 24h) — not just generic trending. default + hi audio lanes;
  // TV warms S1E1 (the entry point), matching the trending pattern.
  const watched = await readWatched(WATCHED_WARM_LIMIT);
  cmdBudget -= 1;
  let watchedWarmed = 0;
  let warmed = 0;
  let skipped = 0;
  let failed = 0;
  let processed = 0;
  let deadlineHit = false;
  const over = (): boolean => Date.now() > deadline || cmdBudget <= 0;

  const watchedVariants: (string | undefined)[] = [undefined, "hi"];
  for (const t of watched) {
    if (over()) {
      if (cmdBudget <= 0) budgetHit = true;
      else deadlineHit = true;
      break;
    }
    const season = t.type === "tv" ? 1 : undefined;
    const episode = t.type === "tv" ? 1 : undefined;
    for (const audio of watchedVariants) {
      if (over()) break;
      try {
        cmdBudget -= 1;
        if (await isStreamCachedFresh(t.type, t.id, season, episode, audio)) {
          skipped++;
          continue;
        }
        cmdBudget -= 8;
        await resolveStream(t.id, t.type, season, episode, audio, undefined, "background");
        warmed++;
        watchedWarmed++;
      } catch {
        failed++;
      }
    }
    // NOTE: `processed` is intentionally NOT incremented here — it drives
    // the trending cursor below, and watched titles must not advance it.
  }
  if (cmdBudget <= 0) budgetHit = true;

  const titles = await trendingTitles();
  const total = titles.length;

  let cursor = 0;
  const raw = await redisCommand(["GET", cursorKey()]);
  if (typeof raw === "string" && /^\d+$/.test(raw)) cursor = parseInt(raw, 10);
  if (total > 0) cursor = cursor % total;

  const batch: WarmTitle[] = [];
  for (let i = 0; i < Math.min(limit, total); i++) {
    batch.push(titles[(cursor + i) % total]);
  }

  let idx = 0;
  // P1-5 (2026-10-09): the old loop warmed ONLY the default (Hindi-first)
  // audio key — ?audio=hi / ?audio=en keys were always cold, so a
  // dub-button tap paid the full live race. Now each title warms all
  // three variants (hi first — it is the dub-button tap). Each variant is
  // an independent cache key with its own fresh check, so already-warm
  // variants skip in ~1 Redis read; the hard deadline bounds the extra
  // live resolves.
  const variants: (string | undefined)[] = [undefined, "hi", "en"];
  const workers = Array.from(
    { length: Math.min(CONCURRENCY, batch.length) },
    async (): Promise<void> => {
      while (idx < batch.length) {
        // Phase D: stop on the 50s deadline OR the 120-command budget.
        if (cmdBudget <= 0) {
          budgetHit = true;
          break;
        }
        if (Date.now() > deadline) {
          deadlineHit = true;
          break;
        }
        const t = batch[idx++];
        const season = t.type === "tv" ? 1 : undefined;
        const episode = t.type === "tv" ? 1 : undefined;
        for (const audio of variants) {
          if (cmdBudget <= 0 || Date.now() > deadline) break;
          try {
            cmdBudget -= 1; // skip-if-fresh ≈ 1 GET
            if (await isStreamCachedFresh(t.type, t.id, season, episode, audio)) {
              skipped++;
              continue;
            }
            // Default (Hindi-first) chain — writes to the shared stream cache
            // on success via resolveStreamCached.
            cmdBudget -= 8; // a real resolve ≈ ~8 commands
            await resolveStream(t.id, t.type, season, episode, audio, undefined, "background");
            warmed++;
          } catch {
            failed++;
          }
        }
        processed++;
      }
    }
  );
  await Promise.all(workers);

  const newCursor = total > 0 ? (cursor + processed) % total : 0;
  await redisCommand(["SET", cursorKey(), String(newCursor), "EX", 7 * 24 * 3600]);
  // Phase D: keep the watched ZSET bounded (top-500 by recency) and sliding
  // 30d — the ZADDs on Play are 1 command each; the trim happens here.
  await redisCommand(["ZREMRANGEBYRANK", watchedKey(), "0", "-501"]).catch(() => null);
  await redisCommand(["EXPIRE", watchedKey(), WATCHED_TTL_S]).catch(() => null);
  return { total, cursor: newCursor, processed, warmed, skipped, failed, deadlineHit, budgetHit, watchedWarmed };
}

// ── FZMovies batch warm (2026-10-09) ─────────────────────────────────────────
// The FZMovies Hindi tier is cache-only on the request path — without a warm
// cron it is effectively DEAD. This batch endpoint (called every 6h from a
// Hatch cron, api1 only — Redis is shared; Phase D 2026-10-09: 4h → 6h)
// cycles through the top ~200 trending movies with cursor pf:v3:fzwarm:cursor.
// P1-4 (2026-10-09): the old shape (2 workers, 20–60s gaps, limit 8) warmed
// ~2–3 cold titles/run (~15/day) against a ~480/day need — 30x short. Now:
// 6 workers, 5–10s gaps ONLY after a real upstream scrape (fresh entries
// skip in ~1 Redis read with no gap at all), and a bigger per-run limit.
const fzCursorKey = (): string => ck("fzwarm", "cursor");
const FZ_DEADLINE_MS = 50_000;
const FZ_CONCURRENCY = 6;
const FZ_MAX_TITLES = 200;
const FZ_PAGES = 10; // 10 pages x 20 = 200 movies

/** Top ~200 trending movies (TMDB-cached 6h — cheap after the first call). */
async function topMovieIds(): Promise<string[]> {
  const out: string[] = [];
  for (let page = 1; page <= FZ_PAGES; page++) {
    const m = (await tmdb.trendingMovie("day", String(page))) as any;
    for (const r of (m?.results || []) as any[]) {
      if (r?.id) out.push(String(r.id));
    }
    if (out.length >= FZ_MAX_TITLES) break;
  }
  return out.slice(0, FZ_MAX_TITLES);
}

export interface FZWarmReport {
  total: number;
  cursor: number;
  processed: number;
  warmed: number;
  skipped: number;
  failed: number;
  deadlineHit: boolean;
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

export async function warmFZMoviesBatch(limit: number): Promise<FZWarmReport> {
  const deadline = Date.now() + FZ_DEADLINE_MS;
  const ids = await topMovieIds();
  const total = ids.length;

  let cursor = 0;
  const raw = await redisCommand(["GET", fzCursorKey()]);
  if (typeof raw === "string" && /^\d+$/.test(raw)) cursor = parseInt(raw, 10);
  if (total > 0) cursor = cursor % total;

  const batch: string[] = [];
  for (let i = 0; i < Math.min(limit, total); i++) {
    batch.push(ids[(cursor + i) % total]);
  }

  let warmed = 0;
  let skipped = 0;
  let failed = 0;
  let processed = 0;
  let deadlineHit = false;
  let idx = 0;
  const workers = Array.from(
    { length: Math.min(FZ_CONCURRENCY, batch.length) },
    async (): Promise<void> => {
      while (idx < batch.length) {
        if (Date.now() > deadline) {
          deadlineHit = true;
          break;
        }
        const id = batch[idx++];
        let scraped = false;
        try {
          // warmFZMovies checks Redis first (2026-10-09 fix) — fresh
          // entries skip in ~1 command, no re-scrape.
          const w = await warmFZMovies(id, "movie", true);
          if (w.warmed && w.reason === "warmed") {
            warmed++;
            scraped = true;
          } else if (w.warmed) {
            skipped++;
          } else {
            failed++;
          }
        } catch {
          failed++;
        }
        processed++;
        // P1-4 (2026-10-09): gap ONLY after a real upstream scrape — a
        // fresh skip cost ~1 Redis read, so there is nothing to be polite
        // about. Skip the gap entirely when the deadline looms.
        if (scraped && idx < batch.length && Date.now() + 12_000 < deadline) {
          await sleep(Math.min(warmFZGapMs(), Math.max(0, deadline - Date.now() - 1000)));
        }
      }
    }
  );
  await Promise.all(workers);

  const newCursor = total > 0 ? (cursor + processed) % total : 0;
  await redisCommand(["SET", fzCursorKey(), String(newCursor), "EX", 7 * 24 * 3600]);
  return { total, cursor: newCursor, processed, warmed, skipped, failed, deadlineHit };
}
