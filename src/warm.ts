/**
 * Nightly stream pre-warm (P1 — 2026-10-09).
 *
 * Warms the top ~200 TMDB trending titles (100 movies + 100 TV, day window)
 * into the shared stream cache, so members hit cache instead of the provider
 * race. Triggered via GET /v1/cron/stream-warm (CRON_SECRET header, same
 * pattern as /v1/cron/fz-warm).
 *
 * Bounded for Vercel's 60s maxDuration: a Redis cursor cycles through the
 * title list (`limit` titles per run, default 40, 4-way concurrency, hard
 * 50s deadline). TV titles warm S1E1 (the usual entry point). Titles that
 * are already fresh-cached are skipped, not re-resolved. Warmed entries land
 * in Redis, so BOTH clusters serve them.
 */
import { tmdb } from "./tmdb.js";
import { resolveStream } from "./chain.js";
import { isStreamCachedFresh } from "./streamcache.js";
import { redisCommand } from "./security/redis.js";

const CURSOR_KEY = "pf:warm:cursor";
const DEADLINE_MS = 50_000;
const CONCURRENCY = 4;
const MAX_TITLES = 200;
const PAGES_PER_LIST = 5; // 5 pages × 20 = 100 per list

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

export interface WarmReport {
  total: number;
  cursor: number;
  processed: number;
  warmed: number;
  skipped: number;
  failed: number;
  deadlineHit: boolean;
}

export async function warmTrendingStreams(limit: number): Promise<WarmReport> {
  const deadline = Date.now() + DEADLINE_MS;
  const titles = await trendingTitles();
  const total = titles.length;

  let cursor = 0;
  const raw = await redisCommand(["GET", CURSOR_KEY]);
  if (typeof raw === "string" && /^\d+$/.test(raw)) cursor = parseInt(raw, 10);
  if (total > 0) cursor = cursor % total;

  const batch: WarmTitle[] = [];
  for (let i = 0; i < Math.min(limit, total); i++) {
    batch.push(titles[(cursor + i) % total]);
  }

  let warmed = 0;
  let skipped = 0;
  let failed = 0;
  let processed = 0;
  let deadlineHit = false;
  let idx = 0;
  const workers = Array.from(
    { length: Math.min(CONCURRENCY, batch.length) },
    async (): Promise<void> => {
      while (idx < batch.length) {
        if (Date.now() > deadline) {
          deadlineHit = true;
          break;
        }
        const t = batch[idx++];
        const season = t.type === "tv" ? 1 : undefined;
        const episode = t.type === "tv" ? 1 : undefined;
        try {
          if (await isStreamCachedFresh(t.type, t.id, season, episode, undefined)) {
            skipped++;
            continue;
          }
          // Default (Hindi-first) chain — writes to the shared stream cache
          // on success via resolveStreamCached.
          await resolveStream(t.id, t.type, season, episode, undefined);
          warmed++;
        } catch {
          failed++;
        }
        processed++;
      }
    }
  );
  await Promise.all(workers);

  const newCursor = total > 0 ? (cursor + processed) % total : 0;
  await redisCommand(["SET", CURSOR_KEY, String(newCursor), "EX", 7 * 24 * 3600]);
  return { total, cursor: newCursor, processed, warmed, skipped, failed, deadlineHit };
}
