/**
 * Best-effort background work after the response is sent (2026-10-09).
 *
 * Uses Vercel's waitUntil (from @vercel/functions) — it keeps the invocation
 * alive so the work actually completes. Never throws, never blocks the response.
 *
 * P1 FIX (2026-10-09, QA Phase F): the previous dynamic
 * `await import("@vercel/functions")` never yielded a working waitUntil in the
 * deployed bundle — waitUntilImpl stayed null and every post-response task
 * (watch-history ZADD, /languages prefetch writer, stale revalidation) died
 * when Vercel froze the function after the response. Verified live: the
 * pf:v3:watched ZSET stayed empty all day despite successful Plays, and no
 * prefetch keys appeared 30s after a cache-served /languages. Static import
 * guarantees waitUntil is resolved synchronously at module init (the package
 * is a declared dependency, always installed in the deployment).
 */
import { waitUntil } from "@vercel/functions";

const waitUntilImpl: ((p: Promise<unknown>) => void) | null =
  typeof waitUntil === "function" ? waitUntil : null;

let backgroundWarned = false;

/**
 * For /health: is post-response background work actually wired? QA uses this
 * to verify the Phase F fix live (must be true on both clusters).
 */
export function backgroundWired(): boolean {
  return waitUntilImpl !== null;
}

/** Run `p` in the background. The promise's rejection is swallowed. */
export function background(p: Promise<unknown>): void {
  const guarded = p.catch(() => undefined);
  if (waitUntilImpl) {
    try {
      waitUntilImpl(guarded);
      return;
    } catch {
      /* fall through to fire-and-forget */
    }
  }
  if (!backgroundWarned) {
    backgroundWarned = true;
    console.warn(
      "[revalidate] waitUntil unavailable — background work (prefetch, watch-history, revalidate) may not complete"
    );
  }
  void guarded;
}
