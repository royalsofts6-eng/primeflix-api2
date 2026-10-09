/**
 * Human-shaped timing (2026-10-09, anti-block Layer 1).
 *
 * Bot detectors look at SHAPE, not volume. Our three machine-smells:
 *  1. parallel bursts (one user action -> N outbound calls in the same ms)
 *  2. fixed-time crons (exact :00 every day = machine signature)
 *  3. fixed-interval polling (perfect rhythm over weeks)
 *
 * Defenses: sequential wrapper calls with human gaps, jittered cron
 * scheduling (never :00), randomized gaps between warm titles.
 */

/**
 * Human pause between two sequential wrapper calls: 800–2500ms uniform.
 *
 * P0-1 (2026-10-09): aborts early when `signal` fires (chain 45s
 * deadline) instead of sleeping through it — the rejection is the signal's
 * AbortError, which tryProvider treats as a silent miss.
 */
export function humanPause(signal?: AbortSignal): Promise<void> {
  const ms = 800 + Math.random() * 1700;
  return new Promise<void>((resolve, reject) => {
    const onAbort = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      try {
        signal?.throwIfAborted();
      } catch (e) {
        reject(e);
      }
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    if (signal?.aborted) {
      onAbort();
      return;
    }
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/** Gap between two warmed titles in a cron run: 20–60s (browsing a list). */
export function warmTitleGapMs(): number {
  return 20_000 + Math.random() * 40_000;
}

/**
 * Gap between two FZMovies warm scrapes: 5–10s (P1-4, 2026-10-09).
 * FZMovies is our own scrape domain — not the anti-block-sensitive
 * MovieBox wrapper — so the 20–60s human gap is overkill here and was
 * throttling the 4h FZ cycle to ~15 cold warms/day against a ~480/day
 * need. Skipped entirely when the title was already fresh (no upstream
 * hit happened, so there is nothing to be polite about).
 */
export function warmFZGapMs(): number {
  return 5_000 + Math.random() * 5_000;
}

/**
 * Jittered cron time inside [startH, endH) — deterministic per date so two
 * clusters never double-schedule, but never a round :00 (machine smell).
 * Returns { h, m } for building the schedule.
 */
export function jitteredMinute(
  dateStr: string,
  startH: number,
  endH: number
): { h: number; m: number } {
  let hsh = 0;
  for (const c of dateStr) hsh = (hsh * 31 + c.charCodeAt(0)) >>> 0;
  const totalMin = startH * 60 + (hsh % ((endH - startH) * 60));
  return { h: Math.floor(totalMin / 60), m: totalMin % 60 };
}
