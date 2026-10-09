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

/** Human pause between two sequential wrapper calls: 800–2500ms uniform. */
export function humanPause(): Promise<void> {
  const ms = 800 + Math.random() * 1700;
  return new Promise((r) => setTimeout(r, ms));
}

/** Gap between two warmed titles in a cron run: 20–60s (browsing a list). */
export function warmTitleGapMs(): number {
  return 20_000 + Math.random() * 40_000;
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
