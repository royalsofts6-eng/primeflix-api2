/**
 * Best-effort background work after the response is sent (2026-10-09).
 *
 * Uses Vercel's waitUntil (from @vercel/functions) when the package is
 * available — it keeps the invocation alive so the work actually completes.
 * Falls back to fire-and-forget when the package isn't installed (local
 * dev, or any runtime without it). Never throws, never blocks the response.
 */
let waitUntilImpl: ((p: Promise<unknown>) => void) | null = null;
let loaded = false;

async function load(): Promise<void> {
  if (loaded) return;
  loaded = true;
  try {
    const mod = (await import("@vercel/functions")) as {
      waitUntil?: (p: Promise<unknown>) => void;
    };
    if (mod && typeof mod.waitUntil === "function") waitUntilImpl = mod.waitUntil;
  } catch {
    /* package not installed — fire-and-forget fallback below */
  }
}

void load();

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
  void guarded;
}
