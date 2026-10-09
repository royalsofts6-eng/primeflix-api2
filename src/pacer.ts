/**
 * Outbound pacing for third-party providers (2026-10-09, anti-block).
 *
 * Principle: per-IDENTITY pacing, not just global — but the MovieBox
 * wrapper is keyless and shared, so its bucket is global and deliberately
 * polite: burst 4, sustained 1 req / 5s. ~150 req/day actual vs ~17k/day
 * theoretical ceiling — 100x under any plausible threshold.
 *
 * CYBER RULE: the wrapper leg is NEVER in a race. This pacer additionally
 * serializes wrapper calls (max 1 concurrent) so we can never become the
 * wrapper operator's top talker by accident.
 */
interface PaceSpec {
  capacity: number;
  refillPerSec: number;
}

const PACE: Record<string, PaceSpec> = {
  // MovieBox wrapper (third-party, keyless): be the ideal citizen.
  mb_wrapper: { capacity: 4, refillPerSec: 0.2 }, // burst 4, sustained 1/5s
};

interface Bucket {
  tokens: number;
  last: number;
}

const buckets = new Map<string, Bucket>();

/** Max time acquirePace waits for a token before giving up (never hang a request). */
const ACQUIRE_TIMEOUT_MS = 15_000;

/** Serialize wrapper calls: at most 1 in flight, ever. */
let wrapperMutex: Promise<void> = Promise.resolve();

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

function takeToken(key: string): boolean {
  const spec = PACE[key];
  if (!spec) return true;
  const now = Date.now();
  let b = buckets.get(key);
  if (!b) {
    b = { tokens: spec.capacity, last: now };
    buckets.set(key, b);
  }
  const elapsed = (now - b.last) / 1000;
  b.tokens = Math.min(spec.capacity, b.tokens + elapsed * spec.refillPerSec);
  b.last = now;
  if (b.tokens >= 1) {
    b.tokens -= 1;
    return true;
  }
  return false;
}

/**
 * Acquire the right to make one paced outbound call. Resolves when a token
 * is available AND no other paced call is in flight (max 1 concurrent).
 * Rejects after ACQUIRE_TIMEOUT_MS — the caller treats it as a miss, never
 * a failure (no circuit impact).
 *
 * P0-1 (2026-10-09): when `signal` aborts (the chain's 45s deadline) the
 * wait throws an AbortError immediately instead of burning up to 15s —
 * tryProvider already treats a settled AbortError as a silent miss.
 */
export async function acquirePace(key: string, signal?: AbortSignal): Promise<() => void> {
  const t0 = Date.now();
  while (!takeToken(key)) {
    if (signal?.aborted) signal.throwIfAborted(); // AbortError, never hangs
    if (Date.now() - t0 > ACQUIRE_TIMEOUT_MS) {
      throw new Error(`pacer: timeout waiting for ${key} token`);
    }
    await sleep(250);
  }
  // Serialize: chain onto the mutex.
  let release!: () => void;
  const gate = new Promise<void>((res) => {
    release = res;
  });
  const prev = wrapperMutex;
  wrapperMutex = prev.then(() => gate);
  await prev;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    release();
  };
}

/** Pacer state for /health (wrapper status). */
export function pacerStats(): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, spec] of Object.entries(PACE)) {
    const b = buckets.get(key);
    out[key] = {
      capacity: spec.capacity,
      refillPerSec: spec.refillPerSec,
      tokens: b ? +b.tokens.toFixed(2) : spec.capacity,
    };
  }
  return out;
}
