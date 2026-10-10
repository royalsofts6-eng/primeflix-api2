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

// ── Priority mutex (P1-12, 2026-10-10) ─────────────────────────────────────
// The old code serialized wrapper calls with an UNBOUNDED FIFO mutex:
// /languages probes + 2 prefetch legs queued ahead of a real Play and each
// held the mutex ~10s (token wait + human pause + fetch) — Play starved and
// timed out. Now the queue is priority-ordered ("play" overtakes
// "background") and the wait is bounded (20s): on timeout the caller gets a
// pacer-timeout MISS (the chain falls through honestly) instead of hanging.
type PacePriority = "play" | "background";

interface Waiter {
  pri: number; // 0 = play, 1 = background
  seq: number;
  resolve: () => void;
  reject: (e: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

let mutexHeld = false;
const waitQueue: Waiter[] = [];
let waiterSeq = 0;
/** Bounded mutex wait — never hang a request behind background work. */
const MUTEX_WAIT_MS = 20_000;

const byPriority = (a: Waiter, b: Waiter): number => a.pri - b.pri || a.seq - b.seq;

function pumpMutex(): void {
  if (mutexHeld) return;
  const w = waitQueue.shift();
  if (!w) return;
  mutexHeld = true;
  clearTimeout(w.timer);
  w.resolve();
}

function releaseMutex(): void {
  mutexHeld = false;
  pumpMutex();
}

/**
 * Acquire the 1-concurrent wrapper mutex, priority-ordered. Rejects after
 * MUTEX_WAIT_MS (caller treats it as a miss, never a failure).
 */
function acquireMutex(priority: PacePriority, signal?: AbortSignal): Promise<() => void> {
  if (signal?.aborted) signal.throwIfAborted();
  if (!mutexHeld && waitQueue.length === 0) {
    mutexHeld = true;
    return Promise.resolve(releaseMutex);
  }
  return new Promise<() => void>((resolve, reject) => {
    const waiter: Waiter = {
      pri: priority === "play" ? 0 : 1,
      seq: waiterSeq++,
      resolve: () => resolve(releaseMutex),
      reject,
      timer: setTimeout(() => {
        const i = waitQueue.indexOf(waiter);
        if (i >= 0) waitQueue.splice(i, 1);
        reject(new Error("pacer: timeout waiting for wrapper mutex"));
      }, MUTEX_WAIT_MS),
    };
    const onAbort = (): void => {
      const i = waitQueue.indexOf(waiter);
      if (i >= 0) {
        waitQueue.splice(i, 1);
        clearTimeout(waiter.timer);
        reject(new DOMException("aborted", "AbortError"));
      }
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    waitQueue.push(waiter);
    waitQueue.sort(byPriority);
    pumpMutex();
  });
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
      mutexHeld,
      mutexQueued: waitQueue.length,
    };
  }
  return out;
}

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
 * is available AND the (priority-ordered, 1-concurrent) mutex is acquired.
 * Rejects after ACQUIRE_TIMEOUT_MS on the token wait, or MUTEX_WAIT_MS on
 * the mutex wait — the caller treats either as a miss, never a failure
 * (no circuit impact).
 *
 * P0-1 (2026-10-09): when `signal` aborts (the chain's 45s deadline) the
 * wait throws an AbortError immediately instead of burning up to 15s —
 * tryProvider already treats a settled AbortError as a silent miss.
 * P1-12 (2026-10-10): `priority` orders the mutex queue — "play" overtakes
 * "background" (probes/prefetch/warm) so background work can never starve
 * a real Play into a timeout.
 */
export async function acquirePace(
  key: string,
  signal?: AbortSignal,
  priority: PacePriority = "play"
): Promise<() => void> {
  const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
  const t0 = Date.now();
  while (!takeToken(key)) {
    if (signal?.aborted) signal.throwIfAborted(); // AbortError, never hangs
    if (Date.now() - t0 > ACQUIRE_TIMEOUT_MS) {
      throw new Error(`pacer: timeout waiting for ${key} token`);
    }
    await sleep(250);
  }
  // Serialize (max 1 concurrent), priority-ordered, bounded wait.
  const release = await acquireMutex(priority, signal);
  let released = false;
  return () => {
    if (released) return;
    released = true;
    release();
  };
}
