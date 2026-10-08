/**
 * In-memory LRU cache with TTL + stale-while-revalidate.
 *
 * NOTE: Serverless instances do NOT share memory. This cache is per-instance.
 * We ALSO set `Cache-Control: s-maxage, stale-while-revalidate` headers so
 * Vercel's edge network caches responses across instances.
 * Iteration 2: replace with Upstash Redis for shared state.
 */

interface Entry {
  value: unknown;
  expiresAt: number; // hard expiry
  staleUntil: number; // stale-while-revalidate window
}

const MAX_ENTRIES = 2000;
const store = new Map<string, Entry>();

export function cacheGet<T>(key: string): { value: T; stale: boolean } | null {
  const e = store.get(key);
  if (!e) return null;
  const now = Date.now();
  if (now > e.staleUntil) {
    store.delete(key);
    return null;
  }
  // LRU touch
  store.delete(key);
  store.set(key, e);
  return { value: e.value as T, stale: now > e.expiresAt };
}

export function cacheSet(key: string, value: unknown, ttlMs: number, staleMs: number): void {
  if (store.size >= MAX_ENTRIES) {
    // evict oldest
    const oldest = store.keys().next().value;
    if (oldest) store.delete(oldest);
  }
  const now = Date.now();
  store.set(key, { value, expiresAt: now + ttlMs, staleUntil: now + ttlMs + staleMs });
}

export function cacheDel(key: string): void {
  store.delete(key);
}

/** Stats for /health */
export function cacheStats(): { entries: number } {
  return { entries: store.size };
}
