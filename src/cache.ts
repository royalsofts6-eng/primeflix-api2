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

// ── Phase D (2026-10-09): versioned shared-cache namespace ─────────────────
// All Redis-shared cache keys live under `pf:v{N}:...` so a schema change is
// one constant bump — old keys orphan-expire (≤24h), never FLUSHDB (that
// would wipe rate limits + the device registry). Operational/security state
// (pf:dev:*, pf:revoked, pf:cooldown:*, pf:report:*, pf:dead:*, pf:kill:*,
// pf:tb:*, pf:rl:*, pf:backstop:*) is NOT versioned — it is not cache.
// CACHE_SCHEMA_OVERRIDE lets ops bump the schema via env without a code
// change; it is read hot on every key build (no redeploy needed).
export const CACHE_SCHEMA = 3;

function cacheSchema(): number {
  const o = parseInt(process.env.CACHE_SCHEMA_OVERRIDE || "", 10);
  return Number.isFinite(o) && o > 0 ? o : CACHE_SCHEMA;
}

/** Versioned shared-cache key: `pf:v3:{domain}:{parts...}` (lowercased). */
export function ck(domain: string, ...parts: (string | number)[]): string {
  return `pf:v${cacheSchema()}:${domain}:${parts.join(":")}`.toLowerCase();
}
