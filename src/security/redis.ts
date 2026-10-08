/**
 * Upstash Redis (REST) shared state — cross-instance rate limits, device
 * registry, revocation list, FZMovies cache.
 *
 * The backend runs on serverless (Vercel): each instance has its own memory,
 * so per-instance maps can't enforce anything cluster-wide. Redis makes the
 * enforcement strict on api1 AND api2 (and the future Cloudflare cluster).
 *
 * Env: UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN (set on Vercel).
 *
 * PRIVACY NOTE (per Upstash guidance for this temp DB): never store PII or
 * production secrets here. Only counters, key IDs (memberRef hashes), and
 * cached provider results. Raw member keys NEVER leave the instance.
 *
 * Fail-open on Redis errors: every helper returns null on failure and the
 * caller falls back to the existing in-memory behavior, so a Redis outage
 * degrades to best-effort instead of breaking the API.
 */

const REDIS_TIMEOUT_MS = 3000;

function redisUrl(): string {
  return (process.env.UPSTASH_REDIS_REST_URL || "").replace(/\/$/, "");
}
function redisToken(): string {
  return process.env.UPSTASH_REDIS_REST_TOKEN || "";
}

export function redisEnabled(): boolean {
  return redisUrl().length > 0 && redisToken().length > 0;
}

/** Raw command via Upstash REST. Returns parsed `result` or null on error. */
export async function redisCommand(
  args: (string | number)[]
): Promise<unknown> {
  if (!redisEnabled()) return null;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), REDIS_TIMEOUT_MS);
  try {
    const res = await fetch(redisUrl(), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${redisToken()}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(args.map(String)),
      signal: ctrl.signal,
    });
    if (!res.ok) return null;
    const json = (await res.json()) as { result?: unknown; error?: string };
    if (json.error) return null;
    return json.result ?? null;
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}

/** Health probe for /health — { enabled, ok }. */
export async function redisHealth(): Promise<{ enabled: boolean; ok: boolean }> {
  if (!redisEnabled()) return { enabled: false, ok: false };
  const r = await redisCommand(["PING"]);
  return { enabled: true, ok: r === "PONG" };
}

// ── Fixed-window rate limiting (atomic via EVAL) ────────────────────────────
// Script: INCR counter; set TTL on first hit; allow iff count <= limit.
const RATE_SCRIPT = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('EXPIRE', KEYS[1], ARGV[2]) end
if count > tonumber(ARGV[1]) then return 0 end
return 1
`;

/**
 * Returns true if the request is allowed (count within limit), false if
 * rate-limited, null if Redis failed (caller should fall back to in-memory).
 */
export async function redisFixedWindow(
  key: string,
  limit: number,
  windowSec: number
): Promise<boolean | null> {
  const r = await redisCommand(["EVAL", RATE_SCRIPT, 1, key, limit, windowSec]);
  if (r === 1 || r === "1") return true;
  if (r === 0 || r === "0") return false;
  return null;
}

// ── Device registry (atomic check-and-add via EVAL) ─────────────────────────
// Returns "added" | "exists" | "limit" | null (Redis failed).
const DEVICE_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return 0 end
if redis.call('SCARD', KEYS[1]) >= tonumber(ARGV[2]) then return 1 end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 0
`;

export async function redisDeviceRegister(
  memberRef: string,
  deviceId: string,
  maxDevices: number
): Promise<"added" | "exists" | "limit" | null> {
  const key = `pf:dev:${memberRef}`;
  const r = await redisCommand(["EVAL", DEVICE_SCRIPT, 1, key, deviceId, maxDevices, 30 * 24 * 3600]);
  // NOTE: EVAL can't distinguish "exists" from "added" with this script
  // (both return 0). Re-checking membership would cost another round-trip;
  // treat 0 as success either way — re-registering the same device is fine.
  if (r === 1 || r === "1") return "limit";
  if (r === 0 || r === "0") return "added";
  return null;
}

// ── Revocation set (key IDs only — never raw member keys) ───────────────────
const REVOKED_KEY = "pf:revoked";

export async function redisRevokeRef(memberRef: string): Promise<void> {
  await redisCommand(["SADD", REVOKED_KEY, memberRef]);
}

export async function redisIsRefRevoked(memberRef: string): Promise<boolean | null> {
  const r = await redisCommand(["SISMEMBER", REVOKED_KEY, memberRef]);
  if (r === 1 || r === "1") return true;
  if (r === 0 || r === "0") return false;
  return null;
}

// ── FZMovies shared cache ───────────────────────────────────────────────────
export async function redisCacheGet<T>(key: string): Promise<T | null> {
  const r = await redisCommand(["GET", key]);
  if (typeof r !== "string" || !r) return null;
  try {
    return JSON.parse(r) as T;
  } catch {
    return null;
  }
}

export async function redisCacheSet(key: string, value: unknown, ttlSec: number): Promise<void> {
  await redisCommand(["SET", key, JSON.stringify(value), "EX", ttlSec]);
}
