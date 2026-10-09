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

// ── Phase D (2026-10-09): Redis-failure circuit breaker ─────────────────────
// After 5 consecutive Redis errors (network fail, timeout, Upstash 429
// quota-exhausted, or a JSON error), stop calling Redis for 60s and fail
// open immediately. Rationale: when the quota is exhausted, every extra
// HTTP round-trip is pure waste (and adds latency); when Redis is down, the
// breaker stops the latency pile-up. Callers already treat null as
// "Redis unavailable", so this is behavior-preserving — only faster.
let consecRedisFails = 0;
let redisCoolUntil = 0;
const REDIS_CB_FAILS = 5;
const REDIS_CB_COOLDOWN_MS = 60_000;

function redisCoolingDown(): boolean {
  return Date.now() < redisCoolUntil;
}

/** Observability for /health. Never throws. */
export function redisCircuitState(): { circuitOpen: boolean; consecFails: number } {
  return { circuitOpen: redisCoolingDown(), consecFails: consecRedisFails };
}

/** Raw command via Upstash REST. Returns parsed `result` or null on error. */
export async function redisCommand(
  args: (string | number)[]
): Promise<unknown> {
  if (!redisEnabled()) return null;
  if (redisCoolingDown()) return null; // fail open, skip the HTTP round-trip
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), REDIS_TIMEOUT_MS);
  const fail = (): null => {
    consecRedisFails++;
    if (consecRedisFails >= REDIS_CB_FAILS) {
      redisCoolUntil = Date.now() + REDIS_CB_COOLDOWN_MS;
    }
    return null;
  };
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
    if (!res.ok) return fail();
    const json = (await res.json()) as { result?: unknown; error?: string };
    if (json.error) return fail();
    consecRedisFails = 0;
    return json.result ?? null;
  } catch {
    return fail();
  } finally {
    clearTimeout(t);
  }
}

/** Health probe for /health — { enabled, ok, circuitOpen, consecFails }. */
export async function redisHealth(): Promise<{
  enabled: boolean;
  ok: boolean;
  circuitOpen: boolean;
  consecFails: number;
}> {
  if (!redisEnabled()) return { enabled: false, ok: false, circuitOpen: false, consecFails: 0 };
  const r = await redisCommand(["PING"]);
  const c = redisCircuitState();
  return { enabled: true, ok: r === "PONG", ...c };
}

// ── Combined rate check: token bucket + fixed window, ONE atomic EVAL ──────
// P1 (2026-10-09): the old fixed-window-only check had the boundary
// double-burst problem (60 requests in the last second of window N + 60 in
// the first second of window N+1 = 120 in 2s, every minute). The token bucket
// (burst 120, refill 2/s per device identity) is continuous — it does not
// reset at window boundaries — so the double-burst is gone. The per-class
// fixed window is kept alongside as the sustained-rate guard.
//
// KEYS[1] = token bucket hash key, KEYS[2] = fixed-window counter key.
// ARGV: tb_capacity, tb_refill_per_sec, now_ms, fw_limit, fw_window_sec.
// Returns {tb_allowed, tb_tokens, tb_reset_sec, tb_retry_sec, fw_allowed, fw_count}.
// Denied requests consume nothing from either limiter.
const RATE_SCRIPT = `
local tb = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tb_cap = tonumber(ARGV[1])
local tb_refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local tokens = tonumber(tb[1])
local ts = tonumber(tb[2])
if tokens == nil or ts == nil then
  tokens = tb_cap
  ts = now
end
local elapsed = (now - ts) / 1000
if elapsed < 0 then elapsed = 0 end
tokens = math.min(tb_cap, tokens + elapsed * tb_refill)
local tb_allowed = 0
local tb_retry = 0
if tokens >= 1 then
  tokens = tokens - 1
  tb_allowed = 1
else
  tb_retry = (1 - tokens) / tb_refill
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], 180)
local tb_reset = (tb_cap - tokens) / tb_refill

local fw_allowed = 1
local fw_count = 0
if tb_allowed == 1 then
  fw_count = redis.call('INCR', KEYS[2])
  if fw_count == 1 then redis.call('EXPIRE', KEYS[2], tonumber(ARGV[5])) end
  if fw_count > tonumber(ARGV[4]) then fw_allowed = 0 end
end
-- NOTE: Redis converts Lua NUMBER replies to integers, which would silently
-- truncate the fractional token values. Format them as strings so the caller
-- gets full precision (verified live 2026-10-09).
local f6 = function(x) return string.format('%.6f', x) end
return {tb_allowed, f6(tokens), f6(tb_reset), f6(tb_retry), fw_allowed, fw_count}
`;

export interface RateCheckResult {
  tbAllowed: boolean;
  tbRemaining: number;
  tbResetSec: number;
  tbRetrySec: number;
  fwAllowed: boolean;
  fwCount: number;
}

/**
 * Atomic token-bucket + fixed-window check.
 * Returns null if Redis failed (caller falls back to in-memory).
 */
export async function redisRateCheck(
  tbKey: string,
  tbCapacity: number,
  tbRefillPerSec: number,
  fwKey: string,
  fwLimit: number,
  fwWindowSec: number
): Promise<RateCheckResult | null> {
  const r = await redisCommand([
    "EVAL", RATE_SCRIPT, 2, tbKey, fwKey,
    tbCapacity, tbRefillPerSec, Date.now(), fwLimit, fwWindowSec,
  ]);
  if (!Array.isArray(r) || r.length < 6) return null;
  const n = (v: unknown): number => {
    const x = typeof v === "string" ? parseFloat(v) : Number(v);
    return Number.isFinite(x) ? x : 0;
  };
  return {
    tbAllowed: n(r[0]) === 1,
    tbRemaining: n(r[1]),
    tbResetSec: n(r[2]),
    tbRetrySec: n(r[3]),
    fwAllowed: n(r[4]) === 1,
    fwCount: n(r[5]),
  };
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
