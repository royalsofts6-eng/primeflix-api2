/**
 * Member keys, device registry, revocation.
 *
 * Design (stateless-first, serverless-safe):
 *  - Member keys come from MEMBER_KEYS env (comma-separated). Firebase
 *    integration is Phase 2; env allowlist is the pragmatic day-1 store.
 *  - Device secrets are DERIVED deterministically:
 *        deviceSecret = HMAC-SHA256(API_SECRET, "pf-device:" + memberRef + ":" + deviceId)
 *    Any instance can recompute it — no shared storage needed for verification.
 *  - 2-device limit is enforced BEST-EFFORT in-memory per instance.
 *    Strict cross-instance enforcement needs shared Redis (Phase 2).
 *  - Revocation: REVOKED_KEYS env (comma-separated) + in-memory blocklist
 *    (POST /v1/auth/revoke with admin key). Checked on /v1/stream/*.
 */
import { sha256Hex, hmacSha256Hex } from "./crypto.js";
import {
  redisEnabled,
  redisCommand,
  redisDeviceRegister,
  redisRevokeRef,
  redisIsRefRevoked,
} from "./redis.js";

function envList(name: string): Set<string> {
  const raw = process.env[name] || "";
  return new Set(
    raw
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
  );
}

/** Opaque reference for a member key — never put raw keys in JWTs/logs. */
export async function memberRef(memberKey: string): Promise<string> {
  return (await sha256Hex("pf-member:" + memberKey)).slice(0, 32);
}

export function isMemberKeyValid(memberKey: string): boolean {
  if (!memberKey) return false;
  const keys = envList("MEMBER_KEYS");
  if (keys.size === 0) return false; // fail closed: no keys configured = no registration
  return keys.has(memberKey.trim());
}

export async function isRevoked(memberKey: string): Promise<boolean> {
  if (inMemoryBlocklist.has(memberKey.trim())) return true;
  if (envList("REVOKED_KEYS").has(memberKey.trim())) return true;
  // Redis revocation set (key IDs only) — checked by memberRef.
  if (redisEnabled()) {
    const ref = await memberRef(memberKey.trim());
    const hit = await redisIsRefRevoked(ref);
    if (hit === true) return true;
  }
  return false;
}

const inMemoryBlocklist = new Set<string>();
/**
 * Revoke a member key.
 *
 * P4 (2026-10-10): the Redis fan-out is now AWAITED. The old fire-and-forget
 * `.then()` could die with the serverless invocation after `res.end()` but
 * before the SADD landed — the response said "revoked" while api1/api2
 * instances kept honoring the key. Now returns whether cross-instance
 * propagation is CONFIRMED durable; callers surface `propagated: false`
 * instead of silently lying.
 */
export async function revokeMemberKey(memberKey: string): Promise<boolean> {
  inMemoryBlocklist.add(memberKey.trim());
  // Fan out to Redis so ALL instances (api1 + api2) honor it immediately.
  if (redisEnabled()) {
    try {
      const ref = await memberRef(memberKey.trim());
      return await redisRevokeRef(ref);
    } catch {
      return false;
    }
  }
  return true; // no Redis configured: the in-memory blocklist IS the whole story
}
/** Check revocation by memberRef (hash) — for use with JWT claims. */
export async function isRevokedRaw(ref: string): Promise<boolean> {
  for (const k of inMemoryBlocklist) {
    if ((await memberRef(k)).slice(0, 32) === ref) return true;
  }
  for (const k of envList("REVOKED_KEYS")) {
    if ((await memberRef(k)).slice(0, 32) === ref) return true;
  }
  if (redisEnabled()) {
    const hit = await redisIsRefRevoked(ref);
    if (hit === true) return true;
  }
  return false;
}
export function blocklistSize(): number {
  return inMemoryBlocklist.size + envList("REVOKED_KEYS").size;
}

/** Deterministic per-device secret — verifiable on any instance. */
export async function deriveDeviceSecret(memberKey: string, deviceId: string): Promise<string> {
  const master = process.env.API_SECRET || process.env.API_KEY || "";
  if (!master) throw new Error("API_SECRET not configured");
  const ref = await memberRef(memberKey);
  return hmacSha256Hex(master, `pf-device:${ref}:${deviceId}`);
}

// ── Device registry (best-effort, per-instance) ──────────────────────────────
const MAX_DEVICES = 2;
const registry = new Map<string, Set<string>>(); // memberRef -> deviceIds

export async function registerDevice(
  memberKey: string,
  deviceId: string
): Promise<{ ok: true } | { ok: false; reason: string }> {
  if (!deviceId || deviceId.length < 8 || deviceId.length > 128) {
    return { ok: false, reason: "bad device id" };
  }
  const ref = await memberRef(memberKey);
  // Redis atomic check-and-add (shared across api1/api2) — preferred path.
  if (redisEnabled()) {
    const r = await redisDeviceRegister(ref, deviceId, MAX_DEVICES);
    if (r === "limit") return { ok: false, reason: "device limit reached (2 max)" };
    if (r === "added" || r === "exists") return { ok: true };
    // null = Redis failed → fall through to in-memory registry.
  }
  let set = registry.get(ref);
  if (!set) {
    set = new Set();
    registry.set(ref, set);
  }
  if (set.has(deviceId)) return { ok: true }; // re-register same device = fine
  if (set.size >= MAX_DEVICES) {
    return { ok: false, reason: "device limit reached (2 max)" };
  }
  set.add(deviceId);
  return { ok: true };
}

export function registryStats(): { members: number; devices: number } {
  let devices = 0;
  for (const s of registry.values()) devices += s.size;
  return { members: registry.size, devices };
}

// ── Device unregister / replace (P4 2026-10-10) ─────────────────────────────
// The 2-device limit had no unregister path: a reinstall (new deviceId) with
// both slots taken meant a 30-day lockout (the Redis set expires after
// 30d). POST /v1/auth/device/remove frees one slot.
//
// Abuse resistance (do NOT weaken the anti-abuse posture):
//  - A caller can only remove devices belonging to their OWN memberRef
//    (HMAC-bound ref from the gate, or the member key itself) — never
//    another member's.
//  - One replacement per member per 24h (`pf:devreplace:<ref>`, atomic
//    SET NX EX) — a shared member key can't be slot-churned across dozens
//    of devices. The limit is enforced BEFORE the removal; a read-only
//    membership probe runs first so probing bogus ids neither burns the
//    daily token nor leaks anything beyond "not registered".

const REPLACE_LIMIT_S = 24 * 3600; // one device replacement per member per 24h

/** Read-only membership probe (no state change). True if the device id is
 *  currently registered for this memberRef (Redis set or in-memory). */
export async function isDeviceRegistered(ref: string, deviceId: string): Promise<boolean> {
  if (redisEnabled()) {
    try {
      const r = await redisCommand(["SISMEMBER", `pf:dev:${ref}`, deviceId]);
      if (r === 1 || r === "1") return true;
      if (r === 0 || r === "0") return false;
      // null = Redis failed → fall through to in-memory (best-effort)
    } catch {
      /* fall through to in-memory */
    }
  }
  return registry.get(ref)?.has(deviceId) ?? false;
}

/** Remove one device id from the member's registry. True if it was present. */
export async function unregisterDevice(ref: string, deviceId: string): Promise<boolean> {
  let removed = false;
  if (redisEnabled()) {
    try {
      const r = await redisCommand(["SREM", `pf:dev:${ref}`, deviceId]);
      removed = typeof r === "number" ? r > 0 : /^\d+$/.test(String(r ?? "")) && Number(r) > 0;
    } catch {
      removed = false;
    }
  }
  const set = registry.get(ref);
  if (set && set.delete(deviceId)) removed = true;
  return removed;
}

/**
 * Abuse limit: one device replacement per member per 24h. Atomic
 * SET key 1 NX EX 86400 — true when this replacement is allowed (and the
 * token is now consumed), false when the member already swapped within 24h.
 *
 * Fail-closed when Redis is enabled but unreachable: without shared state
 * the limit can't be enforced, so the swap is denied (a reinstall can wait
 * for Redis; slot-churning can't).
 */
export async function checkReplaceAllowed(ref: string): Promise<boolean> {
  if (!redisEnabled()) return true; // no shared state — same best-effort posture as registerDevice
  try {
    const r = await redisCommand(["SET", `pf:devreplace:${ref}`, "1", "NX", "EX", String(REPLACE_LIMIT_S)]);
    return r === "OK";
  } catch {
    return false;
  }
}
