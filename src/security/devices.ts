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

export function isRevoked(memberKey: string): boolean {
  if (inMemoryBlocklist.has(memberKey.trim())) return true;
  return envList("REVOKED_KEYS").has(memberKey.trim());
}

const inMemoryBlocklist = new Set<string>();
export function revokeMemberKey(memberKey: string): void {
  inMemoryBlocklist.add(memberKey.trim());
}
/** Check revocation by memberRef (hash) — for use with JWT claims. */
export async function isRevokedRaw(ref: string): Promise<boolean> {
  for (const k of inMemoryBlocklist) {
    if ((await memberRef(k)).slice(0, 32) === ref) return true;
  }
  for (const k of envList("REVOKED_KEYS")) {
    if ((await memberRef(k)).slice(0, 32) === ref) return true;
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
