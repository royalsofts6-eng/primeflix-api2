/**
 * Plain Node (IncomingMessage/ServerResponse) adapter for the security layer.
 * Used by api/index.ts (Vercel serverless function, no framework).
 */
import { verifyHmacParts, isMemberRefRevoked } from "./hmac.js";
import { issueToken, verifyToken, JWT_TTL_S } from "./jwt.js";
import {
  isMemberKeyValid,
  isRevoked,
  memberRef,
  registerDevice,
  revokeMemberKey,
  unregisterDevice,
  isDeviceRegistered,
  checkReplaceAllowed,
} from "./devices.js";
import { checkRateLimit, type RateLimit } from "./ratelimit.js";
import { deriveDeviceSecretFromRef } from "./hmac.js";
import { redisEnabled, redisCommand, redisCircuitState } from "./redis.js";
import { timingSafeEqualStr } from "./crypto.js";

export interface GateResult {
  ok: boolean;
  status?: number;
  code?: string;
  error?: string;
  retryAfter?: number;
  /** Standard RateLimit-Limit/Remaining/Reset headers for the response. */
  rlHeaders?: Record<string, string>;
  memberRef?: string;
  deviceId?: string;
  mode?: "apikey" | "hmac";
}

/** Draft-ietf RateLimit header fields from a rate-limit decision. */
function rateLimitHeaders(rl: RateLimit): Record<string, string> {
  const h: Record<string, string> = {};
  if (rl.limit !== undefined) h["RateLimit-Limit"] = String(rl.limit);
  if (rl.remaining !== undefined)
    h["RateLimit-Remaining"] = String(Math.max(0, Math.floor(rl.remaining)));
  if (rl.resetSec !== undefined)
    h["RateLimit-Reset"] = String(Math.max(0, Math.ceil(rl.resetSec)));
  return h;
}

type HeaderGetter = (name: string) => string | null;

export function nodeHeaderGetter(req: any): HeaderGetter {
  const h = req.headers || {};
  return (name: string) => {
    const v = h[name.toLowerCase()];
    if (Array.isArray(v)) return v[0] ?? null;
    return (v as string) ?? null;
  };
}

/**
 * P0 429 fix (2026-10-09): per-device rate identity.
 * A valid X-Device-Id header moves the requester out of the shared
 * "apikey"/member bucket into its own `dev:<id>` bucket, so per-device
 * capacity scales linearly with users. Missing/invalid values fall back
 * to the existing identity logic (never 400 here — just ignored).
 */
const DEVICE_ID_RE = /^[A-Za-z0-9-_]{1,64}$/;
export function sanitizeDeviceId(raw: string | null): string | null {
  if (!raw) return null;
  const v = raw.trim();
  return DEVICE_ID_RE.test(v) ? v : null;
}

export function readBody(req: any): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (c: any) => {
      data += c;
      if (data.length > 64 * 1024) {
        // 64KB cap — auth bodies are tiny. Destroy and fail LOUDLY with 413
        // (the old code resolved truncated data and produced a misleading
        // BAD_SIGNATURE instead).
        req.destroy();
        reject(new Error("request body too large"));
      }
    });
    req.on("end", () => resolve(data));
    req.on("error", () => resolve(""));
  });
}

/**
 * Security gate for the plain handler.
 * Order: X-API-Key (day-1) → HMAC-SHA256 + JWT.
 * Returns { ok: true } or { ok: false, status, code, error }.
 */
// ── P1-7 (2026-10-09): admin/app key split + kill-switch ─────────────────────
// The day-1 shared API key (hardcoded in the APK) used to double as the
// ADMIN key — anyone who decompiled the APK could revoke member keys.
// Admin operations now need ADMIN_KEY (a distinct Vercel env var). When
// ADMIN_KEY is unset, the day-1 key still works as admin (backward compat —
// nothing breaks on deploy); set ADMIN_KEY in Vercel to complete the split.
const ADMIN_KEY = process.env.ADMIN_KEY || null;
let adminFallbackWarned = false;

/** True when ADMIN_KEY is set (the split is complete). For /health. */
export function adminKeyConfigured(): boolean {
  return ADMIN_KEY !== null && ADMIN_KEY.length > 0;
}

function isAdminKey(got: string | null): boolean {
  if (!got) return false;
  // P4 (2026-10-10): the old code accepted the day-1 API key as admin even
  // when ADMIN_KEY was set — setting ADMIN_KEY changed nothing (verified:
  // the fallback `return !!apiKey && got === apiKey` ran unconditionally).
  // Now the split is real: with ADMIN_KEY configured, ONLY ADMIN_KEY is
  // admin. Without it, the day-1 key keeps working (backward compat — the
  // deploy itself breaks nothing; set ADMIN_KEY to complete the split).
  if (ADMIN_KEY && timingSafeEqualStr(got, ADMIN_KEY)) return true;
  if (adminKeyConfigured()) return false;
  if (!adminFallbackWarned) {
    adminFallbackWarned = true;
    console.warn("[security] ADMIN_KEY not set — day-1 API key still accepted for admin ops (backward compat). Set ADMIN_KEY in Vercel env to complete the split.");
  }
  const apiKey = process.env.API_KEY;
  return !!apiKey && timingSafeEqualStr(got, apiKey);
}

// Runtime kill-switch for the day-1 shared app key (P1-7): SET pf:kill:apikey
// in Redis (Upstash console or CLI) and every instance rejects the shared
// key immediately — no redeploy, no env change. HMAC members and the admin
// key keep working. Memoized 10s per instance so the check costs ~1 Redis
// read per 10s, not per request.
const KILL_KEY = "pf:kill:apikey";
const KILL_MEMO_MS = 10_000;
let killMemo: { at: number; killed: boolean } | null = null;
// P4 (2026-10-10): last CONFIRMED kill state. The old code returned
// `killed = false` on ANY Redis error — a Redis blip (or an attacker
// DDoSing Redis) silently un-killed a compromised key. Now a Redis outage
// serves the last-known state: a killed key STAYS killed through the blip.
// Initialized false; updated only on reads we are confident about.
let lastKnownKilled = false;

/** True when the shared day-1 API key is runtime-killed. For the gate + /health. */
export async function apiKeyKilled(): Promise<boolean> {
  const now = Date.now();
  if (killMemo && now - killMemo.at < KILL_MEMO_MS) return killMemo.killed;
  // Default: last-known state (fail to last-known, NOT to false).
  let killed = lastKnownKilled;
  try {
    if (redisEnabled()) {
      if (!redisCircuitState().circuitOpen) {
        const v = await redisCommand(["GET", KILL_KEY]).catch(() => null);
        const st = redisCircuitState();
        if (v !== null) {
          killed = true; // key present — definitely killed
          lastKnownKilled = true;
        } else if (st.consecFails === 0 && !st.circuitOpen) {
          // Genuine absence (no Redis errors on this read) — definitely
          // not killed. A null WITH errors is ambiguous (transient
          // failure vs absent key): keep last-known, don't overwrite it.
          killed = false;
          lastKnownKilled = false;
        }
      }
    } else {
      killed = false;
      lastKnownKilled = false;
    }
  } catch {
    // keep lastKnownKilled
  }
  killMemo = { at: now, killed };
  return killed;
}

export async function authGatePlain(
  method: string,
  pathname: string,
  header: HeaderGetter,
  clientIp: string,
  bodyText = "",
  /** raw query string (bound into the HMAC signature) */
  query = ""
): Promise<GateResult> {
  // P0 429 fix: per-device rate identity from X-Device-Id (sanitized).
  // A valid device id moves the requester into its own `dev:<id>` bucket;
  // missing/invalid → existing fallback identity per mode (unchanged).
  const devId = sanitizeDeviceId(header("X-Device-Id"));

  // ── Mode 1: day-1 API key ──
  const apiKey = process.env.API_KEY;
  const gotKey = header("X-API-Key");
  if (apiKey && timingSafeEqualStr(gotKey || "", apiKey)) {
    // P1-7: runtime kill-switch — a compromised shared key is cut off in
    // ~10s (memo window) with zero redeploy.
    if (await apiKeyKilled()) {
      return { ok: false, status: 403, code: "APIKEY_KILLED", error: "api key disabled" };
    }
    // P4 (2026-10-10): X-Device-Id is self-asserted — one client can rotate
    // it to mint unlimited fresh rate buckets. Anomaly detection collapses
    // churners into a shared per-IP bucket (see apikeyRateIdentity).
    const rl = await checkRateLimit(pathname, await apikeyRateIdentity(clientIp, devId));
    if (!rl.allowed)
      return { ok: false, status: 429, code: "RATE_LIMITED", error: "rate limited", retryAfter: rl.retryAfterSec, rlHeaders: rateLimitHeaders(rl) };
    return { ok: true, mode: "apikey", rlHeaders: rateLimitHeaders(rl) };
  }

  // ── Mode 2: HMAC signed request ──
  const check = await verifyHmacParts(method, pathname, header, bodyText, query);
  if (!check.ok) {
    return { ok: false, status: 401, code: check.code, error: check.error };
  }
  // Revocation is enforced on EVERY authenticated route — a revoked member
  // is cut off immediately, not just on /v1/stream/* (their old JWTs die at
  // the 24h expiry at the latest).
  if (check.memberRef && (await isMemberRefRevoked(check.memberRef))) {
    return { ok: false, status: 403, code: "REVOKED", error: "member key revoked" };
  }
  // P4 (2026-10-10): the rate identity is bound to the JWT-verified device
  // id (server-issued at registration, cross-checked against the token's
  // `did` claim in verifyHmacParts) — NOT the self-asserted X-Device-Id
  // header, which the old code trusted for bucketing.
  const hmacIdentity = check.deviceId ? `dev:${check.deviceId}` : (check.memberRef || "unknown");
  const rl = await checkRateLimit(pathname, hmacIdentity);
  if (!rl.allowed) {
    return { ok: false, status: 429, code: "RATE_LIMITED", error: "rate limited", retryAfter: rl.retryAfterSec, rlHeaders: rateLimitHeaders(rl) };
  }
  return { ok: true, mode: "hmac", memberRef: check.memberRef, deviceId: check.deviceId, rlHeaders: rateLimitHeaders(rl) };
}

/**
 * P4 (2026-10-10): device-churn anomaly detection for day-1 API-key mode.
 *
 * X-Device-Id is self-asserted: without this, one client rotates the header
 * value per request and every rotation gets a fresh 120-burst token bucket —
 * the per-device rate limit becomes decorative. The global backstop still
 * caps the cluster aggregate; this adds the per-IP layer.
 *
 * Mechanism: HyperLogLog of distinct device ids per IP per hour
 * (`pf:churn:<ip>:<hour>`). More than DEVICE_CHURN_LIMIT distinct ids in an
 * hour collapses the caller into a single shared `ipchurn:<ip>` bucket.
 * Legitimate reinstalls (1–2 ids/IP/hour) never trip it; the verdict is
 * memoized per instance so the common case costs one PFADD.
 */
const DEVICE_CHURN_LIMIT = 20; // distinct device ids per IP per hour
const churnTripped = new Set<string>(); // per-instance memo of "ip:hour" keys over the limit

async function apikeyRateIdentity(clientIp: string, devId: string | null): Promise<string> {
  if (!devId) return "apikey";
  if (!clientIp || clientIp === "unknown") return `dev:${devId}`; // can't attribute — backstop still applies
  if (!redisEnabled()) return `dev:${devId}`; // no shared state — can't detect churn; backstop still applies
  const hourKey = `${clientIp}:${Math.floor(Date.now() / 3600000)}`;
  if (churnTripped.has(hourKey)) return `ipchurn:${clientIp}`;
  if (churnTripped.size > 10000) churnTripped.clear(); // bound memory; keys rotate hourly anyway
  try {
    const pfKey = `pf:churn:${hourKey}`;
    const added = await redisCommand(["PFADD", pfKey, devId]).catch(() => null);
    if (added === 1 || added === "1") {
      await redisCommand(["EXPIRE", pfKey, 3720]).catch(() => null);
      const n = Number(await redisCommand(["PFCOUNT", pfKey]).catch(() => 0));
      if (Number.isFinite(n) && n > DEVICE_CHURN_LIMIT) {
        churnTripped.add(hourKey);
        return `ipchurn:${clientIp}`;
      }
    }
  } catch {
    /* fail open to the per-device bucket — backstop still caps the aggregate */
  }
  return `dev:${devId}`;
}

/** POST /v1/auth/register */
export async function registerPlain(bodyText: string, clientIp: string): Promise<{ status: number; json: unknown; rlHeaders?: Record<string, string> }> {
  // P4 (2026-10-10): countBackstop=false — this route is unauthenticated, so
  // its requests must not feed the cluster-wide backstop counter (the INCR
  // ran before the per-IP decision, letting one IP trip the backstop for
  // everyone). The 10/min/IP auth-class limit below still stands.
  const rl = await checkRateLimit("/v1/auth/register", `ip:${clientIp}`, false);
  if (!rl.allowed)
    return { status: 429, json: { success: false, error: "rate limited", code: "RATE_LIMITED" }, rlHeaders: rateLimitHeaders(rl) };
  let body: { memberKey?: string; deviceId?: string };
  try {
    body = JSON.parse(bodyText || "{}");
  } catch {
    return { status: 400, json: { success: false, error: "invalid JSON", code: "BAD_BODY" } };
  }
  const memberKey = (body.memberKey || "").trim();
  const deviceId = (body.deviceId || "").trim();
  if (!isMemberKeyValid(memberKey)) {
    return { status: 401, json: { success: false, error: "invalid member key", code: "BAD_MEMBER_KEY" } };
  }
  if (await isRevoked(memberKey)) {
    return { status: 403, json: { success: false, error: "member key revoked", code: "REVOKED" } };
  }
  const reg = await registerDevice(memberKey, deviceId);
  if (!reg.ok) {
    return { status: 403, json: { success: false, error: reg.reason, code: "DEVICE_LIMIT" } };
  }
  const ref = await memberRef(memberKey);
  const token = await issueToken(ref, deviceId);
  const deviceSecret = await deriveDeviceSecretFromRef(ref, deviceId);
  return {
    status: 200,
    json: { success: true, data: { token, deviceSecret, expiresIn: JWT_TTL_S, tokenType: "Bearer" } },
  };
}

/** POST /v1/auth/refresh — rate-limited by IP (a stolen token must not be
 *  refreshable at machine speed; refresh also re-checks revocation). */
export async function refreshPlain(bodyText: string, clientIp: string): Promise<{ status: number; json: unknown; rlHeaders?: Record<string, string> }> {
  // P4 (2026-10-10): countBackstop=false — same reasoning as registerPlain
  // (unauthenticated route must not inflate the cluster-wide counter).
  const rl = await checkRateLimit("/v1/auth/refresh", `ip:${clientIp}`, false);
  if (!rl.allowed)
    return { status: 429, json: { success: false, error: "rate limited", code: "RATE_LIMITED" }, rlHeaders: rateLimitHeaders(rl) };
  let body: { token?: string };
  try {
    body = JSON.parse(bodyText || "{}");
  } catch {
    return { status: 400, json: { success: false, error: "invalid JSON", code: "BAD_BODY" } };
  }
  const claims = await verifyToken(body.token || "");
  if (!claims) {
    return { status: 401, json: { success: false, error: "invalid or expired token", code: "BAD_TOKEN" } };
  }
  if (await isMemberRefRevoked(claims.sub)) {
    return { status: 403, json: { success: false, error: "member key revoked", code: "REVOKED" } };
  }
  const token = await issueToken(claims.sub, claims.did);
  return { status: 200, json: { success: true, data: { token, expiresIn: JWT_TTL_S } } };
}

/** POST /v1/auth/revoke (admin: ADMIN_KEY, or day-1 X-API-Key as fallback) */
export async function revokePlain(
  bodyText: string,
  header: HeaderGetter
): Promise<{ status: number; json: unknown }> {
  // P1-7: admin key is now DISTINCT from the app key. Falls back to the
  // day-1 key only until ADMIN_KEY is set in Vercel env.
  if (!isAdminKey(header("X-API-Key"))) {
    return { status: 403, json: { success: false, error: "admin only", code: "FORBIDDEN" } };
  }
  let body: { memberKey?: string };
  try {
    body = JSON.parse(bodyText || "{}");
  } catch {
    return { status: 400, json: { success: false, error: "invalid JSON", code: "BAD_BODY" } };
  }
  const memberKey = (body.memberKey || "").trim();
  if (!memberKey) {
    return { status: 400, json: { success: false, error: "memberKey required", code: "BAD_BODY" } };
  }
  // P4 (2026-10-10): the Redis fan-out is AWAITED now (revokeMemberKey
  // returns whether cross-instance propagation is confirmed). The response
  // reports `propagated: false` instead of silently claiming a revocation
  // that never left this instance.
  const propagated = await revokeMemberKey(memberKey);
  return { status: 200, json: { success: true, data: { revoked: true, propagated } } };
}

/**
 * POST /v1/auth/device/remove — free one device slot (reinstall flow).
 *
 * The 2-device limit had no unregister: a reinstall (new deviceId) with both
 * slots taken meant a 30-day lockout. This removes ONE device id from the
 * caller's own registry so the new install can register.
 *
 * Auth (either):
 *  - HMAC mode: the gate already verified the caller; authedMemberRef binds
 *    the removal to the caller's OWN member (never another member's).
 *  - memberKey in body: the lost-device case (no working registered device
 *    left to sign with). Validated + revocation-checked like /register, and
 *    throttled 10/min/IP (auth class, no backstop feed — see registerPlain).
 *
 * Abuse limits: one replacement per member per 24h (checkReplaceAllowed —
 * atomic SET NX EX), enforced BEFORE the removal. A read-only membership
 * probe runs first so probing bogus ids neither burns the daily token nor
 * leaks anything beyond "not registered".
 */
export async function deviceRemovePlain(
  bodyText: string,
  clientIp: string,
  authedMemberRef: string | null,
  authedMode: string | null
): Promise<{ status: number; json: unknown }> {
  let body: { memberKey?: string; deviceId?: string };
  try {
    body = JSON.parse(bodyText || "{}");
  } catch {
    return { status: 400, json: { success: false, error: "invalid JSON", code: "BAD_BODY" } };
  }
  const deviceId = (body.deviceId || "").trim();
  if (!deviceId) {
    return { status: 400, json: { success: false, error: "deviceId required", code: "BAD_BODY" } };
  }
  let ref: string;
  if (authedMode === "hmac" && authedMemberRef) {
    ref = authedMemberRef;
  } else {
    // Lost-device case: prove ownership with the member key itself.
    const rl = await checkRateLimit("/v1/auth/device/remove", `ip:${clientIp}`, false);
    if (!rl.allowed) {
      return { status: 429, json: { success: false, error: "rate limited", code: "RATE_LIMITED" } };
    }
    const memberKey = (body.memberKey || "").trim();
    if (!isMemberKeyValid(memberKey)) {
      return { status: 401, json: { success: false, error: "invalid member key", code: "BAD_MEMBER_KEY" } };
    }
    if (await isRevoked(memberKey)) {
      return { status: 403, json: { success: false, error: "member key revoked", code: "REVOKED" } };
    }
    ref = await memberRef(memberKey);
  }
  // Read-only membership probe first: probing bogus ids must not burn the
  // daily swap token (and tells the caller honestly nothing was removed).
  if (!(await isDeviceRegistered(ref, deviceId))) {
    return { status: 200, json: { success: true, data: { removed: false } } };
  }
  // Abuse limit BEFORE the removal (atomic SET NX EX): one replacement per
  // member per 24h, so a shared member key can't be slot-churned across
  // dozens of devices.
  if (!(await checkReplaceAllowed(ref))) {
    return { status: 429, json: { success: false, error: "device replacement limit reached (1 per 24h)", code: "REPLACE_LIMIT" } };
  }
  const removed = await unregisterDevice(ref, deviceId);
  return { status: 200, json: { success: true, data: { removed } } };
}
