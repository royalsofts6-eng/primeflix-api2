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
} from "./devices.js";
import { checkRateLimit, type RateLimit } from "./ratelimit.js";
import { deriveDeviceSecretFromRef } from "./hmac.js";
import { redisEnabled, redisCommand } from "./redis.js";

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
  if (ADMIN_KEY && got === ADMIN_KEY) return true;
  if (!adminKeyConfigured() && !adminFallbackWarned) {
    adminFallbackWarned = true;
    console.warn("[security] ADMIN_KEY not set — day-1 API key still accepted for admin ops (backward compat). Set ADMIN_KEY in Vercel env to complete the split.");
  }
  const apiKey = process.env.API_KEY;
  return !!apiKey && got === apiKey;
}

// Runtime kill-switch for the day-1 shared app key (P1-7): SET pf:kill:apikey
// in Redis (Upstash console or CLI) and every instance rejects the shared
// key immediately — no redeploy, no env change. HMAC members and the admin
// key keep working. Memoized 10s per instance so the check costs ~1 Redis
// read per 10s, not per request.
const KILL_KEY = "pf:kill:apikey";
const KILL_MEMO_MS = 10_000;
let killMemo: { at: number; killed: boolean } | null = null;

/** True when the shared day-1 API key is runtime-killed. For the gate + /health. */
export async function apiKeyKilled(): Promise<boolean> {
  const now = Date.now();
  if (killMemo && now - killMemo.at < KILL_MEMO_MS) return killMemo.killed;
  let killed = false;
  try {
    if (redisEnabled()) {
      killed = (await redisCommand(["GET", KILL_KEY]).catch(() => null)) !== null;
    }
  } catch {
    killed = false;
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
  if (apiKey && gotKey === apiKey) {
    // P1-7: runtime kill-switch — a compromised shared key is cut off in
    // ~10s (memo window) with zero redeploy.
    if (await apiKeyKilled()) {
      return { ok: false, status: 403, code: "APIKEY_KILLED", error: "api key disabled" };
    }
    const rl = await checkRateLimit(pathname, devId ? `dev:${devId}` : "apikey");
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
  const rl = await checkRateLimit(pathname, devId ? `dev:${devId}` : (check.memberRef || "unknown"));
  if (!rl.allowed) {
    return { ok: false, status: 429, code: "RATE_LIMITED", error: "rate limited", retryAfter: rl.retryAfterSec, rlHeaders: rateLimitHeaders(rl) };
  }
  return { ok: true, mode: "hmac", memberRef: check.memberRef, deviceId: check.deviceId, rlHeaders: rateLimitHeaders(rl) };
}

/** POST /v1/auth/register */
export async function registerPlain(bodyText: string, clientIp: string): Promise<{ status: number; json: unknown; rlHeaders?: Record<string, string> }> {
  const rl = await checkRateLimit("/v1/auth/register", `ip:${clientIp}`);
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
  const rl = await checkRateLimit("/v1/auth/refresh", `ip:${clientIp}`);
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
  revokeMemberKey(memberKey);
  return { status: 200, json: { success: true, data: { revoked: true } } };
}
