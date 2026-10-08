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
import { checkRateLimit } from "./ratelimit.js";
import { deriveDeviceSecretFromRef } from "./hmac.js";

export interface GateResult {
  ok: boolean;
  status?: number;
  code?: string;
  error?: string;
  retryAfter?: number;
  memberRef?: string;
  deviceId?: string;
  mode?: "apikey" | "hmac";
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
export async function authGatePlain(
  method: string,
  pathname: string,
  header: HeaderGetter,
  clientIp: string,
  bodyText = "",
  /** raw query string (bound into the HMAC signature) */
  query = ""
): Promise<GateResult> {
  // ── Mode 1: day-1 API key ──
  const apiKey = process.env.API_KEY;
  const gotKey = header("X-API-Key");
  if (apiKey && gotKey === apiKey) {
    const rl = await checkRateLimit(pathname, "apikey");
    if (!rl.allowed)
      return { ok: false, status: 429, code: "RATE_LIMITED", error: "rate limited", retryAfter: rl.retryAfterSec };
    return { ok: true, mode: "apikey" };
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
  const rl = await checkRateLimit(pathname, check.memberRef || "unknown");
  if (!rl.allowed) {
    return { ok: false, status: 429, code: "RATE_LIMITED", error: "rate limited", retryAfter: rl.retryAfterSec };
  }
  return { ok: true, mode: "hmac", memberRef: check.memberRef, deviceId: check.deviceId };
}

/** POST /v1/auth/register */
export async function registerPlain(bodyText: string, clientIp: string): Promise<{ status: number; json: unknown }> {
  const rl = await checkRateLimit("/v1/auth/register", `ip:${clientIp}`);
  if (!rl.allowed)
    return { status: 429, json: { success: false, error: "rate limited", code: "RATE_LIMITED" } };
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
export async function refreshPlain(bodyText: string, clientIp: string): Promise<{ status: number; json: unknown }> {
  const rl = await checkRateLimit("/v1/auth/refresh", `ip:${clientIp}`);
  if (!rl.allowed)
    return { status: 429, json: { success: false, error: "rate limited", code: "RATE_LIMITED" } };
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

/** POST /v1/auth/revoke (admin: X-API-Key) */
export async function revokePlain(
  bodyText: string,
  header: HeaderGetter
): Promise<{ status: number; json: unknown }> {
  const apiKey = process.env.API_KEY;
  if (!apiKey || header("X-API-Key") !== apiKey) {
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
