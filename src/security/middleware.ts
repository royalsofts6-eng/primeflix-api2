/**
 * PrimeFlix security layer — Hono middleware + auth routes.
 *
 * Auth modes (in order):
 *   1. X-API-Key (day-1, backward compat) → authMode "apikey"
 *   2. HMAC-SHA256 signed request + JWT → authMode "hmac"
 *
 * Public (no auth):  / , /health
 * Auth endpoints:    /v1/auth/register (member key, rate-limited by IP)
 *                    /v1/auth/revoke   (admin: X-API-Key)
 */
import type { Context, Next } from "hono";
import { issueToken, verifyToken, JWT_TTL_S } from "./jwt.js";
import { verifyHmacRequest, isMemberRefRevoked, deriveDeviceSecretFromRef } from "./hmac.js";
import {
  isMemberKeyValid,
  isRevoked,
  memberRef,
  registerDevice,
  revokeMemberKey,
  registryStats,
  blocklistSize,
} from "./devices.js";
import { checkRateLimit, rateLimitStats } from "./ratelimit.js";

const PUBLIC = new Set(["/", "/health"]);

export interface PfAuthState {
  mode: "apikey" | "hmac";
  memberRef?: string;
  deviceId?: string;
}

function unauthorized(c: Context, code: string, error: string, status = 401) {
  return c.json({ success: false, error, code }, status as 401);
}

/** Rate-limit gate. Identity = memberRef/device or client IP. */
async function gateRateLimit(c: Context, identity: string): Promise<Response | null> {
  const pathname = new URL(c.req.url).pathname;
  const rl = checkRateLimit(pathname, identity);
  if (!rl.allowed) {
    return c.json(
      { success: false, error: "rate limited", code: "RATE_LIMITED" },
      429,
      { "Retry-After": String(rl.retryAfterSec || 60) }
    );
  }
  return null;
}

/** Main auth middleware — mount BEFORE routes. */
export async function pfAuth(c: Context, next: Next): Promise<Response | void> {
  const pathname = new URL(c.req.url).pathname;
  if (PUBLIC.has(pathname)) return next();

  // Auth endpoints handle their own auth (register = member key, revoke = admin key)
  if (pathname.startsWith("/v1/auth/")) return next();

  // Cron endpoints have their own CRON_SECRET auth — pass through
  if (pathname.startsWith("/v1/cron/")) return next();

  // ── Mode 1: day-1 API key (backward compat) ──
  const apiKey = process.env.API_KEY;
  const gotKey = c.req.header("X-API-Key") || c.req.query("api_key");
  if (apiKey && gotKey === apiKey) {
    const rl = await gateRateLimit(c, "apikey");
    if (rl) return rl;
    c.set("pfAuth", { mode: "apikey" } as PfAuthState);
    return next();
  }

  // ── Mode 2: HMAC signed request ──
  const check = await verifyHmacRequest(c.req.raw);
  if (!check.ok) {
    return unauthorized(c, check.code || "UNAUTHORIZED", check.error || "unauthorized");
  }

  // Revocation check on expensive endpoints (/v1/stream/*)
  if (pathname.startsWith("/v1/stream/") && check.memberRef) {
    if (await isMemberRefRevoked(check.memberRef)) {
      return unauthorized(c, "REVOKED", "member key revoked", 403);
    }
  }

  const rl = await gateRateLimit(c, check.memberRef || "unknown");
  if (rl) return rl;

  c.set("pfAuth", {
    mode: "hmac",
    memberRef: check.memberRef,
    deviceId: check.deviceId,
  } as PfAuthState);
  return next();
}

/** POST /v1/auth/register — { memberKey, deviceId, appVersion? } */
export async function handleRegister(c: Context): Promise<Response> {
  // brute-force protection by IP
  const ip =
    c.req.header("x-forwarded-for")?.split(",")[0]?.trim() ||
    c.req.header("x-real-ip") ||
    "unknown";
  const rl = await gateRateLimit(c, `ip:${ip}`);
  if (rl) return rl;

  let body: { memberKey?: string; deviceId?: string; appVersion?: string };
  try {
    body = await c.req.json();
  } catch {
    return unauthorized(c, "BAD_BODY", "invalid JSON", 400);
  }
  const memberKey = (body.memberKey || "").trim();
  const deviceId = (body.deviceId || "").trim();

  if (!isMemberKeyValid(memberKey)) {
    return unauthorized(c, "BAD_MEMBER_KEY", "invalid member key");
  }
  if (isRevoked(memberKey)) {
    return unauthorized(c, "REVOKED", "member key revoked", 403);
  }

  const reg = await registerDevice(memberKey, deviceId);
  if (!reg.ok) {
    return unauthorized(c, "DEVICE_LIMIT", reg.reason, 403);
  }

  const ref = await memberRef(memberKey);
  const token = await issueToken(ref, deviceId);
  const deviceSecret = await deriveDeviceSecretFromRef(ref, deviceId);

  return c.json({
    success: true,
    data: {
      token,
      deviceSecret,
      expiresIn: JWT_TTL_S,
      tokenType: "Bearer",
    },
  });
}

/** POST /v1/auth/refresh — { token } → new 24h JWT (same device binding) */
export async function handleRefresh(c: Context): Promise<Response> {
  let body: { token?: string };
  try {
    body = await c.req.json();
  } catch {
    return unauthorized(c, "BAD_BODY", "invalid JSON", 400);
  }
  const claims = await verifyToken(body.token || "");
  if (!claims) {
    return unauthorized(c, "BAD_TOKEN", "invalid or expired token");
  }
  if (await isMemberRefRevoked(claims.sub)) {
    return unauthorized(c, "REVOKED", "member key revoked", 403);
  }
  const token = await issueToken(claims.sub, claims.did);
  return c.json({ success: true, data: { token, expiresIn: JWT_TTL_S } });
}

/** POST /v1/auth/revoke — admin (X-API-Key) { memberKey } */
export async function handleRevoke(c: Context): Promise<Response> {
  const apiKey = process.env.API_KEY;
  const gotKey = c.req.header("X-API-Key") || "";
  if (!apiKey || gotKey !== apiKey) {
    return unauthorized(c, "FORBIDDEN", "admin only", 403);
  }
  let body: { memberKey?: string };
  try {
    body = await c.req.json();
  } catch {
    return unauthorized(c, "BAD_BODY", "invalid JSON", 400);
  }
  const memberKey = (body.memberKey || "").trim();
  if (!memberKey) return unauthorized(c, "BAD_BODY", "memberKey required", 400);
  revokeMemberKey(memberKey);
  return c.json({ success: true, data: { revoked: true } });
}

/** Security stats for /health */
export function securityStats(): Record<string, unknown> {
  return {
    mode: "hmac-sha256+jwt24h",
    devices: registryStats(),
    blocklist: blocklistSize(),
    rateLimitBuckets: rateLimitStats().buckets,
    memberKeysConfigured: (process.env.MEMBER_KEYS || "").split(",").filter(Boolean).length > 0,
  };
}
