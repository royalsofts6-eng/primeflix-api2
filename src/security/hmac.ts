/**
 * HMAC-SHA256 request signing verification (portable, WebCrypto).
 *
 * Client sends:
 *   X-PF-Device:     <deviceId>
 *   X-PF-Timestamp:  <unix millis>
 *   X-PF-Token:      <JWT from /v1/auth/register>
 *   X-PF-Signature:  <hex HMAC-SHA256>
 *
 * Signature input (canonical):
 *   timestamp + "\n" + METHOD + "\n" + pathname + "\n" + sha256hex(body) + "\n" + deviceId
 *
 * Device secret is derived deterministically (see devices.ts):
 *   deviceSecret = HMAC-SHA256(API_SECRET, "pf-device:" + memberRef + ":" + deviceId)
 * Any instance can recompute it — no shared storage needed.
 *
 * Security properties: request integrity + replay protection (300s window).
 * NOT anti-reverse-engineering — the member key + JWT is the real gate
 * (security review H5: honest framing).
 */
import { sha256Hex, hmacSha256Hex, hmacSha256Verify } from "./crypto.js";
import { verifyToken } from "./jwt.js";
import { isRevokedRaw } from "./devices.js";

export const TIMESTAMP_WINDOW_MS = 300_000; // 300s (H4: 60s too tight for clock skew)

export interface HmacCheck {
  ok: boolean;
  code?: string;
  error?: string;
  memberRef?: string;
  deviceId?: string;
}

function masterSecret(): string {
  return process.env.API_SECRET || process.env.API_KEY || "";
}

export async function deriveDeviceSecretFromRef(
  memberRef: string,
  deviceId: string
): Promise<string> {
  const master = masterSecret();
  if (!master) throw new Error("API_SECRET not configured");
  return hmacSha256Hex(master, `pf-device:${memberRef}:${deviceId}`);
}

export interface HmacRequestParts {
  method: string;
  pathname: string;
  /** header lookup — case-insensitive */
  header: (name: string) => string | null;
  bodyText: string;
}

function getHeader(h: Headers | ((name: string) => string | null), name: string): string {
  if (typeof h === "function") return h(name) || "";
  return h.get(name) || "";
}

export async function verifyHmacParts(
  method: string,
  pathname: string,
  header: (name: string) => string | null,
  bodyText: string
): Promise<HmacCheck> {
  const deviceId = header("X-PF-Device") || "";
  const tsRaw = header("X-PF-Timestamp") || "";
  const signature = (header("X-PF-Signature") || "").toLowerCase();
  const token = header("X-PF-Token") || "";

  if (!deviceId || !tsRaw || !signature || !token) {
    return { ok: false, code: "MISSING_AUTH_HEADERS", error: "missing signed-request headers" };
  }
  const ts = parseInt(tsRaw, 10);
  if (!Number.isFinite(ts)) {
    return { ok: false, code: "BAD_TIMESTAMP", error: "bad timestamp" };
  }
  if (Math.abs(Date.now() - ts) > TIMESTAMP_WINDOW_MS) {
    return { ok: false, code: "STALE_TIMESTAMP", error: "timestamp outside 300s window" };
  }

  // 1. Verify JWT (member identity + device binding)
  const claims = await verifyToken(token);
  if (!claims) {
    return { ok: false, code: "BAD_TOKEN", error: "invalid or expired token" };
  }
  if (claims.did !== deviceId) {
    return { ok: false, code: "DEVICE_MISMATCH", error: "token bound to a different device" };
  }

  // 2. Recompute device secret deterministically
  let deviceSecret: string;
  try {
    deviceSecret = await deriveDeviceSecretFromRef(claims.sub, deviceId);
  } catch {
    return { ok: false, code: "NO_API_SECRET", error: "server misconfigured" };
  }

  // 3. Verify signature over canonical input
  const bodyHash = await sha256Hex(bodyText);
  const canonical = `${tsRaw}\n${method}\n${pathname}\n${bodyHash}\n${deviceId}`;
  const valid = await hmacSha256Verify(deviceSecret, canonical, signature);
  if (!valid) {
    return { ok: false, code: "BAD_SIGNATURE", error: "signature mismatch" };
  }

  return { ok: true, memberRef: claims.sub, deviceId };
}

/** Web Request wrapper (for Hono middleware). */
export async function verifyHmacRequest(req: Request): Promise<HmacCheck> {
  const url = new URL(req.url);
  let bodyText = "";
  if (req.method !== "GET" && req.method !== "HEAD") {
    try {
      bodyText = await req.clone().text();
    } catch {
      bodyText = "";
    }
  }
  return verifyHmacParts(req.method, url.pathname, (n) => req.headers.get(n), bodyText);
}

/** Revocation check for expensive endpoints (/v1/stream/*). */
export async function isMemberRefRevoked(ref: string): Promise<boolean> {
  return isRevokedRaw(ref);
}
