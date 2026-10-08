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
 *   timestamp + "\n" + METHOD + "\n" + pathname + "\n" + sortedQuery + "\n"
 *     + sha256hex(body) + "\n" + deviceId
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
  // FAIL CLOSED: the device-secret HMAC key MUST be a real secret. It used
  // to fall back to API_KEY — the same key hardcoded in the APK — which let
  // anyone holding the app key derive any device secret. Never again.
  const s = process.env.API_SECRET || "";
  if (!s) throw new Error("API_SECRET not configured");
  if (process.env.API_KEY && s === process.env.API_KEY) {
    throw new Error("API_SECRET must differ from API_KEY");
  }
  return s;
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
  /** raw query string (sorted); bound into the signature so signed GET
   *  params can't be swapped inside the replay window */
  query?: string;
}

function getHeader(h: Headers | ((name: string) => string | null), name: string): string {
  if (typeof h === "function") return h(name) || "";
  return h.get(name) || "";
}

export async function verifyHmacParts(
  method: string,
  pathname: string,
  header: (name: string) => string | null,
  bodyText: string,
  /** raw query string (e.g. "audio=hi&page=2"); sorted before signing */
  query = ""
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

  // 3. Verify signature over canonical input. The sorted query string is
  // bound in so an intercepted signed GET can't have its params swapped
  // (e.g. ?audio=hi -> ?audio=en) inside the 300s replay window.
  const bodyHash = await sha256Hex(bodyText);
  const sortedQuery = query
    .split("&")
    .filter(Boolean)
    .sort()
    .join("&");
  const canonical = `${tsRaw}\n${method}\n${pathname}\n${sortedQuery}\n${bodyHash}\n${deviceId}`;
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
  return verifyHmacParts(req.method, url.pathname, (n) => req.headers.get(n), bodyText, url.search.slice(1));
}

/** Revocation check — enforced on ALL authenticated routes for HMAC mode. */
export async function isMemberRefRevoked(ref: string): Promise<boolean> {
  return isRevokedRaw(ref);
}
