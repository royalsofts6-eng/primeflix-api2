/**
 * Minimal JWT (HS256) — 24h expiry, portable (WebCrypto only).
 *
 * Claims: { sub: memberKey, did: deviceId, iat, exp }
 * Secret: JWT_SECRET env, falls back to API_SECRET.
 */
import { hmacSha256Hex, b64urlEncode, b64urlDecode } from "./crypto.js";

const enc = new TextEncoder();
const dec = new TextDecoder();

function jwtSecret(): string {
  const s = process.env.JWT_SECRET || process.env.API_SECRET || process.env.API_KEY || "";
  if (!s) throw new Error("JWT secret not configured (set JWT_SECRET or API_SECRET)");
  return s;
}

export const JWT_TTL_S = 24 * 3600; // 24h (final plan v1.0 — was 7d, C5 fixed)

export interface PfClaims {
  sub: string; // member key (hashed ref, not raw — see devices.ts)
  did: string; // device id
  iat: number;
  exp: number;
}

export async function issueToken(memberRef: string, deviceId: string): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  const payload: PfClaims = {
    sub: memberRef,
    did: deviceId,
    iat: now,
    exp: now + JWT_TTL_S,
  };
  const header = b64urlEncode(enc.encode(JSON.stringify({ alg: "HS256", typ: "JWT" })));
  const body = b64urlEncode(enc.encode(JSON.stringify(payload)));
  const sig = await hmacSha256Hex(jwtSecret(), `${header}.${body}`);
  // signature as b64url of raw bytes
  const sigBytes = new Uint8Array(sig.match(/../g)!.map((b) => parseInt(b, 16)));
  return `${header}.${body}.${b64urlEncode(sigBytes)}`;
}

export async function verifyToken(token: string): Promise<PfClaims | null> {
  try {
    const [header, body, sig] = token.split(".");
    if (!header || !body || !sig) return null;
    const expectedHex = await hmacSha256Hex(jwtSecret(), `${header}.${body}`);
    const expectedBytes = new Uint8Array(expectedHex.match(/../g)!.map((b) => parseInt(b, 16)));
    const gotBytes = b64urlDecode(sig);
    if (gotBytes.length !== expectedBytes.length) return null;
    let diff = 0;
    for (let i = 0; i < gotBytes.length; i++) diff |= gotBytes[i] ^ expectedBytes[i];
    if (diff !== 0) return null;
    const claims = JSON.parse(dec.decode(b64urlDecode(body))) as PfClaims;
    const now = Math.floor(Date.now() / 1000);
    if (typeof claims.exp !== "number" || claims.exp <= now) return null;
    if (typeof claims.iat !== "number" || claims.iat > now + 60) return null;
    return claims;
  } catch {
    return null;
  }
}
