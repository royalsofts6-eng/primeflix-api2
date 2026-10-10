/**
 * Portable crypto helpers — WebCrypto API only.
 * Works on: Node 18+, Vercel serverless, Cloudflare Workers.
 * NO node:crypto, NO external deps.
 */

// Minimal WebCrypto types (tsconfig has no DOM lib — keep it dependency-free)
type Bs = ArrayBuffer | ArrayBufferView;
interface WSubtle {
  digest(alg: string, data: Bs): Promise<ArrayBuffer>;
  importKey(
    fmt: string,
    keyData: Bs,
    alg: { name: string; hash: string },
    extractable: boolean,
    uses: string[]
  ): Promise<WCryptoKey>;
  sign(alg: string, key: WCryptoKey, data: Bs): Promise<ArrayBuffer>;
  verify(alg: string, key: WCryptoKey, sig: Bs, data: Bs): Promise<boolean>;
}
interface WCryptoKey {}
declare const crypto: {
  subtle: WSubtle;
  getRandomValues<T extends ArrayBufferView>(arr: T): T;
};

const enc = new TextEncoder();

function toHex(buf: ArrayBuffer): string {
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export async function sha256Hex(data: string | Uint8Array): Promise<string> {
  const bytes: Bs = typeof data === "string" ? enc.encode(data) : data;
  return toHex(await crypto.subtle.digest("SHA-256", bytes));
}

async function importHmacKey(secret: string): Promise<WCryptoKey> {
  return crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"]
  );
}

export async function hmacSha256Hex(secret: string, data: string): Promise<string> {
  const key = await importHmacKey(secret);
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(data));
  return toHex(sig);
}

export async function hmacSha256Verify(
  secret: string,
  data: string,
  expectedHex: string
): Promise<boolean> {
  const key = await importHmacKey(secret);
  const expected = new Uint8Array(expectedHex.match(/../g)?.map((b) => parseInt(b, 16)) ?? []);
  if (expected.length !== 32) return false;
  return crypto.subtle.verify("HMAC", key, expected, enc.encode(data));
}

/** Cryptographically secure random hex string. */
export function randomHex(bytes = 32): string {
  return toHex(crypto.getRandomValues(new Uint8Array(bytes)).buffer as ArrayBuffer);
}

/**
 * Constant-time string comparison for secrets (API keys, admin keys, cron
 * secrets). No early exit on the first differing byte — a remote timing
 * probe can't learn the secret byte-by-byte. Returns false on length
 * mismatch (still scans the full longer input).
 *
 * P4 (2026-10-10): the auth chain compared secrets with `===`.
 */
export function timingSafeEqualStr(a: string, b: string): boolean {
  const ab = enc.encode(a ?? "");
  const bb = enc.encode(b ?? "");
  const n = Math.max(ab.length, bb.length, 1);
  let diff = ab.length ^ bb.length;
  for (let i = 0; i < n; i++) {
    // Guard the modulo against zero-length inputs (ab[NaN] is undefined).
    const x = ab.length ? ab[i % ab.length] : 0;
    const y = bb.length ? bb[i % bb.length] : 0;
    diff |= x ^ y;
  }
  return diff === 0;
}

const B64URL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

export function b64urlEncode(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i += 3) {
    const a = bytes[i],
      b = bytes[i + 1] ?? 0,
      c = bytes[i + 2] ?? 0;
    s += B64URL[a >> 2] + B64URL[((a & 3) << 4) | (b >> 4)];
    if (i + 1 < bytes.length) s += B64URL[((b & 15) << 2) | (c >> 6)];
    if (i + 2 < bytes.length) s += B64URL[c & 63];
  }
  return s;
}

export function b64urlDecode(s: string): Uint8Array {
  const out: number[] = [];
  let acc = 0,
    bits = 0;
  for (const ch of s) {
    const v = B64URL.indexOf(ch);
    if (v < 0) throw new Error("bad b64url");
    acc = (acc << 6) | v;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out.push((acc >> bits) & 0xff);
    }
  }
  return new Uint8Array(out);
}
