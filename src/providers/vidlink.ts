/**
 * VidLink provider — token-free streaming via XSalsa20-Poly1305 encrypted TMDB IDs.
 * Key verified working 2026-10-08. Daily canary probe watches for rotation.
 */
import nacl from "tweetnacl";
import type { ProviderCallOpts, ProviderResult, StreamQuality, Subtitle } from "./types.js";
import { fetchUpstream } from "./failures.js";

/**
 * VidLink encryption key — P1-8 (2026-10-09): read from the VIDLINK_KEY env
 * var first. The built-in value stays ONLY as a fallback default so prod
 * never breaks on a missing env var; a loud warning goes to the Vercel
 * logs when the fallback is in use. Set VIDLINK_KEY in the Vercel project
 * env to complete the move.
 */
const BUILTIN_KEY_HEX = "c75136c5668bbfe65a7ecad431a745db68b5f381555b38d8f6c699449cf11fcd";
const KEY_HEX = process.env.VIDLINK_KEY || BUILTIN_KEY_HEX;
if (!process.env.VIDLINK_KEY) {
  console.warn("[vidlink] VIDLINK_KEY env not set — using built-in fallback key. Set VIDLINK_KEY in Vercel env.");
}
const KEY = Buffer.from(KEY_HEX, "hex");
const NONCE = new Uint8Array(24);
const TIME_OFFSET_S = 480;

function generateToken(mediaId: string): string {
  const timestamp = Math.floor(Date.now() / 1000) + TIME_OFFSET_S;
  const idBytes = Buffer.from(mediaId, "utf-8");
  const message = Buffer.alloc(idBytes.length + 8);
  idBytes.copy(message, 0);
  message.writeBigUInt64BE(BigInt(timestamp), idBytes.length);

  const box = nacl.secretbox(new Uint8Array(message), NONCE, new Uint8Array(KEY));
  const payload = Buffer.concat([Buffer.from(NONCE), Buffer.from(box)]);
  return payload.toString("base64url");
}

function parseResponse(data: any): ProviderResult | null {
  const qualities: StreamQuality[] = [];
  const q = data?.stream?.qualities || {};
  for (const [qname, qdata] of Object.entries(q) as any) {
    if (!qdata?.url) continue;
    qualities.push({
      quality: qname,
      url: qdata.url,
      codec: qdata.codecName || "unknown",
      size: parseInt(qdata.size || "0", 10),
    });
  }
  if (qualities.length === 0) return null;

  const subtitles: Subtitle[] = [];
  // VidLink field is `stream.captions` (verified live 2026-10-08) — NOT `subtitles`.
  // Caption objects: { id, url, language (native name e.g. "हिन्दी"), type, hasCorsRestrictions }
  const subs = data?.stream?.captions || data?.stream?.subtitles || data?.subtitles || [];
  for (const s of subs as any[]) {
    if (s?.url) subtitles.push({ lang: s.language || s.lang || s.label || "und", url: s.url });
  }

  return { provider: "vidlink", qualities, subtitles };
}

/**
 * VidLink null-body signal (P0-2, 2026-10-09, refined live 2026-10-09).
 *
 * HTTP 200 with a literal `null` body is VidLink's documented DEAD-KEY
 * shape (antiblock report §3: wrong key -> 200+null, no 401/403). BUT a
 * live probe on 2026-10-09 proved a VALID key also returns 200+null for
 * individual titles it can't serve (Interstellar/157336 nulled 3/3 while
 * Matrix/603 and Shawshank/278 streamed fine on the same key) — so a
 * single null is NOT key-death. chain.ts corroborates: only 3+ DISTINCT
 * titles nulling within 10 minutes cools the provider. An isolated null
 * stays a silent miss, exactly like the old behavior.
 */
export class VidLinkKeyDeadError extends Error {
  /** The TMDB id whose lookup returned the null body (for corroboration). */
  readonly tmdbId: string;
  constructor(tmdbId: string) {
    super(`vidlink: null body for ${tmdbId} (possible key-death — needs corroboration)`);
    this.name = "VidLinkKeyDeadError";
    this.tmdbId = tmdbId;
  }
}

const HEADERS = {
  "User-Agent": "Mozilla/5.0 (Linux; Android 14)",
  Origin: "https://vidlink.pro",
};

export async function vidlink(
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number,
  opts?: ProviderCallOpts
): Promise<ProviderResult | null> {
  const token = generateToken(tmdbId);
  const url =
    type === "movie"
      ? `https://vidlink.pro/api/b/movie/${token}?multiLang=1`
      : `https://vidlink.pro/api/b/tv/${token}/${season ?? 1}/${episode ?? 1}?multiLang=1`;

  // Classified fetch: non-2xx throws ProviderFailure (handled per-class in
  // chain.ts — 404/403 -> next provider immediately, 429 -> Redis cooldown
  // honoring Retry-After, timeout/5xx -> exactly 1 retry).
  const res = await fetchUpstream("vidlink", url, {
    headers: {
      ...HEADERS,
      Referer:
        type === "movie"
          ? `https://vidlink.pro/movie/${tmdbId}`
          : `https://vidlink.pro/tv/${tmdbId}/${season ?? 1}/${episode ?? 1}`,
    },
    // Lane loser-abort composes with the internal 8s timeout.
    signal: opts?.signal
      ? AbortSignal.any([opts.signal, AbortSignal.timeout(8000)])
      : AbortSignal.timeout(8000),
  });
  const data = await res.json();
  // P0-2 (2026-10-09): null body is the documented dead-KEY shape — but a
  // valid key also nulls individual titles it can't serve (live-proven),
  // so this is only a CANDIDATE signal; chain.ts corroborates across
  // distinct titles before cooling anyone. Empty-qualities stays a miss.
  if (data === null || data === undefined) throw new VidLinkKeyDeadError(tmdbId);
  return parseResponse(data);
}
