/**
 * VidLink provider — token-free streaming via XSalsa20-Poly1305 encrypted TMDB IDs.
 * Key verified working 2026-10-08. Daily canary probe watches for rotation.
 */
import nacl from "tweetnacl";
import type { ProviderResult, StreamQuality, Subtitle } from "./types.js";
import { fetchUpstream } from "./failures.js";

const KEY_HEX =
  process.env.VIDLINK_KEY ||
  "c75136c5668bbfe65a7ecad431a745db68b5f381555b38d8f6c699449cf11fcd";
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

const HEADERS = {
  "User-Agent": "Mozilla/5.0 (Linux; Android 14)",
  Origin: "https://vidlink.pro",
};

export async function vidlink(
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number
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
    signal: AbortSignal.timeout(8000),
  });
  const data = await res.json();
  return parseResponse(data);
}
