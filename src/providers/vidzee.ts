/**
 * VidZee provider — Hindi-dubbed streams for Hollywood movies & shows.
 *
 * Verified live 2026-10-08:
 *   GET https://core.vidzee.wtf/streams/movie/{tmdb}?s=v6:Hindi
 *       → {"language":"Hindi","url":"<m3u8>","headers":{}}
 *   GET https://core.vidzee.wtf/streams/tv/{tmdb}/{s}/{e}?s=v6:Hindi
 *       → same shape
 * No API key, no Referer needed. Stream URL is a signed .m3u8 (HTTP 200,
 * valid HLS manifest verified). Missing titles → 404/502, classified as
 * CONTENT_MISS (not a failure — never counted, never trips the circuit).
 *
 * Ali's preference (2026-10-08): Hindi dubbed audio FIRST, English fallback.
 * Chain tries VidZee before VidLink; on miss the normal chain takes over.
 */
import type { ProviderCallOpts, ProviderResult, StreamQuality } from "./types.js";
import { fetchUpstream } from "./failures.js";

const BASE = "https://core.vidzee.wtf";
// 7s timeout (was 3.5s — too aggressive: live TV requests take 3.8s+,
// Jawan 3.2s. Vercel maxDuration is 60s so we have room).
// ?audio=hi path: 7s VidZee + fast FZMovies cache + VidLink — well under budget.
// Default Hindi-first path: 7s + 8s chain = 15s < 60s. Safe.
const FETCH_TIMEOUT_MS = 7000;

function extractQuality(url: string): string {
  // URLs look like: .../Avengers_Infinity_War_720/index_384.m3u8
  //              or .../Hindi_720/index_384.m3u8
  const m = url.match(/[_-](\d{3,4})[_/]/);
  if (m) {
    const n = parseInt(m[1], 10);
    if (n >= 2160) return "2160p";
    if (n >= 1080) return "1080p";
    if (n >= 720) return "720p";
    if (n >= 480) return "480p";
    return `${n}p`;
  }
  return "720p";
}

export async function vidzee(
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number,
  opts?: ProviderCallOpts
): Promise<ProviderResult | null> {
  const url =
    type === "movie"
      ? `${BASE}/streams/movie/${tmdbId}?s=v6:Hindi`
      : `${BASE}/streams/tv/${tmdbId}/${season ?? 1}/${episode ?? 1}?s=v6:Hindi`;

  // Classified fetch. 404/502 = "this title has no Hindi dub" — NOT a
  // provider failure: CONTENT_MISS (no retry, no fail counting, never trips
  // the circuit — the 2026-10-08 'no Hindi' fix). Network errors and other
  // statuses throw classified ProviderFailures handled per-class in chain.ts.
  const res = await fetchUpstream(
    "vidzee",
    url,
    {
      headers: { "User-Agent": "Mozilla/5.0 (Linux; Android 14)" },
      // Lane loser-abort composes with the internal 7s timeout.
      signal: opts?.signal
        ? AbortSignal.any([opts.signal, AbortSignal.timeout(FETCH_TIMEOUT_MS)])
        : AbortSignal.timeout(FETCH_TIMEOUT_MS),
    },
    { contentMissStatuses: [404, 502] }
  );

  let data: any;
  try {
    data = await res.json();
  } catch {
    return null;
  }
  const streamUrl: unknown = data?.url;
  if (typeof streamUrl !== "string" || !streamUrl.startsWith("http")) return null;

  const qualities: StreamQuality[] = [
    {
      quality: extractQuality(streamUrl),
      url: streamUrl,
      codec: "unknown",
      size: 0,
    },
  ];

  // VidZee serves Hindi-dubbed audio; no subtitle tracks in its response.
  return { provider: "vidzee", qualities, subtitles: [] };
}
