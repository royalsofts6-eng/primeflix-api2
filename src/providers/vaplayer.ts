/**
 * VaPlayer provider — direct .m3u8 via streamdata API.
 * Format verified from stremio-addon-streamimdb (2026-04).
 *
 * *** DEAD — DO NOT WIRE (live-verified 2026-10-09) ***
 *   - https://streamdata.vaplayer.ru/api.php?imdb=tt0133093&type=movie
 *     -> HTTP 404, 0 bytes (endpoint gone; /api on streamdata also 404s)
 *   - https://vaplayer.ru/ -> HTTP 200 but now serves a "PlayBox - Stream &
 *     Share Videos" product page, NOT the streamdata JSON API
 *   - https://brightpathsignals.com/embed/movie/tt0133093 (the documented
 *     Referer/Origin) -> connection failed
 * The provider code is kept for reference (VAPLAYER_API_URL env override
 * exists) but it is NOT in any race lane — wiring a dead endpoint would
 * only burn the English lane's budget. Re-verify before any future wiring.
 *
 * Needs IMDb ID — resolved from TMDB via imdb_id field.
 */
import type { ProviderResult, StreamQuality } from "./types.js";
import { fetchUpstream } from "./failures.js";

const API_URL = process.env.VAPLAYER_API_URL || "https://streamdata.vaplayer.ru/api.php";
const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

export async function vaplayer(
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number,
  imdbId?: string
): Promise<ProviderResult | null> {
  if (!imdbId) return null; // need IMDb ID from TMDB

  const isTv = type === "tv";
  const referer = isTv
    ? `https://brightpathsignals.com/embed/tv/${imdbId}/${season ?? 1}/${episode ?? 1}`
    : `https://brightpathsignals.com/embed/movie/${imdbId}`;

  const params = new URLSearchParams({
    imdb: imdbId,
    type: isTv ? "tv" : "movie",
  });
  if (isTv) {
    params.set("season", String(season ?? 1));
    params.set("episode", String(episode ?? 1));
  }

  // Classified fetch: the dead api.php 404s -> NOT_FOUND -> chain moves to
  // the next provider immediately with no retry. (Chain marks this provider
  // stub:true, so misses are never counted against the circuit.)
  const res = await fetchUpstream("vaplayer", `${API_URL}?${params}`, {
    headers: {
      "User-Agent": UA,
      Referer: referer,
      Origin: "https://brightpathsignals.com",
      Accept: "application/json, text/javascript, */*; q=0.01",
      "X-Requested-With": "XMLHttpRequest",
    },
    signal: AbortSignal.timeout(8000),
  });
  const body = (await res.json()) as any;
  const urls: string[] = body?.data?.stream_urls || [];
  if (!Array.isArray(urls) || urls.length === 0) return null;

  const qualities: StreamQuality[] = urls.map((u, i) => ({
    quality: `source${i + 1}`,
    url: u,
    codec: "h264",
    size: 0,
  }));
  return { provider: "vaplayer", qualities };
}
