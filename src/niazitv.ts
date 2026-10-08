/**
 * NiaziTV scraper — Turkish dramas with Urdu subtitles. (v3 — fixed 2026-10-08)
 *
 * Source: https://play.niazitv.pk (server-rendered HTML, no JS needed)
 *
 * URL pattern (verified live 2026-10-08, matches the working client-side
 * NiaziTvClient.kt — the backend previously used stale forms):
 *   GET /all-series                          -> drama catalog (27 series)
 *   GET /all-seasons?serie={serieId}          -> SEASON cards (/drama/{seasonId}/{slug})
 *   GET /drama/{seasonId}/{slug}              -> EPISODE cards
 *     (relative links: single-serie?watch=1&episode={episodeId})
 *   GET /drama/{seasonId}/single-serie?watch=1&episode={episodeId}
 *                                            -> JSON-LD contentUrl (.m3u8)
 *
 * NOTE: the site-root /single-serie?... 302-redirects to /error — the watch
 * URL must be built from the season page base (/drama/{seasonId}).
 *
 * CRITICAL (C2): contentUrl MUST be validated against the CDN allowlist.
 * Promo/trailer placeholders (e.g. video.twimg.com) are NEVER returned
 * as playable streams.
 *
 * Caching:
 *   series list : 24h (+ 7d stale)
 *   seasons     : 24h (+ 7d stale)
 *   episodes    : 6h  (+ 1d stale)
 *   stream URLs : NO cache (signed/expiring)
 */
import { cacheGet, cacheSet } from "./cache.js";

const BASE = "https://play.niazitv.pk";
const UA =
  "Mozilla/5.0 (Linux; Android 13; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36";

const H = 3600_000;
const D = 24 * H;

export const NIAZI_TTL = {
  series: 24 * H,
  seasons: 24 * H,
  episodes: 6 * H,
  staleSeries: 7 * D,
  staleSeasons: 7 * D,
  staleEpisodes: 1 * D,
};

// ── Allowlist (CRITICAL C2) ─────────────────────────────────────────────────
// Only NiaziTV CDN hosts are playable. Everything else (twitter promos,
// third-party embeds) is rejected.
const ALLOWED_SUFFIXES = ["niazitv.pk", "urduflix.pk"];

export function isAllowedStreamUrl(url: string): boolean {
  let host: string;
  try {
    host = new URL(url).hostname.toLowerCase();
  } catch {
    return false;
  }
  return ALLOWED_SUFFIXES.some((s) => host === s || host.endsWith("." + s));
}

// ── Input validation (SSRF protection) ──────────────────────────────────────
function numId(v: string, name: string): string {
  if (!/^\d{1,10}$/.test(v)) throw new Error(`invalid ${name}`);
  return v;
}

function absUrl(u: string): string {
  return u.startsWith("http") ? u : BASE + (u.startsWith("/") ? u : "/" + u);
}

async function fetchPage(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: { "User-Agent": UA, Accept: "text/html" },
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) throw new Error(`NiaziTV HTTP ${res.status}`);
  return res.text();
}

// ── Types ───────────────────────────────────────────────────────────────────
export interface NiaziSeries {
  id: string;
  title: string;
  image: string;
  seasons: number;
}

export interface NiaziSeason {
  id: string;
  title: string;
  image: string;
  year: string;
  url: string;
}

export interface NiaziEpisode {
  id: string;
  title: string;
  thumbnail: string;
  lang: "urdu" | "english" | "unknown";
}

export interface NiaziStream {
  url: string;
  referer: string;
  title: string;
}

// ── 1. Series list ──────────────────────────────────────────────────────────
const RE_SERIES = new RegExp(
  '<img src="([^"]+)" alt="([^"]+)"[^>]*>.*?' +
    '<a class="uk-position-cover" href="https://play\\.niazitv\\.pk/all-seasons\\?serie=(\\d+)"></a>.*?' +
    "<h5[^>]*>\\s*([^<]+?)</h5>\\s*.*?<p[^>]*>\\s*Total Seasons:\\s*(\\d+)\\s*</p>",
  "gs"
);

export async function getSeries(): Promise<NiaziSeries[]> {
  const cacheKey = "niazi:series";
  const cached = cacheGet<NiaziSeries[]>(cacheKey);
  if (cached && !cached.stale) return cached.value;

  try {
    const html = await fetchPage(`${BASE}/all-series`);
    const out: NiaziSeries[] = [];
    const seen = new Set<string>();
    for (const m of html.matchAll(RE_SERIES)) {
      const id = m[3];
      if (seen.has(id)) continue;
      seen.add(id);
      out.push({
        id,
        title: m[4].trim(),
        image: absUrl(m[1]),
        seasons: parseInt(m[5], 10) || 1,
      });
    }
    if (out.length === 0) throw new Error("no series parsed (site structure changed?)");
    cacheSet(cacheKey, out, NIAZI_TTL.series, NIAZI_TTL.staleSeries);
    return out;
  } catch (e) {
    if (cached) return cached.value; // stale fallback
    throw e;
  }
}

// ── 2. Season list (per series) ─────────────────────────────────────────────
const RE_SEASON = new RegExp(
  '<img src="([^"]+)"[^>]*alt="([^"]+)"[^>]*>.*?' +
    '<a class="uk-position-cover" href="https://play\\.niazitv\\.pk/drama/(\\d+)/([^"]+)"></a>.*?' +
    '<h5[^>]*>\\s*([^<]+?)</h5>',
  "gs"
);

export async function getSeasons(serieId: string): Promise<NiaziSeason[]> {
  serieId = numId(serieId, "serieId");
  const cacheKey = `niazi:seasons:${serieId}`;
  const cached = cacheGet<NiaziSeason[]>(cacheKey);
  if (cached && !cached.stale) return cached.value;

  try {
    const html = await fetchPage(`${BASE}/all-seasons?serie=${serieId}`);
    const out: NiaziSeason[] = [];
    const seen = new Set<string>();
    for (const m of html.matchAll(RE_SEASON)) {
      const id = m[3];
      if (seen.has(id)) continue;
      seen.add(id);
      const yearMatch = m[0].match(/>\s*(20\d\d)\s*</);
      out.push({
        id,
        title: m[5].trim(),
        image: absUrl(m[1]),
        year: yearMatch ? yearMatch[1] : "",
        url: `${BASE}/drama/${id}/${m[4]}`,
      });
    }
    if (out.length === 0) throw new Error("no seasons parsed (site structure changed?)");
    cacheSet(cacheKey, out, NIAZI_TTL.seasons, NIAZI_TTL.staleSeasons);
    return out;
  } catch (e) {
    if (cached) return cached.value; // stale fallback
    throw e;
  }
}

// ── 3. Episode list (per season page) ───────────────────────────────────────
// Episode cards on /drama/{seasonId}/{slug} (verified 2026-10-08 — same
// pattern as the working client-side scraper):
//   <img src="..." alt="Episode 1 - Urdu Subtitles">
//   <a class="uk-position-cover" href="single-serie?watch=1&amp;episode=4079"></a>
//   <dt ...>Episode 1 - Urdu Subtitles</dt>
const RE_EPISODE = new RegExp(
  '<img src="([^"]+)"[^>]*alt="([^"]*)"[^>]*>.*?' +
    'href="single-serie\\?watch=1&amp;episode=(\\d+)".*?' +
    "<dt[^>]*>\\s*([^<]+?)\\s*</dt>",
  "gs"
);

function detectLang(title: string): NiaziEpisode["lang"] {
  const t = title.toLowerCase();
  if (t.includes("urdu")) return "urdu";
  if (t.includes("english")) return "english";
  return "unknown";
}

export async function getEpisodes(seasonId: string): Promise<NiaziEpisode[]> {
  seasonId = numId(seasonId, "seasonId");
  const cacheKey = `niazi:episodes:${seasonId}`;
  const cached = cacheGet<NiaziEpisode[]>(cacheKey);
  if (cached && !cached.stale) return cached.value;

  try {
    // /drama/{seasonId} resolves to the season page (verified live 2026-10-08:
    // same episode cards as the slugged URL).
    const html = await fetchPage(`${BASE}/drama/${seasonId}`);
    const out: NiaziEpisode[] = [];
    const seen = new Set<string>();
    for (const m of html.matchAll(RE_EPISODE)) {
      const id = m[3];
      // Prefer the <dt> title text; fall back to img alt.
      const title = (m[4] || "").trim() || (m[2] || "").trim();
      // Skip logo/nav artifacts
      if (/logo/i.test(title) && /whitelogo/i.test(m[1])) continue;
      if (seen.has(id)) continue;
      seen.add(id);
      out.push({
        id,
        title,
        thumbnail: absUrl(m[1]),
        lang: detectLang(title),
      });
    }
    if (out.length === 0) throw new Error("no episodes parsed (site structure changed?)");
    cacheSet(cacheKey, out, NIAZI_TTL.episodes, NIAZI_TTL.staleEpisodes);
    return out;
  } catch (e) {
    if (cached) return cached.value; // stale fallback
    throw e;
  }
}

/**
 * All episodes for a SERIES (aggregates every season page).
 * Route: GET /v1/niazi/series/:id/episodes — the old route passed the series
 * id into the season-scoped getEpisodes() (wrong); this is the honest fix.
 * Seasons are fetched in parallel (15s each) behind the 6h in-memory cache.
 */
export async function getEpisodesForSerie(serieId: string): Promise<NiaziEpisode[]> {
  serieId = numId(serieId, "serieId");
  const seasons = await getSeasons(serieId);
  const perSeason = await Promise.all(
    seasons.map(async (s, i) => {
      try {
        const eps = await getEpisodes(s.id);
        const prefix = seasons.length > 1 ? `${seasonLabel(s.title, i)} • ` : "";
        return eps.map((e) => ({ ...e, title: prefix + e.title }));
      } catch {
        return [] as NiaziEpisode[];
      }
    })
  );
  const out = perSeason.flat();
  if (out.length === 0) throw new Error("no episodes parsed (site structure changed?)");
  return out;
}

/** Season display label, e.g. "Season 2" (mirrors the client-side scraper). */
function seasonLabel(title: string, fallbackIndex: number): string {
  const m = title.match(/season\s*(\d+)/i);
  return m ? `Season ${m[1]}` : `Season ${fallbackIndex + 1}`;
}

// ── 4. Stream URL ───────────────────────────────────────────────────────────
const RE_JSONLD = /<script type="application\/ld\+json">(.*?)<\/script>/gs;
const RE_CONTENTURL = /"contentUrl"\s*:\s*"([^"]+\.m3u8[^"]*)"/i;

function extractContentUrl(html: string): { url: string; title: string } | null {
  // Primary: JSON-LD VideoObject
  for (const m of html.matchAll(RE_JSONLD)) {
    try {
      const data = JSON.parse(m[1]);
      const nodes = Array.isArray(data)
        ? data
        : data["@graph"]
          ? data["@graph"]
          : [data];
      for (const n of nodes) {
        if (n && typeof n === "object" && n["@type"] === "VideoObject" && typeof n["contentUrl"] === "string") {
          return { url: n["contentUrl"], title: String(n["name"] || "") };
        }
      }
    } catch {
      /* malformed block, try next */
    }
  }
  // Fallback: regex
  const f = html.match(RE_CONTENTURL);
  if (f) return { url: f[1], title: "" };
  return null;
}

export async function getStreamUrl(seasonId: string, episodeId: string): Promise<NiaziStream> {
  seasonId = numId(seasonId, "seasonId");
  episodeId = numId(episodeId, "episodeId");

  // NOTE: stream URLs are signed/time-limited — NEVER cache.
  // Watch-URL form verified live 2026-10-08 (same as the working client-side
  // scraper): /drama/{seasonId}/single-serie?watch=1&episode={episodeId}.
  const seasonBase = `${BASE}/drama/${seasonId}`;
  const pageUrl = `${seasonBase}/single-serie?watch=1&episode=${episodeId}`;
  const html = await fetchPage(pageUrl);
  const found = extractContentUrl(html);
  if (!found) throw new Error("no stream URL found on episode page");

  // CRITICAL C2: allowlist validation — reject promo/trailer URLs
  if (!isAllowedStreamUrl(found.url)) {
    // Log the host only, never URL fragments (may contain signed material).
    let host = "?";
    try {
      host = new URL(found.url).hostname;
    } catch { /* keep "?" */ }
    throw new Error(`stream URL rejected by allowlist (host not a NiaziTV CDN): ${host}`);
  }

  return {
    url: found.url,
    referer: pageUrl, // CDN requires Referer header on playlist + segments
    title: found.title,
  };
}
