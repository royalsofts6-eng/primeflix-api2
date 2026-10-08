/**
 * PrimeFlix Live TV — auto-updating channel system.
 *
 * Design:
 * - Curated channel list (static, always available as fallback)
 * - 12h refresh: fetch dearbulut health-checked M3U playlists,
 *   match curated channels by name, probe streams, cache results.
 * - GET /v1/livetv/channels serves from cache (fast, edge-cached).
 * - GET /v1/cron/livetv-refresh triggers background refresh (Vercel cron).
 *
 * NOTE: Vercel Hobby cron only allows DAILY schedules. The channel data
 * itself uses 12h TTL + stale-while-revalidate, so on-demand requests
 * trigger refresh every 12h regardless of cron frequency.
 */

import { cacheGet, cacheSet } from "./cache.js";

// ── Types ───────────────────────────────────────────────────────────────────

export interface Channel {
  id: string;
  name: string;
  category: ChannelCategory;
  country: "pk" | "in";
  type: "hls" | "youtube";
  url: string;
  fallbacks: string[];
  logo?: string;
}

export type ChannelCategory =
  | "pk-entertainment"
  | "pk-sports"
  | "pk-news"
  | "in-entertainment"
  | "in-movies"
  | "sports"
  | "in-news";

export const CATEGORY_LABELS: Record<ChannelCategory, string> = {
  "pk-entertainment": "Pakistani Entertainment",
  "pk-sports": "Pakistani Sports",
  "pk-news": "Pakistani News",
  "in-entertainment": "Indian Entertainment",
  "in-movies": "Indian Movies",
  sports: "Sports & Cricket",
  "in-news": "Indian News",
};

// ── Curated channel list ────────────────────────────────────────────────────
// Primary URLs from verified research (2026-10-08). The 12h refresh job
// replaces expiring/signed URLs with fresh ones from health-checked playlists.

const CURATED: Channel[] = [
  // — Pakistani Entertainment —
  {
    id: "a-plus-tv",
    name: "A-Plus TV",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "hum-tv",
    name: "Hum TV",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "https://g4wlkwx8l23a-hls-live.5centscdn.com/HUM/271ddf829afeece44d8732757fba1a66.sdp/playlist.m3u8",
    fallbacks: [],
  },
  {
    id: "express-entertainment",
    name: "Express Entertainment",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "https://ml-pull-dvc-myco.io:2096/EXPRESS_ENTERTAINMENT/index.m3u8",
    fallbacks: [],
  },
  {
    id: "atv-pk",
    name: "ATV",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "",
    fallbacks: [],
  },

  // — Pakistani Sports —
  {
    id: "m-sports",
    name: "M Sports",
    category: "pk-sports",
    country: "pk",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "ptv-sports",
    name: "PTV Sports",
    category: "pk-sports",
    country: "pk",
    type: "hls",
    url: "",
    fallbacks: [],
  },

  // — Pakistani News (verified HLS 2026-10-08) —
  {
    id: "dunya-news",
    name: "Dunya News",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://intl.dunyanews.tv/livehd/ngrp:dunyalivehd_2_all/playlist.m3u8",
    fallbacks: [],
  },
  {
    id: "92-news",
    name: "92 News HD",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "http://92news.vdn.dstreamone.net/92newshd/92hd/playlist.m3u8",
    fallbacks: [],
  },
  {
    id: "samaa-tv",
    name: "Samaa TV",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://vodzong.mjunoon.tv:8087/streamtest/SAMAA-173/playlist.m3u8",
    fallbacks: [],
  },
  {
    id: "geo-news",
    name: "Geo News",
    category: "pk-news",
    country: "pk",
    type: "youtube",
    url: "https://www.youtube.com/@GeoNews/live",
    fallbacks: [],
  },
  {
    id: "ary-news",
    name: "ARY News",
    category: "pk-news",
    country: "pk",
    type: "youtube",
    url: "https://www.youtube.com/@ARYNews/live",
    fallbacks: [],
  },

  // — Indian Entertainment —
  {
    id: "star-plus",
    name: "StarPlus",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "colors-tv",
    name: "Colors",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "sony-tv",
    name: "Sony TV",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },

  // — Indian Movies —
  {
    id: "star-gold",
    name: "Star Gold",
    category: "in-movies",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "sony-max",
    name: "Sony Max",
    category: "in-movies",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "zee-cinema",
    name: "Zee Cinema",
    category: "in-movies",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "colors-cineplex",
    name: "Colors Cineplex",
    category: "in-movies",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },

  // — Sports & Cricket —
  {
    id: "willow",
    name: "Willow",
    category: "sports",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "star-sports-2",
    name: "Star Sports 2",
    category: "sports",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "sony-ten-1",
    name: "Sony Ten 1",
    category: "sports",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "dd-sports",
    name: "DD Sports",
    category: "sports",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "cricket-gold",
    name: "Cricket Gold",
    category: "sports",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },

  // — Indian News —
  {
    id: "abp-news",
    name: "ABP News",
    category: "in-news",
    country: "in",
    type: "hls",
    url: "https://d1rc86nwwc9fag.cloudfront.net/vglive-sk-472500/abpnews/master.m3u8",
    fallbacks: [],
  },
  {
    id: "aaj-tak",
    name: "Aaj Tak",
    category: "in-news",
    country: "in",
    type: "youtube",
    url: "https://www.youtube.com/@aajtak/live",
    fallbacks: [],
  },
];

// ── Playlist sources ────────────────────────────────────────────────────────

const PLAYLISTS = {
  pk: "https://dearbulut.github.io/iptv/playlists/country/pk.m3u",
  in: "https://dearbulut.github.io/iptv/playlists/country/in.m3u",
};

const CACHE_KEY = "livetv:channels:v1";
const TTL_MS = 12 * 60 * 60 * 1000; // 12h
const STALE_MS = 7 * 24 * 60 * 60 * 1000; // 7d stale fallback

// ── M3U parsing ─────────────────────────────────────────────────────────────

interface PlaylistEntry {
  name: string;
  url: string;
  logo?: string;
  group?: string;
}

function parseM3U(text: string): PlaylistEntry[] {
  const entries: PlaylistEntry[] = [];
  const lines = text.split("\n");
  let pending: Partial<PlaylistEntry> | null = null;

  for (const raw of lines) {
    const line = raw.trim();
    if (line.startsWith("#EXTINF")) {
      // #EXTINF:-1 tvg-logo="..." group-title="...",Channel Name
      const nameMatch = line.match(/,(.*)$/);
      const logoMatch = line.match(/tvg-logo="([^"]*)"/);
      const groupMatch = line.match(/group-title="([^"]*)"/);
      pending = {
        name: (nameMatch?.[1] || "").trim(),
        logo: logoMatch?.[1] || undefined,
        group: groupMatch?.[1] || undefined,
      };
    } else if (line && !line.startsWith("#") && pending) {
      if (line.startsWith("http")) {
        entries.push({ name: pending.name || "Unknown", url: line, logo: pending.logo, group: pending.group });
      }
      pending = null;
    }
  }
  return entries;
}

// Normalize names for fuzzy matching: "Star Plus HD" -> "starplus"
function norm(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]/g, "");
}

// ── HTTP helpers ────────────────────────────────────────────────────────────

async function fetchText(url: string, timeoutMs = 15000): Promise<string | null> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: { "User-Agent": "PrimeFlix/1.0" },
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}

/** HEAD-probe a stream URL. Returns true if it looks alive. */
async function probeUrl(url: string, timeoutMs = 5000): Promise<boolean> {
  if (!url || !url.startsWith("http")) return false;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "HEAD",
      signal: ctrl.signal,
      headers: { "User-Agent": "PrimeFlix/1.0" },
      redirect: "follow",
    });
    return res.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(t);
  }
}

// ── Refresh pipeline ────────────────────────────────────────────────────────

export interface RefreshResult {
  refreshedAt: number;
  total: number;
  alive: number;
  channels: Channel[];
}

/**
 * Fetch health-checked playlists, match curated channels, probe streams.
 * Runs in background (cron) or on cache miss.
 */
export async function refreshChannels(): Promise<RefreshResult> {
  // 1. Fetch both playlists in parallel
  const [pkText, inText] = await Promise.all([
    fetchText(PLAYLISTS.pk),
    fetchText(PLAYLISTS.in),
  ]);

  // 2. Build name -> entries index
  const index = new Map<string, PlaylistEntry[]>();
  for (const text of [pkText, inText]) {
    if (!text) continue;
    for (const e of parseM3U(text)) {
      const key = norm(e.name);
      const arr = index.get(key) || [];
      arr.push(e);
      index.set(key, arr);
    }
  }

  // 3. For each curated channel: find playlist matches, probe, pick best
  const channels: Channel[] = await Promise.all(
    CURATED.map(async (ch): Promise<Channel> => {
      const key = norm(ch.name);
      const candidates: string[] = [];

      // Direct name match + common variants
      const variants = [key, key.replace(/tv$/, ""), key.replace(/^sony/, "set")];
      for (const v of variants) {
        const entries = index.get(v);
        if (entries) {
          for (const e of entries) {
            if (!candidates.includes(e.url)) candidates.push(e.url);
          }
        }
      }

      // Also try partial matching (e.g. "starplus" in "starplushd")
      if (candidates.length === 0) {
        for (const [k, entries] of index) {
          if (k.includes(key) || key.includes(k)) {
            for (const e of entries) {
              if (!candidates.includes(e.url) && candidates.length < 5) {
                candidates.push(e.url);
              }
            }
          }
        }
      }

      // Keep curated URL as first candidate (it's verified)
      const all = ch.url ? [ch.url, ...candidates] : candidates;

      // Probe in parallel, keep alive ones
      const probes = await Promise.all(all.map((u) => probeUrl(u)));
      const alive = all.filter((_, i) => probes[i]);

      // YouTube channels: no probing (NewPipe resolves at play time)
      if (ch.type === "youtube") {
        return { ...ch, fallbacks: candidates.slice(0, 3) };
      }

      return {
        ...ch,
        url: alive[0] || "",
        fallbacks: alive.slice(1, 4),
        logo: ch.logo,
      };
    })
  );

  const alive = channels.filter((c) => c.url || c.type === "youtube").length;
  const result: RefreshResult = {
    refreshedAt: Date.now(),
    total: channels.length,
    alive,
    channels,
  };

  cacheSet(CACHE_KEY, result, TTL_MS, STALE_MS);
  return result;
}

/**
 * Get channels — fast path serves cache, triggers background refresh on stale.
 */
export async function getChannels(): Promise<RefreshResult> {
  const cached = cacheGet<RefreshResult>(CACHE_KEY);
  if (cached && !cached.stale) {
    return cached.value;
  }
  if (cached && cached.stale) {
    // Serve stale immediately, refresh in background
    refreshChannels().catch(() => {});
    return cached.value;
  }
  // Cold start: serve curated list immediately (fast, avoids Vercel 10s kill),
  // refresh in background so next request gets probed URLs.
  const result: RefreshResult = {
    refreshedAt: Date.now(),
    total: CURATED.length,
    alive: CURATED.filter((c) => c.url || c.type === "youtube").length,
    channels: CURATED,
  };
  cacheSet(CACHE_KEY, result, TTL_MS, STALE_MS);
  refreshChannels().catch(() => {});
  return result;
}

/** Group channels by category for the API response. */
export function groupByCategory(channels: Channel[]) {
  const groups: Record<string, { label: string; channels: Channel[] }> = {};
  for (const ch of channels) {
    if (!groups[ch.category]) {
      groups[ch.category] = { label: CATEGORY_LABELS[ch.category], channels: [] };
    }
    groups[ch.category].channels.push(ch);
  }
  return groups;
}
