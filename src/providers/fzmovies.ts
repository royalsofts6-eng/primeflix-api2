/**
 * FZMovies provider — Hindi-dubbed MP4s via automated 4-hop scraping.
 *
 * Verified live 2026-10-08 (sandbox):
 *   1. POST {base}/csearch.php (searchname, searchby=Name, category)
 *      → result links: movie--Dubbed--{Title} [Hindi]--hmp4.htm
 *   2. GET {base}/movie-*.htm (detail)
 *      → ul.moviesfiles → onclick window.location.href="download1.php?downloadoptionskey={k1}&pt={pt}"
 *   3. GET {base}/download1.php?downloadoptionskey={k1}&pt={pt}
 *      → a#downloadlink → download.php?downloadkey={k2}&pt={pt}
 *   4. GET {base}/download.php?downloadkey={k2}&pt={pt}
 *      → input[name='download1'] (×2-3 mirrors) → DIRECT MP4 URLs
 *
 * Critical constraints (all verified):
 * - Keys expire in ~60s: all 4 hops MUST run back-to-back in one invocation.
 * - Final MP4 links valid 12h → we cache 10h.
 * - Full chain takes 35-60s (site is slow) → NEVER block a user request on it.
 *   Request path is cache-only; warming happens ONLY via /v1/cron/fz-warm,
 *   which runs a bounded single-attempt scrape inside the Vercel 60s
 *   maxDuration (verified 2026-10-08). Fire-and-forget "background" warming
 *   from the request path does NOT work on serverless and was removed.
 * - Mirror hosts are DNS-sinkholed from datacenter IPs, but phones
 *   (residential/mobile IP) resolve them fine. Backend only extracts URLs,
 *   never fetches video bytes.
 * - No API, pure HTML scraping. Base URL is env-driven (domain rotates).
 *
 * Chain position (Ali 2026-10-08): VidZee → FZMovies → VidLink.
 * FZMovies is a PROPER Hindi tier, not a backup.
 */
import { cacheGet, cacheSet } from "../cache.js";
import { tmdb } from "../tmdb.js";
import { redisEnabled, redisCacheGet, redisCacheSet } from "../security/redis.js";
import type { ProviderFn, ProviderResult, StreamQuality } from "./types.js";

const FZ_BASE = (process.env.FZ_BASE_URL || "https://www.fzmovies.host").replace(/\/$/, "");
// NOTE: "https://fzmovies.host" (no www) is DEAD (verified 2026-10-08) —
// do NOT add it as a fallback, it just wastes time. Add working mirrors via env.
const FZ_FALLBACKS = (process.env.FZ_FALLBACK_URLS || "")
  .split(",")
  .map((s) => s.trim().replace(/\/$/, ""))
  .filter(Boolean);

const UA = "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36";
// Per-hop fetch timeout for the full interactive scrape (Ali 2026-10-08:
// site takes 7-15s normally, spikes to 30s+ when flaky; search POST took
// 14.7s on a good attempt).
const HOP_TIMEOUT_MS = 40000;
// Bounded warm (used by /v1/cron/fz-warm): the whole warm MUST complete
// inside Vercel maxDuration (60s, verified 2026-10-08 on both clusters).
// One attempt, tighter hops, hard overall deadline — the cron caller retries
// on transient failures instead of one invocation gambling 300s+.
const WARM_HOP_TIMEOUT_MS = 12000;
const WARM_OVERALL_MS = 52000;
const CACHE_TTL_MS = 10 * 60 * 60_000; // 10h (links live 12h)

// ── Circuit breaker: 3 consecutive fails → 15 min cooldown ───────────────────
let consecFails = 0;
let lastFailAt = 0;
const CB_FAILS = 3;
const CB_COOLDOWN_MS = 15 * 60_000;

function circuitOpen(): boolean {
  return consecFails >= CB_FAILS && Date.now() - lastFailAt < CB_COOLDOWN_MS;
}
function recordOk(): void {
  consecFails = 0;
}
function recordFail(): void {
  consecFails++;
  lastFailAt = Date.now();
}

// ── Minimal cookie jar (FZMovies sets session cookies between hops) ─────────
class Jar {
  private cookies = new Map<string, string>();
  ingest(setCookie: string | null): void {
    if (!setCookie) return;
    // may contain multiple cookies comma-joined; split on ", " followed by name=
    const parts = setCookie.split(/,(?=[^;,=]+=[^;,]*;)/);
    for (const p of parts) {
      const m = p.match(/^\s*([^=;\s]+)=([^;]*)/);
      if (m) this.cookies.set(m[1].trim(), m[2].trim());
    }
  }
  header(): string {
    return [...this.cookies.entries()].map(([k, v]) => `${k}=${v}`).join("; ");
  }
}

async function fzFetch(
  jar: Jar,
  url: string,
  init: RequestInit & { referer?: string } = {},
  timeoutMs: number = HOP_TIMEOUT_MS
): Promise<string> {
  const headers: Record<string, string> = {
    "User-Agent": UA,
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
  };
  const ck = jar.header();
  if (ck) headers["Cookie"] = ck;
  if (init.referer) headers["Referer"] = init.referer;
  const res = await fetch(url, {
    ...init,
    headers: { ...headers, ...(init.headers as Record<string, string>) },
    signal: AbortSignal.timeout(timeoutMs),
  });
  jar.ingest(res.headers.get("set-cookie"));
  if (!res.ok) throw new Error(`fz http ${res.status} for ${url}`);
  return res.text();
}

interface SearchHit {
  slug: string; // movie--Dubbed--X [Hindi]--hmp4.htm
  label: string; // display title
  year: string; // "2019" or ""
}

/** Parse search results: href='movie....htm' + <b>Title</b> + (year). */
function parseSearch(html: string): SearchHit[] {
  const hits: SearchHit[] = [];
  // Each result: <a ... href='movie--...--hmp4.htm'>...<b>Title</b></small></a> <small>(2020)</small>
  const re = /href='(movie[^']*?\.htm)'[^>]*>[\s\S]{0,400}?<b>([^<]+)<\/b><\/small><\/a>\s*<small>\((\d{4})\)<\/small>/g;
  let m: RegExpExecArray | null;
  const seen = new Set<string>();
  while ((m = re.exec(html)) !== null) {
    if (seen.has(m[1])) continue;
    seen.add(m[1]);
    hits.push({ slug: m[1], label: m[2].trim(), year: m[3] });
  }
  // Fallback: looser parse if the strict one finds nothing
  if (hits.length === 0) {
    const re2 = /href='(movie[^']*?\.htm)'/g;
    while ((m = re2.exec(html)) !== null) {
      if (seen.has(m[1])) continue;
      seen.add(m[1]);
      const lm = m[1].match(/--(.+?)--hmp4\.htm$/);
      hits.push({ slug: m[1], label: (lm?.[1] || m[1]).replace(/--/g, " ").trim(), year: "" });
    }
  }
  return hits;
}

function norm(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

/** Score a hit: Hindi slug + title similarity + year match. Higher = better. */
function scoreHit(hit: SearchHit, title: string, year: string): number {
  let score = 0;
  const nTitle = norm(title);
  const nLabel = norm(hit.label);
  const nSlug = norm(hit.slug);
  if (hit.slug.includes("[Hindi]") || nSlug.includes("hindi")) score += 50;
  // title words overlap
  const tWords = new Set(nTitle.split(" ").filter((w) => w.length > 2));
  const lWords = nLabel.split(" ");
  let overlap = 0;
  for (const w of lWords) if (tWords.has(w)) overlap++;
  score += overlap * 10;
  // slug should contain most title words too (guards against wrong movie)
  const sWords = nSlug.split(" ");
  let sOverlap = 0;
  for (const w of tWords) if (nSlug.includes(w)) sOverlap++;
  score += sOverlap * 5;
  if (year && hit.year === year) score += 20;
  // penalize obvious mismatches: less than half the title words matched
  if (tWords.size > 0 && overlap < tWords.size / 2) score -= 40;
  // HARD REJECT: zero title-word overlap = definitely the wrong movie.
  // (Prevents the [Hindi] bonus from rescuing a complete mismatch.)
  if (tWords.size > 0 && overlap === 0) return -1000;
  return score;
}

interface DlOption {
  label: string; // "[Standard Quality]" / "[HD Quality]"
  file: string; // "Avengers Endgame [Hindi] BluRay 480p.mp4"
  url: string; // download1.php?downloadoptionskey=...&pt=...
}

/** Parse ul.moviesfiles quality options from detail page. */
function parseDlOptions(html: string): DlOption[] {
  const opts: DlOption[] = [];
  const re =
    /<li>[\s\S]{0,200}?\[([^\]]*Quality[^\]]*)\][\s\S]{0,600}?window\.location\.href=\\?"(download1\.php\?downloadoptionskey=[^"\\]+)\\?"[\s\S]{0,200}?>([^<]+\.mp4)<\/a>/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(html)) !== null) {
    opts.push({ label: m[1].trim(), file: m[3].trim(), url: m[2].replace(/\\"/g, '"') });
  }
  if (opts.length === 0) {
    // fallback: any download1 link
    const re2 = /window\.location\.href=\\?"(download1\.php\?downloadoptionskey=[^"\\]+)\\?"/g;
    while ((m = re2.exec(html)) !== null) {
      opts.push({ label: "", file: "", url: m[1].replace(/\\"/g, '"') });
    }
  }
  return opts;
}

function qualityRank(opt: DlOption): number {
  const t = (opt.label + " " + opt.file).toLowerCase();
  if (/1080p/.test(t)) return 4;
  if (/720p/.test(t)) return 3;
  if (/hd/.test(t)) return 3;
  if (/480p/.test(t)) return 2;
  if (/360p/.test(t)) return 1;
  return 2;
}

function mp4Quality(url: string): string {
  const m = url.match(/(2160|1080|720|480|360)p/i);
  if (m) return `${m[1]}p`;
  return "480p";
}

/**
 * Full live 4-hop resolve. SLOW (35-60s) — never call in the request path.
 * Throws on any failure (caller decides fallback).
 *
 * @param opts.hopTimeoutMs per-hop fetch timeout (default 40s)
 * @param opts.attempts     full-chain attempts with a 3s breather (default 2)
 */
export async function fzmoviesLive(
  tmdbId: string,
  type: "movie" | "tv",
  opts: { hopTimeoutMs?: number; attempts?: number } = {}
): Promise<ProviderResult | null> {
  if (type !== "movie") return null; // FZMovies is movie-focused
  if (circuitOpen()) return null;

  const hopTimeoutMs = opts.hopTimeoutMs ?? HOP_TIMEOUT_MS;
  const attempts = opts.attempts ?? 2;

  // Resolve title + year from TMDB (cached 24h)
  let title = "";
  let year = "";
  try {
    const details = (await tmdb.movie(tmdbId)) as any;
    title = details?.title || "";
    const rd: string = details?.release_date || "";
    year = rd.slice(0, 4);
  } catch {
    return null;
  }
  if (!title) return null;

  const bases = [FZ_BASE, ...FZ_FALLBACKS];
  let lastErr: unknown = null;
  // Two attempts by default: the site is flaky (verified 2026-10-08: search
  // sometimes 200 in 7s, sometimes 30s+ timeout). One retry with a breather
  // often works. The bounded cron warm uses attempts: 1.
  for (let attempt = 0; attempt < attempts; attempt++) {
    for (const base of bases) {
      try {
        const result = await resolveOnBase(base, title, year, hopTimeoutMs);
        recordOk();
        return result;
      } catch (e) {
        lastErr = e;
      }
    }
    if (attempt === 0 && attempts > 1) await new Promise((r) => setTimeout(r, 3000));
  }
  recordFail();
  throw lastErr instanceof Error ? lastErr : new Error("fzmovies resolve failed");
}

async function resolveOnBase(
  base: string,
  title: string,
  year: string,
  hopTimeoutMs: number
): Promise<ProviderResult | null> {
  const jar = new Jar();

  // Hop 1: search (prefer Hindi-dubbed category)
  const form = new URLSearchParams();
  form.set("searchname", `${title} Hindi`);
  form.set("searchby", "Name");
  form.set("category", "DHollywood");
  let html = await fzFetch(jar, `${base}/csearch.php`, {
    method: "POST",
    body: form,
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    referer: `${base}/`,
  }, hopTimeoutMs);
  let hits = parseSearch(html).filter((h) => scoreHit(h, title, year) > 0);

  // Fallback: Bollywood category (for Bollywood originals VidZee missed)
  if (hits.length === 0) {
    const form2 = new URLSearchParams();
    form2.set("searchname", title);
    form2.set("searchby", "Name");
    form2.set("category", "Bollywood");
    html = await fzFetch(jar, `${base}/csearch.php`, {
      method: "POST",
      body: form2,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      referer: `${base}/csearch.php`,
    }, hopTimeoutMs);
    hits = parseSearch(html).filter((h) => scoreHit(h, title, year) > 0);
  }
  if (hits.length === 0) throw new Error("fzmovies: no search hits");

  hits.sort((a, b) => scoreHit(b, title, year) - scoreHit(a, title, year));
  const best = hits[0];

  // Hop 2: detail page → quality options
  const detailUrl = `${base}/${best.slug}`;
  html = await fzFetch(jar, detailUrl, { referer: `${base}/csearch.php` }, hopTimeoutMs);
  const opts = parseDlOptions(html);
  if (opts.length === 0) throw new Error("fzmovies: no download options");
  opts.sort((a, b) => qualityRank(b) - qualityRank(a));
  const opt = opts[0];

  // Hop 3: download1 → download link
  html = await fzFetch(jar, `${base}/${opt.url}`, { referer: detailUrl }, hopTimeoutMs);
  let m = html.match(/href="(download\.php\?downloadkey=[^"]+)"/) || html.match(/id="downloadlink"[^>]*href="([^"]+)"/);
  if (!m) throw new Error("fzmovies: no download.php link");
  const dlUrl = `${base}/${m[1]}`;

  // Hop 4: download page → direct MP4 mirrors
  html = await fzFetch(jar, dlUrl, { referer: `${base}/${opt.url}` }, hopTimeoutMs);
  let mp4s = [...html.matchAll(/name="download1"[^>]*value="([^"]+)"/g)].map((x) => x[1]);
  if (mp4s.length === 0) {
    mp4s = [...html.matchAll(/(https:\/\/[^"']+\.mp4[^"']*)/g)].map((x) => x[1]);
  }
  // de-dupe
  mp4s = [...new Set(mp4s)].filter((u) => u.startsWith("https://"));
  if (mp4s.length === 0) throw new Error("fzmovies: no mp4 mirrors");
  if (html.includes("download keys have expired")) throw new Error("fzmovies: keys expired");

  // Quality from the detail-page option label (e.g. "BluRay 480p.mp4"),
  // fallback to URL sniffing.
  const optQ = (() => {
    const m = (opt.label + " " + opt.file).match(/(2160|1080|720|480|360)\s*p/i);
    return m ? `${m[1]}p` : "";
  })();
  const qualities: StreamQuality[] = mp4s.slice(0, 3).map((url) => ({
    quality: optQ || mp4Quality(url),
    url,
    codec: "unknown",
    size: 0,
  }));

  return { provider: "fzmovies", qualities, subtitles: [] };
}

// ── Cache + request-path entry points ───────────────────────────────────────

const cacheKey = (tmdbId: string) => `fz:hi:${tmdbId}`;

/**
 * Cache-ONLY lookup for the request path. Never blocks, never scrapes.
 * Returns the cached ProviderResult or null.
 */
export const fzmovies: ProviderFn = async (tmdbId, type) => {
  if (type !== "movie") return null;
  if (circuitOpen()) return null;
  // Redis shared cache FIRST (api1 + api2 see the same warmed entries),
  // then per-instance memory. Cache-ONLY — never blocks, never scrapes.
  if (redisEnabled()) {
    const rhit = await redisCacheGet<ProviderResult>(cacheKey(tmdbId));
    if (rhit) {
      cacheSet(cacheKey(tmdbId), rhit, CACHE_TTL_MS, 0); // backfill local
      return rhit;
    }
  }
  const hit = cacheGet<ProviderResult>(cacheKey(tmdbId));
  if (!hit) return null;
  return hit.value;
};

/**
 * Bounded warm for /v1/cron/fz-warm: runs the live chain with a hard overall
 * deadline so the invocation ALWAYS completes inside Vercel's 60s
 * maxDuration. Single attempt, 12s hops — the cron caller retries on
 * transient failures.
 *
 * Single-flight: concurrent warms for the same title collapse into one
 * (prevents double 35-60s scrapes from two cron triggers / two requests).
 *
 * Returns an honest result — never throws.
 */
export interface WarmResult {
  warmed: boolean;
  /** cached | warmed | already-warming | circuit-open | timeout | failed | not-movie */
  reason: string;
  qualities?: number;
}

const warming = new Set<string>();

export async function warmFZMovies(
  tmdbId: string,
  type: "movie" | "tv",
  bounded = false
): Promise<WarmResult> {
  if (type !== "movie") return { warmed: false, reason: "not-movie" };
  if (circuitOpen()) return { warmed: false, reason: "circuit-open" };
  const key = cacheKey(tmdbId);
  const existing = cacheGet<ProviderResult>(key);
  if (existing) return { warmed: true, reason: "cached", qualities: existing.value.qualities.length };
  if (warming.has(key)) return { warmed: false, reason: "already-warming" };
  warming.add(key);
  try {
    const run = (async (): Promise<WarmResult> => {
      try {
        const r = await fzmoviesLive(
          tmdbId,
          type,
          bounded ? { hopTimeoutMs: WARM_HOP_TIMEOUT_MS, attempts: 1 } : {}
        );
        if (r && r.qualities.length > 0) {
          cacheSet(key, r, CACHE_TTL_MS, 0);
          // Fan out to Redis so BOTH clusters (api1 + api2) serve it.
          if (redisEnabled()) {
            await redisCacheSet(key, r, Math.floor(CACHE_TTL_MS / 1000));
          }
          return { warmed: true, reason: "warmed", qualities: r.qualities.length };
        }
        return { warmed: false, reason: "failed" };
      } catch {
        // failures are recorded by the circuit breaker inside fzmoviesLive
        return { warmed: false, reason: "failed" };
      }
    })();
    if (!bounded) return run;
    // Hard deadline: the cron invocation must respond inside maxDuration.
    const r = await Promise.race([
      run,
      new Promise<WarmResult>((res) =>
        setTimeout(() => res({ warmed: false, reason: "timeout" }), WARM_OVERALL_MS)
      ),
    ]);
    return r;
  } finally {
    warming.delete(key);
  }
}

/** Stats for /health */
export function fzStats(): Record<string, unknown> {
  return { circuitOpen: circuitOpen(), consecutiveFails: consecFails };
}
