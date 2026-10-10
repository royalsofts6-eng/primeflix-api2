/**
 * MovieBox provider via third-party wrapper (2026-10-09).
 *
 * Live verified 2026-10-09: GET /search?query= + /get_stream work end-to-end
 * (DASH manifest + Edge-Cache-Cookie + HEVC, 3 qualities reported).
 * Direct MovieBox API is NOT usable (407 signature-invalid) — wrapper only.
 *
 * CYBER RULES (standing):
 *  1. The wrapper leg is NEVER in a parallel race — chain.ts gives it a
 *     MovieBox-first 5s head-start window (Ali 2026-10-10), then the lanes.
 *     The provider itself paces (1 req/5s, max 1 concurrent) and humanizes
 *     (800–2500ms gaps); the kill switch bypasses instantly.
 *  2. Max 1 concurrent wrapper call — enforced here by the pacer mutex.
 *  3. Kill switch: MOVIEBOX_ENABLED=false (or "0") bypasses instantly,
 *     no redeploy. Redis flag pf:kill:moviebox is the ops-level switch.
 *  4. Serves movies AND TV series (Ali 2026-10-10) — the wrapper's
 *     get_stream takes season/episode; subjectType 1=movie, 2=series.
 *
 * Self-policing: every call goes through acquirePace (1 req/5s sustained,
 * burst 4) + a human gap (800–2500ms) before get_stream. 404 "no streaming
 * link" is a catalog gap — negative-cached 24h, never a circuit failure.
 */
import type { ProviderFn, ProviderResult, StreamQuality, ProviderCallOpts } from "./types.js";
import { fetchUpstream, ProviderFailure } from "./failures.js";
import { tmdb } from "../tmdb.js";
import { redisEnabled, redisCacheGet, redisCacheSet, redisCommand } from "../security/redis.js";
import { acquirePace, pacerStats } from "../pacer.js";
import { humanPause } from "../humanize.js";
import { ck } from "../cache.js";

const WRAPPER = (process.env.MB_WRAPPER_BASE || "https://moviebox-fastapi.vercel.app").replace(/\/$/, "");
const FETCH_TIMEOUT_MS = 8000; // wrapper p95 unmeasured — conservative
const NEG_TTL_S = 24 * 3600; // negative cache: catalog gaps don't change hourly

const enabled = (): boolean => {
  const v = (process.env.MOVIEBOX_ENABLED || "").toLowerCase().trim();
  return v !== "false" && v !== "0";
};

/** Ops-level kill flag (Redis). Fail-open: Redis down -> tier stays alive. */
export async function wrapperAlive(): Promise<boolean> {
  if (!redisEnabled()) return true;
  try {
    return (await redisCommand(["GET", "pf:kill:moviebox"])) !== "1";
  } catch {
    return true;
  }
}

export async function wrapperStatus(): Promise<Record<string, unknown>> {
  return {
    enabled: enabled(),
    killSwitch: !(await wrapperAlive()),
    base: WRAPPER,
    pacer: pacerStats(),
  };
}

function composeSignal(external?: AbortSignal): AbortSignal {
  const t = AbortSignal.timeout(FETCH_TIMEOUT_MS);
  return external ? AbortSignal.any([external, t]) : t;
}

// ── Negative cache (catalog gaps — don't re-hit dead titles) ────────────────
// Phase D (2026-10-09): versioned key pf:v3:mbneg:*.
const negKey = (k: string) => ck("mbneg", k);

// ── MovieBox-Hindi language verdict (P1 2026-10-09) ──────────────────────
// The dub button (/languages) must know about MovieBox Hindi without
// live-probing the wrapper on every info-screen open. This verdict cache
// (24h, matching the pf:v3:mbneg catalog-gap cadence) is written "1" when a
// chain win/alternate or a probe proves MovieBox has Hindi for a title,
// "0" when a probe proves it doesn't. Cheap Redis read, shared api1+api2.
// The 12h pf:v3:lang envelope stays the primary throttle — this key only guards
// the probe path underneath it.
// Phase D (2026-10-09): versioned key pf:v3:mbhilang:{type}:{id}:{season}.
// Exported so the report route evicts the identical key. Ali 2026-10-10:
// TV verdicts namespaced per type (movie/tv); P0-5 (2026-10-10): per
// season — an S1E1 probe/win must never promise Hindi for S2.
const MBHILANG_TTL_S = 24 * 3600;
/**
 * Shared MovieBox-Hindi verdict key (exported for the report route).
 * P0-5 (2026-10-10): namespaced per SEASON — a probe/win for S1E1 must
 * never claim Hindi for S2 (dub availability varies by season). Movies
 * use season 0. Never throws.
 */
export const mbhilangKey = (tmdbId: string, type: "movie" | "tv" = "movie", season = 0): string =>
  ck("mbhilang", type, tmdbId, season);

/** Record the MovieBox-Hindi verdict for a title+season. Never throws. */
export async function noteMovieboxHindi(tmdbId: string, found: boolean, type: "movie" | "tv" = "movie", season = 0): Promise<void> {
  if (!redisEnabled()) return;
  try {
    await redisCacheSet(mbhilangKey(tmdbId, type, season), found ? "1" : "0", MBHILANG_TTL_S);
  } catch {
    /* best-effort */
  }
}

/** Shared verdict read: true/false, or null when unknown. Never throws. */
export async function movieboxHindiVerdict(tmdbId: string, type: "movie" | "tv" = "movie", season = 0): Promise<boolean | null> {
  if (!redisEnabled()) return null;
  try {
    const v = await redisCacheGet<string>(mbhilangKey(tmdbId, type, season));
    return v === "1" ? true : v === "0" ? false : null;
  } catch {
    return null;
  }
}

/** True when a chain run already proved MovieBox has NO Hindi for this title. */
export async function movieboxHindiKnownMissing(tmdbId: string, type: "movie" | "tv" = "movie"): Promise<boolean> {
  return negGet(type === "tv" ? `hi:tv:${tmdbId}` : `hi:${tmdbId}`);
}

async function negGet(k: string): Promise<boolean> {
  if (!redisEnabled()) return false;
  try {
    return (await redisCacheGet<string>(negKey(k))) === "1";
  } catch {
    return false;
  }
}

async function negSet(k: string): Promise<void> {
  if (!redisEnabled()) return;
  try {
    await redisCacheSet(negKey(k), "1", NEG_TTL_S);
  } catch {
    /* best-effort */
  }
}

/** Combined liveness: env flag AND the Redis kill switch. Cheap (one Redis GET). */
export async function movieboxLive(): Promise<boolean> {
  return enabled() && (await wrapperAlive());
}

// ── Title matching (D1 verified algorithm + D4 junk filter) ──────────────────
const norm = (s: string): string => s.toLowerCase().replace(/[^a-z0-9 ]/g, " ");

/**
 * Hardened title similarity (P1-7, 2026-10-10). The old 50%-overlap with
 * SUBSTRING matching let "Dark" match "Dark Matter" and junk uploads win
 * the matcher.
 * - Exact token matching (no substrings): "dark" no longer matches
 *   "darkness"/"dark matter" via containment.
 * - Extra-token penalty: junk words in the candidate ("Soundtrack", "Best
 *   Songs ONLY") drag the score down.
 * - Short titles (<=2 significant tokens): require EVERY title token
 *   present exactly AND a tight candidate — the wrong-movie engine for
 *   short titles. Marker words ([Hindi], dubbed, ...) don't count as
 *   extras.
 * Exported for unit tests.
 */
const MARKER_TOKENS = new Set([
  "hindi", "dubbed", "esub", "esubs", "subbed", "uncut", "unrated",
  "extended", "remastered", "proper", "repack",
]);

export function titleScore(title: string, candidate: string): number {
  const tTokens = norm(title).split(/\s+/).filter((t) => t.length > 2);
  if (tTokens.length === 0) return 0;
  const cTokens = norm(candidate).split(/\s+/).filter((t) => t.length > 2);
  const cSet = new Set(cTokens);
  let overlap = 0;
  for (const tok of tTokens) if (cSet.has(tok)) overlap++;
  const recall = overlap / tTokens.length;
  const extra = cTokens.filter((t) => !tTokens.includes(t) && !MARKER_TOKENS.has(t)).length;
  if (tTokens.length === 1) {
    // Single-token titles ("Dark", "Dune", "Mirzapur"): the candidate must
    // be exactly this title (+ optional markers) — anything else is a
    // different title ("Dark Matter").
    return recall === 1 && extra === 0 ? 1 : 0;
  }
  if (tTokens.length === 2) {
    // Two-token titles: full exact recall, at most one non-marker extra.
    if (recall < 1 || extra > 1) return 0;
  }
  // Longer titles: recall minus a capped junk-token penalty.
  return Math.max(0, recall - Math.min(0.45, extra * 0.09));
}

const subjectYear = (it: any): number =>
  parseInt(String(it.releaseDate ?? it.release_date ?? it.year ?? "0"), 10) || 0;

const subjectIdOf = (it: any): string | null => {
  const id = it.subject_id ?? it.subjectId ?? it.id;
  return id != null ? String(id) : null;
};

interface SearchHit {
  id: string;
  title: string;
}

async function wrapperSearch(query: string, signal: AbortSignal): Promise<any[]> {
  const res = await fetchUpstream(
    "moviebox",
    `${WRAPPER}/search?query=${encodeURIComponent(query.toLowerCase())}`,
    { signal }
  );
  const data: any = await res.json().catch(() => null);
  const items: any[] = data?.results ?? data?.data ?? (Array.isArray(data) ? data : []) ?? [];
  return items;
}

/**
 * Wrapper kind discriminator. The wrapper returns type:"movie"|"series"
 * (older shapes used subjectType 1/2). Unknown shapes keep the historical
 * movies-only leniency: movies accept anything that isn't explicitly a
 * series; TV requires an explicit series marker.
 */
function kindMatches(it: any, type: "movie" | "tv"): boolean {
  const t = String(it.type ?? "").toLowerCase();
  if (t === "series") return type === "tv";
  if (t === "movie") return type === "movie";
  const st = String(it.subjectType ?? it.subject_type ?? "1");
  return type === "tv" ? st === "2" : st === "1";
}

/**
 * Hindi signals on a wrapper subject (live shapes 2026-10-10).
 * - Explicit title marker: "Title [Hindi]" / "Title (Hindi)" — the
 *   reliable signal (the languages array is noise: "Breaking Bad [Hindi]"
 *   lists ["English","Spanish"], "Premalu [Hindi]" lists
 *   ["Malayalam","Telugu"]).
 * - Languages array: entries may be "Hindi", "hi", "Hindi; English", etc.
 */
export function hindiMarkedTitle(t: string): boolean {
  return /\[hindi\]|\(hindi\)/i.test(t);
}

export function hindiMarkedLangs(langs: string[]): boolean {
  return langs.some((l) =>
    String(l)
      .toLowerCase()
      .split(/[;,/]/)
      .some((p) => {
        const w = p.trim();
        return w === "hindi" || w === "hi";
      })
  );
}

/**
 * D1 mapping: "{title} hindi" -> Hindi subject.
 *
 * Hindi signal (P0-4, 2026-10-10 — live-verified: the languages array is
 * noise; "Breaking Bad [Hindi]" lists ["English","Spanish"], "Premalu
 * [Hindi]" lists ["Malayalam","Telugu"]):
 * - STRONG: explicit [Hindi]/(Hindi) title marker. Year gate ±1 (movies)
 *   or ±10 (TV — dub uploads carry the dub-release year, e.g. GoT
 *   [Hindi]=2019 vs TMDB 2011; P0-3: do not regress). P1-8: a missing
 *   year no longer skips the gate silently — score x0.7 penalty.
 * - MEDIUM ("equivalent strong signal"): languages-array Hindi WITHOUT a
 *   title marker (Hindi originals like 'Mirzapur' 2018, languages
 *   ['Hindi']). Accepted ONLY on near-exact title (score >=0.9) AND strict
 *   year (both present, ±1 — no dub-year widening, no missing-year pass).
 */
async function findHindiSubject(
  title: string,
  year: number,
  signal: AbortSignal,
  type: "movie" | "tv"
): Promise<SearchHit | null> {
  const items = await wrapperSearch(`${title} hindi`, signal);
  let best: SearchHit | null = null;
  let bestScore = 0;
  for (const it of items) {
    const t = String(it.title ?? it.name ?? "");
    const langs: string[] = Array.isArray(it.languages) ? it.languages.map((l: any) => String(l)) : [];
    const titleMarked = hindiMarkedTitle(t);
    const langMarked = hindiMarkedLangs(langs);
    if (!titleMarked && !langMarked) continue; // Hindi signal required
    const y = subjectYear(it);
    if (!kindMatches(it, type)) continue;
    const score = titleScore(title, t);
    if (titleMarked) {
      // STRONG: explicit marker. TV keeps the ±10 dub-year gate.
      const yearGate = type === "tv" ? 10 : 1;
      if (y && year && Math.abs(y - year) > yearGate) continue;
      const adj = y && year ? score : score * 0.7; // P1-8: missing year penalized, never silently skipped
      if (adj > bestScore && adj >= 0.5) {
        const id = subjectIdOf(it);
        if (id) {
          bestScore = adj;
          best = { id, title: t };
        }
      }
    } else {
      // MEDIUM: languages-array Hindi only — near-exact title + strict year.
      if (!y || !year || Math.abs(y - year) > 1) continue;
      if (score > bestScore && score >= 0.9) {
        const id = subjectIdOf(it);
        if (id) {
          bestScore = score;
          best = { id, title: t };
        }
      }
    }
  }
  return best;
}

/** Default-resource pick: same similarity gates, no [Hindi] requirement. */
async function findDefaultSubject(
  title: string,
  year: number,
  signal: AbortSignal,
  type: "movie" | "tv"
): Promise<SearchHit | null> {
  const items = await wrapperSearch(title, signal);
  let best: SearchHit | null = null;
  let bestScore = 0;
  for (const it of items) {
    const t = String(it.title ?? it.name ?? "");
    const y = subjectYear(it);
    if (y && year && Math.abs(y - year) > 1) continue;
    if (!kindMatches(it, type)) continue;
    // P0-3 (2026-10-10): the English lane must NEVER serve a Hindi-marked
    // stream — exclude, don't just deprioritize (the old ×0.9 penalty let
    // Hindi-marked dubs win the English lane). Bollywood originals are
    // unaffected: their default resource isn't [Hindi]-marked, and an
    // explicit ?audio=en still falls through to VidLink (original track).
    const langs: string[] = Array.isArray(it.languages) ? it.languages.map((l: any) => String(l)) : [];
    if (hindiMarkedTitle(t) || hindiMarkedLangs(langs)) continue;
    const score = titleScore(title, t);
    // P1-8: a missing year no longer skips the gate silently — penalize.
    const adj = y && year ? score : score * 0.7;
    if (adj > bestScore && adj >= 0.5) {
      const id = subjectIdOf(it);
      if (id) {
        bestScore = adj;
        best = { id, title: t };
      }
    }
  }
  return best;
}

function extractQuality(url: string): string {
  const m = url.match(/[_-](\d{3,4})[_p/.-]/);
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

async function getStream(
  subjectId: string,
  type: "movie" | "tv",
  season: number,
  episode: number,
  signal: AbortSignal
): Promise<ProviderResult | null> {
  // TV: one subject serves many episodes — the neg key is per (subject, s, e).
  // Movies keep the legacy key (backward compatible with the 24h neg cache).
  const streamNegK = type === "tv" ? `stream:tv:${subjectId}:${season}:${episode}` : `stream:${subjectId}`;
  let res;
  try {
    res = await fetchUpstream(
      "moviebox",
      `${WRAPPER}/get_stream?subject_id=${encodeURIComponent(subjectId)}&season=${season}&episode=${episode}`,
      { signal }
    );
  } catch (e) {
    // 404 "no streaming link" = catalog gap: negative-cache, silent miss.
    // Anything else (5xx, network) rethrows -> chain classifies + counts.
    if (e instanceof ProviderFailure && e.status === 404) {
      await negSet(streamNegK);
      return null;
    }
    throw e;
  }
  const d: any = await res.json().catch(() => null);
  if (!d || d.status !== "success" || typeof d.stream_url !== "string" || !d.stream_url.startsWith("http")) {
    return null;
  }
  const qualities: StreamQuality[] = [
    {
      quality: extractQuality(d.stream_url),
      url: d.stream_url as string,
      codec: d.codec ?? "hevc", // live-verified HEVC; ExoPlayer HW-decodes
      size: parseInt(d.size ?? "0", 10) || 0,
    },
  ];
  return {
    provider: "moviebox",
    qualities,
    subtitles: [],
    // Edge-Cache-Cookie -> the app sends it as a Cookie header on the
    // manifest + every segment (without it the CDN 403s — live verified).
    cookie: typeof d.cookie === "string" ? d.cookie : undefined,
    // 2026-10-10 (428 fix): wrapper get_stream may also return extra request
    // headers (e.g. Referer) required by the CDN for MP4 progressive URLs.
    // Forward them so the app sends them (without them: 428).
    headers:
      d.headers && typeof d.headers === "object" && !Array.isArray(d.headers)
        ? (d.headers as Record<string, string>)
        : undefined,
  };
}

async function resolveTitle(tmdbId: string, type: "movie" | "tv"): Promise<{ title: string; year: number }> {
  const details = (await (type === "movie" ? tmdb.movie(tmdbId) : tmdb.tv(tmdbId))) as any;
  return {
    title: String(details?.title ?? details?.name ?? ""),
    year: parseInt(String(details?.release_date ?? details?.first_air_date ?? "0").slice(0, 4), 10) || 0,
  };
}

/**
 * One paced, humanized wrapper call. Never throws for catalog gaps.
 * P0-1 (2026-10-09): `signal` is the chain's 45s deadline — the pacer
 * wait AND the human pause both abort early on it (AbortError), so a
 * congested wrapper tier can never stretch a request past the deadline.
 */
async function pacedCall<T>(work: () => Promise<T>, opts?: ProviderCallOpts): Promise<T> {
  // P1-12: thread the pacer priority — background probes/prefetches queue
  // behind real Play requests in the wrapper mutex.
  const release = await acquirePace("mb_wrapper", opts?.signal, opts?.priority ?? "play");
  try {
    await humanPause(opts?.signal); // 800–2500ms between sequential wrapper calls
    return await work();
  } finally {
    release();
  }
}

/** L1 Hindi lane's MovieBox tier — movies + TV series (Ali 2026-10-10). */
export const movieboxHindi: ProviderFn = async (tmdbId, type, s, e, opts) => {
  if (!enabled()) return null;
  if (!(await wrapperAlive())) return null;
  const season = s ?? 1;
  const episode = e ?? 1;
  // TV neg keys are namespaced so a same-numbered movie/TV pair can't collide.
  const negK = type === "tv" ? `hi:tv:${tmdbId}` : `hi:${tmdbId}`;
  if (await negGet(negK)) return null;
  const signal = composeSignal(opts?.signal);
  return pacedCall(async () => {
    const { title, year } = await resolveTitle(tmdbId, type);
    if (!title) return null;
    let hit: SearchHit | null = null;
    try {
      hit = await findHindiSubject(title, year, signal, type);
    } catch (e) {
      if (e instanceof ProviderFailure && e.status === 404) {
        await negSet(negK);
        return null;
      }
      throw e;
    }
    if (!hit) {
      await negSet(negK); // no Hindi subject — don't re-search for 24h
      return null;
    }
    const streamNegK = type === "tv" ? `stream:tv:${hit.id}:${season}:${episode}` : `stream:${hit.id}`;
    if (await negGet(streamNegK)) return null;
    const r = await getStream(hit.id, type, season, episode, signal);
    return r ? { ...r, provider: "moviebox-hi" } : null;
  }, opts);
};

/** L2 English lane's MovieBox tier — default resource, movies + TV series (Ali 2026-10-10). */
export const moviebox: ProviderFn = async (tmdbId, type, s, e, opts) => {
  if (!enabled()) return null;
  if (!(await wrapperAlive())) return null;
  const season = s ?? 1;
  const episode = e ?? 1;
  const negK = type === "tv" ? `en:tv:${tmdbId}` : `en:${tmdbId}`;
  if (await negGet(negK)) return null;
  const signal = composeSignal(opts?.signal);
  return pacedCall(async () => {
    const { title, year } = await resolveTitle(tmdbId, type);
    if (!title) return null;
    let hit: SearchHit | null = null;
    try {
      hit = await findDefaultSubject(title, year, signal, type);
    } catch (e) {
      if (e instanceof ProviderFailure && e.status === 404) {
        await negSet(negK);
        return null;
      }
      throw e;
    }
    if (!hit) {
      await negSet(negK);
      return null;
    }
    const streamNegK = type === "tv" ? `stream:tv:${hit.id}:${season}:${episode}` : `stream:${hit.id}`;
    if (await negGet(streamNegK)) return null;
    return getStream(hit.id, type, season, episode, signal);
  }, opts);
};
