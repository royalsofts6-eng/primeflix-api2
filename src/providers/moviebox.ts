/**
 * MovieBox provider via third-party wrapper (2026-10-09).
 *
 * Live verified 2026-10-09: GET /search?query= + /get_stream work end-to-end
 * (DASH manifest + Edge-Cache-Cookie + HEVC, 3 qualities reported).
 * Direct MovieBox API is NOT usable (407 signature-invalid) — wrapper only.
 *
 * CYBER RULES (standing):
 *  1. The wrapper leg is NEVER in a race — chain.ts calls these as a
 *     SEQUENTIAL last tier only.
 *  2. Max 1 concurrent wrapper call — enforced here by the pacer mutex.
 *  3. Kill switch: MOVIEBOX_ENABLED=false (or "0") bypasses instantly,
 *     no redeploy. Redis flag pf:kill:moviebox is the ops-level switch.
 *
 * Self-policing: every call goes through acquirePace (1 req/5s sustained,
 * burst 4) + a human gap (800–2500ms) before get_stream. 404 "no streaming
 * link" is a catalog gap — negative-cached 24h, never a circuit failure.
 */
import type { ProviderFn, ProviderResult, StreamQuality } from "./types.js";
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

// ── MovieBox-Hindi language verdict (P1 2026-10-09) ──────────────────────
// The dub button (/languages) must know about MovieBox Hindi without
// live-probing the wrapper on every info-screen open. This verdict cache
// (24h, matching the pf:v3:mbneg catalog-gap cadence) is written "1" when a
// chain win/alternate or a probe proves MovieBox has Hindi for a title,
// "0" when a probe proves it doesn't. Cheap Redis read, shared api1+api2.
// The 12h pf:v3:lang envelope stays the primary throttle — this key only guards
// the probe path underneath it.
const MBHILANG_TTL_S = 24 * 3600;
// Phase D (2026-10-09): versioned key pf:v3:mbhilang:movie:{id}. Exported so
// the report route evicts the identical key.
/** Shared MovieBox-Hindi verdict key (exported for the report route). Never throws. */
export const mbhilangKey = (tmdbId: string): string => ck("mbhilang", "movie", tmdbId);

/** Record the MovieBox-Hindi verdict for a movie. Never throws. */
export async function noteMovieboxHindi(tmdbId: string, found: boolean): Promise<void> {
  if (!redisEnabled()) return;
  try {
    await redisCacheSet(mbhilangKey(tmdbId), found ? "1" : "0", MBHILANG_TTL_S);
  } catch {
    /* best-effort */
  }
}

/** Shared verdict read: true/false, or null when unknown. Never throws. */
export async function movieboxHindiVerdict(tmdbId: string): Promise<boolean | null> {
  if (!redisEnabled()) return null;
  try {
    const v = await redisCacheGet<string>(mbhilangKey(tmdbId));
    return v === "1" ? true : v === "0" ? false : null;
  } catch {
    return null;
  }
}

/** True when a chain run already proved MovieBox has NO Hindi for this movie. */
export async function movieboxHindiKnownMissing(tmdbId: string): Promise<boolean> {
  return negGet(`hi:${tmdbId}`);
}

/** Combined liveness: env flag AND the Redis kill switch. Cheap (one Redis GET). */
export async function movieboxLive(): Promise<boolean> {
  return enabled() && (await wrapperAlive());
}

// ── Title matching (D1 verified algorithm + D4 junk filter) ──────────────────
const norm = (s: string): string => s.toLowerCase().replace(/[^a-z0-9 ]/g, " ");

/** Fraction of the TMDB title's significant tokens present in the candidate. */
function titleScore(title: string, candidate: string): number {
  const tokens = norm(title).split(/\s+/).filter((t) => t.length > 2);
  if (tokens.length === 0) return 0;
  const ct = norm(candidate);
  const overlap = tokens.filter((tok) => ct.includes(tok)).length;
  return overlap / tokens.length;
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
 * D1 mapping: "{title} hindi" -> [Hindi]-marked subject, year ±1,
 * subjectType 1 (movie), ≥50% token overlap (D4 junk filter — drops
 * mislabeled uploads like "Cheetah on Fire").
 */
async function findHindiSubject(title: string, year: number, signal: AbortSignal): Promise<SearchHit | null> {
  const items = await wrapperSearch(`${title} hindi`, signal);
  let best: SearchHit | null = null;
  let bestScore = 0;
  for (const it of items) {
    const t = String(it.title ?? it.name ?? "");
    if (!/\[hindi\]|\(hindi\)/i.test(t)) continue; // Hindi marker required
    const y = subjectYear(it);
    if (y && year && Math.abs(y - year) > 1) continue; // year ±1
    if (String(it.subjectType ?? it.subject_type ?? "1") !== "1") continue; // movie
    const score = titleScore(title, t);
    if (score > bestScore && score >= 0.5) {
      const id = subjectIdOf(it);
      if (id) {
        bestScore = score;
        best = { id, title: t };
      }
    }
  }
  return best;
}

/** Default-resource pick: same similarity gates, no [Hindi] requirement. */
async function findDefaultSubject(title: string, year: number, signal: AbortSignal): Promise<SearchHit | null> {
  const items = await wrapperSearch(title, signal);
  let best: SearchHit | null = null;
  let bestScore = 0;
  for (const it of items) {
    const t = String(it.title ?? it.name ?? "");
    const y = subjectYear(it);
    if (y && year && Math.abs(y - year) > 1) continue;
    if (String(it.subjectType ?? it.subject_type ?? "1") !== "1") continue;
    const score = titleScore(title, t);
    // Prefer non-Hindi-marked for the English lane, but accept Hindi-marked
    // over nothing (a Bollywood original's default resource IS Hindi).
    const adjusted = /\[hindi\]|\(hindi\)/i.test(t) ? score * 0.9 : score;
    if (adjusted > bestScore && score >= 0.5) {
      const id = subjectIdOf(it);
      if (id) {
        bestScore = adjusted;
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

async function getStream(subjectId: string, signal: AbortSignal): Promise<ProviderResult | null> {
  let res;
  try {
    res = await fetchUpstream(
      "moviebox",
      `${WRAPPER}/get_stream?subject_id=${encodeURIComponent(subjectId)}&season=0&episode=0`,
      { signal }
    );
  } catch (e) {
    // 404 "no streaming link" = catalog gap: negative-cache, silent miss.
    // Anything else (5xx, network) rethrows -> chain classifies + counts.
    if (e instanceof ProviderFailure && e.status === 404) {
      await negSet(`stream:${subjectId}`);
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
async function pacedCall<T>(work: () => Promise<T>, signal?: AbortSignal): Promise<T> {
  const release = await acquirePace("mb_wrapper", signal);
  try {
    await humanPause(signal); // 800–2500ms between sequential wrapper calls
    return await work();
  } finally {
    release();
  }
}

/** L1 Hindi lane's sequential tier (movies only — series Hindi is spotty). */
export const movieboxHindi: ProviderFn = async (tmdbId, type, _s, _e, opts) => {
  if (!enabled()) return null;
  if (type !== "movie") return null;
  if (!(await wrapperAlive())) return null;
  const negK = `hi:${tmdbId}`;
  if (await negGet(negK)) return null;
  const signal = composeSignal(opts?.signal);
  return pacedCall(async () => {
    const { title, year } = await resolveTitle(tmdbId, type);
    if (!title) return null;
    let hit: SearchHit | null = null;
    try {
      hit = await findHindiSubject(title, year, signal);
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
    if (await negGet(`stream:${hit.id}`)) return null;
    const r = await getStream(hit.id, signal);
    return r ? { ...r, provider: "moviebox-hi" } : null;
  }, opts?.signal);
};

/** L2 English lane's sequential tier (default resource — original audio). */
export const moviebox: ProviderFn = async (tmdbId, type, _s, _e, opts) => {
  if (!enabled()) return null;
  if (type !== "movie") return null;
  if (!(await wrapperAlive())) return null;
  const negK = `en:${tmdbId}`;
  if (await negGet(negK)) return null;
  const signal = composeSignal(opts?.signal);
  return pacedCall(async () => {
    const { title, year } = await resolveTitle(tmdbId, type);
    if (!title) return null;
    let hit: SearchHit | null = null;
    try {
      hit = await findDefaultSubject(title, year, signal);
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
    if (await negGet(`stream:${hit.id}`)) return null;
    return getStream(hit.id, signal);
  }, opts?.signal);
};
