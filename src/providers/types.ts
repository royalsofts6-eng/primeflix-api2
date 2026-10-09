/**
 * Shared provider types.
 */
export interface StreamQuality {
  quality: string;
  url: string;
  codec: string;
  size: number;
}

export interface Subtitle {
  lang: string;
  url: string;
}

export interface ProviderResult {
  provider: string;
  qualities: StreamQuality[];
  subtitles?: Subtitle[];
  /** e.g. MovieBox wrapper Edge-Cache-Cookie — the player sends it as a
   *  Cookie header on manifest + segment requests (without it: 403). */
  cookie?: string;
}

/** Options passed to every provider call (race.ts). */
export interface ProviderCallOpts {
  /** Lane-shared signal — set when the lane settles so losers abort. */
  signal?: AbortSignal;
}

/** Provider function signature. */
export type ProviderFn = (
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number,
  opts?: ProviderCallOpts
) => Promise<ProviderResult | null>;

/**
 * Thrown when a provider correctly reports "this title is not available"
 * (e.g. VidZee 404/502 for a title with no Hindi dub). This is NOT a
 * provider failure — the circuit breaker must NOT count it as a fail,
 * otherwise 5x "no Hindi" responses would open the circuit and block
 * Hindi for titles that DO have it.
 */
export class NotAvailableError extends Error {
  constructor(message = "not available") {
    super(message);
    this.name = "NotAvailableError";
  }
}
