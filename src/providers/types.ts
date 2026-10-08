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
}

/** Provider function signature. */
export type ProviderFn = (
  tmdbId: string,
  type: "movie" | "tv",
  season?: number,
  episode?: number
) => Promise<ProviderResult | null>;
