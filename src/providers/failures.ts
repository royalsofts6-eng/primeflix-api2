/**
 * Failure classification for provider errors (P2 polish, Ali 2026-10-09:
 * "backend optimize kro ek ek cheez professionally plan krke implement kro").
 *
 * Every upstream failure is classified into one of:
 *   NETWORK      — timeout, DNS failure, connection reset/refused
 *   NOT_FOUND    — HTTP 404 (endpoint or title path gone; retry is pointless)
 *   FORBIDDEN    — HTTP 403 (key/origin rejected; retry is pointless)
 *   RATE_LIMITED — HTTP 429 (back off; honor Retry-After cluster-wide)
 *   SERVER       — HTTP 5xx or unexpected local error (transient; 1 retry)
 *   CONTENT_MISS — provider answered fine but has no such title
 *                  (e.g. VidZee 404/502 = "no Hindi dub for this title").
 *                  This is NOT a failure: no retry, no fail counting, no
 *                  cooldown. (The 2026-10-08 'no Hindi' fix: 'no Hindi'
 *                  responses must never trip the circuit.)
 *
 * Per-class handling (enforced in chain.ts tryProvider):
 *   NOT_FOUND / FORBIDDEN -> next provider immediately, no retry.
 *   RATE_LIMITED          -> no in-request retry; Redis cooldown honoring
 *                            Retry-After (min 5 min) so all instances back off.
 *   NETWORK / SERVER      -> exactly 1 retry, then next provider.
 *   CONTENT_MISS          -> return null, invisible to health/circuit.
 */
import { NotAvailableError } from "./types.js";

/** Classified upstream failure. Thrown by providers, handled in chain.ts. */
export class ProviderFailure extends Error {
  readonly provider: string;
  readonly cls: FailureClass;
  readonly status?: number;
  /** Parsed Retry-After in ms (RATE_LIMITED only). */
  readonly retryAfterMs?: number;

  constructor(
    provider: string,
    cls: FailureClass,
    message: string,
    opts: { status?: number; retryAfterMs?: number; cause?: unknown } = {}
  ) {
    super(message);
    this.name = "ProviderFailure";
    this.provider = provider;
    this.cls = cls;
    this.status = opts.status;
    this.retryAfterMs = opts.retryAfterMs;
    if (opts.cause !== undefined) (this as any).cause = opts.cause;
  }
}

export type FailureClass =
  | "network"
  | "not_found"
  | "forbidden"
  | "rate_limited"
  | "server"
  | "content_miss";

/**
 * HTTP 429 — back off cluster-wide via the Redis cooldown. This is NEVER a
 * circuit failure: a rate-limited provider is healthy but asking us to slow
 * down, so it must not count toward the 5-consecutive-fails circuit.
 */
export class RateLimitedError extends ProviderFailure {
  constructor(provider: string, message: string, retryAfterMs?: number) {
    super(provider, "rate_limited", message, { status: 429, retryAfterMs });
    this.name = "RateLimitedError";
  }
}

/** Map an HTTP status to its failure class. */
export function classifyStatus(status: number): FailureClass {
  if (status === 404) return "not_found";
  if (status === 403) return "forbidden";
  if (status === 429) return "rate_limited";
  if (status >= 500) return "server";
  return "server"; // other 4xx (400/401/402/405/...) — treat as server-side-ish
}

/** Classify a thrown (non-HTTP) error: timeouts/DNS -> network, else server. */
export function classifyUnknown(err: unknown): FailureClass {
  if (err instanceof ProviderFailure) return err.cls;
  if (err instanceof NotAvailableError) return "content_miss";
  const name = (err as any)?.name || "";
  const msg = String((err as any)?.message || err || "").toLowerCase();
  if (
    name === "TimeoutError" ||
    name === "AbortError" ||
    msg.includes("timeout") ||
    msg.includes("timed out") ||
    msg.includes("abort")
  ) {
    return "network";
  }
  // Node fetch TypeErrors: "fetch failed" — dig into the cause.
  const cause = String((err as any)?.cause?.message || (err as any)?.cause?.code || "").toLowerCase();
  const code = String((err as any)?.cause?.code || (err as any)?.code || "").toUpperCase();
  if (
    msg.includes("fetch failed") ||
    cause.includes("enotfound") ||
    cause.includes("econnrefused") ||
    cause.includes("econnreset") ||
    cause.includes("eai_again") ||
    cause.includes("enetunreach") ||
    code === "ENOTFOUND" ||
    code === "ECONNREFUSED" ||
    code === "ECONNRESET" ||
    code === "EAI_AGAIN"
  ) {
    return "network";
  }
  return "server";
}

/** Normalize any caught error into a ProviderFailure (never throws). */
export function toProviderFailure(provider: string, err: unknown): ProviderFailure {
  if (err instanceof ProviderFailure) return err;
  const cls = classifyUnknown(err);
  const msg = err instanceof Error ? err.message : String(err);
  return new ProviderFailure(provider, cls, `${provider}: ${msg}`, { cause: err });
}

/** Parse a Retry-After header (delta-seconds or HTTP date) into ms. */
export function parseRetryAfterMs(headers: Headers): number | undefined {
  const raw = headers.get("retry-after");
  if (!raw) return undefined;
  const secs = parseInt(raw.trim(), 10);
  if (Number.isFinite(secs) && secs >= 0) return secs * 1000;
  const dateMs = Date.parse(raw.trim());
  if (Number.isFinite(dateMs)) {
    const delta = dateMs - Date.now();
    return delta > 0 ? delta : 0;
  }
  return undefined;
}

export interface FetchUpstreamOpts {
  /** Statuses that mean "no such title", not a failure (default: none). */
  contentMissStatuses?: number[];
}

/**
 * fetch() wrapper that throws classified ProviderFailures instead of
 * returning nulls. Network errors -> NETWORK; non-2xx -> classifyStatus()
 * (with contentMissStatuses mapped to CONTENT_MISS); 429 carries
 * retryAfterMs parsed from Retry-After.
 */
export async function fetchUpstream(
  provider: string,
  url: string,
  init: RequestInit = {},
  opts: FetchUpstreamOpts = {}
): Promise<Response> {
  let res: Response;
  try {
    res = await fetch(url, init);
  } catch (err) {
    throw toProviderFailure(provider, err);
  }
  if (res.ok) return res;
  if (opts.contentMissStatuses?.includes(res.status)) {
    throw new ProviderFailure(
      provider,
      "content_miss",
      `${provider}: no such title (http ${res.status})`,
      { status: res.status }
    );
  }
  const cls = classifyStatus(res.status);
  if (cls === "rate_limited") {
    throw new RateLimitedError(
      provider,
      `${provider}: http 429 for ${shortUrl(url)}`,
      parseRetryAfterMs(res.headers)
    );
  }
  throw new ProviderFailure(
    provider,
    cls,
    `${provider}: http ${res.status} for ${shortUrl(url)}`,
    { status: res.status }
  );
}

function shortUrl(url: string): string {
  try {
    const u = new URL(url);
    return u.host + u.pathname.slice(0, 60);
  } catch {
    return url.slice(0, 80);
  }
}
