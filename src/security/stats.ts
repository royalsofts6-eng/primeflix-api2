/**
 * Security stats for /health — no framework dependency.
 * (Moved out of the deleted Hono middleware; the live api/index.ts imports it.)
 */
import { registryStats, blocklistSize } from "./devices.js";
import { rateLimitStats } from "./ratelimit.js";

export function securityStats(): Record<string, unknown> {
  return {
    mode: "hmac-sha256+jwt24h",
    devices: registryStats(),
    blocklist: blocklistSize(),
    rateLimitBuckets: rateLimitStats().buckets,
    memberKeysConfigured: (process.env.MEMBER_KEYS || "").split(",").filter(Boolean).length > 0,
  };
}
