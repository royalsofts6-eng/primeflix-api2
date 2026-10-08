/**
 * Minimal API-key auth middleware.
 * Phase 1: X-API-Key header must match API_KEY env var.
 * Phase 2: replaced/augmented by HMAC + JWT (see final plan).
 *
 * Public paths (no auth): / , /health
 */
import type { Context, Next } from "hono";

const PUBLIC = new Set(["/", "/health"]);

export async function apiKeyAuth(c: Context, next: Next): Promise<Response | void> {
  const path = new URL(c.req.url).pathname;
  if (PUBLIC.has(path)) return next();

  const expected = process.env.API_KEY;
  if (!expected) {
    // Fail closed: if no key configured, deny everything (except public).
    return c.json({ success: false, error: "server misconfigured", code: "NO_API_KEY" }, 500);
  }
  const got = c.req.header("X-API-Key") || c.req.query("api_key");
  if (got !== expected) {
    return c.json({ success: false, error: "unauthorized", code: "BAD_API_KEY" }, 401);
  }
  return next();
}
