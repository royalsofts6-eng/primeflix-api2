declare module "@vercel/functions" {
  export function waitUntil(promise: Promise<unknown>): void;
}
