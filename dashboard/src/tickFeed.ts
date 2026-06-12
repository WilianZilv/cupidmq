import { normalizeTick } from "./tickNormalize";
import type { TransferTick } from "./types";

/**
 * Fetches `/metrics/ticks` — normalizes and returns the batch.
 * No playback, routes, or history: read-only.
 */
export async function fetchTickBatch(
  ticksUrl: string,
  signal?: AbortSignal,
): Promise<TransferTick[]> {
  const res = await fetch(ticksUrl, { signal });
  if (!res.ok) throw new Error(`ticks HTTP ${res.status}`);
  const body = (await res.json()) as { ticks?: unknown[] };
  if (!Array.isArray(body.ticks)) return [];
  return body.ticks.map(normalizeTick);
}
