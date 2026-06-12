import type { HistoryPoint, RangeKey } from "./types";
import { RANGE_MS } from "./types";

export function filterHistoryRange(
  history: HistoryPoint[],
  range: RangeKey,
): HistoryPoint[] {
  const cutoff = Date.now() - RANGE_MS[range];
  const filtered = history.filter((p) => p.ts >= cutoff);
  return filtered.length > 0 ? filtered : history.slice(-1);
}

/** Drops no intervalo (delta de drops_total nos producers). */
export function dropsInHistoryRange(
  history: HistoryPoint[],
  range: RangeKey,
): number {
  const points = filterHistoryRange(history, range);
  if (points.length === 0) return 0;

  if (points.length >= 2) {
    const first = points[0]!.dropsTotal;
    const last = points[points.length - 1]!.dropsTotal;
    if (last >= first) {
      return Math.round(last - first);
    }
  }

  return 0;
}
