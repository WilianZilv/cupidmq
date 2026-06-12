import type { MetricsSnapshot } from "./types";

let cached: MetricsSnapshot | null = null;

/** Topology cache — never flash empty between polls. */
export function getDisplayTopology(metrics: MetricsSnapshot): MetricsSnapshot {
  if (metrics.producers.length > 0 || metrics.consumers.length > 0) {
    cached = metrics;
    return metrics;
  }
  return cached ?? metrics;
}

export function hasTopology(metrics: MetricsSnapshot): boolean {
  const m = getDisplayTopology(metrics);
  return m.producers.length > 0 || m.consumers.length > 0;
}
