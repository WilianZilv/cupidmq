import type { HistoryPoint } from "./types";

export interface ServerHistoryPoint {
  ts: number;
  producer_backlog: number;
  producer_pending: number;
  producer_ring: number;
  consumer_inflight: number;
  transfer_per_sec: number;
  batches_per_sec: number;
  bytes_per_sec: number;
  delivery_failures_per_sec: number;
  drops_per_sec: number;
  drops_total: number;
  producers_ready: number;
  producers_busy: number;
  consumers_waiting: number;
}

export interface HistoryResponse {
  interval_ms: number;
  points: ServerHistoryPoint[];
}

export function avgBatchBytesFromRates(
  bytesPerSec: number,
  batchesPerSec: number,
): number {
  return batchesPerSec > 0 ? bytesPerSec / batchesPerSec : 0;
}

export function mapServerHistoryPoint(p: ServerHistoryPoint): HistoryPoint {
  return {
    ts: p.ts,
    producerBacklog: p.producer_backlog,
    producerPending: p.producer_pending,
    producerRing: p.producer_ring,
    consumerInflight: p.consumer_inflight,
    transferPerSec: p.transfer_per_sec,
    batchesPerSec: p.batches_per_sec,
    bytesPerSec: p.bytes_per_sec,
    avgBatchBytes: avgBatchBytesFromRates(
      p.bytes_per_sec,
      p.batches_per_sec,
    ),
    deliveryFailuresPerSec:
      p.delivery_failures_per_sec ?? 0,
    dropsPerSec: p.drops_per_sec ?? 0,
    dropsTotal: p.drops_total,
    producersReady: p.producers_ready,
    producersBusy: p.producers_busy,
    consumersWaiting: p.consumers_waiting,
  };
}

export function metricsBaseUrl(metricsUrl: string): string {
  return metricsUrl.replace(/\/metrics\/?$/, "");
}
