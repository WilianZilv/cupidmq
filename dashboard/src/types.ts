export type RangeKey = "1m" | "10m" | "1h";

export const RANGE_MS: Record<RangeKey, number> = {
  "1m": 60_000,
  "10m": 600_000,
  "1h": 3_600_000,
};

export interface ProducerRow {
  id: number;
  addr: string;
  state: string;
  pending_messages: number;
  pending_bytes: number;
  ring_messages: number;
  ring_bytes: number;
  batches_per_sec: number;
  msgs_per_sec: number;
  errors_per_sec: number;
  drops_per_sec: number;
  connected_secs: number;
  /** In-flight ASGN — consumer awaiting BATC. */
  assigned_consumer_id?: number | null;
}

export type TransferPhase = "assign" | "deliver" | "fail";

export interface TransferTick {
  ts_ms: number;
  producer_id: number;
  consumer_id: number;
  msg_count: number;
  batch_bytes: number;
  phase?: TransferPhase;
  ok: boolean;
  producer_state?: string;
  consumer_state?: string;
}

export interface ConsumerRow {
  id: number;
  consumer_tag: string;
  addr: string;
  data_addr: string;
  state: string;
  batches_per_sec: number;
  bytes_per_sec: number;
  unacked: number;
  last_batch_items: number;
  last_batch_bytes: number;
  connected_secs: number;
  capacity_pct: number;
  ready_cycle_ms: number;
  max_batch_requested: number;
  assigned_producer_id?: number | null;
}

export interface HistoryPoint {
  ts: number;
  producerBacklog: number;
  producerPending: number;
  producerRing: number;
  consumerInflight: number;
  transferPerSec: number;
  batchesPerSec: number;
  bytesPerSec: number;
  /** bytes/s ÷ batch/s over the point interval. */
  avgBatchBytes: number;
  deliveryFailuresPerSec: number;
  dropsPerSec: number;
  dropsTotal: number;
  producersReady: number;
  producersBusy: number;
  consumersWaiting: number;
}

export interface MetricsSnapshot {
  ts_ms: number;
  uptime_secs: number;
  transfers_total: number;
  transfer_bytes_total: number;
  batches_total: number;
  delivery_failures_total: number;
  requeue_total: number;
  producer_drops_total: number;
  producer_backlog_messages: number;
  producer_backlog_bytes: number;
  producer_pending_messages: number;
  producer_ring_messages: number;
  consumer_inflight: number;
  transfer_per_sec: number;
  batches_per_sec: number;
  bytes_per_sec: number;
  producers_connected: number;
  consumers_connected: number;
  consumers_waiting: number;
  consumers_await_batch: number;
  producers_ready: number;
  producers_busy: number;
  consumers_queued: number;
  /** Σμ/λ — global pool capacity (may differ from per-consumer average). */
  consumer_capacity_pct: number;
  producers: ProducerRow[];
  consumers: ConsumerRow[];
  ticks_buffered: number;
}

export type HistorySeriesKey = keyof Pick<
  HistoryPoint,
  | "producerBacklog"
  | "producerPending"
  | "producerRing"
  | "consumerInflight"
  | "transferPerSec"
  | "batchesPerSec"
  | "bytesPerSec"
  | "avgBatchBytes"
  | "deliveryFailuresPerSec"
  | "dropsPerSec"
  | "producersReady"
  | "producersBusy"
  | "consumersWaiting"
>;

export interface SeriesDef {
  key: HistorySeriesKey;
  label: string;
  color: string;
  tip: string;
  axis?: "primary" | "secondary";
  legendHidden?: boolean;
  /** Paired instantaneous value in the legend (e.g. bitrate or average batch size). */
  pairedKey?: HistorySeriesKey;
  pairedFormat?: "bitrate" | "bytes";
  pairedLabel?: string;
  /** Outline-only dots in the match pool chart. */
  hollow?: boolean;
}
