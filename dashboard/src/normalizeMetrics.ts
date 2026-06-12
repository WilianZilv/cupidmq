import type { ConsumerRow, MetricsSnapshot, ProducerRow } from "./types";

export function normalizeMetrics(raw: Record<string, unknown>): MetricsSnapshot {
  const producersRaw = raw.producers;
  const consumersRaw = raw.consumers;

  return {
    ts_ms: num(raw.ts_ms),
    uptime_secs: num(raw.uptime_secs),
    transfers_total: num(raw.transfers_total),
    transfer_bytes_total: num(raw.transfer_bytes_total),
    batches_total: num(raw.batches_total),
    delivery_failures_total: num(raw.delivery_failures_total),
    requeue_total: num(raw.requeue_total),
    producer_drops_total: num(raw.producer_drops_total),
    producer_backlog_messages: num(raw.producer_backlog_messages),
    producer_backlog_bytes: num(raw.producer_backlog_bytes),
    producer_pending_messages: num(raw.producer_pending_messages),
    producer_ring_messages: num(raw.producer_ring_messages),
    consumer_inflight: num(raw.consumer_inflight),
    transfer_per_sec: num(raw.transfer_per_sec),
    batches_per_sec: num(raw.batches_per_sec),
    bytes_per_sec: num(raw.bytes_per_sec),
    producers_connected: num(raw.producers_connected),
    consumers_connected: num(raw.consumers_connected),
    consumers_waiting: num(raw.consumers_waiting),
    consumers_await_batch: num(raw.consumers_await_batch),
    producers_ready: num(raw.producers_ready),
    producers_busy: num(raw.producers_busy),
    producers: Array.isArray(producersRaw)
      ? producersRaw.map((p, i) => normalizeProducer(p, i))
      : [],
    consumers: Array.isArray(consumersRaw)
      ? consumersRaw.map((c, i) => normalizeConsumer(c, i))
      : [],
    ticks_buffered: num(raw.ticks_buffered),
    consumers_queued: num(raw.consumers_queued),
    consumer_capacity_pct: num(raw.consumer_capacity_pct, 100),
  };
}

/** Arithmetic mean of capacity_pct in the table (header summary). */
export function avgConsumerCapacityPct(consumers: ConsumerRow[]): number | null {
  if (consumers.length === 0) return null;
  const sum = consumers.reduce((s, c) => s + c.capacity_pct, 0);
  return sum / consumers.length;
}

function normalizeProducer(raw: unknown, index: number): ProducerRow {
  const p = (raw ?? {}) as Record<string, unknown>;
  return {
    id: num(p.id, index + 1),
    addr: str(p.addr, "—"),
    state: str(p.state, "unknown"),
    pending_messages: num(p.pending_messages),
    pending_bytes: num(p.pending_bytes),
    ring_messages: num(p.ring_messages),
    ring_bytes: num(p.ring_bytes),
    batches_per_sec: num(p.batches_per_sec),
    msgs_per_sec: num(p.msgs_per_sec),
    errors_per_sec: num(p.errors_per_sec),
    drops_per_sec: num(p.drops_per_sec),
    connected_secs: num(p.connected_secs),
    assigned_consumer_id:
      p.assigned_consumer_id == null ? null : num(p.assigned_consumer_id),
  };
}

function normalizeConsumer(raw: unknown, index: number): ConsumerRow {
  const c = (raw ?? {}) as Record<string, unknown>;
  return {
    id: num(c.id, index + 1),
    consumer_tag: str(c.consumer_tag, `consumer-${index + 1}`),
    addr: str(c.addr, "—"),
    data_addr: str(c.data_addr, "—"),
    state: str(c.state, "unknown"),
    batches_per_sec: num(c.batches_per_sec),
    bytes_per_sec: num(c.bytes_per_sec),
    unacked: num(c.unacked),
    last_batch_items: num(c.last_batch_items),
    last_batch_bytes: num(c.last_batch_bytes),
    connected_secs: num(c.connected_secs),
    capacity_pct: num(c.capacity_pct, 100),
    ready_cycle_ms: num(c.ready_cycle_ms),
    max_batch_requested: num(c.max_batch_requested, 32),
    assigned_producer_id:
      c.assigned_producer_id == null ? null : num(c.assigned_producer_id),
  };
}

function num(v: unknown, fallback = 0): number {
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function str(v: unknown, fallback: string): string {
  return typeof v === "string" && v.length > 0 ? v : fallback;
}

export function producersListStale(
  raw: Record<string, unknown>,
  m: MetricsSnapshot,
): boolean {
  return m.producers_connected > 0 && !Array.isArray(raw.producers);
}

export function consumersListStale(
  raw: Record<string, unknown>,
  m: MetricsSnapshot,
): boolean {
  return m.consumers_connected > 0 && !Array.isArray(raw.consumers);
}
