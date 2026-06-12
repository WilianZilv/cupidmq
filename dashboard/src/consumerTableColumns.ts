import type { ReactNode } from "react";
import type { ConsumerRow } from "./types";
import { COLUMN_TIPS } from "./metricsHelp";

export type ConsumerColumnKey =
  | "id"
  | "consumer_tag"
  | "addr"
  | "state"
  | "capacity"
  | "ready_cycle_ms"
  | "max_batch_requested"
  | "unacked"
  | "batches_per_sec"
  | "bitrate"
  | "last_batch_items"
  | "last_batch_bytes"
  | "connected_secs";

export const CONSUMER_COLUMNS_STORAGE_KEY = "cupidmq-client-table-columns-v3";

export interface ConsumerColumnDef {
  key: ConsumerColumnKey;
  label: string;
  defaultVisible: boolean;
  numeric?: boolean;
  mono?: boolean;
  tip: string;
}

export const CONSUMER_COLUMN_DEFS: ConsumerColumnDef[] = [
  { key: "consumer_tag", label: "Consumer tag", defaultVisible: true, mono: true, tip: COLUMN_TIPS.consumer_tag },
  { key: "capacity", label: "Capacity", defaultVisible: true, numeric: true, tip: COLUMN_TIPS.capacity },
  { key: "last_batch_items", label: "Last items", defaultVisible: true, numeric: true, tip: COLUMN_TIPS.last_batch_items },
  { key: "max_batch_requested", label: "Batch max", defaultVisible: true, numeric: true, tip: COLUMN_TIPS.max_batch_requested },
  { key: "unacked", label: "Unacked", defaultVisible: true, numeric: true, tip: COLUMN_TIPS.unacked },
  { key: "batches_per_sec", label: "Batches/s", defaultVisible: true, numeric: true, tip: COLUMN_TIPS.batches_per_sec },
  { key: "bitrate", label: "Bitrate", defaultVisible: true, numeric: true, tip: COLUMN_TIPS.bitrate },
  { key: "connected_secs", label: "Connected", defaultVisible: true, numeric: true, tip: COLUMN_TIPS.connected_secs },
  { key: "id", label: "#", defaultVisible: false, numeric: true, tip: COLUMN_TIPS.id },
  { key: "addr", label: "Address", defaultVisible: false, mono: true, tip: COLUMN_TIPS.addr },
  { key: "state", label: "State", defaultVisible: false, tip: COLUMN_TIPS.state },
  { key: "ready_cycle_ms", label: "Ready ms", defaultVisible: false, numeric: true, tip: COLUMN_TIPS.ready_cycle_ms },
  { key: "last_batch_bytes", label: "Last batch", defaultVisible: false, numeric: true, tip: COLUMN_TIPS.last_batch_bytes },
];

const ALL_KEYS = new Set(CONSUMER_COLUMN_DEFS.map((c) => c.key));

export function defaultVisibleColumnKeys(): ConsumerColumnKey[] {
  return CONSUMER_COLUMN_DEFS.filter((c) => c.defaultVisible).map((c) => c.key);
}

export function loadVisibleColumnKeys(): ConsumerColumnKey[] {
  try {
    const raw = localStorage.getItem(CONSUMER_COLUMNS_STORAGE_KEY);
    if (!raw) return defaultVisibleColumnKeys();
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return defaultVisibleColumnKeys();
    const keys = parsed.filter(
      (k): k is ConsumerColumnKey =>
        typeof k === "string" && ALL_KEYS.has(k as ConsumerColumnKey),
    );
    return keys.length > 0 ? keys : defaultVisibleColumnKeys();
  } catch {
    return defaultVisibleColumnKeys();
  }
}

export function saveVisibleColumnKeys(keys: ConsumerColumnKey[]): void {
  try {
    localStorage.setItem(CONSUMER_COLUMNS_STORAGE_KEY, JSON.stringify(keys));
  } catch {
    /* ignore quota / private mode */
  }
}

export interface ConsumerCellContext {
  renderCapacity: (pct: number) => ReactNode;
  renderState: (state: string) => ReactNode;
  formatBatchRate: (n: number) => string;
  formatBitrate: (bytesPerSec: number) => string;
  formatBytes: (n: number) => string;
  formatDuration: (secs: number) => string;
  formatReadyMs: (ms: number) => string;
}

export function renderConsumerCell(
  key: ConsumerColumnKey,
  c: ConsumerRow,
  ctx: ConsumerCellContext,
): ReactNode {
  switch (key) {
    case "id":
      return c.id;
    case "consumer_tag":
      return c.consumer_tag;
    case "addr":
      return c.addr;
    case "state":
      return ctx.renderState(c.state);
    case "capacity":
      return ctx.renderCapacity(c.capacity_pct);
    case "ready_cycle_ms":
      return ctx.formatReadyMs(c.ready_cycle_ms);
    case "max_batch_requested":
      return c.max_batch_requested;
    case "unacked":
      return c.unacked.toLocaleString("en-US");
    case "batches_per_sec":
      return ctx.formatBatchRate(c.batches_per_sec);
    case "bitrate":
      return ctx.formatBitrate(c.bytes_per_sec);
    case "last_batch_items":
      return c.last_batch_items.toLocaleString("en-US");
    case "last_batch_bytes":
      return ctx.formatBytes(c.last_batch_bytes);
    case "connected_secs":
      return ctx.formatDuration(c.connected_secs);
    default:
      return "—";
  }
}
