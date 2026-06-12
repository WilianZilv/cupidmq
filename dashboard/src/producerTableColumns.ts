import type { ReactNode } from "react";
import type { ProducerRow } from "./types";
import { PRODUCER_COLUMN_TIPS } from "./metricsHelp";

export type ProducerColumnKey =
  | "id"
  | "addr"
  | "state"
  | "pending_messages"
  | "pending_bytes"
  | "ring_messages"
  | "ring_bytes"
  | "backlog_messages"
  | "backlog_bytes"
  | "drops_per_sec"
  | "batches_per_sec"
  | "msgs_per_sec"
  | "errors_per_sec"
  | "connected_secs";

export const PRODUCER_COLUMNS_STORAGE_KEY = "cupidmq-producer-table-columns-v2";

export interface ProducerColumnDef {
  key: ProducerColumnKey;
  label: string;
  defaultVisible: boolean;
  numeric?: boolean;
  mono?: boolean;
  tip: string;
}

export const PRODUCER_COLUMN_DEFS: ProducerColumnDef[] = [
  { key: "id", label: "#", defaultVisible: false, numeric: true, tip: PRODUCER_COLUMN_TIPS.id },
  { key: "addr", label: "Address", defaultVisible: true, mono: true, tip: PRODUCER_COLUMN_TIPS.addr },
  { key: "state", label: "State", defaultVisible: true, tip: PRODUCER_COLUMN_TIPS.state },
  { key: "pending_messages", label: "Pending msgs", defaultVisible: true, numeric: true, tip: PRODUCER_COLUMN_TIPS.pending_messages },
  { key: "ring_messages", label: "Ring msgs", defaultVisible: true, numeric: true, tip: PRODUCER_COLUMN_TIPS.ring_messages },
  { key: "backlog_messages", label: "Backlog msgs", defaultVisible: false, numeric: true, tip: PRODUCER_COLUMN_TIPS.backlog_messages },
  { key: "pending_bytes", label: "Pending", defaultVisible: false, numeric: true, tip: PRODUCER_COLUMN_TIPS.pending_bytes },
  { key: "ring_bytes", label: "Ring", defaultVisible: false, numeric: true, tip: PRODUCER_COLUMN_TIPS.ring_bytes },
  { key: "backlog_bytes", label: "Backlog", defaultVisible: true, numeric: true, tip: PRODUCER_COLUMN_TIPS.backlog_bytes },
  { key: "drops_per_sec", label: "Drops/s", defaultVisible: true, numeric: true, tip: PRODUCER_COLUMN_TIPS.drops_per_sec },
  { key: "batches_per_sec", label: "Batches/s", defaultVisible: true, numeric: true, tip: PRODUCER_COLUMN_TIPS.batches_per_sec },
  { key: "msgs_per_sec", label: "Msgs/s", defaultVisible: true, numeric: true, tip: PRODUCER_COLUMN_TIPS.msgs_per_sec },
  { key: "errors_per_sec", label: "FAIL/s", defaultVisible: true, numeric: true, tip: PRODUCER_COLUMN_TIPS.errors_per_sec },
  { key: "connected_secs", label: "Connected", defaultVisible: true, numeric: true, tip: PRODUCER_COLUMN_TIPS.connected_secs },
];

const ALL_KEYS = new Set(PRODUCER_COLUMN_DEFS.map((c) => c.key));

export function defaultVisibleProducerColumnKeys(): ProducerColumnKey[] {
  return PRODUCER_COLUMN_DEFS.filter((c) => c.defaultVisible).map((c) => c.key);
}

export function loadVisibleProducerColumnKeys(): ProducerColumnKey[] {
  try {
    const raw = localStorage.getItem(PRODUCER_COLUMNS_STORAGE_KEY);
    if (!raw) return defaultVisibleProducerColumnKeys();
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return defaultVisibleProducerColumnKeys();
    const keys = parsed.filter(
      (k): k is ProducerColumnKey =>
        typeof k === "string" && ALL_KEYS.has(k as ProducerColumnKey),
    );
    return keys.length > 0 ? keys : defaultVisibleProducerColumnKeys();
  } catch {
    return defaultVisibleProducerColumnKeys();
  }
}

export function saveVisibleProducerColumnKeys(keys: ProducerColumnKey[]): void {
  try {
    localStorage.setItem(PRODUCER_COLUMNS_STORAGE_KEY, JSON.stringify(keys));
  } catch {
    /* ignore */
  }
}

export interface ProducerCellContext {
  renderState: (state: string) => ReactNode;
  formatRate: (n: number) => string;
  formatBytes: (n: number) => string;
  formatDuration: (secs: number) => string;
}

export function renderProducerCell(
  key: ProducerColumnKey,
  p: ProducerRow,
  ctx: ProducerCellContext,
): ReactNode {
  switch (key) {
    case "id":
      return p.id;
    case "addr":
      return p.addr;
    case "state":
      return ctx.renderState(p.state);
    case "pending_messages":
      return p.pending_messages.toLocaleString("en-US");
    case "ring_messages":
      return p.ring_messages.toLocaleString("en-US");
    case "backlog_messages":
      return (p.pending_messages + p.ring_messages).toLocaleString("en-US");
    case "pending_bytes":
      return ctx.formatBytes(p.pending_bytes);
    case "ring_bytes":
      return ctx.formatBytes(p.ring_bytes);
    case "backlog_bytes":
      return ctx.formatBytes(p.pending_bytes + p.ring_bytes);
    case "drops_per_sec":
      return ctx.formatRate(p.drops_per_sec);
    case "batches_per_sec":
      return ctx.formatRate(p.batches_per_sec);
    case "msgs_per_sec":
      return ctx.formatRate(p.msgs_per_sec);
    case "errors_per_sec":
      return ctx.formatRate(p.errors_per_sec);
    case "connected_secs":
      return ctx.formatDuration(p.connected_secs);
    default:
      return "—";
  }
}
