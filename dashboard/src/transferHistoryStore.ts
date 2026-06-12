import type { TransferTick } from "./types";

export type TransferHistoryEntry = {
  id: string;
  ts: number;
  producer: string;
  /** Endereço TCP completo (tooltip). */
  producerAddr?: string;
  message: string;
  consumer: string;
  ok: boolean;
};

const HISTORY_CAP = 500;

export const TRANSFER_HISTORY_LIMIT = HISTORY_CAP;
const entries: TransferHistoryEntry[] = [];
const listeners = new Set<() => void>();
let snapshot: readonly TransferHistoryEntry[] = [];
let seq = 0;

function refreshSnapshot() {
  snapshot = [...entries];
}

function emit() {
  refreshSnapshot();
  for (const fn of listeners) {
    fn();
  }
}

refreshSnapshot();

export function subscribeTransferHistory(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function getTransferHistorySnapshot(): readonly TransferHistoryEntry[] {
  return snapshot;
}

export function appendTransferHistory(
  tick: TransferTick,
  producer: string,
  consumer: string,
  producerAddr?: string,
  emitNow = true,
): void {
  const phase = tick.phase ?? (tick.ok ? "deliver" : "fail");
  if (phase === "deliver" && tick.msg_count <= 0) return;

  entries.unshift({
    id: `${tick.ts_ms}-${++seq}`,
    ts: Date.now(),
    producer,
    producerAddr,
    message: formatTransferMessage(tick),
    consumer,
    ok: tick.ok,
  });
  if (entries.length > HISTORY_CAP) {
    entries.length = HISTORY_CAP;
  }
  if (emitNow) emit();
}

export function flushTransferHistory(): void {
  emit();
}

export function formatTransferMessage(tick: TransferTick): string {
  const phase = tick.phase ?? (tick.ok ? "deliver" : "fail");
  if (phase === "assign") {
    return "ASGN · match";
  }
  const n = tick.msg_count;
  const size = formatBytes(tick.batch_bytes);
  if (!tick.ok) {
    return n > 0 ? `fail · ${n} msg${n === 1 ? "" : "s"}` : "fail";
  }
  return `${n} msg${n === 1 ? "" : "s"} · ${size}`;
}

function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return "0 B";
  if (n < 1024) return `${Math.round(n)} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
}
