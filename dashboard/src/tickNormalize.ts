import type { TransferPhase, TransferTick } from "./types";

export function normalizeTick(raw: unknown): TransferTick {
  const t = (raw ?? {}) as Record<string, unknown>;
  const num = (v: unknown, fb = 0) => {
    const n = typeof v === "number" ? v : Number(v);
    return Number.isFinite(n) ? n : fb;
  };
  const phaseRaw = typeof t.phase === "string" ? t.phase : undefined;
  const phase =
    phaseRaw === "assign" || phaseRaw === "deliver" || phaseRaw === "fail"
      ? (phaseRaw as TransferPhase)
      : undefined;
  return {
    ts_ms: num(t.ts_ms),
    producer_id: num(t.producer_id),
    consumer_id: num(t.consumer_id),
    msg_count: num(t.msg_count),
    batch_bytes: num(t.batch_bytes),
    phase,
    ok: t.ok !== false,
    producer_state:
      typeof t.producer_state === "string" ? t.producer_state : undefined,
    consumer_state:
      typeof t.consumer_state === "string" ? t.consumer_state : undefined,
  };
}
