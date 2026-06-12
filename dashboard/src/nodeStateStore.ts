import type { TransferTick } from "./types";
import { consumerNodeId, producerNodeId } from "./flowGraphModel";
export type NodeStateOverlay = {
  state: string;
  lastTs: number;
  /** Master matched — purple ring on nodes. */
  assign?: boolean;
};

/** Post-deliver state until the next tick on the node. */
export const NODE_STATE_OVERLAY_MS = 2500;

function overlayTtl(_overlay: NodeStateOverlay): number {
  return NODE_STATE_OVERLAY_MS;
}

const overlays = new Map<string, NodeStateOverlay>();
const listeners = new Set<() => void>();
let snapshot: ReadonlyMap<string, NodeStateOverlay> = new Map();

function refreshSnapshot() {
  snapshot = new Map(overlays);
}

function emit() {
  refreshSnapshot();
  for (const fn of listeners) {
    fn();
  }
}

refreshSnapshot();

export function subscribeNodeStates(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function getNodeStateSnapshot(): ReadonlyMap<string, NodeStateOverlay> {
  return snapshot;
}

function applyTickOverlay(tick: TransferTick, ts: number): void {
  const phase = tick.phase ?? (tick.ok ? "deliver" : "fail");
  const isAssign = phase === "assign";

  if (isAssign) {
    overlays.set(producerNodeId(tick.producer_id), {
      state: "busy",
      lastTs: ts,
      assign: true,
    });
    overlays.set(consumerNodeId(tick.consumer_id), {
      state: "awaiting_batch",
      lastTs: ts,
      assign: true,
    });
    return;
  }

  const producerState = tick.producer_state?.trim();
  const consumerState = tick.consumer_state?.trim();

  if (producerState) {
    overlays.set(producerNodeId(tick.producer_id), {
      state: producerState,
      lastTs: ts,
    });
  } else if (tick.ok) {
    overlays.set(producerNodeId(tick.producer_id), {
      state: "busy",
      lastTs: ts,
    });
  }

  if (consumerState) {
    overlays.set(consumerNodeId(tick.consumer_id), {
      state: consumerState,
      lastTs: ts,
    });
  } else {
    overlays.set(consumerNodeId(tick.consumer_id), {
      state: tick.ok ? "processing" : "delivery_failed",
      lastTs: ts,
    });
  }
}

export function markNodeStatesFromTick(tick: TransferTick): void {
  overlays.clear();
  applyTickOverlay(tick, Date.now());
  emit();
}

/** Accumulates overlays — assign on node until deliver/fail of the same tick. */
export function mergeNodeStatesFromTicks(ticks: TransferTick[]): void {
  if (ticks.length === 0) return;
  const ts = Date.now();
  for (const tick of ticks) {
    applyTickOverlay(tick, ts);
  }
  emit();
}

export function clearNodeStateOverlays(): void {
  if (overlays.size === 0) return;
  overlays.clear();
  emit();
}

/** Node state in the flow — tick overlay only; no metrics fallback. */
export function resolveNodeState(
  nodeId: string,
  overlayMap: ReadonlyMap<string, NodeStateOverlay>,
  idleState: string,
  now = Date.now(),
): string {
  const overlay = overlayMap.get(nodeId);
  if (overlay && now - overlay.lastTs < overlayTtl(overlay)) {
    return overlay.assign ? `${overlay.state} assign` : overlay.state;
  }
  return idleState;
}

export function hasActiveNodeOverlays(now = Date.now()): boolean {
  for (const o of overlays.values()) {
    if (now - o.lastTs < overlayTtl(o)) return true;
  }
  return false;
}
