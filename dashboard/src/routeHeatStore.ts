import type { TransferPhase, TransferTick } from "./types";
import { ROUTE_VISIBLE_MS } from "./tickPlayback";

export const TRANSFER_ACCENT = "#4db8d8";
export const TRANSFER_FAIL = "#e07070";
export const TRANSFER_ASSIGN = "#c084fc";

export { ROUTE_VISIBLE_MS };

export type RoutePhase = TransferPhase;

export type ActiveRouteTick = {
  producerKey: string;
  consumerKey: string;
  phase: RoutePhase;
  ok: boolean;
  msgCount: number;
  batchBytes: number;
  tickTsMs: number;
  shownAtMs: number;
  visibleUntil: number;
  strokeWidth: number;
};

export function stableRouteKey(producerKey: string, consumerKey: string) {
  return `${producerKey}|${consumerKey}`;
}

function tickPhase(tick: TransferTick): RoutePhase {
  if (tick.phase === "assign" || tick.phase === "deliver" || tick.phase === "fail") {
    return tick.phase;
  }
  return tick.ok ? "deliver" : "fail";
}

const activeRoutes = new Map<string, ActiveRouteTick>();
let routesSnapshot: readonly ActiveRouteTick[] = [];
const listeners = new Set<() => void>();
let ticksSeen = 0;
let liveMode = false;

function refreshSnapshot() {
  routesSnapshot = [...activeRoutes.values()];
}

function emit() {
  refreshSnapshot();
  for (const fn of listeners) {
    fn();
  }
}

export function subscribeRouteHeat(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function isLiveMode(): boolean {
  return liveMode;
}

export function setLiveMode(on: boolean): void {
  if (liveMode === on) return;
  liveMode = on;
  emit();
}

export function getRouteHeatSnapshot(): ActiveRouteTick | null {
  const now = Date.now();
  for (let i = routesSnapshot.length - 1; i >= 0; i -= 1) {
    const r = routesSnapshot[i]!;
    if (now < r.visibleUntil) return r;
  }
  return null;
}

export function getActiveRoutesSnapshot(now = Date.now()): readonly ActiveRouteTick[] {
  return routesSnapshot.filter((r) => now < r.visibleUntil);
}

/** Opacity 1→0 linear over [shownAtMs, visibleUntil]. */
export function routeFadeAlpha(route: ActiveRouteTick, now: number): number {
  if (now >= route.visibleUntil) return 0;
  const span = route.visibleUntil - route.shownAtMs;
  if (span <= 0) return 0;
  const remaining = route.visibleUntil - now;
  return Math.max(0, Math.min(1, remaining / span));
}

export function getTicksSeen(): number {
  return ticksSeen;
}

function makeRoute(
  tick: TransferTick,
  producerKey: string,
  consumerKey: string,
): ActiveRouteTick {
  const phase = tickPhase(tick);
  const now = Date.now();

  return {
    producerKey,
    consumerKey,
    phase,
    ok: tick.ok,
    msgCount: tick.msg_count,
    batchBytes: tick.batch_bytes,
    tickTsMs: tick.ts_ms || now,
    shownAtMs: now,
    visibleUntil: now + ROUTE_VISIBLE_MS,
    strokeWidth:
      phase === "assign"
        ? 2
        : Math.min(4, 1.8 + Math.log10(tick.batch_bytes + 1) * 0.25),
  };
}

export function applyRouteTicks(
  ticks: TransferTick[],
  resolveKeys: (tick: TransferTick) => {
    producerKey: string;
    consumerKey: string;
  },
): void {
  if (ticks.length === 0) return;
  setLiveMode(false);

  for (const tick of ticks) {
    const { producerKey, consumerKey } = resolveKeys(tick);
    activeRoutes.set(
      stableRouteKey(producerKey, consumerKey),
      makeRoute(tick, producerKey, consumerKey),
    );
    ticksSeen += 1;
  }
  pruneExpiredRoutes();
  emit();
}

/** Prune without notifying listeners — canvas rAF hot path. */
export function pruneExpiredRoutesQuiet(now = Date.now()): boolean {
  const before = activeRoutes.size;
  for (const [key, route] of activeRoutes) {
    if (now >= route.visibleUntil) activeRoutes.delete(key);
  }
  if (activeRoutes.size !== before) {
    refreshSnapshot();
    return true;
  }
  return false;
}

export function pruneExpiredRoutes(now = Date.now()): boolean {
  const before = activeRoutes.size;
  for (const [key, route] of activeRoutes) {
    if (now >= route.visibleUntil) activeRoutes.delete(key);
  }
  if (activeRoutes.size !== before) {
    emit();
    return true;
  }
  return false;
}

export function hasActiveRoute(now = Date.now()): boolean {
  for (const r of activeRoutes.values()) {
    if (now < r.visibleUntil) return true;
  }
  return false;
}

export function clearActiveRoutes(): void {
  if (activeRoutes.size === 0 && !liveMode) return;
  activeRoutes.clear();
  setLiveMode(false);
  emit();
}

export const HOT_MS = ROUTE_VISIBLE_MS;
