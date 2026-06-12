import type { TransferTick } from "./types";

export const BUCKET_MS = 100;
export const ROUTE_VISIBLE_MS = 240;

function phaseOrder(tick: TransferTick): number {
  if (tick.phase === "assign") return 0;
  if (tick.phase === "deliver") return 1;
  return 2;
}

function sortTicksForPlayback(ticks: TransferTick[]): TransferTick[] {
  return [...ticks].sort((a, b) => {
    if (a.ts_ms !== b.ts_ms) return a.ts_ms - b.ts_ms;
    return phaseOrder(a) - phaseOrder(b);
  });
}

export function playbackBucketCount(
  playMs: number,
  bucketMs = BUCKET_MS,
): number {
  return Math.max(1, Math.round(playMs / bucketMs));
}

export function buildBucketSnapshots(
  ticks: TransferTick[],
  playMs: number,
  bucketMs = BUCKET_MS,
): TransferTick[][] {
  const sorted = sortTicksForPlayback(ticks);
  const bucketCount = playbackBucketCount(playMs, bucketMs);
  const maxBucketIdx = bucketCount - 1;
  const raw: TransferTick[][] = Array.from({ length: bucketCount }, () => []);

  if (sorted.length === 0) return raw;

  const n = sorted.length;
  for (let i = 0; i < n; i += 1) {
    const idx =
      n === 1
        ? 0
        : Math.min(
            maxBucketIdx,
            Math.max(0, Math.round((i / (n - 1)) * maxBucketIdx)),
          );
    raw[idx]!.push(sorted[i]!);
  }

  return raw;
}

export type TickPlaybackHandlers = {
  onBucket: (ticks: TransferTick[], bucketIdx: number) => void;
};

let windowMs = 2000;
let buffer: TransferTick[] = [];
let bucketsCache: TransferTick[][] = [];
let bucketCount = 0;

let clockEpoch = 0;
let lastBucketIdx = -1;
let bucketTimer = 0;
let registeredHandlers: TickPlaybackHandlers | null = null;

const pendingLots: TransferTick[][] = [];

let lastLotStats = {
  totalTicks: 0,
  bucketCount: 0,
  queueDepth: 0,
};

const statsListeners = new Set<() => void>();

function emitStats(): void {
  for (const fn of statsListeners) {
    fn();
  }
}

function refreshStats(): void {
  const next = {
    totalTicks: buffer.length,
    bucketCount,
    queueDepth: buffer.length,
  };
  if (
    next.totalTicks === lastLotStats.totalTicks &&
    next.bucketCount === lastLotStats.bucketCount &&
    next.queueDepth === lastLotStats.queueDepth
  ) {
    return;
  }
  lastLotStats = next;
  emitStats();
}

export function subscribePlaybackStats(listener: () => void): () => void {
  statsListeners.add(listener);
  return () => statsListeners.delete(listener);
}

export function getPlaybackStats(): typeof lastLotStats {
  return lastLotStats;
}

function mergeBuffer(incoming: TransferTick[]): void {
  if (incoming.length === 0) return;
  buffer = sortTicksForPlayback([...buffer, ...incoming]);
  const newest = buffer[buffer.length - 1]!.ts_ms;
  const cutoff = newest > windowMs ? newest - windowMs : 0;
  buffer = buffer.filter((t) => t.ts_ms >= cutoff);
}

function rebuildBuckets(): void {
  bucketCount = playbackBucketCount(windowMs, BUCKET_MS);
  bucketsCache = buildBucketSnapshots(buffer, windowMs, BUCKET_MS);
  refreshStats();
}

function fireBucket(idx: number): void {
  if (!registeredHandlers) return;
  const ticks = bucketsCache[idx] ?? [];
  registeredHandlers.onBucket(ticks, idx);
}

function fireBucketGroup(indices: number[]): void {
  if (!registeredHandlers || indices.length === 0) return;
  if (indices.length === 1) {
    fireBucket(indices[0]!);
    return;
  }
  const ticks: TransferTick[] = [];
  for (const idx of indices) {
    ticks.push(...(bucketsCache[idx] ?? []));
  }
  registeredHandlers.onBucket(ticks, indices[indices.length - 1]!);
}

function advanceBucketClock(now: number): void {
  if (!registeredHandlers || bucketCount <= 0) return;

  const elapsed = now - clockEpoch;
  const idx = Math.min(
    bucketCount - 1,
    Math.floor((elapsed % windowMs) / BUCKET_MS),
  );

  if (lastBucketIdx < 0) {
    lastBucketIdx = idx;
    fireBucket(idx);
    return;
  }

  if (idx === lastBucketIdx) return;

  const indices: number[] = [];
  let next = (lastBucketIdx + 1) % bucketCount;
  while (next !== idx) {
    indices.push(next);
    next = (next + 1) % bucketCount;
  }
  indices.push(idx);
  fireBucketGroup(indices);
  lastBucketIdx = idx;
}

function bucketDelayMs(now: number): number {
  const elapsed = now - clockEpoch;
  const elapsedInWindow = elapsed % windowMs;
  const idx = Math.floor(elapsedInWindow / BUCKET_MS);
  const nextBoundary = Math.min(windowMs, (idx + 1) * BUCKET_MS);
  const delay = nextBoundary - elapsedInWindow;
  return delay > 0 ? delay : BUCKET_MS;
}

function tickClock(): void {
  bucketTimer = 0;
  if (!registeredHandlers || bucketCount <= 0) return;

  const now = performance.now();
  advanceBucketClock(now);
  bucketTimer = window.setTimeout(tickClock, bucketDelayMs(now));
}

function startClock(): void {
  if (bucketTimer !== 0) return;
  clockEpoch = performance.now();
  lastBucketIdx = -1;
  bucketTimer = window.setTimeout(tickClock, 0);
}

function stopClock(): void {
  if (bucketTimer !== 0) {
    clearTimeout(bucketTimer);
    bucketTimer = 0;
  }
}

export function registerTickPlaybackHandlers(
  handlers: TickPlaybackHandlers,
): void {
  registeredHandlers = handlers;
  startClock();
  for (const lot of pendingLots) {
    mergeBuffer(lot);
  }
  pendingLots.length = 0;
  rebuildBuckets();
  const idx = Math.min(
    bucketCount - 1,
    Math.floor(
      ((performance.now() - clockEpoch) % windowMs) / BUCKET_MS,
    ),
  );
  fireBucket(idx);
}

export function unregisterTickPlaybackHandlers(): void {
  registeredHandlers = null;
}

export function cancelTickPlayback(): void {
  stopClock();
  buffer = [];
  bucketsCache = [];
  pendingLots.length = 0;
  lastBucketIdx = -1;
  refreshStats();
}

export function getPlaybackQueueDepth(): number {
  return buffer.length;
}

export type TickPlaybackResult = {
  bucketCount: number;
  stepCount: number;
  totalTicks: number;
  queueDepth: number;
  queued: boolean;
};

function ingestTicks(ticks: TransferTick[], playMs: number): TickPlaybackResult {
  windowMs = playMs;
  mergeBuffer(ticks);
  rebuildBuckets();

  if (registeredHandlers) {
    startClock();
  } else {
    pendingLots.push(ticks);
  }

  return {
    bucketCount,
    stepCount: bucketCount,
    totalTicks: buffer.length,
    queueDepth: buffer.length,
    queued: ticks.length > 0,
  };
}

export function pushTickLot(
  ticks: TransferTick[],
  playMs: number,
  _bucketMs = BUCKET_MS,
): TickPlaybackResult | null {
  if (ticks.length === 0) {
    return {
      bucketCount: playbackBucketCount(playMs, BUCKET_MS),
      stepCount: 0,
      totalTicks: buffer.length,
      queueDepth: buffer.length,
      queued: false,
    };
  }
  return ingestTicks(ticks, playMs);
}
