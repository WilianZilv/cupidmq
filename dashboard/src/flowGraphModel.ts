import type { Edge, Node } from "@xyflow/react";

import type { MetricsSnapshot } from "./types";
import type { ActiveRouteTick } from "./routeHeatStore";
import {
  TRANSFER_ACCENT,
  TRANSFER_ASSIGN,
  TRANSFER_FAIL,
} from "./routeHeatStore";
import {
  resolveNodeState,
  type NodeStateOverlay,
} from "./nodeStateStore";

export const NODE_HEIGHT = 48;
export const NODE_ROW_STEP = 36;
export const NODE_WIDTH = 300;
export const PRODUCER_X = 0;
export const CONSUMER_X = 400;
const HANDLE_HALF = 4.5;

export function producerAnchor(
  producerId: number,
  producers: MetricsSnapshot["producers"],
): { x: number; y: number } | null {
  const idx = producers.findIndex((p) => p.id === producerId);
  if (idx < 0) return null;
  return {
    x: PRODUCER_X + NODE_WIDTH,
    y: idx * NODE_ROW_STEP + NODE_HEIGHT / 2,
  };
}

export function consumerAnchor(
  consumerId: number,
  consumers: MetricsSnapshot["consumers"],
): { x: number; y: number } | null {
  const idx = consumers.findIndex((c) => c.id === consumerId);
  if (idx < 0) return null;
  return {
    x: CONSUMER_X,
    y: idx * NODE_ROW_STEP + NODE_HEIGHT / 2,
  };
}

/** Centro da bolinha out — layout fixo (sem React Flow). */
export function producerHandleCenter(
  producerId: number,
  producers: MetricsSnapshot["producers"],
): { x: number; y: number } | null {
  const idx = producers.findIndex((p) => p.id === producerId);
  if (idx < 0) return null;
  return {
    x: PRODUCER_X + NODE_WIDTH + HANDLE_HALF,
    y: idx * NODE_ROW_STEP + NODE_HEIGHT / 2,
  };
}

/** Centro da bolinha in — layout fixo (sem React Flow). */
export function consumerHandleCenter(
  consumerId: number,
  consumers: MetricsSnapshot["consumers"],
): { x: number; y: number } | null {
  const idx = consumers.findIndex((c) => c.id === consumerId);
  if (idx < 0) return null;
  return {
    x: CONSUMER_X - HANDLE_HALF,
    y: idx * NODE_ROW_STEP + NODE_HEIGHT / 2,
  };
}

export function producerNodeId(producerId: number) {
  return `p-${producerId}`;
}

/** Stable node id — keyed by consumer id (tags may duplicate). */
export function consumerNodeId(consumerId: number) {
  return `c-${consumerId}`;
}

export function routeEdgeId(producerId: number, consumerId: number) {
  return `e-${producerId}-${consumerId}`;
}

export type AssignmentLink = {
  producerId: number;
  consumerId: number;
};

/** Linha roxa P→C — só o ASGN ativo (producer busy). */
export function collectAssignmentLinks(
  metrics: MetricsSnapshot,
): AssignmentLink[] {
  const out: AssignmentLink[] = [];

  for (const p of metrics.producers) {
    if (p.state !== "busy") continue;
    const cid = p.assigned_consumer_id;
    if (cid == null || cid <= 0) continue;
    if (!metrics.consumers.some((c) => c.id === cid)) continue;
    out.push({ producerId: p.id, consumerId: cid });
  }

  return out;
}

/** Evita tick de ciclo anterior enquanto producer busy com outro consumer. */
export function routeMatchesActiveAssignment(
  route: { producerKey: string; consumerKey: string; phase?: string },
  metrics: MetricsSnapshot,
): boolean {
  const producerId = Number(route.producerKey);
  const consumerId = Number(route.consumerKey);
  if (!Number.isFinite(producerId) || !Number.isFinite(consumerId)) return false;
  const producer = metrics.producers.find((p) => p.id === producerId);
  if (!producer) return false;
  const consumer = metrics.consumers.find((c) => c.id === consumerId);
  if (!consumer) return false;
  if (producer.state !== "busy") {
    return true;
  }
  return producer.assigned_consumer_id === consumer.id;
}

function edgeFromActiveTick(
  route: ActiveRouteTick,
  now: number,
): Pick<Edge, "animated" | "className" | "style" | "data" | "markerEnd"> | null {
  if (now >= route.visibleUntil) return null;

  if (route.phase === "assign") {
    return {
      animated: true,
      className: "rf-tick-assign",
      style: {
        strokeWidth: route.strokeWidth,
        stroke: TRANSFER_ASSIGN,
        strokeDasharray: "7 5",
      },
      markerEnd: "url(#rf-arrow-assign)",
      data: {
        phase: route.phase,
        ok: true,
        msgCount: route.msgCount,
        batchBytes: route.batchBytes,
        tickTsMs: route.tickTsMs,
      },
    };
  }

  const transferred = route.ok;
  const stroke = transferred ? TRANSFER_ACCENT : TRANSFER_FAIL;
  return {
    animated: true,
    className: transferred ? "rf-tick-ok" : "rf-tick-fail",
    style: { strokeWidth: route.strokeWidth, stroke },
    markerEnd: transferred ? "url(#rf-arrow-ok)" : "url(#rf-arrow-fail)",
    data: {
      phase: route.phase,
      ok: route.ok,
      msgCount: route.msgCount,
      batchBytes: route.batchBytes,
      tickTsMs: route.tickTsMs,
    },
  };
}

/** Pulse nos 2 nodes do tick — cor conforme fase (assign vs transfer). */
export function pulseFromRoute(
  route: ActiveRouteTick | null,
  now: number,
): { ids: Set<string>; kind: "transfer" | "assign" } | null {
  if (!route || now >= route.visibleUntil) return null;
  const producerId = Number(route.producerKey);
  const consumerId = Number(route.consumerKey);
  if (!Number.isFinite(producerId) || !Number.isFinite(consumerId)) return null;
  return {
    ids: new Set([
      producerNodeId(producerId),
      consumerNodeId(consumerId),
    ]),
    kind: route.phase === "assign" ? "assign" : "transfer",
  };
}

export function pulseIdsFromRoute(
  route: ActiveRouteTick | null,
  now: number,
): Set<string> {
  return pulseFromRoute(route, now)?.ids ?? new Set();
}

/** Pulse nos nodes — só rotas vindas de ticks. */
export function resolveNodePulseMap(
  routes: readonly ActiveRouteTick[],
  now: number,
): Map<string, "transfer" | "assign"> {
  const out = new Map<string, "transfer" | "assign">();
  for (const route of routes) {
    const pulse = pulseFromRoute(route, now);
    if (!pulse) continue;
    for (const id of pulse.ids) {
      const prev = out.get(id);
      if (!prev || pulse.kind === "transfer") {
        out.set(id, pulse.kind);
      }
    }
  }
  return out;
}

export function buildFlowNodes(
  metrics: MetricsSnapshot,
  activeRoutes: readonly ActiveRouteTick[],
  nodeStates: ReadonlyMap<string, NodeStateOverlay>,
  now = Date.now(),
): Node[] {
  const pulseMap = resolveNodePulseMap(activeRoutes, now);
  const nodes: Node[] = [];
  metrics.producers.forEach((p, i) => {
    const id = producerNodeId(p.id);
    const pulseKind = pulseMap.get(id);
    nodes.push({
      id,
      type: "producer",
      position: { x: 0, y: i * NODE_ROW_STEP },
      width: 300,
      height: NODE_HEIGHT,
      data: {
        label: p.addr,
        sub: `${p.ring_messages + p.pending_messages} backlog`,
        state: resolveNodeState(id, nodeStates, "ready", now),
        pulse: pulseKind ?? false,
      },
    });
  });
  metrics.consumers.forEach((c, i) => {
    const id = consumerNodeId(c.id);
    const pulseKind = pulseMap.get(id);
    nodes.push({
      id,
      type: "consumer",
      position: { x: 400, y: i * NODE_ROW_STEP },
      width: 300,
      height: NODE_HEIGHT,
      data: {
        label: c.consumer_tag,
        sub: c.data_addr,
        state: resolveNodeState(id, nodeStates, "idle", now),
        pulse: pulseKind ?? false,
      },
    });
  });
  return nodes;
}

/** No máximo 1 aresta — tick atual P→C. */
export function buildFlowEdges(
  metrics: MetricsSnapshot,
  route: ActiveRouteTick | null,
  now: number,
): Edge[] {
  if (!route) return [];

  const producerId = Number(route.producerKey);
  const consumerId = Number(route.consumerKey);
  if (!Number.isFinite(producerId) || !Number.isFinite(consumerId)) return [];

  const producerOk = metrics.producers.some((p) => p.id === producerId);
  const consumerOk = metrics.consumers.some((c) => c.id === consumerId);
  if (!producerOk || !consumerOk) return [];

  const visual = edgeFromActiveTick(route, now);
  if (!visual) return [];

  return [
    {
      id: routeEdgeId(producerId, consumerId),
      source: producerNodeId(producerId),
      target: consumerNodeId(consumerId),
      type: "transfer",
      ...visual,
    },
  ];
}

export function topologySignature(metrics: MetricsSnapshot) {
  return [
    ...metrics.producers.map((p) => p.id),
    "|",
    ...metrics.consumers.map((c) => c.id),
  ].join(",");
}

export function nodeVisualSignature(nodes: Node[]): string {
  return nodes
    .map((n) => {
      const d = n.data as {
        state?: string;
        pulse?: string | boolean;
        sub?: string;
      };
      return `${n.id}:${d.state}:${d.pulse ?? 0}:${d.sub ?? ""}`;
    })
    .join(";");
}

export function edgeVisualSignature(edges: Edge[]): string {
  return edges
    .map(
      (e) =>
        `${e.id}|${e.className ?? ""}|${e.animated ? 1 : 0}`,
    )
    .join(";");
}
