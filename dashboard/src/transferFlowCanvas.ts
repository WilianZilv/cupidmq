import type { InternalNode } from "@xyflow/react";

import {
  collectAssignmentLinks,
  consumerNodeId,
  producerNodeId,
} from "./flowGraphModel";
import {
  TRANSFER_ACCENT,
  TRANSFER_ASSIGN,
  TRANSFER_FAIL,
  routeFadeAlpha,
  type ActiveRouteTick,
} from "./routeHeatStore";
import type { MetricsSnapshot } from "./types";

type NodeLookup = Map<string, InternalNode>;

const DEFAULT_NODE_W = 300;
const DEFAULT_NODE_H = 48;
const HANDLE_HALF = 4.5;

function handleCenter(
  node: InternalNode,
  type: "source" | "target",
): { x: number; y: number } {
  const handle = node.internals.handleBounds?.[type]?.[0];
  if (handle) {
    const pos = node.internals.positionAbsolute;
    return {
      x: pos.x + handle.x + handle.width / 2,
      y: pos.y + handle.y + handle.height / 2,
    };
  }

  const pos = node.internals.positionAbsolute;
  const w = node.measured?.width ?? node.width ?? DEFAULT_NODE_W;
  const h = node.measured?.height ?? node.height ?? DEFAULT_NODE_H;
  const midY = pos.y + h / 2;
  if (type === "source") {
    return { x: pos.x + w + HANDLE_HALF, y: midY };
  }
  return { x: pos.x - HANDLE_HALF, y: midY };
}

function producerHandleAnchor(
  producerId: number,
  nodeLookup: NodeLookup,
): { x: number; y: number } | null {
  const node = nodeLookup.get(producerNodeId(producerId));
  if (!node) return null;
  return handleCenter(node, "source");
}

function consumerHandleAnchor(
  consumerId: number,
  nodeLookup: NodeLookup,
): { x: number; y: number } | null {
  const node = nodeLookup.get(consumerNodeId(consumerId));
  if (!node) return null;
  return handleCenter(node, "target");
}

function strokeForPhase(route: ActiveRouteTick): string {
  if (route.phase === "assign") return TRANSFER_ASSIGN;
  return route.ok ? TRANSFER_ACCENT : TRANSFER_FAIL;
}

export function drawAssignmentLinks(
  ctx: CanvasRenderingContext2D,
  metrics: MetricsSnapshot,
  nodeLookup: NodeLookup,
): void {
  const links = collectAssignmentLinks(metrics);
  if (links.length === 0) return;

  ctx.save();
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  ctx.globalAlpha = 0.82;
  ctx.strokeStyle = TRANSFER_ASSIGN;
  ctx.lineWidth = 2.25;
  ctx.setLineDash([7, 5]);
  ctx.lineDashOffset = -((performance.now() / 45) % 12);

  for (const link of links) {
    const from = producerHandleAnchor(link.producerId, nodeLookup);
    const to = consumerHandleAnchor(link.consumerId, nodeLookup);
    if (!from || !to) continue;
    const midX = (from.x + to.x) * 0.5;
    ctx.beginPath();
    ctx.moveTo(from.x, from.y);
    ctx.bezierCurveTo(midX, from.y, midX, to.y, to.x, to.y);
    ctx.stroke();
  }

  ctx.restore();
}

export function drawTransferRoutes(
  ctx: CanvasRenderingContext2D,
  routes: readonly ActiveRouteTick[],
  now: number,
  nodeLookup: NodeLookup,
): void {
  if (routes.length === 0) return;

  ctx.lineCap = "round";
  ctx.lineJoin = "round";

  for (const route of routes) {
    if (now >= route.visibleUntil) continue;

    const producerId = Number(route.producerKey);
    if (!Number.isFinite(producerId)) continue;

    const from = producerHandleAnchor(producerId, nodeLookup);
    const consumerId = Number(route.consumerKey);
    if (!Number.isFinite(consumerId)) continue;
    const to = consumerHandleAnchor(consumerId, nodeLookup);
    if (!from || !to) continue;

    const stroke = strokeForPhase(route);
    const isAssign = route.phase === "assign";
    const fade = routeFadeAlpha(route, now);
    const base = isAssign ? 0.9 : 1;
    ctx.globalAlpha = base * fade;
    ctx.strokeStyle = stroke;
    ctx.lineWidth = route.strokeWidth;
    if (isAssign) {
      ctx.setLineDash([7, 5]);
      ctx.lineDashOffset = -((now / 45) % 12);
    } else {
      ctx.setLineDash([]);
      ctx.lineDashOffset = 0;
    }

    const midX = (from.x + to.x) * 0.5;
    ctx.beginPath();
    ctx.moveTo(from.x, from.y);
    ctx.bezierCurveTo(midX, from.y, midX, to.y, to.x, to.y);
    ctx.stroke();

    if (route.phase !== "assign") {
      const t = 0.85;
      const tx = cubicAt(from.x, midX, midX, to.x, t);
      const ty = cubicAt(from.y, from.y, to.y, to.y, t);
      drawArrowHead(ctx, from.x, from.y, to.x, to.y, tx, ty, stroke);
    }
  }

  ctx.globalAlpha = 1;
  ctx.setLineDash([]);
}

function cubicAt(p0: number, p1: number, p2: number, p3: number, t: number) {
  const u = 1 - t;
  return (
    u * u * u * p0 + 3 * u * u * t * p1 + 3 * u * t * t * p2 + t * t * t * p3
  );
}

function drawArrowHead(
  ctx: CanvasRenderingContext2D,
  x0: number,
  y0: number,
  x1: number,
  y1: number,
  tipX: number,
  tipY: number,
  color: string,
) {
  const angle = Math.atan2(y1 - y0, x1 - x0);
  const size = 7;
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.moveTo(tipX, tipY);
  ctx.lineTo(
    tipX - size * Math.cos(angle - 0.45),
    tipY - size * Math.sin(angle - 0.45),
  );
  ctx.lineTo(
    tipX - size * Math.cos(angle + 0.45),
    tipY - size * Math.sin(angle + 0.45),
  );
  ctx.closePath();
  ctx.fill();
}
