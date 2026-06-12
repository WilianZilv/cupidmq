import { BaseEdge, getSmoothStepPath, type EdgeProps } from "@xyflow/react";

import { TRANSFER_ACCENT, TRANSFER_ASSIGN, TRANSFER_FAIL } from "./routeHeatStore";

type TickEdgeData = {
  phase?: string;
  ok?: boolean;
  msgCount?: number;
  batchBytes?: number;
  tickTsMs?: number;
};

function tickEdgeTitle(data: TickEdgeData | undefined): string | undefined {
  if (data?.phase === "assign") {
    return "master → ASGN → producer POST";
  }
  if (data?.ok == null) return undefined;
  const msgs = data.msgCount ?? 0;
  const kb = Math.round((data.batchBytes ?? 0) / 1024);
  return data.ok
    ? `transferiu · ${msgs} msgs · ${kb} KB`
    : `falhou · ${msgs} msgs · ${kb} KB`;
}

export function TransferEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style,
  markerEnd,
  data,
}: EdgeProps) {
  const [edgePath] = getSmoothStepPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    borderRadius: 14,
  });

  const title = tickEdgeTitle(data as TickEdgeData | undefined);
  const edgeData = data as TickEdgeData | undefined;
  const isAssign = edgeData?.phase === "assign";
  const ok = edgeData?.ok !== false;
  const edgeStyle = {
    ...style,
    stroke:
      (style?.stroke as string | undefined) ??
      (isAssign ? TRANSFER_ASSIGN : ok ? TRANSFER_ACCENT : TRANSFER_FAIL),
    strokeWidth: style?.strokeWidth ?? (isAssign ? 2.5 : 2.5),
  };

  return (
    <>
      {title ? <title>{title}</title> : null}
      <BaseEdge
        id={id}
        path={edgePath}
        style={edgeStyle}
        markerEnd={markerEnd}
        interactionWidth={20}
      />
    </>
  );
}
