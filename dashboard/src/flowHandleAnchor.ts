export type ScreenToFlow = (client: { x: number; y: number }) => {
  x: number;
  y: number;
};

/** Centro da bolinha — client rect → flow coords (mesmo que a bolinha no DOM). */
export function handleFlowCenter(
  nodeId: string,
  handleClass: "rf-handle-out" | "rf-handle-in",
  screenToFlow: ScreenToFlow,
  rootSelector = ".flow-graph-wrap",
): { x: number; y: number } | null {
  const root = document.querySelector(rootSelector);
  if (!root) return null;

  const handle = root.querySelector(
    `.react-flow__node[data-id="${nodeId}"] .${handleClass}`,
  );
  if (!(handle instanceof HTMLElement)) return null;

  const r = handle.getBoundingClientRect();
  return screenToFlow({
    x: r.left + r.width / 2,
    y: r.top + r.height / 2,
  });
}
