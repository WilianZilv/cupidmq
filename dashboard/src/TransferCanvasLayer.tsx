import { useEffect, useRef } from "react";
import { useOnViewportChange, useReactFlow, useStoreApi } from "@xyflow/react";

import {
  getActiveRoutesSnapshot,
  hasActiveRoute,
  pruneExpiredRoutesQuiet,
} from "./routeHeatStore";
import { drawTransferRoutes } from "./transferFlowCanvas";
import type { FlowVisualChangeRef } from "./FlowGraph";

const FADE_FRAME_MS = 50;

export type FlowDrawRef = { current: () => void };

type Props = {
  drawRef: FlowDrawRef;
  onVisualChangeRef: FlowVisualChangeRef;
};

export function TransferCanvasLayer({ drawRef, onVisualChangeRef }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const wrapRef = useRef<HTMLDivElement>(null);
  const { getViewport } = useReactFlow();
  const storeApi = useStoreApi();
  const fadeTimerRef = useRef(0);
  const drawRafRef = useRef(0);
  const scheduleDrawRef = useRef<() => void>(() => {});

  useOnViewportChange({
    onChange: () => scheduleDrawRef.current(),
    onEnd: () => scheduleDrawRef.current(),
  });

  useEffect(() => {
    const canvas = canvasRef.current;
    const wrap = wrapRef.current;
    if (!canvas || !wrap) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let alive = true;

    const clearFadeTimer = () => {
      if (fadeTimerRef.current) {
        clearTimeout(fadeTimerRef.current);
        fadeTimerRef.current = 0;
      }
    };

    const resize = () => {
      const rect = wrap.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      canvas.width = Math.max(1, Math.floor(rect.width * dpr));
      canvas.height = Math.max(1, Math.floor(rect.height * dpr));
      canvas.style.width = `${rect.width}px`;
      canvas.style.height = `${rect.height}px`;
    };

    const draw = () => {
      if (!alive) return;
      drawRafRef.current = 0;

      const now = Date.now();
      const pruned = pruneExpiredRoutesQuiet(now);
      const routes = getActiveRoutesSnapshot(now);
      const nodeLookup = storeApi.getState().nodeLookup;
      const vp = getViewport();
      const dpr = window.devicePixelRatio || 1;

      resize();
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.clearRect(0, 0, canvas.width, canvas.height);

      if (routes.length > 0) {
        ctx.setTransform(
          dpr * vp.zoom,
          0,
          0,
          dpr * vp.zoom,
          dpr * vp.x,
          dpr * vp.y,
        );
        drawTransferRoutes(ctx, routes, now, nodeLookup);
      } else if (pruned) {
        onVisualChangeRef.current();
      }

      clearFadeTimer();
      if (hasActiveRoute(now)) {
        fadeTimerRef.current = window.setTimeout(scheduleDraw, FADE_FRAME_MS);
      }
    };

    const scheduleDraw = () => {
      if (drawRafRef.current) return;
      drawRafRef.current = requestAnimationFrame(draw);
    };

    drawRef.current = scheduleDraw;
    scheduleDrawRef.current = scheduleDraw;

    resize();
    const ro = new ResizeObserver(() => scheduleDraw());
    ro.observe(wrap);
    scheduleDraw();

    return () => {
      alive = false;
      clearFadeTimer();
      if (drawRafRef.current) cancelAnimationFrame(drawRafRef.current);
      drawRef.current = () => {};
      scheduleDrawRef.current = () => {};
      ro.disconnect();
    };
  }, [drawRef, getViewport, onVisualChangeRef, storeApi]);

  return (
    <div ref={wrapRef} className="transfer-canvas-layer" aria-hidden="true">
      <canvas ref={canvasRef} />
    </div>
  );
}
