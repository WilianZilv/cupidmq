import { useEffect, useMemo, useRef, useState } from "react";
import type { HistoryPoint, RangeKey, SeriesDef } from "./types";
import { RANGE_MS } from "./types";
import { filterHistoryRange } from "./historyStats";
import { MetricTip } from "./MetricTip";

const H = 80;
const PAD = { t: 4, r: 32, b: 20, l: 32 };
const X_TICKS = 5;
const MIN_CHART_W = 200;
const DOT_R = 2.5;
const DOT_PITCH = DOT_R * 2 + 1.2;

function formatTime(ts: number, range: RangeKey): string {
  const d = new Date(ts);
  if (range === "1h") {
    return d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });
  }
  return d.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function peakCount(points: HistoryPoint[], keys: SeriesDef["key"][]): number {
  let max = 1;
  for (const p of points) {
    for (const key of keys) {
      max = Math.max(max, Math.max(0, Math.round(Number(p[key]) || 0)));
    }
  }
  return max;
}

interface MatchPoolPanelProps {
  title: string;
  subtitle: string;
  titleTip?: string;
  subtitleTip?: string;
  history: HistoryPoint[];
  range: RangeKey;
  series: SeriesDef[];
  current: Record<string, number>;
}

export function MatchPoolPanel({
  title,
  subtitle,
  titleTip,
  subtitleTip,
  history,
  range,
  series,
  current,
}: MatchPoolPanelProps) {
  const chartWrapRef = useRef<HTMLDivElement>(null);
  const [chartW, setChartW] = useState(MIN_CHART_W);

  useEffect(() => {
    const el = chartWrapRef.current;
    if (!el) return;
    const sync = () => setChartW(Math.max(MIN_CHART_W, el.clientWidth));
    sync();
    const ro = new ResizeObserver(sync);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const points = filterHistoryRange(history, range);
  const tMin = points[0]?.ts ?? Date.now() - RANGE_MS[range];
  const tMax = points[points.length - 1]?.ts ?? Date.now();
  const tSpan = Math.max(1, tMax - tMin);

  const plotW = chartW - PAD.l - PAD.r;
  const plotH = H - PAD.t - PAD.b;
  const rowH = plotH / Math.max(series.length, 1);
  const xScale = (ts: number) => PAD.l + ((ts - tMin) / tSpan) * plotW;

  const seriesKeys = useMemo(() => series.map((s) => s.key), [series]);
  const globalMax = useMemo(
    () => peakCount(points, seriesKeys),
    [points, seriesKeys],
  );

  const xBand =
    points.length > 1 ? plotW / Math.max(points.length - 1, 1) : plotW;
  const pitchCap =
    globalMax > 0 ? Math.min(DOT_PITCH, (xBand * 0.92) / globalMax) : DOT_PITCH;
  const dotR = Math.min(DOT_R, pitchCap * 0.42);

  const dots = useMemo(() => {
    const out: {
      cx: number;
      cy: number;
      r: number;
      color: string;
      id: string;
      hollow: boolean;
    }[] = [];

    for (const p of points) {
      const xCenter = xScale(p.ts);
      series.forEach((s, si) => {
        const n = Math.max(0, Math.round(Number(p[s.key]) || 0));
        const cy = PAD.t + si * rowH + rowH / 2;
        const pitch = n > 1 ? Math.min(DOT_PITCH, pitchCap) : 0;
        const groupW = n > 0 ? (n - 1) * pitch : 0;
        const startX = xCenter - groupW / 2;
        for (let j = 0; j < n; j++) {
          out.push({
            cx: startX + j * pitch,
            cy,
            r: dotR,
            color: s.color,
            id: `${s.key}-${p.ts}-${j}`,
            hollow: s.hollow === true,
          });
        }
      });
    }
    return out;
  }, [points, series, rowH, tMin, tSpan, plotW, pitchCap, dotR]);

  return (
    <section className="rmq-panel chart-panel match-pool-panel">
      <header className="rmq-panel-head chart-panel-head">
        <div>
          <MetricTip label={titleTip ?? title}>
            <h2>{title}</h2>
          </MetricTip>
          <MetricTip label={subtitleTip ?? subtitle}>
            <span className="rmq-sub">{subtitle}</span>
          </MetricTip>
        </div>
        <div className="chart-panel-legend">
          {series.map((s) => {
            const n = Math.max(0, Math.round(current[s.key] ?? 0));
            return (
              <MetricTip
                key={s.key}
                label={s.tip}
                as="span"
                className="chart-legend-item"
              >
                <span
                  className="chart-legend-tag"
                  style={{ borderColor: s.color, color: s.color }}
                >
                  {s.label}
                </span>
                <strong className="chart-legend-value">{n}</strong>
              </MetricTip>
            );
          })}
        </div>
      </header>
      <div className="rmq-panel-body chart-panel-body match-pool-body">
        <div className="match-pool-chart-wrap" ref={chartWrapRef}>
          <svg
            className="rmq-chart match-pool-chart"
            viewBox={`0 0 ${chartW} ${H}`}
            preserveAspectRatio="xMinYMin meet"
            role="img"
            aria-label={title}
          >
            {series.slice(0, -1).map((s, si) => {
              const y = PAD.t + (si + 1) * rowH;
              return (
                <line
                  key={`sep-${s.key}`}
                  x1={PAD.l}
                  x2={chartW - PAD.r}
                  y1={y}
                  y2={y}
                  className="match-pool-row-line"
                />
              );
            })}

          {dots.map((d) => (
            <circle
              key={d.id}
              cx={d.cx}
              cy={d.cy}
              r={d.r}
              fill={d.hollow ? "none" : d.color}
              stroke={d.hollow ? d.color : "none"}
              strokeWidth={d.hollow ? 1.2 : 0}
              vectorEffect="non-scaling-stroke"
            />
          ))}

          {Array.from({ length: X_TICKS + 1 }, (_, i) => {
            const ts = tMin + (tSpan * i) / X_TICKS;
            const x =
              i === 0
                ? PAD.l
                : i === X_TICKS
                  ? chartW - PAD.r
                  : xScale(ts);
            const anchorClass =
              i === 0
                ? "match-pool-x-start"
                : i === X_TICKS
                  ? "match-pool-x-end"
                  : "match-pool-x-mid";
            return (
              <text
                key={`x${i}`}
                x={x}
                y={H - 4}
                className={`axis-label x match-pool-x-label ${anchorClass}`}
              >
                {formatTime(ts, range)}
              </text>
            );
          })}
        </svg>
        </div>
      </div>
    </section>
  );
}
