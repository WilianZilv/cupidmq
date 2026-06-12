import { useEffect, useMemo, useRef, useState } from "react";
import type { HistoryPoint, RangeKey, SeriesDef } from "./types";
import { RANGE_MS } from "./types";
import { filterHistoryRange } from "./historyStats";
import {
  CONSUMER_COLUMN_DEFS,
  loadVisibleColumnKeys,
  renderConsumerCell,
  saveVisibleColumnKeys,
  type ConsumerColumnKey,
} from "./consumerTableColumns";
import { MetricTip } from "./MetricTip";
import { TIP } from "./metricsHelp";
import {
  consumerStateClass,
  consumerStateLabel,
  consumerStateTip,
} from "./consumerStateDisplay";

const H = 165;
const PAD = { t: 10, r: 8, b: 24, l: 44 };
const PAD_DUAL_R = 46;
const MIN_CHART_W = 200;

function filterRange(history: HistoryPoint[], range: RangeKey): HistoryPoint[] {
  return filterHistoryRange(history, range);
}

function niceMax(v: number): number {
  if (!Number.isFinite(v) || v <= 0) return 1;
  const mag = Math.pow(10, Math.floor(Math.log10(v)));
  const norm = v / mag;
  const step = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10;
  return step * mag;
}

function peakValue(points: HistoryPoint[], series: SeriesDef[]): number {
  let max = 0;
  for (const p of points) {
    for (const s of series) {
      const v = Number(p[s.key]);
      if (Number.isFinite(v)) max = Math.max(max, v);
    }
  }
  return niceMax(Math.max(max, 1));
}

function buildPath(
  points: HistoryPoint[],
  key: SeriesDef["key"],
  xScale: (ts: number) => number,
  yScale: (v: number) => number,
): string {
  if (points.length === 0) return "";
  return points
    .map((p, i) => {
      const x = xScale(p.ts);
      const y = yScale(p[key]);
      return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
}

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

function fmtRate(v: number): string {
  if (!Number.isFinite(v)) return "0.00/s";
  if (v >= 100) return `${Math.round(v)}/s`;
  if (v >= 10) return `${v.toFixed(1)}/s`;
  return `${v.toFixed(2)}/s`;
}

function fmtRateAxis(v: number): string {
  if (!Number.isFinite(v) || v <= 0) return "0";
  if (v >= 100) return `${Math.round(v)}/s`;
  if (v >= 1) return `${v.toFixed(1)}/s`;
  return `${v.toFixed(2)}/s`;
}

function fmtCount(v: number): string {
  if (!Number.isFinite(v)) return "0";
  return v.toLocaleString("en-US");
}

function fmtDurationAxis(v: number): string {
  if (!Number.isFinite(v) || v <= 0) return "0";
  if (v >= 60_000) return `${(v / 60_000).toFixed(0)} min`;
  if (v >= 1000) return `${(v / 1000).toFixed(1)} s`;
  return `${Math.round(v)} ms`;
}

function fmtByteRate(v: number): string {
  return formatNetworkBitrate(v);
}

function fmtByteRateAxis(v: number): string {
  return formatNetworkBitrateAxis(v);
}

function safeNum(v: number): number {
  return Number.isFinite(v) ? v : 0;
}

interface PanelProps {
  title: string;
  subtitle: string;
  titleTip?: string;
  subtitleTip?: string;
  history: HistoryPoint[];
  range: RangeKey;
  series: SeriesDef[];
  current: Record<string, number>;
  rateMode?: boolean;
  byteRateMode?: boolean;
  durationMode?: boolean;
  /** msg/s on the left, bytes/s on the right in the chart + paired legend. */
  dualByteAxis?: boolean;
  /** drops/s (or other rate) on the right Y axis. */
  dualRateAxis?: boolean;
}

export function RabbitPanel({
  title,
  subtitle,
  titleTip,
  subtitleTip,
  history,
  range,
  series,
  current,
  rateMode = false,
  byteRateMode = false,
  durationMode = false,
  dualByteAxis = false,
  dualRateAxis = false,
}: PanelProps) {
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

  const points = filterRange(history, range);
  const tMin = points[0]?.ts ?? Date.now() - RANGE_MS[range];
  const tMax = points[points.length - 1]?.ts ?? Date.now();
  const tSpan = Math.max(1, tMax - tMin);

  const primarySeries = series.filter((s) => s.axis !== "secondary");
  const secondarySeries = series.filter((s) => s.axis === "secondary");
  const splitAxes =
    secondarySeries.length > 0 && (dualByteAxis || dualRateAxis);
  const chartPrimary = splitAxes ? primarySeries : series;
  const chartSecondary = splitAxes ? secondarySeries : [];
  const rightAxisByteRate = splitAxes && dualByteAxis;
  const rightAxisRate = splitAxes && dualRateAxis && !dualByteAxis;

  const maxVal = peakValue(points, chartPrimary);
  const maxValSecondary =
    chartSecondary.length > 0 ? peakValue(points, chartSecondary) : maxVal;

  const pad = splitAxes ? { ...PAD, r: PAD_DUAL_R } : PAD;
  const plotW = chartW - pad.l - pad.r;
  const plotH = H - pad.t - pad.b;
  const xScale = (ts: number) => pad.l + ((ts - tMin) / tSpan) * plotW;
  const yScale = (v: number) => {
    const n = safeNum(v);
    return pad.t + plotH - (n / maxVal) * plotH;
  };
  const yScaleSecondary = (v: number) => {
    const n = safeNum(v);
    return pad.t + plotH - (n / maxValSecondary) * plotH;
  };
  const plotBottom = pad.t + plotH;

  const legendSeries = series.filter((s) => !s.legendHidden);

  const yTicks = 4;
  const xTicks = 5;

  const fmtLegendValue = (v: number, s?: SeriesDef): string => {
    if (s?.axis === "secondary" && dualRateAxis) return fmtRate(v);
    if (byteRateMode || s?.axis === "secondary" && dualByteAxis) return fmtByteRate(v);
    if (rateMode) return fmtRate(v);
    if (durationMode) return formatQueueLagMs(v);
    return fmtCount(v);
  };

  const fmtPairedValue = (v: number, format?: "bitrate" | "bytes"): string => {
    if (format === "bytes") return formatBytes(v);
    return fmtByteRate(v);
  };

  return (
    <section className="rmq-panel chart-panel chart-panel-line">
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
          {legendSeries.map((s) => {
            const hasPaired = s.pairedKey != null;
            const primaryVal = safeNum(current[s.key] ?? 0);
            const pairedVal = hasPaired
              ? safeNum(current[s.pairedKey!] ?? 0)
              : null;

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
                <strong className="chart-legend-value">
                  {fmtLegendValue(primaryVal, s)}
                </strong>
                {hasPaired && s.pairedLabel && pairedVal !== null && (
                  <>
                    <span
                      className="chart-legend-tag chart-legend-tag-muted"
                      style={{ borderColor: s.color, color: s.color }}
                    >
                      {s.pairedLabel}
                    </span>
                    <strong className="chart-legend-value chart-legend-value-secondary">
                      {fmtPairedValue(pairedVal, s.pairedFormat)}
                    </strong>
                  </>
                )}
              </MetricTip>
            );
          })}
        </div>
      </header>
      <div className="rmq-panel-body chart-panel-body">
        <div className="chart-line-wrap" ref={chartWrapRef}>
          <svg
            className="rmq-chart chart-line-chart"
            viewBox={`0 0 ${chartW} ${H}`}
            preserveAspectRatio="xMinYMin meet"
            role="img"
            aria-label={title}
          >
          {Array.from({ length: yTicks + 1 }, (_, i) => {
            const y = pad.t + (plotH * i) / yTicks;
            return (
              <line
                key={`g${i}`}
                x1={pad.l}
                x2={chartW - pad.r}
                y1={y}
                y2={y}
                className="grid-line"
              />
            );
          })}

          {Array.from({ length: yTicks + 1 }, (_, i) => {
            const v = maxVal - (maxVal * i) / yTicks;
            const y = pad.t + (plotH * i) / yTicks;
            return (
              <text key={`yl${i}`} x={pad.l - 6} y={y + 4} className="axis-label left">
                {byteRateMode
                  ? fmtByteRateAxis(v)
                  : rateMode
                  ? v >= 100
                    ? `${Math.round(v)}/s`
                    : `${v.toFixed(0)}/s`
                  : durationMode
                    ? fmtDurationAxis(v)
                    : v >= 1000
                      ? `${(v / 1000).toFixed(1)}k`
                      : Math.round(v)}
              </text>
            );
          })}

          {splitAxes &&
            Array.from({ length: yTicks + 1 }, (_, i) => {
              const v = maxValSecondary - (maxValSecondary * i) / yTicks;
              const y = pad.t + (plotH * i) / yTicks;
              return (
                <text key={`yr${i}`} x={chartW - 4} y={y + 4} className="axis-label right">
                  {rightAxisByteRate
                    ? fmtByteRateAxis(v)
                    : rightAxisRate
                      ? fmtRateAxis(v)
                      : Math.round(v)}
                </text>
              );
            })}

          {Array.from({ length: xTicks + 1 }, (_, i) => {
            const ts = tMin + (tSpan * i) / xTicks;
            const x =
              i === 0
                ? pad.l
                : i === xTicks
                  ? chartW - pad.r
                  : xScale(ts);
            const anchorClass =
              i === 0
                ? "chart-x-start"
                : i === xTicks
                  ? "chart-x-end"
                  : "chart-x-mid";
            return (
              <text
                key={`x${i}`}
                x={x}
                y={H - 5}
                className={`axis-label x chart-x-label ${anchorClass}`}
              >
                {formatTime(ts, range)}
              </text>
            );
          })}

          {chartPrimary.map((s) => (
            <path
              key={s.key}
              d={buildPath(points, s.key, xScale, yScale)}
              fill="none"
              stroke={s.color}
              strokeWidth="2"
              vectorEffect="non-scaling-stroke"
            />
          ))}

          {chartSecondary.map((s) => {
            const linePath = buildPath(points, s.key, xScale, yScaleSecondary);
            if (!linePath) return null;
            const areaPath =
              points.length > 0
                ? `${linePath} L${xScale(points[points.length - 1]!.ts).toFixed(1)},${plotBottom} L${xScale(points[0]!.ts).toFixed(1)},${plotBottom} Z`
                : "";
            return (
              <g key={s.key}>
                {areaPath && (
                  <path
                    d={areaPath}
                    fill={s.color}
                    fillOpacity="0.12"
                    stroke="none"
                  />
                )}
                <path
                  d={linePath}
                  fill="none"
                  stroke={s.color}
                  strokeWidth="2.5"
                  vectorEffect="non-scaling-stroke"
                />
              </g>
            );
          })}
        </svg>
        </div>
      </div>
    </section>
  );
}

interface TableProps {
  consumers: import("./types").ConsumerRow[];
  connectedCount: number;
  listStale: boolean;
}

export function ConsumersTable({
  consumers,
  connectedCount,
  listStale,
}: TableProps) {
  const count = consumers.length > 0 ? consumers.length : connectedCount;
  const [visibleKeys, setVisibleKeys] = useState<ConsumerColumnKey[]>(() =>
    loadVisibleColumnKeys(),
  );
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  const visibleColumns = useMemo(
    () =>
      CONSUMER_COLUMN_DEFS.filter((col) => visibleKeys.includes(col.key)),
    [visibleKeys],
  );

  useEffect(() => {
    saveVisibleColumnKeys(visibleKeys);
  }, [visibleKeys]);

  useEffect(() => {
    if (!menuOpen) return;
    const onDoc = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [menuOpen]);

  const toggleColumn = (key: ConsumerColumnKey) => {
    setVisibleKeys((prev) => {
      if (prev.includes(key)) {
        if (prev.length <= 1) return prev;
        return prev.filter((k) => k !== key);
      }
      const order = CONSUMER_COLUMN_DEFS.map((c) => c.key);
      return [...prev, key].sort(
        (a, b) => order.indexOf(a) - order.indexOf(b),
      );
    });
  };

  const cellCtx = {
    renderCapacity: (pct: number) => (
      <span className={capacityClass(pct)}>{formatCapacityPct(pct)}</span>
    ),
    renderState: (state: string) => (
      <span
        className={`state ${consumerStateClass(state)}`}
        title={consumerStateTip(state)}
      >
        {consumerStateLabel(state)}
      </span>
    ),
    formatBatchRate,
    formatBitrate: formatNetworkBitrate,
    formatBytes,
    formatDuration,
    formatReadyMs,
  };

  return (
    <section className="rmq-panel" aria-label={TIP.consumersTable}>
      <header className="rmq-panel-head">
        <div>
          <MetricTip label={TIP.consumersTable}>
            <h2>Consumers</h2>
          </MetricTip>
          <MetricTip label={TIP.consumers}>
            <span className="rmq-sub">{count} connected</span>
          </MetricTip>
        </div>
        <div className="col-menu-wrap" ref={menuRef}>
          <button
            type="button"
            className="col-menu-btn"
            aria-expanded={menuOpen}
            aria-label={TIP.colMenu}
            title={TIP.colMenu}
            onClick={() => setMenuOpen((o) => !o)}
          >
            Columns
          </button>
          {menuOpen && (
            <div className="col-menu-dropdown" role="menu">
              {CONSUMER_COLUMN_DEFS.map((col) => (
                <label key={col.key} className="col-menu-item">
                  <input
                    type="checkbox"
                    checked={visibleKeys.includes(col.key)}
                    onChange={() => toggleColumn(col.key)}
                  />
                  {col.label}
                </label>
              ))}
            </div>
          )}
        </div>
      </header>
      {listStale && (
        <p className="warn-banner">
          Metrics snapshot missing per-consumer list — restart{" "}
          <code>cupidmq</code> master (current build).
        </p>
      )}
      <div className="rmq-table-wrap">
        <table className="rmq-table">
          <thead>
            <tr>
              {visibleColumns.map((col) => (
                <th
                  key={col.key}
                  className={col.numeric ? "num" : undefined}
                  aria-label={col.tip}
                  title={col.tip}
                >
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {consumers.length === 0 ? (
              <tr>
                <td colSpan={visibleColumns.length || 1} className="empty">
                  {listStale
                    ? `${connectedCount} consumer(s) connected — per-client list not in metrics yet`
                    : "No consumers connected"}
                </td>
              </tr>
            ) : (
              consumers.map((c) => (
                <tr key={c.id}>
                  {visibleColumns.map((col) => (
                    <td
                      key={col.key}
                      className={[
                        col.numeric ? "num" : "",
                        col.mono ? "mono" : "",
                        col.key === "capacity" ? capacityClass(c.capacity_pct) : "",
                      ]
                        .filter(Boolean)
                        .join(" ") || undefined}
                      aria-label={`${col.label}: ${col.tip}`}
                      title={col.tip}
                    >
                      {renderConsumerCell(col.key, c, cellCtx)}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function formatDuration(secs: number): string {
  if (secs < 60) return `${secs}s`;
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return "—";
  if (n < 1024) return `${Math.round(n)} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}

export { formatBytes };

function formatBatchRate(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return "0.00/s";
  if (n >= 10) return `${n.toFixed(1)}/s`;
  return `${n.toFixed(2)}/s`;
}

export function formatCapacityPct(v: number): string {
  if (!Number.isFinite(v)) return "—";
  if (v >= 1000) return `${Math.round(v / 10) * 10}%`;
  if (v >= 100) return `${Math.round(v)}%`;
  return `${Math.round(v)}%`;
}

function formatReadyMs(v: number): string {
  if (!Number.isFinite(v) || v <= 0) return "—";
  return `${Math.round(v)}`;
}

export function capacityClass(v: number): string {
  if (!Number.isFinite(v)) return "";
  if (v > 100) return "capacity-headroom";
  if (v >= 80) return "capacity-ok";
  if (v >= 40) return "capacity-warn";
  return "capacity-low";
}

export function ringFillPct(metrics: {
  ring_bytes: number;
  ring_max_bytes: number;
  ring_messages: number;
  ring_max_messages: number;
}): number {
  const bytePct =
    metrics.ring_max_bytes > 0
      ? (metrics.ring_bytes / metrics.ring_max_bytes) * 100
      : 0;
  const msgPct =
    metrics.ring_max_messages > 0
      ? (metrics.ring_messages / metrics.ring_max_messages) * 100
      : 0;
  return Math.max(bytePct, msgPct);
}

/** ≤5% green, middle yellow, nearly full red. */
export function ringFillClass(pct: number): string {
  if (!Number.isFinite(pct) || pct <= 5) return "ring-ok";
  if (pct >= 85) return "ring-full";
  return "ring-warn";
}

/** bytes/s → Gbps / Mbps (decimal, network standard). */
export function formatNetworkBitrate(bytesPerSec: number): string {
  if (!Number.isFinite(bytesPerSec) || bytesPerSec <= 0) return "0 Mbps";
  const bps = bytesPerSec * 8;
  if (bps >= 1_000_000_000) {
    const gbps = bps / 1_000_000_000;
    return gbps >= 10 ? `${gbps.toFixed(1)} Gbps` : `${gbps.toFixed(2)} Gbps`;
  }
  if (bps >= 1_000_000) {
    const mbps = bps / 1_000_000;
    return mbps >= 100 ? `${Math.round(mbps)} Mbps` : `${mbps.toFixed(1)} Mbps`;
  }
  if (bps >= 1_000) {
    return `${(bps / 1_000).toFixed(0)} Kbps`;
  }
  return `${Math.round(bps)} bps`;
}

/** Compact Y axis — same scale, short suffix. */
export function formatNetworkBitrateAxis(bytesPerSec: number): string {
  if (!Number.isFinite(bytesPerSec) || bytesPerSec <= 0) return "0";
  const bps = bytesPerSec * 8;
  if (bps >= 1_000_000_000) {
    return `${(bps / 1_000_000_000).toFixed(1)} G`;
  }
  if (bps >= 1_000_000) {
    return `${(bps / 1_000_000).toFixed(0)} M`;
  }
  if (bps >= 1_000) {
    return `${(bps / 1_000).toFixed(0)} K`;
  }
  return `${Math.round(bps)}`;
}

/** @deprecated use formatNetworkBitrate */
export function formatBytesPerSec(bytesPerSec: number): string {
  return formatNetworkBitrate(bytesPerSec);
}

export function formatQueueLagMs(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return "0 ms";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  return `${(ms / 60_000).toFixed(1)} min`;
}

export function formatRingMb(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0.0 MB";
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

export function formatRingMaxLabel(maxBytes: number): string {
  if (!Number.isFinite(maxBytes) || maxBytes <= 0) return "max: —";
  const gb = maxBytes / (1024 * 1024 * 1024);
  if (gb >= 1) {
    const n = Number.isInteger(gb) ? String(gb) : gb.toFixed(1);
    return `max: ${n} GB`;
  }
  const mb = Math.max(1, Math.round(maxBytes / (1024 * 1024)));
  return `max: ${mb} MB`;
}

/** @deprecated use formatRingMaxLabel */
export function formatRingMaxChip(maxBytes: number): string {
  return formatRingMaxLabel(maxBytes);
}
