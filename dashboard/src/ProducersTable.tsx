import { useEffect, useMemo, useRef, useState } from "react";
import type { ProducerRow } from "./types";
import { MetricTip } from "./MetricTip";
import { TIP } from "./metricsHelp";
import {
  producerStateClass,
  producerStateLabel,
} from "./producerStateDisplay";
import {
  PRODUCER_COLUMN_DEFS,
  loadVisibleProducerColumnKeys,
  renderProducerCell,
  saveVisibleProducerColumnKeys,
  type ProducerColumnKey,
} from "./producerTableColumns";

interface Props {
  producers: ProducerRow[];
  connectedCount: number;
  listStale: boolean;
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

function formatRate(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return "0.00/s";
  if (n >= 10) return `${n.toFixed(1)}/s`;
  return `${n.toFixed(2)}/s`;
}

export function ProducersTable({
  producers,
  connectedCount,
  listStale,
}: Props) {
  const count = producers.length > 0 ? producers.length : connectedCount;
  const [visibleKeys, setVisibleKeys] = useState(loadVisibleProducerColumnKeys);
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  const visibleColumns = useMemo(
    () => PRODUCER_COLUMN_DEFS.filter((col) => visibleKeys.includes(col.key)),
    [visibleKeys],
  );

  useEffect(() => {
    saveVisibleProducerColumnKeys(visibleKeys);
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

  const toggleColumn = (key: ProducerColumnKey) => {
    setVisibleKeys((prev) => {
      if (prev.includes(key)) {
        if (prev.length <= 1) return prev;
        return prev.filter((k) => k !== key);
      }
      const order = PRODUCER_COLUMN_DEFS.map((c) => c.key);
      return [...prev, key].sort(
        (a, b) => order.indexOf(a) - order.indexOf(b),
      );
    });
  };

  const cellCtx = {
    renderState: (state: string) => (
      <span className={`state ${producerStateClass(state)}`}>
        {producerStateLabel(state)}
      </span>
    ),
    formatRate,
    formatBytes,
    formatDuration,
  };

  return (
    <section className="rmq-panel" aria-label={TIP.producersTable}>
      <header className="rmq-panel-head">
        <div>
          <MetricTip label={TIP.producersTable}>
            <h2>Producers</h2>
          </MetricTip>
          <MetricTip label={TIP.producers}>
            <span className="rmq-sub">
              {count} connected — backlog via HBRP (~2s)
            </span>
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
              {PRODUCER_COLUMN_DEFS.map((col) => (
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
          Metrics snapshot missing per-producer list — restart{" "}
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
            {producers.length === 0 ? (
              <tr>
                <td colSpan={visibleColumns.length || 1} className="empty">
                  {connectedCount > 0
                    ? `${connectedCount} producer(s) connected — per-client list not in metrics yet`
                    : "No producers connected"}
                </td>
              </tr>
            ) : (
              producers.map((p) => (
                <tr key={p.id}>
                  {visibleColumns.map((col) => (
                    <td
                      key={col.key}
                      className={[
                        col.numeric ? "num" : "",
                        col.mono ? "mono" : "",
                      ]
                        .filter(Boolean)
                        .join(" ") || undefined}
                      aria-label={`${col.label}: ${col.tip}`}
                      title={col.tip}
                    >
                      {renderProducerCell(col.key, p, cellCtx)}
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
