import { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  ConsumersTable,
  RabbitPanel,
  capacityClass,
  formatBytes,
  formatCapacityPct,
  formatRingMb,
} from "./RabbitPanel";
import { MatchPoolPanel } from "./MatchPoolPanel";
import { ProducersTable } from "./ProducersTable";
import { FlowGraph } from "./FlowGraph";
import { HelpDialog } from "./HelpDialog";
import {
  avgBatchBytesFromRates,
  mapServerHistoryPoint,
  metricsBaseUrl,
  type HistoryResponse,
} from "./historyApi";
import { dropsInHistoryRange } from "./historyStats";
import { MetricTip } from "./MetricTip";
import { TIP } from "./metricsHelp";
import {
  avgConsumerCapacityPct,
  consumersListStale,
  normalizeMetrics,
  producersListStale,
} from "./normalizeMetrics";
import type {
  HistoryPoint,
  MetricsSnapshot,
  RangeKey,
  SeriesDef,
} from "./types";
import { RANGE_MS } from "./types";
import { fetchTickBatch } from "./tickFeed";
import { pushTickLot } from "./tickPlayback";
import brandIcon from "../../assets/icon.png";
import brandIconText from "../../assets/icon-text.png";
import "./index.css";

const METRICS_URL =
  import.meta.env.VITE_CUPIDMQ_METRICS ?? "http://127.0.0.1:9752/metrics";
const TICKS_URL = `${metricsBaseUrl(METRICS_URL)}/metrics/ticks`;
const HISTORY_URL = `${metricsBaseUrl(METRICS_URL)}/metrics/history`;
export const POLL_MS = 2000;
/** Ticks polled more often than metrics — keeps queue full, no pause between batches. */
export const TICK_POLL_MS = 500;

const BACKLOG_SERIES: SeriesDef[] = [
  {
    key: "producerBacklog",
    label: "Producer backlog",
    color: "#f0c040",
    tip: TIP.producerBacklog,
  },
  {
    key: "producerPending",
    label: "Pending",
    color: "#e0a030",
    tip: TIP.producerPending,
  },
  {
    key: "producerRing",
    label: "Ring",
    color: "#c89020",
    tip: TIP.producerRing,
  },
  {
    key: "consumerInflight",
    label: "Consumer inflight",
    color: "#5bc0de",
    tip: TIP.consumerInflight,
  },
  {
    key: "dropsPerSec",
    label: "Drops →",
    color: "#d9534f",
    tip: TIP.dropsRate,
    axis: "secondary",
  },
];

const RATE_SERIES: SeriesDef[] = [
  {
    key: "transferPerSec",
    label: "Transfer",
    color: "#5cb85c",
    tip: TIP.transferRate,
    pairedKey: "bytesPerSec",
    pairedFormat: "bitrate",
    pairedLabel: "Bitrate",
  },
  {
    key: "batchesPerSec",
    label: "Batches",
    color: "#5bc0de",
    tip: TIP.batchesRate,
    pairedKey: "avgBatchBytes",
    pairedFormat: "bytes",
    pairedLabel: "Avg batch",
  },
  {
    key: "deliveryFailuresPerSec",
    label: "Delivery errors",
    color: "#d9534f",
    tip: TIP.deliveryErrorsRate,
  },
];

const MATCH_SERIES: SeriesDef[] = [
  {
    key: "producersReady",
    label: "Producers ready",
    color: "#f0c040",
    tip: TIP.producersReady,
  },
  {
    key: "producersBusy",
    label: "Producers busy",
    color: "#e0a030",
    tip: TIP.producersBusy,
    hollow: true,
  },
  {
    key: "consumersWaiting",
    label: "Match queue",
    color: "#5bc0de",
    tip: TIP.consumersWaiting,
    hollow: true,
  },
];

const RANGE_TIPS: Record<RangeKey, string> = {
  "1m": TIP.range1m,
  "10m": TIP.range10m,
  "1h": TIP.range1h,
};

function historyUrlForRange(range: RangeKey): string {
  const minutes = Math.ceil(RANGE_MS[range] / 60_000);
  return `${HISTORY_URL}?minutes=${minutes}`;
}

function App() {
  const [metrics, setMetrics] = useState<MetricsSnapshot | null>(null);
  const [metricsRaw, setMetricsRaw] = useState<Record<string, unknown> | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);
  const [history, setHistory] = useState<HistoryPoint[]>([]);
  const [range, setRange] = useState<RangeKey>("10m");
  const [helpOpen, setHelpOpen] = useState(false);

  useEffect(() => {
    const ac = new AbortController();

    const pollMetrics = async () => {
      try {
        const [metricsRes, historyRes] = await Promise.all([
          fetch(METRICS_URL, { signal: ac.signal }),
          fetch(historyUrlForRange(range), { signal: ac.signal }),
        ]);
        if (!metricsRes.ok) throw new Error(`metrics HTTP ${metricsRes.status}`);
        if (!historyRes.ok) throw new Error(`history HTTP ${historyRes.status}`);

        const raw = (await metricsRes.json()) as Record<string, unknown>;
        const hist = (await historyRes.json()) as HistoryResponse;

        setMetrics(normalizeMetrics(raw));
        setMetricsRaw(raw);
        setHistory(hist.points.map(mapServerHistoryPoint));
        setError(null);
      } catch (e) {
        if (ac.signal.aborted) return;
        setError(e instanceof Error ? e.message : String(e));
      }
    };

    pollMetrics();
    const metricsId = setInterval(pollMetrics, POLL_MS);
    return () => {
      ac.abort();
      clearInterval(metricsId);
    };
  }, [range]);

  useEffect(() => {
    const ac = new AbortController();

    const pollTicks = async () => {
      try {
        const ticks = await fetchTickBatch(TICKS_URL, ac.signal);
        pushTickLot(ticks, POLL_MS);
      } catch (e) {
        if (ac.signal.aborted) return;
      }
    };

    pollTicks();
    const ticksId = setInterval(pollTicks, TICK_POLL_MS);
    return () => {
      ac.abort();
      clearInterval(ticksId);
    };
  }, []);

  const rates = history.length > 0 ? history[history.length - 1]! : null;
  const transferPerSec =
    rates?.transferPerSec ?? metrics?.transfer_per_sec ?? 0;
  const batchesPerSec =
    rates?.batchesPerSec ?? metrics?.batches_per_sec ?? 0;
  const dropsInRange = dropsInHistoryRange(history, range);
  const avgMsgBytes =
    metrics && metrics.transfers_total > 0
      ? metrics.transfer_bytes_total / metrics.transfers_total
      : null;
  const avgConsumerCapacity =
    metrics != null
      ? (avgConsumerCapacityPct(metrics.consumers) ??
        metrics.consumer_capacity_pct)
      : null;

  return (
    <>
      <header className="top">
        <div className="top-brand">
          <div className="top-brand-logos">
            <img
              src={brandIcon}
              alt=""
              className="top-brand-icon"
              width={36}
              height={36}
            />
            <img
              src={brandIconText}
              alt="CupidMQ"
              className="top-brand-text"
              height={26}
            />
          </div>
          <p className="sub">
            master · BATC/TCP transfer overview — <code>{METRICS_URL}</code>
          </p>
        </div>
        <div className="top-meta">
          {error ? (
            <MetricTip label={TIP.connectionErr} as="span" className="err">
              offline: {error}
            </MetricTip>
          ) : (
            <MetricTip label={TIP.connectionOk} as="span" className="ok">
              connected
            </MetricTip>
          )}
          <button
            type="button"
            className="help-header-btn"
            aria-label={TIP.helpBtn}
            title={TIP.helpBtn}
            onClick={() => setHelpOpen(true)}
          >
            Help
          </button>
          <div className="range-btns">
            {(["1m", "10m", "1h"] as RangeKey[]).map((r) => (
              <button
                key={r}
                type="button"
                className={range === r ? "active" : ""}
                aria-label={RANGE_TIPS[r]}
                title={RANGE_TIPS[r]}
                onClick={() => setRange(r)}
              >
                last {r}
              </button>
            ))}
          </div>
        </div>
      </header>

      <HelpDialog open={helpOpen} onClose={() => setHelpOpen(false)} />

      {metrics && (
        <>
          <div className="summary">
            <MetricTip label={TIP.producers}>
              <span>
                Producers: <strong>{metrics.producers_connected}</strong> (
                {metrics.producers_ready} ready · {metrics.producers_busy} busy)
              </span>
            </MetricTip>
            <MetricTip label={TIP.consumers}>
              <span>
                Consumers: <strong>{metrics.consumers_connected}</strong> (
                {metrics.consumers_waiting} queue · {metrics.consumers_await_batch}{" "}
                await DELV · {metrics.consumer_inflight} inflight)
              </span>
            </MetricTip>
            <MetricTip
              label={`${TIP.consumerCapacityAvg} Pool: ${formatCapacityPct(metrics.consumer_capacity_pct)}.`}
            >
              <span>
                Avg capacity:{" "}
                <strong className={capacityClass(avgConsumerCapacity ?? 0)}>
                  {avgConsumerCapacity != null
                    ? formatCapacityPct(avgConsumerCapacity)
                    : "—"}
                </strong>
              </span>
            </MetricTip>
            <MetricTip label={TIP.producerBacklog}>
              <span>
                Producer backlog:{" "}
                <strong>
                  {metrics.producer_backlog_messages.toLocaleString("en-US")}
                </strong>{" "}
                msgs ({formatRingMb(metrics.producer_backlog_bytes)})
              </span>
            </MetricTip>
            <MetricTip label={TIP.transferRate}>
              <span>
                Transfer:{" "}
                <strong>{transferPerSec.toFixed(1)}/s</strong> ·{" "}
                {batchesPerSec.toFixed(2)} batch/s
              </span>
            </MetricTip>
            <MetricTip label={TIP.avgMsgSize}>
              <span>
                Avg msg:{" "}
                <strong>
                  {avgMsgBytes != null ? formatBytes(avgMsgBytes) : "—"}
                </strong>
              </span>
            </MetricTip>
            <MetricTip label={TIP.deliveryErrors}>
              <span>
                Delivery errors: <strong>{metrics.delivery_failures_total}</strong> ·
                requeued: <strong>{metrics.requeue_total}</strong>
              </span>
            </MetricTip>
            <MetricTip label={TIP.drops}>
              <span>
                Drops ({range}):{" "}
                <strong>{dropsInRange.toLocaleString("en-US")}</strong>
              </span>
            </MetricTip>
            <MetricTip label={TIP.historyPts}>
              <span>
                History: <strong>{history.length}</strong> pts
              </span>
            </MetricTip>
          </div>

          <RabbitPanel
            title="Producer backlog"
            subtitle={`pending + ring · drops/s on right axis — last ${range}`}
            titleTip={TIP.panelBacklog}
            subtitleTip={TIP.chartBacklog}
            history={history}
            range={range}
            series={BACKLOG_SERIES}
            current={{
              producerBacklog: metrics.producer_backlog_messages,
              producerPending: metrics.producer_pending_messages,
              producerRing: metrics.producer_ring_messages,
              consumerInflight: metrics.consumer_inflight,
              dropsPerSec: rates?.dropsPerSec ?? 0,
            }}
            dualRateAxis
          />

          <RabbitPanel
            title="Transfer rates (BATC)"
            subtitle={`msg/s · batch/s · avg batch size — last ${range}`}
            titleTip={TIP.panelRates}
            subtitleTip={TIP.chartRates}
            history={history}
            range={range}
            series={RATE_SERIES}
            current={{
              transferPerSec,
              bytesPerSec: rates?.bytesPerSec ?? metrics.bytes_per_sec,
              batchesPerSec,
              avgBatchBytes:
                rates?.avgBatchBytes ??
                avgBatchBytesFromRates(
                  rates?.bytesPerSec ?? metrics.bytes_per_sec,
                  batchesPerSec,
                ),
              deliveryFailuresPerSec: rates?.deliveryFailuresPerSec ?? 0,
            }}
            rateMode
          />

          <MatchPoolPanel
            title="Match pool"
            subtitle={`1 dot = 1 unit — last ${range}`}
            titleTip={TIP.panelMatch}
            subtitleTip={TIP.chartMatch}
            history={history}
            range={range}
            series={MATCH_SERIES}
            current={{
              producersReady: metrics.producers_ready,
              producersBusy: metrics.producers_busy,
              consumersWaiting: metrics.consumers_waiting,
            }}
          />

          <ProducersTable
            producers={metrics.producers}
            connectedCount={metrics.producers_connected}
            listStale={metricsRaw ? producersListStale(metricsRaw, metrics) : false}
          />

          <ConsumersTable
            consumers={metrics.consumers}
            connectedCount={metrics.consumers_connected}
            listStale={
              metricsRaw ? consumersListStale(metricsRaw, metrics) : false
            }
          />
        </>
      )}

      <FlowGraph
        metrics={
          metrics ?? {
            ts_ms: 0,
            uptime_secs: 0,
            transfers_total: 0,
            transfer_bytes_total: 0,
            batches_total: 0,
            delivery_failures_total: 0,
            requeue_total: 0,
            producer_drops_total: 0,
            producer_backlog_messages: 0,
            producer_backlog_bytes: 0,
            producer_pending_messages: 0,
            producer_ring_messages: 0,
            consumer_inflight: 0,
            transfer_per_sec: 0,
            batches_per_sec: 0,
            bytes_per_sec: 0,
            producers_connected: 0,
            consumers_connected: 0,
            consumers_waiting: 0,
            consumers_await_batch: 0,
            producers_ready: 0,
            producers_busy: 0,
            consumers_queued: 0,
            ticks_buffered: 0,
            consumer_capacity_pct: 100,
            producers: [],
            consumers: [],
          }
        }
        pollMs={POLL_MS}
        playMs={POLL_MS}
      />
    </>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
