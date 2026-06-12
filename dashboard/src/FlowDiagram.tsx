import type { ConsumerRow, MetricsSnapshot, ProducerRow, RangeKey } from "./types";
import { MetricTip } from "./MetricTip";
import { TIP } from "./metricsHelp";
import { formatRingMb } from "./RabbitPanel";
import {
  producerStateClass,
  producerStateLabel,
} from "./producerStateDisplay";

interface Props {
  metrics: MetricsSnapshot;
  transferPerSec: number;
  batchesPerSec: number;
  dropsInRange: number;
  range: RangeKey;
}

function fmt(n: number): string {
  return n.toLocaleString("en-US");
}

function fmtRate(n: number): string {
  if (n >= 100) return `${Math.round(n)}/s`;
  if (n >= 10) return `${n.toFixed(1)}/s`;
  return `${n.toFixed(2)}/s`;
}

function FlowArrow({
  label,
  tip,
  active,
}: {
  label: string;
  tip: string;
  active?: boolean;
}) {
  return (
    <div className={`flow-arrow${active ? " active" : ""}`}>
      <span className="flow-arrow-line" aria-hidden />
      <MetricTip label={tip} as="span" className="flow-arrow-label">
        {label}
      </MetricTip>
    </div>
  );
}

function producerNode(p: ProducerRow) {
  const backlog = p.pending_messages + p.ring_messages;
  return (
    <div key={p.id} className="flow-node flow-node-producer">
      <span className="flow-node-icon">P</span>
      <span className="flow-node-label">{p.addr}</span>
      <span className={`flow-node-state state ${producerStateClass(p.state)}`}>
        {producerStateLabel(p.state)}
      </span>
      {backlog > 0 && (
        <span className="flow-node-meta">
          {fmt(backlog)} backlog
        </span>
      )}
    </div>
  );
}

function consumerNode(c: ConsumerRow) {
  return (
    <div key={c.id} className="flow-node flow-node-consumer">
      <span className="flow-node-icon">C</span>
      <MetricTip label={TIP.consumers}>
        <span className="flow-node-label">{c.consumer_tag}</span>
      </MetricTip>
      <span className={`flow-node-state state state-${c.state}`}>{c.state}</span>
      {c.data_addr !== "—" && (
        <span className="flow-node-meta mono">{c.data_addr}</span>
      )}
    </div>
  );
}

export function FlowDiagram({
  metrics,
  transferPerSec,
  batchesPerSec,
  dropsInRange,
  range,
}: Props) {
  const producers =
    metrics.producers.length > 0
      ? metrics.producers
      : [];
  const consumers = metrics.consumers;

  return (
    <section className="rmq-panel flow-panel">
      <header className="rmq-panel-head">
        <MetricTip label={TIP.flowMaster}>
          <div>
            <h2>Data flow (master)</h2>
            <span className="rmq-sub">PRDY/CRDY match → BATC delivery</span>
          </div>
        </MetricTip>
      </header>

      <div className="flow-body">
        <div className="flow-stage">
          <MetricTip label={TIP.flowProducers}>
            <div className="flow-stage-title">
              Producers <span>({metrics.producers_connected})</span>
            </div>
          </MetricTip>
          <div className="flow-nodes">
            {producers.length === 0 ? (
              <span className="flow-empty">none connected</span>
            ) : (
              producers.map(producerNode)
            )}
          </div>
          <MetricTip label={TIP.producerBacklog}>
            <span className="flow-ring-cap">
              aggregate backlog: {fmt(metrics.producer_backlog_messages)} msgs ·{" "}
              {formatRingMb(metrics.producer_backlog_bytes)}
              {dropsInRange > 0 && (
                <> · drops {fmt(dropsInRange)} ({range})</>
              )}
            </span>
          </MetricTip>
        </div>

        <FlowArrow
          label={`TCP :9750 · PRDY/HBRP`}
          tip={TIP.flowArrowControl}
          active={metrics.producers_ready > 0 || metrics.producers_busy > 0}
        />

        <div className="flow-stage flow-stage-center">
          <MetricTip label={TIP.flowMaster}>
            <div className="flow-node flow-node-service">
              <strong>Master</strong>
              <span>
                {metrics.producers_ready} ready · {metrics.producers_busy} busy
              </span>
              <span>
                {metrics.consumers_waiting} queue · {metrics.consumers_await_batch}{" "}
                await DELV
              </span>
            </div>
          </MetricTip>
        </div>

        <FlowArrow
          label={`BATC · ${fmtRate(transferPerSec)} msg · ${fmtRate(batchesPerSec)} batch`}
          tip={TIP.flowArrowBatc}
          active={transferPerSec > 0}
        />

        <div className="flow-stage">
          <MetricTip label={TIP.flowConsumers}>
            <div className="flow-stage-title">
              Consumers <span>({metrics.consumers_connected})</span>
            </div>
          </MetricTip>
          <div className="flow-nodes">
            {consumers.length === 0 ? (
              <span className="flow-empty">none — backlog at producer</span>
            ) : (
              consumers.map(consumerNode)
            )}
          </div>
        </div>

        <FlowArrow
          label="TCP :9750 · REG!/CRDY/ACK"
          tip={TIP.flowArrowConsumer}
          active={metrics.consumers_waiting > 0}
        />

        <div className="flow-stage flow-stage-center">
          <MetricTip label={TIP.flowDownstream}>
            <div className="flow-node flow-node-sink">
              <strong>Downstream workers</strong>
              <span>Your application consumers</span>
            </div>
          </MetricTip>
        </div>
      </div>
    </section>
  );
}
