import {
  memo,
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import {
  Background,
  Handle,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useUpdateNodeInternals,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import {
  buildFlowNodes,
  nodeVisualSignature,
  topologySignature,
} from "./flowGraphModel";
import type { MetricsSnapshot, TransferTick } from "./types";
import {
  cancelTickPlayback,
  registerTickPlaybackHandlers,
  unregisterTickPlaybackHandlers,
} from "./tickPlayback";
import {
  applyRouteTicks,
  getActiveRoutesSnapshot,
} from "./routeHeatStore";
import {
  getNodeStateSnapshot,
  mergeNodeStatesFromTicks,
  subscribeNodeStates,
} from "./nodeStateStore";
import { getDisplayTopology, hasTopology } from "./topologyStore";
import { TransferHistorySidebar } from "./TransferHistorySidebar";
import {
  appendTransferHistory,
  flushTransferHistory,
} from "./transferHistoryStore";
import {
  consumerStateClass,
  consumerStateLabel,
  consumerStateTip,
} from "./consumerStateDisplay";
import {
  producerStateClass,
  producerStateLabel,
} from "./producerStateDisplay";
import { MetricTip } from "./MetricTip";
import { TIP } from "./metricsHelp";
import { FlowGraphHeader } from "./FlowGraphHeader";
import { TransferCanvasLayer, type FlowDrawRef } from "./TransferCanvasLayer";

interface Props {
  metrics: MetricsSnapshot;
  pollMs: number;
  playMs: number;
}

type NodeData = {
  label: string;
  sub: string;
  state: string;
  pulse?: false | "transfer" | "assign";
};

function pulseClass(pulse: NodeData["pulse"]): string {
  if (pulse === "assign") return " rf-pulse-assign";
  if (pulse === "transfer") return " rf-pulse";
  return "";
}

const DEFAULT_VIEWPORT = { x: 24, y: 24, zoom: 1 };

const ProducerNode = memo(function ProducerNode({
  id,
  data,
}: NodeProps<Node<NodeData>>) {
  const updateNodeInternals = useUpdateNodeInternals();

  useLayoutEffect(() => {
    updateNodeInternals(id);
  }, [id, data.state, data.pulse, updateNodeInternals]);

  return (
    <div className={`rf-node rf-node-producer${pulseClass(data.pulse)}`}>
      <Handle
        type="source"
        position={Position.Right}
        className="rf-handle rf-handle-out"
      />
      <span className="rf-node-badge">P</span>
      <div className="rf-node-body">
        <strong title={data.label}>{data.label}</strong>
        <span className="rf-node-sep">·</span>
        <span className="rf-node-sub">{data.sub}</span>
      </div>
      <span
        className={`rf-node-state state ${producerStateClass(data.state)}`}
      >
        {producerStateLabel(data.state)}
      </span>
    </div>
  );
});

const ConsumerNode = memo(function ConsumerNode({
  id,
  data,
}: NodeProps<Node<NodeData>>) {
  const updateNodeInternals = useUpdateNodeInternals();

  useLayoutEffect(() => {
    updateNodeInternals(id);
  }, [id, data.state, data.pulse, updateNodeInternals]);

  return (
    <div className={`rf-node rf-node-consumer${pulseClass(data.pulse)}`}>
      <Handle
        type="target"
        position={Position.Left}
        className="rf-handle rf-handle-in"
      />
      <span className="rf-node-badge">C</span>
      <div className="rf-node-body">
        <strong title={data.label}>{data.label}</strong>
        <span className="rf-node-sep">·</span>
        <span className="rf-node-sub">{data.sub}</span>
      </div>
      <span
        className={`rf-node-state state ${consumerStateClass(data.state)}`}
        title={consumerStateTip(data.state)}
      >
        {consumerStateLabel(data.state)}
      </span>
    </div>
  );
});

const nodeTypes = {
  producer: ProducerNode,
  consumer: ConsumerNode,
};

export type FlowVisualChangeRef = { current: () => void };

function FlowGraphCanvas({ metrics, pollMs }: Props) {
  const nodeStates = useSyncExternalStore(
    subscribeNodeStates,
    getNodeStateSnapshot,
    getNodeStateSnapshot,
  );
  const [nodes, setNodes] = useState<Node[]>([]);
  const metricsRef = useRef(metrics);
  const nodesVisualSigRef = useRef("");
  const paintRafRef = useRef(0);
  const onVisualChangeRef = useRef<() => void>(() => {});
  const drawRef = useRef<() => void>(() => {});
  metricsRef.current = metrics;

  const paintNodes = useCallback(() => {
    const m = getDisplayTopology(metricsRef.current);
    const now = Date.now();
    const next = buildFlowNodes(
      m,
      getActiveRoutesSnapshot(now),
      getNodeStateSnapshot(),
      now,
    );
    const sig = nodeVisualSignature(next);
    if (sig === nodesVisualSigRef.current) return;
    nodesVisualSigRef.current = sig;
    setNodes(next);
  }, []);

  const schedulePaintNodes = useCallback(() => {
    if (paintRafRef.current) return;
    paintRafRef.current = requestAnimationFrame(() => {
      paintRafRef.current = 0;
      paintNodes();
    });
  }, [paintNodes]);

  onVisualChangeRef.current = schedulePaintNodes;

  const displayMetrics = getDisplayTopology(metrics);
  const topoSig = topologySignature(displayMetrics);
  const backlogSig = displayMetrics.producers
    .map((p) => `${p.id}:${p.ring_messages + p.pending_messages}`)
    .join(",");

  useEffect(() => {
    paintNodes();
  }, [topoSig, backlogSig, paintNodes]);

  useEffect(() => {
    const id = requestAnimationFrame(() => drawRef.current());
    return () => cancelAnimationFrame(id);
  }, [nodes]);

  useEffect(() => {
    schedulePaintNodes();
  }, [nodeStates, schedulePaintNodes]);

  useLayoutEffect(() => {
    const resolveKeys = (tick: TransferTick) => ({
      producerKey: String(tick.producer_id),
      consumerKey: String(tick.consumer_id),
    });

    registerTickPlaybackHandlers({
      onBucket: (ticks: TransferTick[]) => {
        if (ticks.length === 0) return;
        const m = metricsRef.current;
        for (const tick of ticks) {
          const tag =
            m.consumers.find((c) => c.id === tick.consumer_id)?.consumer_tag ??
            `consumer-${tick.consumer_id}`;
          const p = m.producers.find((row) => row.id === tick.producer_id);
          appendTransferHistory(
            tick,
            `P${tick.producer_id}`,
            tag,
            p?.addr,
            false,
          );
        }
        flushTransferHistory();
        mergeNodeStatesFromTicks(ticks);
        applyRouteTicks(ticks, resolveKeys);
        schedulePaintNodes();
        drawRef.current();
      },
    });

    return () => {
      if (paintRafRef.current) cancelAnimationFrame(paintRafRef.current);
      unregisterTickPlaybackHandlers();
      cancelTickPlayback();
    };
  }, [schedulePaintNodes]);

  return (
    <section className="rmq-panel flow-graph-panel">
      <FlowGraphHeader pollMs={pollMs} />
      <div className="flow-graph-body">
        <div className="flow-graph-wrap">
          <ReactFlow
            nodes={nodes}
            edges={[]}
            nodeTypes={nodeTypes}
            onNodesChange={() => {}}
            defaultViewport={DEFAULT_VIEWPORT}
            minZoom={0.25}
            maxZoom={2}
            nodesDraggable={false}
            nodesConnectable={false}
            elementsSelectable={false}
            panOnDrag
            zoomOnScroll
            zoomOnPinch
            preventScrolling
            proOptions={{ hideAttribution: true }}
          >
            <Background color="#2a2a2a" gap={20} size={1} />
          </ReactFlow>
          <TransferCanvasLayer
            drawRef={drawRef as FlowDrawRef}
            onVisualChangeRef={onVisualChangeRef as FlowVisualChangeRef}
          />
        </div>
        <TransferHistorySidebar />
      </div>
    </section>
  );
}

export const FlowGraph = memo(function FlowGraph({ metrics, pollMs }: Props) {
  if (!hasTopology(metrics)) {
    return (
      <section className="rmq-panel flow-graph-panel">
        <header className="rmq-panel-head">
          <MetricTip label={TIP.flowGraph}>
            <div>
              <h2>Transfer flow</h2>
              <span className="rmq-sub">waiting for connections</span>
            </div>
          </MetricTip>
        </header>
        <div className="flow-graph-body">
          <p className="flow-empty flow-empty-overlay">
            no producers or consumers connected
          </p>
        </div>
      </section>
    );
  }

  return (
    <ReactFlowProvider>
      <FlowGraphCanvas metrics={metrics} pollMs={pollMs} playMs={2000} />
    </ReactFlowProvider>
  );
});
