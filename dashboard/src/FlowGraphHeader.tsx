import { useSyncExternalStore } from "react";

import {
  BUCKET_MS,
  getPlaybackStats,
  subscribePlaybackStats,
} from "./tickPlayback";
import { MetricTip } from "./MetricTip";
import { TIP } from "./metricsHelp";

type Props = {
  pollMs: number;
};

export function FlowGraphHeader({ pollMs }: Props) {
  const stats = useSyncExternalStore(
    subscribePlaybackStats,
    getPlaybackStats,
    getPlaybackStats,
  );

  const sub =
    stats.totalTicks > 0
      ? `${stats.totalTicks} ticks · ${stats.bucketCount}×${BUCKET_MS}ms=${stats.bucketCount * BUCKET_MS}ms · buffer ${stats.queueDepth}`
      : `poll ${pollMs}ms`;

  return (
    <header className="rmq-panel-head">
      <MetricTip label={TIP.flowGraph}>
        <div>
          <h2>Transfer flow</h2>
          <span className="rmq-sub">{sub}</span>
        </div>
      </MetricTip>
    </header>
  );
}
