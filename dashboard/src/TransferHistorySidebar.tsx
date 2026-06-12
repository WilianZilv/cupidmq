import { useSyncExternalStore } from "react";

import {
  getTransferHistorySnapshot,
  subscribeTransferHistory,
  TRANSFER_HISTORY_LIMIT,
} from "./transferHistoryStore";

export function TransferHistorySidebar() {
  const entries = useSyncExternalStore(
    subscribeTransferHistory,
    getTransferHistorySnapshot,
    getTransferHistorySnapshot,
  );

  return (
    <aside className="flow-history-sidebar" aria-label="Transfer history">
      <header className="flow-history-head">
        <span>History</span>
        <span className="flow-history-count">
          {entries.length}/{TRANSFER_HISTORY_LIMIT}
        </span>
      </header>
      <div className="flow-history-scroll">
        <ul className="flow-history-list">
          {entries.length === 0 ? (
            <li className="flow-history-empty">waiting for transfers…</li>
          ) : (
            entries.map((e) => (
              <li
                key={e.id}
                className={`flow-history-item${e.ok ? "" : " flow-history-fail"}`}
                title={
                  e.producerAddr
                    ? `${e.producer} (${e.producerAddr}) → ${e.message} → ${e.consumer}`
                    : undefined
                }
              >
                <span className="fh-producer">{e.producer}</span>
                <span className="fh-arrow" aria-hidden>
                  →
                </span>
                <span className="fh-message">{e.message}</span>
                <span className="fh-arrow" aria-hidden>
                  →
                </span>
                <span className="fh-consumer">{e.consumer}</span>
              </li>
            ))
          )}
        </ul>
      </div>
    </aside>
  );
}
