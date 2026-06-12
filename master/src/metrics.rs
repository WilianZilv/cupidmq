use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::consumers::ConsumerRegistry;
use crate::producers::{ProducerBacklogTotals, ProducerRegistry, ProducerState};
use crate::tick_buffer::{TickBuffer, TransferTick, TICK_BUFFER_CAP};
use crate::capacity::demand_per_sec;

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub ts_ms: u64,
    pub uptime_secs: u64,
    pub transfers_total: u64,
    pub transfer_bytes_total: u64,
    pub batches_total: u64,
    pub delivery_failures_total: u64,
    pub requeue_total: u64,
    pub producer_drops_total: u64,
    pub producer_backlog_messages: u64,
    pub producer_backlog_bytes: u64,
    pub producer_pending_messages: u64,
    pub producer_ring_messages: u64,
    pub consumer_inflight: u64,
    pub transfer_per_sec: f64,
    pub batches_per_sec: f64,
    pub bytes_per_sec: f64,
    pub producers_connected: u64,
    pub consumers_connected: u64,
    /// In matcher queue — CRDY enqueued, ASGN pending (state=matcher_queue).
    pub consumers_waiting: u64,
    /// ASGN sent — awaiting BATC TCP (state=awaiting_batch).
    pub consumers_await_batch: u64,
    pub producers_ready: u64,
    pub producers_busy: u64,
    /// Alias for consumers_waiting (matcher queue).
    pub consumers_queued: u64,
    /// Sum of μ vs demand (transfer + Δbacklog).
    pub consumer_capacity_pct: f64,
    pub producers: Vec<crate::producers::ProducerRow>,
    pub consumers: Vec<crate::consumers::ConsumerRow>,
    /// Ticks pending in buffer (GET /metrics/ticks drains).
    pub ticks_buffered: u64,
}

#[derive(Debug)]
struct MetricsInner {
    start: SystemTime,
    transfers_total: AtomicU64,
    transfer_bytes_total: AtomicU64,
    batches_total: AtomicU64,
    delivery_failures_total: AtomicU64,
    requeue_total: AtomicU64,
    last_backlog_messages: AtomicU64,
    last_backlog_ts_ms: AtomicU64,
}

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
    producers: Arc<ProducerRegistry>,
    consumers: Arc<ConsumerRegistry>,
    ticks: Arc<TickBuffer>,
}

impl Metrics {
    pub fn new(producers: Arc<ProducerRegistry>, consumers: Arc<ConsumerRegistry>) -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                start: SystemTime::now(),
                transfers_total: AtomicU64::new(0),
                transfer_bytes_total: AtomicU64::new(0),
                batches_total: AtomicU64::new(0),
                delivery_failures_total: AtomicU64::new(0),
                requeue_total: AtomicU64::new(0),
                last_backlog_messages: AtomicU64::new(0),
                last_backlog_ts_ms: AtomicU64::new(0),
            }),
            producers,
            consumers,
            ticks: Arc::new(TickBuffer::new(TICK_BUFFER_CAP)),
        }
    }

    pub async fn record_assign_tick(
        &self,
        producer_id: u64,
        consumer_id: u64,
        producer_state: &str,
        consumer_state: &str,
    ) {
        self.push_tick(
            producer_id,
            consumer_id,
            0,
            0,
            "assign",
            true,
            None,
            producer_state,
            consumer_state,
        )
        .await;
    }

    pub async fn record_transfer_tick(
        &self,
        producer_id: u64,
        consumer_id: u64,
        msg_count: u64,
        batch_bytes: u64,
        ok: bool,
        fail_message: Option<String>,
        producer_state: &str,
        consumer_state: &str,
    ) {
        if consumer_id == 0 {
            return;
        }
        let phase = if ok { "deliver" } else { "fail" };
        if ok {
            if msg_count == 0 {
                // ASGN completed with no payload (empty producer ring) — not a transfer.
                return;
            }
            self.record_transfer(msg_count, batch_bytes);
        } else {
            self.record_delivery_failure(msg_count);
        }
        self.push_tick(
            producer_id,
            consumer_id,
            msg_count,
            batch_bytes,
            phase,
            ok,
            fail_message,
            producer_state,
            consumer_state,
        )
        .await;
    }

    async fn push_tick(
        &self,
        producer_id: u64,
        consumer_id: u64,
        msg_count: u64,
        batch_bytes: u64,
        phase: &str,
        ok: bool,
        fail_message: Option<String>,
        producer_state: &str,
        consumer_state: &str,
    ) {
        if consumer_id == 0 {
            return;
        }
        self.ticks
            .push(TransferTick {
                ts_ms: Self::now_ms(),
                producer_id,
                consumer_id,
                msg_count,
                batch_bytes,
                phase: phase.into(),
                ok,
                fail_message,
                producer_state: producer_state.into(),
                consumer_state: consumer_state.into(),
            })
            .await;
    }

    pub async fn drain_ticks(&self) -> Vec<TransferTick> {
        self.ticks.drain().await
    }

    pub async fn ticks_buffered(&self) -> u64 {
        self.ticks.len().await as u64
    }

    fn record_transfer(&self, msg_count: u64, batch_bytes: u64) {
        self.inner
            .transfers_total
            .fetch_add(msg_count, Ordering::Relaxed);
        self.inner
            .transfer_bytes_total
            .fetch_add(batch_bytes, Ordering::Relaxed);
        self.inner.batches_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_delivery_failure(&self, msg_count: u64) {
        self.inner
            .delivery_failures_total
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .requeue_total
            .fetch_add(msg_count, Ordering::Relaxed);
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn uptime_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(self.inner.start)
            .unwrap_or_default()
            .as_secs()
    }

    fn compute_backlog_growth_per_sec(&self, backlog_messages: u64, now_ms: u64) -> f64 {
        let last_ts = self.inner.last_backlog_ts_ms.load(Ordering::Relaxed);
        let last_backlog = self.inner.last_backlog_messages.load(Ordering::Relaxed);
        self.inner
            .last_backlog_messages
            .store(backlog_messages, Ordering::Relaxed);
        self.inner
            .last_backlog_ts_ms
            .store(now_ms, Ordering::Relaxed);

        if last_ts == 0 || now_ms <= last_ts {
            return 0.0;
        }
        let dt = (now_ms - last_ts) as f64 / 1000.0;
        if dt <= 0.0 {
            return 0.0;
        }
        (backlog_messages as f64 - last_backlog as f64) / dt
    }

    pub async fn snapshot(&self, rates: (f64, f64, f64)) -> MetricsSnapshot {
        let now_ms = Self::now_ms();
        let (transfer_per_sec, batches_per_sec, bytes_per_sec) = rates;

        let backlog: ProducerBacklogTotals = self.producers.backlog_totals().await;
        let backlog_growth =
            self.compute_backlog_growth_per_sec(backlog.backlog_messages(), now_ms);
        let mut ingress_per_sec = demand_per_sec(transfer_per_sec, backlog_growth);
        if ingress_per_sec <= 0.01 && backlog.backlog_messages() > 0 {
            // First poll or transfer=0 — high backlog ⇒ do not report false 100%.
            ingress_per_sec = if transfer_per_sec > 0.01 {
                transfer_per_sec
            } else {
                f64::MAX
            };
        }
        let (consumer_capacity_pct, consumer_rows) = self
            .consumers
            .list_with_capacity(ingress_per_sec)
            .await;
        let producer_rows = self.producers.list().await;

        let producers_ready = self.producers.ready_count();
        let producers_busy = self.producers.busy_count();
        let consumers_waiting = consumer_rows
            .iter()
            .filter(|c| c.state == "matcher_queue" || c.state == "awaiting_read")
            .count() as u64;
        let consumers_await_batch = consumer_rows
            .iter()
            .filter(|c| c.state == "awaiting_batch")
            .count() as u64;
        let consumer_inflight = consumer_rows
            .iter()
            .filter(|c| {
                c.state == "awaiting_batch"
                    || c.state == "processing"
                    || c.state == "delivery_failed"
            })
            .count() as u64;

        MetricsSnapshot {
            ts_ms: now_ms,
            uptime_secs: self.uptime_secs(),
            transfers_total: self.inner.transfers_total.load(Ordering::Relaxed),
            transfer_bytes_total: self.inner.transfer_bytes_total.load(Ordering::Relaxed),
            batches_total: self.inner.batches_total.load(Ordering::Relaxed),
            delivery_failures_total: self
                .inner
                .delivery_failures_total
                .load(Ordering::Relaxed),
            requeue_total: self.inner.requeue_total.load(Ordering::Relaxed),
            producer_drops_total: backlog.drops_total,
            producer_backlog_messages: backlog.backlog_messages(),
            producer_backlog_bytes: backlog.backlog_bytes(),
            producer_pending_messages: backlog.pending_messages,
            producer_ring_messages: backlog.ring_messages,
            consumer_inflight,
            transfer_per_sec,
            batches_per_sec,
            bytes_per_sec,
            producers_connected: producer_rows.len() as u64,
            consumers_connected: consumer_rows.len() as u64,
            consumers_waiting,
            consumers_await_batch,
            producers_ready,
            producers_busy,
            consumers_queued: consumers_waiting,
            consumer_capacity_pct,
            producers: producer_rows,
            consumers: consumer_rows,
            ticks_buffered: self.ticks.len().await as u64,
        }
    }
}

pub fn producer_state_str(state: ProducerState) -> &'static str {
    match state {
        ProducerState::Idle => "idle",
        ProducerState::Ready => "ready",
        ProducerState::Busy => "busy",
    }
}

/// Test helper — ring unit tests.
pub fn new_metrics() -> Metrics {
    Metrics::new(
        Arc::new(ProducerRegistry::new()),
        Arc::new(ConsumerRegistry::new()),
    )
}
