use crate::capacity::{
    effective_batch_items, effective_cycle_ms, global_capacity_model,
    per_consumer_capacity_model, ConsumerCapacityInput,
};
use crate::metrics_history::unix_ms;
use crate::protocol::ReadyRequest;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize)]
pub struct ConsumerRow {
    pub id: u64,
    pub consumer_tag: String,
    pub addr: String,
    pub data_addr: String,
    pub state: String,
    pub batches_per_sec: f64,
    pub bytes_per_sec: f64,
    pub unacked: u64,
    pub last_batch_items: u64,
    pub last_batch_bytes: u64,
    pub connected_secs: u64,
    /// 0–100: μ/λ — consegue absorver a cota no publish atual?
    pub capacity_pct: f64,
    /// EMA ms entre entrega do batch e próximo CRDY.
    pub ready_cycle_ms: f64,
    /// EMA itens por batch entregue (não max_items pedido).
    pub avg_batch_items: f64,
    pub max_batch_requested: u16,
    /// Producer do ASGN em voo (await BATC).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assigned_producer_id: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct RateSample {
    ts_ms: u64,
    batches: u64,
    bytes: u64,
}

#[derive(Debug)]
struct ConsumerEntry {
    consumer_tag: String,
    addr: String,
    data_addr: Arc<str>,
    connected_at: u64,
    rate_batches: u64,
    rate_bytes: u64,
    rate_sample: RateSample,
    batches_per_sec: f64,
    bytes_per_sec: f64,
    unacked: u64,
    last_batch_items: u64,
    last_batch_bytes: u64,
    state: String,
    max_batch_requested: u16,
    batch_items_ema: f64,
    process_time_ema_ms: f64,
    last_batch_delivered_ms: u64,
    assigned_producer_id: Option<u64>,
}

pub struct ConsumerRegistry {
    next_id: AtomicU64,
    inner: Mutex<HashMap<u64, ConsumerEntry>>,
    /// producer_id → consumers em await BATC (stale assign O(1)).
    awaiting_by_producer: Mutex<HashMap<u64, HashSet<u64>>>,
}

impl ConsumerRegistry {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            inner: Mutex::new(HashMap::new()),
            awaiting_by_producer: Mutex::new(HashMap::new()),
        }
    }

    pub async fn register(&self, addr: String, data_addr: Arc<str>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now = now_secs();
        self.inner.lock().await.insert(
            id,
            ConsumerEntry {
                consumer_tag: format!("consumer-{id}"),
                addr,
                data_addr,
                connected_at: now,
                rate_batches: 0,
                rate_bytes: 0,
                rate_sample: RateSample {
                    ts_ms: 0,
                    batches: 0,
                    bytes: 0,
                },
                batches_per_sec: 0.0,
                bytes_per_sec: 0.0,
                unacked: 0,
                last_batch_items: 0,
                last_batch_bytes: 0,
                state: "idle".into(),
                max_batch_requested: 0,
                batch_items_ema: 0.0,
                process_time_ema_ms: 0.0,
                last_batch_delivered_ms: 0,
                assigned_producer_id: None,
            },
        );
        id
    }

    pub async fn remove(&self, id: u64) {
        let producer_id = self
            .inner
            .lock()
            .await
            .remove(&id)
            .and_then(|e| e.assigned_producer_id);
        if let Some(pid) = producer_id {
            self.untrack_awaiting(pid, id).await;
        }
    }

    pub async fn set_consumer_tag(&self, id: u64, consumer_tag: String) {
        if consumer_tag.is_empty() {
            return;
        }
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            e.consumer_tag = consumer_tag;
        }
    }

    pub async fn set_state(&self, id: u64, state: &str) {
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            e.state = state.into();
        }
    }

    pub async fn state_of(&self, id: u64) -> Option<String> {
        self.inner
            .lock()
            .await
            .get(&id)
            .map(|e| e.state.clone())
    }

    pub async fn on_ready(&self, id: u64, max_items: u16, ts_ms: u64) {
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            if e.last_batch_delivered_ms > 0 && ts_ms > e.last_batch_delivered_ms {
                let sample = (ts_ms - e.last_batch_delivered_ms) as f64;
                e.process_time_ema_ms = if e.process_time_ema_ms <= 0.0 {
                    sample
                } else {
                    e.process_time_ema_ms * 0.8 + sample * 0.2
                };
            }
            e.max_batch_requested = max_items.max(1);
            e.unacked = 0;
        }
    }

    pub async fn record_batch(&self, id: u64, msg_count: u64, batch_bytes: u64, ts_ms: u64) {
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            apply_batch_delivery(e, msg_count, batch_bytes, ts_ms);
        }
    }

    pub async fn set_unacked(&self, id: u64, unacked: u64) {
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            e.unacked = unacked;
        }
    }

    /// Uma lock por ciclo CRDY — substitui vários set_state/on_ready separados.
    pub async fn begin_cycle(
        &self,
        id: u64,
        max_items: u16,
        consumer_tag: &str,
        ts_ms: u64,
    ) {
        let stale_pid = {
            let mut guard = self.inner.lock().await;
            let Some(e) = guard.get_mut(&id) else {
                return;
            };
            if e.last_batch_delivered_ms > 0 && ts_ms > e.last_batch_delivered_ms {
                let sample = (ts_ms - e.last_batch_delivered_ms) as f64;
                e.process_time_ema_ms = if e.process_time_ema_ms <= 0.0 {
                    sample
                } else {
                    e.process_time_ema_ms * 0.8 + sample * 0.2
                };
            }
            e.assigned_producer_id.take()
        };
        if let Some(pid) = stale_pid {
            self.untrack_awaiting(pid, id).await;
        }
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            e.max_batch_requested = max_items.max(1);
            e.unacked = 0;
            e.state = "matcher_queue".into();
            if !consumer_tag.is_empty() {
                e.consumer_tag = consumer_tag.to_string();
            }
        }
    }

    pub async fn mark_awaiting_batch(&self, id: u64, producer_id: u64) {
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            e.assigned_producer_id = Some(producer_id);
            e.state = "awaiting_batch".into();
        }
        self.track_awaiting(producer_id, id).await;
    }

    /// Producer novo ASGN — limpa await stale de ciclo anterior (DELV não refletiu).
    pub async fn release_stale_assignments(&self, producer_id: u64, keep_consumer_id: u64) {
        let stale: Vec<u64> = {
            let awaiting = self.awaiting_by_producer.lock().await;
            awaiting
                .get(&producer_id)
                .map(|set| {
                    set.iter()
                        .copied()
                        .filter(|&cid| cid != keep_consumer_id)
                        .collect()
                })
                .unwrap_or_default()
        };
        if stale.is_empty() {
            return;
        }
        {
            let mut guard = self.inner.lock().await;
            for cid in &stale {
                let Some(e) = guard.get_mut(cid) else {
                    continue;
                };
                if e.state != "awaiting_batch" {
                    continue;
                }
                tracing::debug!(
                    consumer_id = cid,
                    producer_id,
                    "stale await — producer reassigned, clearing assign"
                );
                e.assigned_producer_id = None;
                e.state = "idle".into();
            }
        }
        let mut awaiting = self.awaiting_by_producer.lock().await;
        if let Some(set) = awaiting.get_mut(&producer_id) {
            for cid in stale {
                set.remove(&cid);
            }
            if set.is_empty() {
                awaiting.remove(&producer_id);
            }
        }
    }

    pub async fn set_phase(&self, id: u64, state: &str) {
        let stale_pid = {
            let mut guard = self.inner.lock().await;
            let Some(e) = guard.get_mut(&id) else {
                return;
            };
            if state != "awaiting_batch" {
                e.assigned_producer_id.take()
            } else {
                None
            }
        };
        if let Some(pid) = stale_pid {
            self.untrack_awaiting(pid, id).await;
        }
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            e.state = state.into();
        }
    }

    /// Contexto do último CRDY — para re-enfileirar após queda do producer.
    pub async fn match_context(&self, id: u64) -> Option<(ReadyRequest, Arc<str>)> {
        let guard = self.inner.lock().await;
        let e = guard.get(&id)?;
        Some((
            ReadyRequest {
                max_items: e.max_batch_requested.max(1),
                consumer_tag: e.consumer_tag.clone(),
            },
            Arc::clone(&e.data_addr),
        ))
    }

    pub async fn find_id_by_assigned_producer(&self, producer_id: u64) -> Option<u64> {
        self.inner.lock().await.iter().find_map(|(id, e)| {
            (e.assigned_producer_id == Some(producer_id)).then_some(*id)
        })
    }

    pub async fn finish_batch(
        &self,
        id: u64,
        msg_count: u64,
        batch_bytes: u64,
        ts_ms: u64,
        empty: bool,
    ) {
        let stale_pid = {
            let mut guard = self.inner.lock().await;
            let Some(e) = guard.get_mut(&id) else {
                return;
            };
            let pid = e.assigned_producer_id;
            if empty {
                e.unacked = 0;
                e.assigned_producer_id = None;
                e.state = "idle".into();
                pid
            } else {
                apply_batch_delivery(e, msg_count, batch_bytes, ts_ms);
                pid
            }
        };
        if let Some(pid) = stale_pid {
            self.untrack_awaiting(pid, id).await;
        }
    }

    pub async fn unacked_total(&self) -> u64 {
        self.inner
            .lock()
            .await
            .values()
            .map(|e| e.unacked)
            .sum()
    }

    pub async fn list(&self) -> Vec<ConsumerRow> {
        let (_, rows) = self.list_with_capacity(0.0).await;
        rows
    }

    pub async fn list_with_capacity(
        &self,
        demand_per_sec: f64,
    ) -> (f64, Vec<ConsumerRow>) {
        let now = now_secs();
        let now_ms = unix_ms();
        let mut guard = self.inner.lock().await;
        for e in guard.values_mut() {
            refresh_rates(e, now_ms);
        }
        let consumer_count = guard.len();

        let inputs: Vec<ConsumerCapacityInput> = guard
            .values()
            .map(|e| ConsumerCapacityInput {
                batch_items: effective_batch_items(
                    e.batch_items_ema,
                    e.last_batch_items,
                    e.max_batch_requested,
                ),
                cycle_ms: effective_cycle_ms(e.process_time_ema_ms),
            })
            .collect();

        let global_capacity = global_capacity_model(&inputs, demand_per_sec);

        let mut rows: Vec<ConsumerRow> = guard
            .iter()
            .map(|(id, e)| {
                let batch_items = effective_batch_items(
                    e.batch_items_ema,
                    e.last_batch_items,
                    e.max_batch_requested,
                );
                let cycle_ms = effective_cycle_ms(e.process_time_ema_ms);
                ConsumerRow {
                    id: *id,
                    consumer_tag: e.consumer_tag.clone(),
                    addr: e.addr.clone(),
                    data_addr: e.data_addr.to_string(),
                    state: e.state.clone(),
                    batches_per_sec: e.batches_per_sec,
                    bytes_per_sec: e.bytes_per_sec,
                    unacked: e.unacked,
                    last_batch_items: e.last_batch_items,
                    last_batch_bytes: e.last_batch_bytes,
                    connected_secs: now.saturating_sub(e.connected_at),
                    capacity_pct: per_consumer_capacity_model(
                        batch_items,
                        cycle_ms,
                        demand_per_sec,
                        consumer_count,
                    ),
                    ready_cycle_ms: e.process_time_ema_ms,
                    avg_batch_items: batch_items,
                    max_batch_requested: e.max_batch_requested.max(1),
                    assigned_producer_id: e.assigned_producer_id,
                }
            })
            .collect();
        rows.sort_by_key(|r| r.id);
        (global_capacity, rows)
    }
}

impl ConsumerRegistry {
    async fn track_awaiting(&self, producer_id: u64, consumer_id: u64) {
        self.awaiting_by_producer
            .lock()
            .await
            .entry(producer_id)
            .or_default()
            .insert(consumer_id);
    }

    async fn untrack_awaiting(&self, producer_id: u64, consumer_id: u64) {
        let mut awaiting = self.awaiting_by_producer.lock().await;
        if let Some(set) = awaiting.get_mut(&producer_id) {
            set.remove(&consumer_id);
            if set.is_empty() {
                awaiting.remove(&producer_id);
            }
        }
    }
}

fn apply_batch_delivery(entry: &mut ConsumerEntry, msg_count: u64, batch_bytes: u64, ts_ms: u64) {
    update_batch_items_ema(entry, msg_count);
    entry.rate_batches += 1;
    entry.rate_bytes += batch_bytes;
    entry.unacked = msg_count;
    entry.last_batch_items = msg_count;
    entry.last_batch_bytes = batch_bytes;
    entry.last_batch_delivered_ms = ts_ms;
    entry.assigned_producer_id = None;
    entry.state = "processing".into();
}

fn refresh_rates(entry: &mut ConsumerEntry, now_ms: u64) {
    let prev = entry.rate_sample;
    if prev.ts_ms > 0 && now_ms > prev.ts_ms {
        let dt = (now_ms - prev.ts_ms) as f64 / 1000.0;
        if dt > 0.0 {
            entry.batches_per_sec =
                (entry.rate_batches.saturating_sub(prev.batches)) as f64 / dt;
            entry.bytes_per_sec =
                (entry.rate_bytes.saturating_sub(prev.bytes)) as f64 / dt;
        }
    }
    entry.rate_sample = RateSample {
        ts_ms: now_ms,
        batches: entry.rate_batches,
        bytes: entry.rate_bytes,
    };
}

fn update_batch_items_ema(entry: &mut ConsumerEntry, msg_count: u64) {
    if msg_count == 0 {
        return;
    }
    let sample = msg_count as f64;
    entry.batch_items_ema = if entry.batch_items_ema <= 0.0 {
        sample
    } else {
        entry.batch_items_ema * 0.8 + sample * 0.2
    };
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
