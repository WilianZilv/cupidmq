use crate::metrics_history::unix_ms;
use crate::protocol::AssignRequest;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerState {
    Idle,
    Ready,
    Busy,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProducerRow {
    pub id: u64,
    pub addr: String,
    pub state: String,
    pub pending_messages: u32,
    pub pending_bytes: u64,
    pub ring_messages: u32,
    pub ring_bytes: u64,
    pub batches_per_sec: f64,
    pub msgs_per_sec: f64,
    pub errors_per_sec: f64,
    pub drops_per_sec: f64,
    pub connected_secs: u64,
    /// Consumer of in-flight ASGN (busy → BATC pending).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assigned_consumer_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct ProducerBacklogTotals {
    pub pending_messages: u64,
    pub pending_bytes: u64,
    pub ring_messages: u64,
    pub ring_bytes: u64,
    pub drops_total: u64,
}

impl ProducerBacklogTotals {
    pub fn backlog_messages(&self) -> u64 {
        self.pending_messages + self.ring_messages
    }

    pub fn backlog_bytes(&self) -> u64 {
        self.pending_bytes + self.ring_bytes
    }
}

#[derive(Debug, Clone, Copy)]
struct ProducerRateSample {
    ts_ms: u64,
    batches: u64,
    msgs: u64,
    errors: u64,
    drops: u64,
}

#[derive(Debug)]
struct ProducerEntry {
    addr: String,
    connected_at: u64,
    state: ProducerState,
    pending_messages: u32,
    pending_bytes: u32,
    ring_messages: u32,
    ring_bytes: u32,
    hb_drops: u64,
    rate_batches: u64,
    rate_msgs: u64,
    rate_errors: u64,
    rate_drops: u64,
    rate_sample: ProducerRateSample,
    batches_per_sec: f64,
    msgs_per_sec: f64,
    errors_per_sec: f64,
    drops_per_sec: f64,
    pending_consumer_id: Option<u64>,
    /// Unix ms — ASGN sent; detects ghost busy (lost DELV/FAIL).
    busy_since_ms: u64,
    /// PRDY received; ring may not yet reflect in heartbeat.
    prdy_pending: bool,
    write: Mutex<OwnedWriteHalf>,
}


/// After ASGN: producer blocked in deliver; HBRP with backlog or timeout releases busy.
const STALE_BUSY_MS: u64 = 15_000;

pub struct ProducerRegistry {
    next_id: AtomicU64,
    ready_count: AtomicU64,
    busy_count: AtomicU64,
    inner: Mutex<HashMap<u64, ProducerEntry>>,
}

impl ProducerRegistry {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            ready_count: AtomicU64::new(0),
            busy_count: AtomicU64::new(0),
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub fn ready_count(&self) -> u64 {
        self.ready_count.load(Ordering::Relaxed)
    }

    pub fn busy_count(&self) -> u64 {
        self.busy_count.load(Ordering::Relaxed)
    }

    fn transition_entry(&self, entry: &mut ProducerEntry, new: ProducerState) {
        let old = entry.state;
        if old == new {
            return;
        }
        match old {
            ProducerState::Ready => {
                self.ready_count.fetch_sub(1, Ordering::Relaxed);
            }
            ProducerState::Busy => {
                self.busy_count.fetch_sub(1, Ordering::Relaxed);
            }
            ProducerState::Idle => {}
        }
        entry.state = new;
        match new {
            ProducerState::Ready => {
                self.ready_count.fetch_add(1, Ordering::Relaxed);
            }
            ProducerState::Busy => {
                self.busy_count.fetch_add(1, Ordering::Relaxed);
            }
            ProducerState::Idle => {}
        }
    }

    fn release_busy_entry(&self, entry: &mut ProducerEntry) {
        self.transition_entry(entry, ProducerState::Idle);
        entry.pending_consumer_id = None;
        entry.busy_since_ms = 0;
    }

    pub async fn register(&self, addr: String, write: OwnedWriteHalf) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now = now_secs();
        self.inner.lock().await.insert(
            id,
            ProducerEntry {
                addr,
                connected_at: now,
                state: ProducerState::Idle,
                pending_messages: 0,
                pending_bytes: 0,
                ring_messages: 0,
                ring_bytes: 0,
                hb_drops: 0,
                rate_batches: 0,
                rate_msgs: 0,
                rate_errors: 0,
                rate_drops: 0,
                rate_sample: ProducerRateSample {
                    ts_ms: 0,
                    batches: 0,
                    msgs: 0,
                    errors: 0,
                    drops: 0,
                },
                batches_per_sec: 0.0,
                msgs_per_sec: 0.0,
                errors_per_sec: 0.0,
                drops_per_sec: 0.0,
                pending_consumer_id: None,
                busy_since_ms: 0,
                prdy_pending: false,
                write: Mutex::new(write),
            },
        );
        id
    }

    pub async fn remove(&self, id: u64) {
        if let Some(e) = self.inner.lock().await.remove(&id) {
            match e.state {
                ProducerState::Ready => {
                    self.ready_count.fetch_sub(1, Ordering::Relaxed);
                }
                ProducerState::Busy => {
                    self.busy_count.fetch_sub(1, Ordering::Relaxed);
                }
                ProducerState::Idle => {}
            }
        }
    }

    pub async fn set_state(&self, id: u64, state: ProducerState) {
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            self.transition_entry(e, state);
        }
    }

    pub async fn state_of(&self, id: u64) -> Option<ProducerState> {
        self.inner
            .lock()
            .await
            .get(&id)
            .map(|e| e.state)
    }

    /// Match-eligible — ring only (+ PRDY). Pending still in accumulator is not drainable on ASGN.
    #[inline]
    fn entry_has_backlog(e: &ProducerEntry) -> bool {
        e.prdy_pending || e.ring_messages > 0
    }

    #[inline]
    fn entry_backlog_weight(e: &ProducerEntry) -> (u64, u64) {
        let mut msgs = u64::from(e.pending_messages) + u64::from(e.ring_messages);
        let bytes = u64::from(e.pending_bytes) + u64::from(e.ring_bytes);
        if msgs == 0 && e.prdy_pending {
            msgs = 1;
        }
        (msgs, bytes)
    }

    pub async fn has_backlog(&self, id: u64) -> bool {
        self.inner
            .lock()
            .await
            .get(&id)
            .is_some_and(Self::entry_has_backlog)
    }

    /// Weight for matcher ranking — msgs first; bytes break ties.
    pub async fn backlog_weight(&self, id: u64) -> (u64, u64) {
        self.inner
            .lock()
            .await
            .get(&id)
            .map_or((0, 0), Self::entry_backlog_weight)
    }

    /// Hot path — one lock, ranking + stale prune.
    pub async fn rank_ready_candidates(
        &self,
        candidates: &[u64],
        producer_rr: &mut usize,
    ) -> (Option<u64>, Vec<u64>) {
        let mut guard = self.inner.lock().await;
        let mut best_msgs = 0u64;
        let mut best_bytes = 0u64;
        let mut tier: Vec<u64> = Vec::with_capacity(candidates.len());
        let mut stale: Vec<u64> = Vec::new();

        for &id in candidates {
            let Some(e) = guard.get_mut(&id) else {
                stale.push(id);
                continue;
            };
            if !Self::entry_has_backlog(e) {
                stale.push(id);
                e.prdy_pending = false;
                if e.state == ProducerState::Ready {
                    self.transition_entry(e, ProducerState::Idle);
                }
                continue;
            }
            let (msgs, bytes) = Self::entry_backlog_weight(e);
            if msgs > best_msgs || (msgs == best_msgs && bytes > best_bytes) {
                best_msgs = msgs;
                best_bytes = bytes;
                tier.clear();
                tier.push(id);
            } else if msgs == best_msgs && bytes == best_bytes {
                tier.push(id);
            }
        }
        drop(guard);

        if tier.is_empty() {
            return (None, stale);
        }
        if tier.len() == 1 {
            return (Some(tier[0]), stale);
        }
        tier.sort_unstable();
        let pick = *producer_rr % tier.len();
        *producer_rr = (*producer_rr + 1) % tier.len().max(1);
        (Some(tier[pick]), stale)
    }

    /// Reconciles registry ↔ ready pool (ticker / maintenance only).
    pub async fn sync_matcher_ready_state(
        &self,
        current: &HashSet<u64>,
    ) -> (HashSet<u64>, HashSet<u64>) {
        let mut guard = self.inner.lock().await;
        let mut add = HashSet::new();
        let mut remove = HashSet::new();

        for &id in current {
            let Some(e) = guard.get_mut(&id) else {
                remove.insert(id);
                continue;
            };
            if !Self::entry_has_backlog(e) {
                remove.insert(id);
                e.prdy_pending = false;
                if e.state == ProducerState::Ready {
                    self.transition_entry(e, ProducerState::Idle);
                }
            }
        }

        for (id, e) in guard.iter_mut() {
            if !Self::entry_has_backlog(e) {
                continue;
            }
            if e.state == ProducerState::Idle {
                e.prdy_pending = true;
                self.transition_entry(e, ProducerState::Ready);
                add.insert(*id);
            } else if e.state == ProducerState::Ready {
                add.insert(*id);
            }
        }

        (add, remove)
    }

    pub async fn mark_prdy(&self, id: u64) {
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            e.prdy_pending = true;
        }
    }

    pub async fn clear_stale_ready(&self, id: u64) {
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            e.prdy_pending = false;
            self.transition_entry(e, ProducerState::Idle);
        }
    }

    pub async fn send_assign(&self, id: u64, frame: &[u8]) -> Result<(), String> {
        let guard = self.inner.lock().await;
        let Some(entry) = guard.get(&id) else {
            return Err("producer gone".into());
        };
        let mut write = entry.write.lock().await;
        write
            .write_all(frame)
            .await
            .map_err(|e| e.to_string())?;
        write.flush().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn mark_assigned(&self, id: u64, consumer_id: u64) {
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            self.transition_entry(e, ProducerState::Busy);
            e.pending_consumer_id = Some(consumer_id);
            e.busy_since_ms = crate::metrics_history::unix_ms();
        }
    }

    /// Clears stale busy — PRDY/HBRP shows producer free but master stuck on ASGN.
    pub async fn clear_stale_busy(&self, id: u64, reason: &str) -> bool {
        let mut guard = self.inner.lock().await;
        let Some(e) = guard.get_mut(&id) else {
            return false;
        };
        if e.state != ProducerState::Busy {
            return false;
        }
        tracing::warn!(producer_id = id, reason, "clear stale producer busy");
        self.release_busy_entry(e);
        true
    }

    /// Consumer dropped — releases producers stuck on assign for that consumer.
    pub async fn clear_assignments_to_consumer(&self, consumer_id: u64) -> usize {
        let mut cleared = 0usize;
        let mut guard = self.inner.lock().await;
        for (pid, e) in guard.iter_mut() {
            if e.pending_consumer_id == Some(consumer_id) {
                tracing::warn!(
                    producer_id = pid,
                    consumer_id,
                    "clear assign — consumer disconnected"
                );
                self.release_busy_entry(e);
                cleared += 1;
            }
        }
        cleared
    }

    pub async fn take_pending_consumer(&self, id: u64) -> Option<u64> {
        self.inner
            .lock()
            .await
            .get_mut(&id)
            .and_then(|e| e.pending_consumer_id.take())
    }

    /// `true` if eligible backlog exists for rematch (stale busy or idle+backlog).
    pub async fn record_heartbeat(&self, id: u64, hb: crate::protocol::ProducerHeartbeat) -> bool {
        let now = crate::metrics_history::unix_ms();
        let mut rematch = false;
        let hb_ring_backlog = hb.ring_messages > 0;
        let hb_any_backlog = hb_ring_backlog || hb.pending_messages > 0;
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            e.pending_messages = hb.pending_messages;
            e.pending_bytes = hb.pending_bytes;
            e.ring_messages = hb.ring_messages;
            e.ring_bytes = hb.ring_bytes;
            if hb.drops_total >= e.hb_drops {
                e.rate_drops += hb.drops_total - e.hb_drops;
            }
            e.hb_drops = hb.drops_total;

            if e.state == ProducerState::Busy {
                let timed_out = e.busy_since_ms > 0
                    && now.saturating_sub(e.busy_since_ms) >= STALE_BUSY_MS;
                if hb_ring_backlog || timed_out {
                    tracing::warn!(
                        producer_id = id,
                        hb_ring_backlog,
                        hb_pending = hb.pending_messages,
                        timed_out,
                        "heartbeat while busy — stale assignment"
                    );
                    self.transition_entry(e, ProducerState::Idle);
                    e.pending_consumer_id = None;
                    e.busy_since_ms = 0;
                    if hb_ring_backlog {
                        e.prdy_pending = true;
                        self.transition_entry(e, ProducerState::Ready);
                        rematch = true;
                    }
                }
            } else if e.state == ProducerState::Idle && hb_ring_backlog {
                // Ring has data but PRDY not received — matcher ignores idle.
                tracing::warn!(
                    producer_id = id,
                    ring = hb.ring_messages,
                    pending = hb.pending_messages,
                    "idle with ring backlog — promote ready"
                );
                e.prdy_pending = true;
                self.transition_entry(e, ProducerState::Ready);
                rematch = true;
            } else if !hb_any_backlog {
                e.prdy_pending = false;
            }
        }
        rematch
    }

    pub async fn backlog_totals(&self) -> ProducerBacklogTotals {
        let guard = self.inner.lock().await;
        let mut out = ProducerBacklogTotals::default();
        for e in guard.values() {
            out.pending_messages += e.pending_messages as u64;
            out.pending_bytes += e.pending_bytes as u64;
            out.ring_messages += e.ring_messages as u64;
            out.ring_bytes += e.ring_bytes as u64;
            out.drops_total = out.drops_total.saturating_add(e.hb_drops);
        }
        out
    }

    pub async fn record_delivery(&self, id: u64, msg_count: u64) {
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            e.rate_batches += 1;
            e.rate_msgs += msg_count;
            self.release_busy_entry(e);
            e.prdy_pending = false;
        }
    }

    pub async fn record_failure(&self, id: u64) {
        if let Some(e) = self.inner.lock().await.get_mut(&id) {
            e.rate_errors += 1;
            self.release_busy_entry(e);
            e.prdy_pending = false;
        }
    }

    pub async fn list(&self) -> Vec<ProducerRow> {
        let now = now_secs();
        let now_ms = unix_ms();
        let mut guard = self.inner.lock().await;
        for e in guard.values_mut() {
            refresh_producer_rates(e, now_ms);
        }
        let mut rows: Vec<ProducerRow> = guard
            .iter()
            .map(|(id, e)| ProducerRow {
                id: *id,
                addr: e.addr.clone(),
                state: match e.state {
                    ProducerState::Idle => "idle",
                    ProducerState::Ready => "ready",
                    ProducerState::Busy => "busy",
                }
                .into(),
                pending_messages: e.pending_messages,
                pending_bytes: e.pending_bytes as u64,
                ring_messages: e.ring_messages,
                ring_bytes: e.ring_bytes as u64,
                batches_per_sec: e.batches_per_sec,
                msgs_per_sec: e.msgs_per_sec,
                errors_per_sec: e.errors_per_sec,
                drops_per_sec: e.drops_per_sec,
                connected_secs: now.saturating_sub(e.connected_at),
                assigned_consumer_id: if e.state == ProducerState::Busy {
                    e.pending_consumer_id
                } else {
                    None
                },
            })
            .collect();
        rows.sort_by_key(|r| r.id);
        rows
    }
}

fn refresh_producer_rates(entry: &mut ProducerEntry, now_ms: u64) {
    let prev = entry.rate_sample;
    if prev.ts_ms > 0 && now_ms > prev.ts_ms {
        let dt = (now_ms - prev.ts_ms) as f64 / 1000.0;
        if dt > 0.0 {
            entry.batches_per_sec =
                (entry.rate_batches.saturating_sub(prev.batches)) as f64 / dt;
            entry.msgs_per_sec =
                (entry.rate_msgs.saturating_sub(prev.msgs)) as f64 / dt;
            entry.errors_per_sec =
                (entry.rate_errors.saturating_sub(prev.errors)) as f64 / dt;
            entry.drops_per_sec =
                (entry.rate_drops.saturating_sub(prev.drops)) as f64 / dt;
        }
    }
    entry.rate_sample = ProducerRateSample {
        ts_ms: now_ms,
        batches: entry.rate_batches,
        msgs: entry.rate_msgs,
        errors: entry.rate_errors,
        drops: entry.rate_drops,
    };
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Matchmaker — consumer CRDY queue (RR) + producer PRDY pool (backlog ranking).
pub struct Matchmaker {
    producers: std::sync::Arc<ProducerRegistry>,
    consumers: std::sync::Arc<crate::consumers::ConsumerRegistry>,
    metrics: std::sync::Arc<crate::metrics::Metrics>,
    inner: Mutex<MatchmakerInner>,
    notify: Notify,
}

struct MatchmakerInner {
    ready: HashSet<u64>,
    waiting: Vec<PendingConsumer>,
    consumer_rr: usize,
    producer_rr: usize,
}

#[derive(Debug)]
struct PendingConsumer {
    consumer_id: u64,
    ready: crate::protocol::ReadyRequest,
    data_addr: Arc<str>,
    done: tokio::sync::oneshot::Sender<Result<(), String>>,
}

fn push_waiting(wait: &mut Vec<PendingConsumer>, entry: PendingConsumer) {
    wait.retain(|p| p.consumer_id != entry.consumer_id);
    wait.push(entry);
}

fn make_pending(consumer_id: u64, ready: crate::protocol::ReadyRequest, data_addr: Arc<str>) -> PendingConsumer {
    let (tx, _rx) = tokio::sync::oneshot::channel();
    PendingConsumer {
        consumer_id,
        ready,
        data_addr,
        done: tx,
    }
}

fn take_next_waiting_inner(inner: &mut MatchmakerInner) -> Option<PendingConsumer> {
    let len = inner.waiting.len();
    if len == 0 {
        return None;
    }
    if inner.consumer_rr >= len {
        inner.consumer_rr = 0;
    }
    let pick = inner.consumer_rr;
    inner.consumer_rr = (inner.consumer_rr + 1) % len;
    Some(inner.waiting.remove(pick))
}

impl Matchmaker {
    pub fn new(
        producers: std::sync::Arc<ProducerRegistry>,
        consumers: std::sync::Arc<crate::consumers::ConsumerRegistry>,
        metrics: std::sync::Arc<crate::metrics::Metrics>,
    ) -> Self {
        Self {
            producers,
            consumers,
            metrics,
            inner: Mutex::new(MatchmakerInner {
                ready: HashSet::new(),
                waiting: Vec::new(),
                consumer_rr: 0,
                producer_rr: 0,
            }),
            notify: Notify::new(),
        }
    }

    pub fn notify(&self) -> &Notify {
        &self.notify
    }

    pub async fn ready_pool_len(&self) -> usize {
        self.inner.lock().await.ready.len()
    }

    /// Reconciles registry ↔ pool — ticker / maintenance only (not hot path).
    pub async fn sync_ready_pool(&self) {
        let current = self.inner.lock().await.ready.clone();
        let (add, remove) = self.producers.sync_matcher_ready_state(&current).await;
        let mut inner = self.inner.lock().await;
        for id in remove {
            inner.ready.remove(&id);
        }
        for id in add {
            inner.ready.insert(id);
        }
    }

    /// Producer ring has items — enters RR pool (if idle).
    pub async fn on_producer_ready(&self, producer_id: u64) -> Result<(), String> {
        match self.producers.state_of(producer_id).await {
            Some(ProducerState::Busy) => {
                self.producers
                    .clear_stale_busy(producer_id, "PRDY while busy")
                    .await;
            }
            Some(ProducerState::Ready) => {
                self.producers.mark_prdy(producer_id).await;
                self.ensure_in_ready_pool(producer_id).await;
                return self.try_match().await;
            }
            Some(ProducerState::Idle) | None => {}
        }

        self.producers.mark_prdy(producer_id).await;
        self.ensure_in_ready_pool(producer_id).await;
        self.producers
            .set_state(producer_id, ProducerState::Ready)
            .await;
        self.try_match().await
    }

    async fn push_consumer_wait_with_done(
        &self,
        consumer_id: u64,
        ready: crate::protocol::ReadyRequest,
        data_addr: Arc<str>,
        done: tokio::sync::oneshot::Sender<Result<(), String>>,
    ) {
        let mut inner = self.inner.lock().await;
        push_waiting(
            &mut inner.waiting,
            PendingConsumer {
                consumer_id,
                ready,
                data_addr,
                done,
            },
        );
    }

    async fn push_consumer_wait(
        &self,
        consumer_id: u64,
        ready: crate::protocol::ReadyRequest,
        data_addr: Arc<str>,
    ) {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        self.push_consumer_wait_with_done(consumer_id, ready, data_addr, tx)
            .await;
    }

    /// Enqueues CRDY — no match yet; `try_match` follows.
    pub async fn enqueue_consumer(
        &self,
        consumer_id: u64,
        ready: crate::protocol::ReadyRequest,
        data_addr: Arc<str>,
    ) {
        self.push_consumer_wait(consumer_id, ready, data_addr).await;
    }

    /// Consumer requested batch — blocks until assign sent or error.
    pub async fn on_consumer_read(
        &self,
        consumer_id: u64,
        ready: crate::protocol::ReadyRequest,
        data_addr: Arc<str>,
    ) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.push_consumer_wait_with_done(consumer_id, ready, data_addr, tx)
            .await;
        self.try_match().await?;
        rx.await.map_err(|_| "match cancelled".to_string())?
    }

    pub async fn on_producer_done(&self, producer_id: u64) {
        self.producers
            .set_state(producer_id, ProducerState::Idle)
            .await;
        self.inner.lock().await.ready.remove(&producer_id);
        let _ = self.try_match().await;
    }

    pub async fn on_producer_disconnect(
        &self,
        producer_id: u64,
        consumers: &crate::consumers::ConsumerRegistry,
    ) {
        self.inner.lock().await.ready.remove(&producer_id);

        let consumer_id = self.producers.take_pending_consumer(producer_id).await;
        if let Some(cid) = consumer_id {
            let state = consumers.state_of(cid).await;
            let should_requeue = matches!(
                state.as_deref(),
                Some("awaiting_batch") | Some("matcher_queue")
            );
            if should_requeue {
                if let Some((ready, data_addr)) = consumers.match_context(cid).await {
                    consumers.set_phase(cid, "matcher_queue").await;
                    self.enqueue_waiting_fire_and_forget(cid, ready, data_addr)
                        .await;
                    tracing::warn!(
                        producer_id,
                        consumer_id = cid,
                        "producer disconnected — consumer requeued for match"
                    );
                }
            }
        }

        self.producers.remove(producer_id).await;
        let _ = self.try_match().await;
    }

    async fn enqueue_waiting_fire_and_forget(
        &self,
        consumer_id: u64,
        ready: crate::protocol::ReadyRequest,
        data_addr: Arc<str>,
    ) {
        self.push_consumer_wait(consumer_id, ready, data_addr).await;
    }

    pub async fn wake_producer(&self, producer_id: u64) {
        self.ensure_in_ready_pool(producer_id).await;
        let _ = self.try_match().await;
    }

    /// Consumer TCP dropped — removes from CRDY queue and unblocks handler.
    pub async fn cancel_consumer(&self, consumer_id: u64) {
        let pending = {
            let mut inner = self.inner.lock().await;
            inner
                .waiting
                .iter()
                .position(|p| p.consumer_id == consumer_id)
                .map(|pos| inner.waiting.swap_remove(pos))
        };
        if let Some(p) = pending {
            let _ = p.done.send(Err("consumer disconnected".into()));
        }
        if self
            .producers
            .clear_assignments_to_consumer(consumer_id)
            .await
            > 0
        {
            let _ = self.try_match().await;
        }
    }

    pub async fn waiting_count(&self) -> usize {
        self.inner.lock().await.waiting.len()
    }

    async fn ensure_in_ready_pool(&self, producer_id: u64) {
        self.inner.lock().await.ready.insert(producer_id);
    }

    pub async fn try_match(&self) -> Result<(), String> {
        loop {
            let (candidates, producer_rr) = {
                let inner = self.inner.lock().await;
                if inner.waiting.is_empty() || inner.ready.is_empty() {
                    return Ok(());
                }
                (
                    inner.ready.iter().copied().collect::<Vec<_>>(),
                    inner.producer_rr,
                )
            };

            let mut rr = producer_rr;
            let (best, stale) = self
                .producers
                .rank_ready_candidates(&candidates, &mut rr)
                .await;

            let consumer = {
                let mut inner = self.inner.lock().await;
                inner.producer_rr = rr;
                for id in stale {
                    inner.ready.remove(&id);
                }
                let Some(producer_id) = best else {
                    return Ok(());
                };
                if inner.waiting.is_empty() {
                    return Ok(());
                }
                inner.ready.remove(&producer_id);
                let Some(consumer) = take_next_waiting_inner(&mut inner) else {
                    inner.ready.insert(producer_id);
                    return Ok(());
                };
                (producer_id, consumer)
            };

            let (producer_id, consumer) = consumer;

            let assign = AssignRequest {
                max_items: consumer.ready.max_items,
                consumer_tag: consumer.ready.consumer_tag.clone(),
                data_addr: consumer.data_addr.to_string(),
            };
            let frame = crate::protocol::encode_assign(&assign).map_err(|e| e.to_string())?;

            if let Err(e) = self.producers.send_assign(producer_id, &frame).await {
                tracing::warn!(producer_id, error = %e, "assign send failed");
                self.producers
                    .set_state(producer_id, ProducerState::Idle)
                    .await;
                let mut inner = self.inner.lock().await;
                inner.ready.remove(&producer_id);
                inner.waiting.push(consumer);
                continue;
            }

            self.producers
                .mark_assigned(producer_id, consumer.consumer_id)
                .await;
            self.consumers
                .release_stale_assignments(producer_id, consumer.consumer_id)
                .await;
            self.consumers
                .mark_awaiting_batch(consumer.consumer_id, producer_id)
                .await;
            self.metrics
                .record_assign_tick(
                    producer_id,
                    consumer.consumer_id,
                    "busy",
                    "awaiting_batch",
                )
                .await;

            let _ = consumer.done.send(Ok(()));
        }
    }

    #[cfg(test)]
    async fn clear_ready_pool_for_test(&self) {
        self.inner.lock().await.ready.clear();
    }

    #[cfg(test)]
    async fn ready_contains_for_test(&self, id: u64) -> bool {
        self.inner.lock().await.ready.contains(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumers::ConsumerRegistry;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    async fn register_test_producer(registry: &ProducerRegistry) -> u64 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream.into_split().1
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let id = registry.register("test".into(), stream.into_split().1).await;
        let _ = accept.await;
        id
    }

    #[tokio::test]
    async fn backlog_weight_prefers_msgs_then_bytes() {
        let registry = ProducerRegistry::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream.into_split().1
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let id = registry.register("test".into(), stream.into_split().1).await;
        let _ = accept.await;

        registry
            .record_heartbeat(
                id,
                crate::protocol::ProducerHeartbeat {
                    pending_messages: 2,
                    pending_bytes: 100,
                    ring_messages: 3,
                    ring_bytes: 500,
                    drops_total: 0,
                },
            )
            .await;

        assert_eq!(registry.backlog_weight(id).await, (5, 600));
    }

    #[tokio::test]
    async fn ready_producer_reenters_pool_on_duplicate_prdy() {
        let producers = Arc::new(ProducerRegistry::new());
        let consumers = Arc::new(ConsumerRegistry::new());
        let metrics = Arc::new(crate::metrics::Metrics::new(producers.clone(), consumers.clone()));
        let matcher = Matchmaker::new(producers.clone(), consumers.clone(), metrics);
        let pid = register_test_producer(&producers).await;

        matcher.on_producer_ready(pid).await.unwrap();
        assert_eq!(matcher.ready_pool_len().await, 1);

        // Simulates desync: ready in registry, empty pool.
        matcher.clear_ready_pool_for_test().await;

        matcher.on_producer_ready(pid).await.unwrap();
        assert!(matcher.ready_contains_for_test(pid).await);
    }

    fn pending(id: u64) -> PendingConsumer {
        make_pending(
            id,
            crate::protocol::ReadyRequest {
                max_items: 32,
                consumer_tag: format!("c-{id}"),
            },
            Arc::from(format!("127.0.0.1:{id}")),
        )
    }

    #[tokio::test]
    async fn consumer_round_robin_not_fifo() {
        let mut inner = MatchmakerInner {
            ready: HashSet::new(),
            waiting: vec![pending(1), pending(2), pending(3)],
            consumer_rr: 0,
            producer_rr: 0,
        };
        let picks = [
            take_next_waiting_inner(&mut inner).unwrap().consumer_id,
            take_next_waiting_inner(&mut inner).unwrap().consumer_id,
            take_next_waiting_inner(&mut inner).unwrap().consumer_id,
        ];
        assert_eq!(picks, [1, 3, 2]);

        inner.waiting = vec![pending(10), pending(20)];
        inner.consumer_rr = 0;
        assert_eq!(take_next_waiting_inner(&mut inner).unwrap().consumer_id, 10);
        assert_eq!(take_next_waiting_inner(&mut inner).unwrap().consumer_id, 20);
        inner.waiting = vec![pending(10), pending(20)];
        assert_eq!(take_next_waiting_inner(&mut inner).unwrap().consumer_id, 10);
        assert_eq!(take_next_waiting_inner(&mut inner).unwrap().consumer_id, 20);
    }

    #[tokio::test]
    async fn producer_disconnect_requeues_awaiting_batch_consumer() {
        let producers = Arc::new(ProducerRegistry::new());
        let consumers = Arc::new(ConsumerRegistry::new());
        let metrics = Arc::new(crate::metrics::Metrics::new(producers.clone(), consumers.clone()));
        let matcher = Matchmaker::new(producers.clone(), consumers.clone(), metrics);
        let pid = register_test_producer(&producers).await;
        let cid = consumers
            .register("127.0.0.1:1".into(), Arc::from("127.0.0.1:9760"))
            .await;
        consumers.begin_cycle(cid, 32, "c1", 0).await;
        consumers.set_phase(cid, "awaiting_batch").await;
        producers.mark_assigned(pid, cid).await;

        matcher
            .on_producer_disconnect(pid, consumers.as_ref())
            .await;

        assert_eq!(
            consumers.state_of(cid).await.as_deref(),
            Some("matcher_queue")
        );
        assert_eq!(matcher.waiting_count().await, 1);
    }
}
