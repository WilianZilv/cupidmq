use super::accumulator::BatchAccumulator;
use super::delivery_pool::{DeliveryPool, DEFAULT_DELIVERY_IDLE_SECS};
use crate::metrics_history::unix_ms;
use crate::protocol::{
    encode_batch_payload, encode_delivered, encode_failed, encode_heartbeat, encode_producer_ready,
    read_assign_payload, tune_tcp, write_batch_vectored, AssignRequest, DeliveryReport,
    FailureReport, ProducerHeartbeat,
};
use crate::ring::{ByteRing, QueuedMessage};
use anyhow::{Context, Result};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::warn;

pub const DEFAULT_MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 100;
pub const DEFAULT_OUTBOUND_MAX_BYTES: usize = 4 * 1024 * 1024 * 1024;
pub const DEFAULT_RECONNECT_DELAY_SECS: u64 = 2;
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 2000;
pub const DEFAULT_DELIVERY_TIMEOUT_SECS: u64 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LinkState {
    Disconnected,
    Idle,
    Ready,
    Busy,
}

#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub master: String,
    pub max_batch_bytes: usize,
    pub flush_timeout_ms: u64,
    pub outbound_max_bytes: usize,
    pub reconnect_delay_secs: u64,
    pub delivery_timeout_secs: u64,
    pub delivery_idle_secs: u64,
    pub heartbeat_interval_ms: u64,
}

impl ProducerConfig {
    pub fn new(master: impl Into<String>) -> Self {
        Self {
            master: master.into(),
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            flush_timeout_ms: DEFAULT_FLUSH_TIMEOUT_MS,
            outbound_max_bytes: DEFAULT_OUTBOUND_MAX_BYTES,
            reconnect_delay_secs: DEFAULT_RECONNECT_DELAY_SECS,
            delivery_timeout_secs: DEFAULT_DELIVERY_TIMEOUT_SECS,
            delivery_idle_secs: DEFAULT_DELIVERY_IDLE_SECS,
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_INTERVAL_MS,
        }
    }

    pub fn max_batch_bytes(mut self, bytes: usize) -> Self {
        self.max_batch_bytes = bytes.max(1);
        self
    }

    pub fn flush_timeout_ms(mut self, ms: u64) -> Self {
        self.flush_timeout_ms = ms.max(1);
        self
    }

    pub fn outbound_max_bytes(mut self, bytes: usize) -> Self {
        self.outbound_max_bytes = bytes.max(1);
        self
    }

    pub fn reconnect_delay_secs(mut self, secs: u64) -> Self {
        self.reconnect_delay_secs = secs.max(1);
        self
    }

    pub fn delivery_timeout_secs(mut self, secs: u64) -> Self {
        self.delivery_timeout_secs = secs.max(1);
        self
    }

    pub fn delivery_idle_secs(mut self, secs: u64) -> Self {
        self.delivery_idle_secs = secs.max(1);
        self
    }

    pub fn heartbeat_interval_ms(mut self, ms: u64) -> Self {
        self.heartbeat_interval_ms = ms.max(1);
        self
    }
}

#[derive(Debug, Default)]
pub struct ProducerStats {
    pub published_items: AtomicU64,
    pub published_batches: AtomicU64,
    pub published_bytes: AtomicU64,
    pub reconnects: AtomicU64,
    pub queue_dropped_items: AtomicU64,
    pub delivery_errors: AtomicU64,
}

struct Inner {
    config: ProducerConfig,
    /// Ingress — `publish_batch` enqueues here.
    pending: ByteRing,
    /// Ready for ASSIGN — only receives after accumulator flush.
    ring: ByteRing,
    accumulator: BatchAccumulator,
    link_state: LinkState,
    connected: bool,
    last_connect_fail: Option<Instant>,
}

/// Master client — pending→accumulator→ring, PRDY/ASGN, TCP BATC → consumer.
pub struct Producer {
    inner: Arc<Mutex<Inner>>,
    delivery_pool: Arc<AsyncMutex<DeliveryPool>>,
    notify: Arc<Notify>,
    stats: Arc<ProducerStats>,
    _accumulator: JoinHandle<()>,
    _control: JoinHandle<()>,
}

impl Producer {
    pub async fn connect(config: ProducerConfig) -> Result<Self> {
        let stats = Arc::new(ProducerStats::default());
        let max_bytes = config.outbound_max_bytes;
        let inner = Arc::new(Mutex::new(Inner {
            config: config.clone(),
            pending: ByteRing::new(max_bytes),
            ring: ByteRing::new(max_bytes),
            accumulator: BatchAccumulator::new(
                config.max_batch_bytes,
                Duration::from_millis(config.flush_timeout_ms),
            ),
            link_state: LinkState::Disconnected,
            connected: false,
            last_connect_fail: None,
        }));
        let delivery_pool = Arc::new(AsyncMutex::new(DeliveryPool::new(
            Duration::from_secs(config.delivery_idle_secs),
        )));
        let notify = Arc::new(Notify::new());
        let accumulator = tokio::spawn(run_accumulator(
            inner.clone(),
            notify.clone(),
            stats.clone(),
            Duration::from_millis(config.flush_timeout_ms),
        ));
        let heartbeat_ms = config.heartbeat_interval_ms;
        let control = tokio::spawn(run_control(
            inner.clone(),
            delivery_pool.clone(),
            notify.clone(),
            stats.clone(),
            heartbeat_ms,
        ));
        Ok(Self {
            inner,
            delivery_pool,
            notify,
            stats,
            _accumulator: accumulator,
            _control: control,
        })
    }

    pub fn stats(&self) -> &ProducerStats {
        &self.stats
    }

    pub fn publish_batch(&self, payload: impl Into<Bytes>) -> Result<()> {
        let item = payload.into();
        {
            let mut st = self.inner.lock().unwrap();
            st.pending.push(item, unix_ms());
            sync_drop_stats(&st, &self.stats);
        }
        self.notify.notify_one();
        Ok(())
    }

    pub fn publish(&self, payload: impl Into<Bytes>) -> Result<()> {
        self.publish_batch(payload)
    }
}

fn sync_drop_stats(st: &Inner, stats: &ProducerStats) {
    stats
        .queue_dropped_items
        .store(st.pending.drops_total + st.ring.drops_total, Ordering::Relaxed);
}

fn push_batch_to_ring(ring: &mut ByteRing, batch: &[Bytes]) {
    let now = unix_ms();
    for item in batch {
        ring.push(item.clone(), now);
    }
}

/// pending + accumulator → ring. ASGN only drains ring; otherwise backlog stuck in pending yields DELV 0.
fn flush_ingress_to_ring(st: &mut Inner) {
    while let Some(msg) = st.pending.pop_front() {
        for batch in st.accumulator.push(msg.payload) {
            push_batch_to_ring(&mut st.ring, &batch);
        }
    }
    if let Some(batch) = st.accumulator.take_batch() {
        push_batch_to_ring(&mut st.ring, &batch);
    }
}

async fn run_accumulator(
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Notify>,
    stats: Arc<ProducerStats>,
    flush_timeout: Duration,
) {
    let mut flush_tick = time::interval(flush_timeout);
    flush_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = notify.notified() => {}
            _ = flush_tick.tick() => {}
        }

        let mut ring_gained = false;
        {
            let mut st = inner.lock().unwrap();
            while let Some(msg) = st.pending.pop_front() {
                for batch in st.accumulator.push(msg.payload) {
                    push_batch_to_ring(&mut st.ring, &batch);
                    ring_gained = true;
                }
            }
            if let Some(batch) = st.accumulator.flush_if_timeout() {
                push_batch_to_ring(&mut st.ring, &batch);
                ring_gained = true;
            }
            sync_drop_stats(&st, &stats);
        }

        if ring_gained {
            notify.notify_one();
        }
    }
}

async fn run_control(
    inner: Arc<Mutex<Inner>>,
    delivery_pool: Arc<AsyncMutex<DeliveryPool>>,
    notify: Arc<Notify>,
    stats: Arc<ProducerStats>,
    heartbeat_interval_ms: u64,
) {
    loop {
        let stream = match connect_if_needed(&inner, &stats).await {
            Some(s) => s,
            None => {
                time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };

        let (mut read_half, mut write_half) = stream.into_split();
        maybe_send_ready(&inner, &mut write_half, &stats).await;
        maybe_send_heartbeat(&inner, &mut write_half).await;

        let mut heartbeat_tick =
            time::interval(Duration::from_millis(heartbeat_interval_ms.max(1)));
        heartbeat_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                read_result = read_assign_frame(&mut read_half) => {
                    match read_result {
                        Ok(assign) => {
                            {
                                let mut st = inner.lock().unwrap();
                                st.link_state = LinkState::Busy;
                            }
                            let delivery =
                                deliver_assign(&inner, &delivery_pool, assign).await;
                            handle_delivery_result(&inner, &stats, &mut write_half, delivery).await;
                            {
                                let mut st = inner.lock().unwrap();
                                st.link_state = LinkState::Idle;
                            }
                            maybe_send_ready(&inner, &mut write_half, &stats).await;
                            maybe_send_heartbeat(&inner, &mut write_half).await;
                        }
                        Err(e) => {
                            warn!(error = %e, "master read failed");
                            mark_disconnected(&inner);
                            break;
                        }
                    }
                }
                _ = notify.notified() => {
                    maybe_send_ready(&inner, &mut write_half, &stats).await;
                }
                _ = heartbeat_tick.tick() => {
                    maybe_send_heartbeat(&inner, &mut write_half).await;
                }
            }
        }
    }
}

async fn read_assign_frame(
    read_half: &mut tokio::net::tcp::OwnedReadHalf,
) -> Result<AssignRequest> {
    let mut magic = [0u8; 4];
    read_half.read_exact(&mut magic).await?;
    if magic != crate::protocol::MAGIC_ASSIGN {
        anyhow::bail!("expected ASGN, got {:?}", magic);
    }
    read_assign_payload(read_half).await
}

async fn maybe_send_heartbeat(
    inner: &Arc<Mutex<Inner>>,
    write_half: &mut tokio::net::tcp::OwnedWriteHalf,
) {
    let hb = {
        let st = inner.lock().unwrap();
        if !st.connected {
            return;
        }
        collect_heartbeat(&st)
    };
    let frame = encode_heartbeat(&hb);
    if write_half.write_all(&frame).await.is_err() {
        mark_disconnected(inner);
    }
}

fn collect_heartbeat(st: &Inner) -> ProducerHeartbeat {
    ProducerHeartbeat {
        pending_messages: st.pending.len() as u32,
        pending_bytes: st.pending.byte_len() as u32,
        ring_messages: st.ring.len() as u32,
        ring_bytes: st.ring.byte_len() as u32,
        drops_total: st.pending.drops_total + st.ring.drops_total,
    }
}

async fn maybe_send_ready(
    inner: &Arc<Mutex<Inner>>,
    write_half: &mut tokio::net::tcp::OwnedWriteHalf,
    stats: &ProducerStats,
) {
    let should = {
        let st = inner.lock().unwrap();
        !st.ring.is_empty()
            && matches!(st.link_state, LinkState::Idle)
            && st.connected
    };
    if !should {
        return;
    }

    {
        let mut st = inner.lock().unwrap();
        if st.ring.is_empty() || !matches!(st.link_state, LinkState::Idle) {
            return;
        }
        st.link_state = LinkState::Ready;
    }

    let frame = encode_producer_ready();
    match write_half.write_all(&frame).await {
        Ok(()) => {
            let _ = write_half.flush().await;
        }
        Err(e) => {
            warn!(error = %e, "PRDY send failed");
            stats.reconnects.fetch_add(1, Ordering::Relaxed);
            mark_disconnected(inner);
        }
    }
}

async fn deliver_assign(
    inner: &Arc<Mutex<Inner>>,
    delivery_pool: &Arc<AsyncMutex<DeliveryPool>>,
    assign: AssignRequest,
) -> Result<DeliveryReport, FailureReport> {
    let (max_items, max_batch_bytes, timeout) = {
        let st = inner.lock().unwrap();
        (
            assign.max_items.max(1) as usize,
            st.config.max_batch_bytes,
            Duration::from_secs(st.config.delivery_timeout_secs),
        )
    };

    let items = {
        let mut st = inner.lock().unwrap();
        flush_ingress_to_ring(&mut st);
        drain_for_assign(&mut st.ring, max_items, max_batch_bytes)
    };

    let msg_count = items.len() as u16;
    if msg_count == 0 {
        return Ok(DeliveryReport {
            msg_count: 0,
            batch_bytes: 0,
        });
    }

    let payload_refs: Vec<&Bytes> = items.iter().map(|m| &m.payload).collect();
    let batch_bytes = encode_batch_payload(&payload_refs).len() as u32;

    let delivery = time::timeout(
        timeout,
        tcp_deliver_batch(delivery_pool, &assign.data_addr, &payload_refs),
    )
    .await;

    match delivery {
        Ok(Ok(())) => Ok(DeliveryReport {
            msg_count,
            batch_bytes,
        }),
        Ok(Err(e)) => {
            if !items.is_empty() {
                requeue_items(inner, items);
            }
            Err(FailureReport {
                msg_count,
                batch_bytes,
                code: 0,
                message: e.to_string(),
            })
        }
        Err(_) => {
            if !items.is_empty() {
                requeue_items(inner, items);
            }
            Err(FailureReport {
                msg_count,
                batch_bytes,
                code: 0,
                message: "delivery timeout".into(),
            })
        }
    }
}

async fn tcp_deliver_batch(
    pool: &Arc<AsyncMutex<DeliveryPool>>,
    addr: &str,
    items: &[&Bytes],
) -> Result<()> {
    let mut last_err = anyhow::anyhow!("delivery failed");
    for attempt in 0..2 {
        let mut stream = {
            let mut p = pool.lock().await;
            p.get_or_connect(addr).await?
        };

        match tcp_deliver_batch_once(&mut stream, items).await {
            Ok(()) => {
                pool.lock().await.put(addr, stream);
                return Ok(());
            }
            Err(e) => {
                pool.lock().await.invalidate(addr);
                last_err = e;
                if attempt == 0 {
                    continue;
                }
            }
        }
    }
    Err(last_err)
}

async fn tcp_deliver_batch_once(stream: &mut TcpStream, items: &[&Bytes]) -> Result<()> {
    write_batch_vectored(stream, items)
        .await
        .context("write BATC")?;
    stream.flush().await.context("flush BATC")?;
    Ok(())
}

/// Drains ring respecting `max_items` and `max_batch_bytes`.
fn drain_for_assign(
    ring: &mut ByteRing,
    max_items: usize,
    max_batch_bytes: usize,
) -> Vec<QueuedMessage> {
    let now = unix_ms();
    let mut out = ring.pop_up_to(max_items, now);
    if out.is_empty() {
        return out;
    }

    let mut total_bytes = 0usize;
    let mut keep = out.len();
    for (i, msg) in out.iter().enumerate() {
        let next = total_bytes + msg.payload.len();
        if next > max_batch_bytes && i > 0 {
            keep = i;
            break;
        }
        total_bytes = next;
    }

    if keep < out.len() {
        let rest: Vec<_> = out.drain(keep..).collect();
        for msg in rest.into_iter().rev() {
            ring.push_front(msg);
        }
    }

    out
}

fn requeue_items(inner: &Arc<Mutex<Inner>>, items: Vec<QueuedMessage>) {
    if items.is_empty() {
        return;
    }
    let mut st = inner.lock().unwrap();
    for msg in items.into_iter().rev() {
        st.ring.push_front(msg);
    }
}

async fn handle_delivery_result(
    inner: &Arc<Mutex<Inner>>,
    stats: &ProducerStats,
    write_half: &mut tokio::net::tcp::OwnedWriteHalf,
    report: Result<DeliveryReport, FailureReport>,
) {
    match report {
        Ok(report) => {
            if report.msg_count > 0 {
                stats
                    .published_items
                    .fetch_add(report.msg_count as u64, Ordering::Relaxed);
                stats
                    .published_bytes
                    .fetch_add(report.batch_bytes as u64, Ordering::Relaxed);
                stats.published_batches.fetch_add(1, Ordering::Relaxed);
            }
            let frame = encode_delivered(&report);
            if write_half.write_all(&frame).await.is_err() {
                mark_disconnected(inner);
            } else {
                let _ = write_half.flush().await;
            }
        }
        Err(report) => {
            stats.delivery_errors.fetch_add(1, Ordering::Relaxed);
            sync_drop_stats(&inner.lock().unwrap(), stats);
            let frame = encode_failed(&report);
            if write_half.write_all(&frame).await.is_err() {
                mark_disconnected(inner);
            } else {
                let _ = write_half.flush().await;
            }
        }
    }
}

fn mark_disconnected(inner: &Arc<Mutex<Inner>>) {
    let mut st = inner.lock().unwrap();
    st.connected = false;
    st.link_state = LinkState::Disconnected;
    st.last_connect_fail = Some(Instant::now());
}

fn may_try_connect(st: &Inner) -> bool {
    if st.connected {
        return true;
    }
    if let Some(t) = st.last_connect_fail {
        if t.elapsed() < Duration::from_secs(st.config.reconnect_delay_secs) {
            return false;
        }
    }
    true
}

async fn connect_if_needed(
    inner: &Arc<Mutex<Inner>>,
    stats: &ProducerStats,
) -> Option<TcpStream> {
    {
        let st = inner.lock().unwrap();
        if !may_try_connect(&st) {
            return None;
        }
    }

    let master = {
        let st = inner.lock().unwrap();
        st.config.master.clone()
    };

    match TcpStream::connect(&master).await {
        Ok(stream) => {
            tune_tcp(&stream);
            let mut st = inner.lock().unwrap();
            st.connected = true;
            st.link_state = LinkState::Idle;
            st.last_connect_fail = None;
            Some(stream)
        }
        Err(e) => {
            warn!(master = %master, error = %e, "master unreachable");
            stats.reconnects.fetch_add(1, Ordering::Relaxed);
            let mut st = inner.lock().unwrap();
            st.last_connect_fail = Some(Instant::now());
            None
        }
    }
}
