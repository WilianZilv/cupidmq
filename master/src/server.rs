use crate::consumers::ConsumerRegistry;
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::metrics_cache::MetricsCache;
use crate::metrics_history::{HistoryResponse, MetricsHistory, unix_ms};
use crate::producers::{Matchmaker, ProducerRegistry};
use crate::protocol::{
    encode_error, read_delivery_report_payload, read_failure_report_payload,
    read_consumer_ready_request, read_heartbeat_payload, read_register_payload, tune_tcp,
    ERR_PROTOCOL,
    MAGIC_DELIVERED, MAGIC_FAILED, MAGIC_HEARTBEAT, MAGIC_PRODUCER_READY, MAGIC_REGISTER,
};
use anyhow::{Context, Result, bail};
use axum::{extract::Query, routing::get, Json, Router};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Duration;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};
use tracing::{info, warn};

const CONTROL_READ_BUF: usize = 64 * 1024;

#[derive(Clone)]
pub struct RelayState {
    pub metrics: Metrics,
    pub consumers: Arc<ConsumerRegistry>,
    pub producers: Arc<ProducerRegistry>,
    pub matcher: Arc<Matchmaker>,
    pub history: Arc<MetricsHistory>,
    pub metrics_cache: Arc<MetricsCache>,
    pub history_interval_ms: u64,
}

pub struct RelayConfig {
    pub control_addr: String,
    pub metrics_addr: String,
    pub history_interval_ms: u64,
    pub history_cap: usize,
    pub dashboard_dir: Option<PathBuf>,
}

pub async fn run(cfg: RelayConfig) -> Result<()> {
    let producers = Arc::new(ProducerRegistry::new());
    let consumers = Arc::new(ConsumerRegistry::new());
    let metrics = Metrics::new(producers.clone(), consumers.clone());
    let matcher = Arc::new(Matchmaker::new(
        producers.clone(),
        consumers.clone(),
        Arc::new(metrics.clone()),
    ));
    let history = Arc::new(MetricsHistory::new(cfg.history_cap));
    let metrics_cache = Arc::new(MetricsCache::new());
    let state = RelayState {
        metrics,
        consumers: consumers.clone(),
        producers: producers.clone(),
        matcher,
        history: history.clone(),
        metrics_cache: metrics_cache.clone(),
        history_interval_ms: cfg.history_interval_ms,
    };

    let control = TcpListener::bind(&cfg.control_addr)
        .await
        .with_context(|| format!("bind control {}", cfg.control_addr))?;
    let metrics_listener = TcpListener::bind(&cfg.metrics_addr)
        .await
        .with_context(|| format!("bind metrics {}", cfg.metrics_addr))?;

    info!(
        control = %cfg.control_addr,
        metrics = %cfg.metrics_addr,
        "cupidmq master listening"
    );

    let metrics_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = run_metrics_server(metrics_listener, metrics_state, cfg.dashboard_dir).await {
            warn!(error = %e, "metrics server stopped");
        }
    });

    let sampler_state = state.clone();
    let sampler_interval = cfg.history_interval_ms;
    tokio::spawn(async move {
        run_history_sampler(sampler_state, sampler_interval).await;
    });

    let match_state = state.clone();
    tokio::spawn(async move {
        run_match_loop(match_state).await;
    });

    loop {
        match control.accept().await {
            Ok((stream, addr)) => {
                let st = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_control(st, stream, addr).await {
                        warn!(%addr, error = %e, "control disconnected");
                    }
                });
            }
            Err(e) => warn!(error = %e, "control accept"),
        }
    }
}

async fn run_metrics_server(
    listener: TcpListener,
    state: RelayState,
    dashboard_dir: Option<PathBuf>,
) -> Result<()> {
    let api = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/metrics/ticks", get(ticks_handler))
        .route("/metrics/history", get(history_handler))
        .route("/health", get(|| async { "ok" }))
        .with_state(state);

    let app = match dashboard_dir.filter(|p| p.is_dir()) {
        Some(dir) => {
            let index = dir.join("index.html");
            if index.is_file() {
                info!(path = %dir.display(), "serving dashboard static files");
            } else {
                warn!(path = %index.display(), "dashboard_dir set but index.html missing");
            }
            Router::new().merge(api).fallback_service(
                ServeDir::new(dir.clone()).not_found_service(ServeFile::new(index)),
            )
        }
        None => api,
    };

    let app = app.layer(CorsLayer::permissive());

    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    since_ms: Option<u64>,
    minutes: Option<u64>,
}

async fn history_handler(
    axum::extract::State(state): axum::extract::State<RelayState>,
    Query(q): Query<HistoryQuery>,
) -> Json<HistoryResponse> {
    let since_ms = q.since_ms.or_else(|| {
        q.minutes
            .map(|m| unix_ms().saturating_sub(m.saturating_mul(60_000)))
    });

    let points = if let Some(since) = since_ms {
        state.history.list_since(since).await
    } else {
        state.history.list_all().await
    };

    Json(HistoryResponse {
        interval_ms: state.history_interval_ms,
        points,
    })
}

async fn run_history_sampler(state: RelayState, interval_ms: u64) {
    let interval_ms = interval_ms.max(500);
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    ticker.tick().await;

    loop {
        ticker.tick().await;
        let ts = unix_ms();
        let rates = state.history.last_rates().await;
        let mut snap = state.metrics.snapshot(rates).await;
        let match_queue = state.matcher.waiting_count().await as u64;
        snap.consumers_queued = match_queue;
        snap.consumers_waiting = match_queue;
        state.metrics_cache.store(snap.clone()).await;
        state
            .history
            .record(
                ts,
                snap.producer_backlog_messages,
                snap.producer_pending_messages,
                snap.producer_ring_messages,
                snap.consumer_inflight,
                snap.transfers_total,
                snap.transfer_bytes_total,
                snap.batches_total,
                snap.delivery_failures_total,
                snap.producer_drops_total,
                snap.producers_ready,
                snap.producers_busy,
                match_queue,
            )
            .await;
    }
}

/// Matcher extra — não depender só de PRDY/CRDY/HBRP + sampler 2s.
async fn run_match_loop(state: RelayState) {
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if state.matcher.waiting_count().await == 0 {
            continue;
        }
        state.matcher.sync_ready_pool().await;
        let _ = state.matcher.try_match().await;
    }
}

async fn metrics_handler(
    axum::extract::State(state): axum::extract::State<RelayState>,
) -> Json<MetricsSnapshot> {
    let mut snap = if let Some(cached) = state.metrics_cache.get().await {
        (*cached).clone()
    } else {
        let rates = state.history.last_rates().await;
        state.metrics.snapshot(rates).await
    };
    let queued = state.matcher.waiting_count().await as u64;
    snap.consumers_queued = queued;
    snap.consumers_waiting = queued;
    Json(snap)
}

#[derive(Serialize)]
struct TicksResponse {
    ticks: Vec<crate::tick_buffer::TransferTick>,
}

async fn ticks_handler(
    axum::extract::State(state): axum::extract::State<RelayState>,
) -> Json<TicksResponse> {
    Json(TicksResponse {
        ticks: state.metrics.drain_ticks().await,
    })
}

async fn handle_control(state: RelayState, stream: TcpStream, addr: SocketAddr) -> Result<()> {
    tune_tcp(&stream);
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::with_capacity(CONTROL_READ_BUF, read_half);

    let mut magic = [0u8; 4];
    crate::protocol::read_exact_async(&mut reader, &mut magic).await?;

    if magic == MAGIC_REGISTER {
        handle_consumer(state, reader, write_half, addr).await
    } else if is_producer_magic(magic) {
        handle_producer(state, reader, write_half, addr, magic).await
    } else {
        let err = encode_error(ERR_PROTOCOL, "expected REG! or producer frame");
        let mut write_half = write_half;
        let _ = write_half.write_all(&err).await;
        bail!("unknown control magic {:?}", &magic);
    }
}

fn is_producer_magic(magic: [u8; 4]) -> bool {
    matches!(
        magic,
        MAGIC_PRODUCER_READY | MAGIC_HEARTBEAT | MAGIC_DELIVERED | MAGIC_FAILED
    )
}

async fn handle_producer(
    state: RelayState,
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    write_half: tokio::net::tcp::OwnedWriteHalf,
    addr: SocketAddr,
    first_magic: [u8; 4],
) -> Result<()> {
    let producer_id = state
        .producers
        .register(addr.to_string(), write_half)
        .await;

    let cleanup = ProducerCleanup {
        state: state.clone(),
        producer_id,
    };

    let mut reader = reader;
    process_producer_magic(&state, producer_id, &mut reader, first_magic).await?;
    let result = producer_read_loop(&state, producer_id, &mut reader).await;
    drop(cleanup);
    result
}

struct ProducerCleanup {
    state: RelayState,
    producer_id: u64,
}

impl Drop for ProducerCleanup {
    fn drop(&mut self) {
        let matcher = self.state.matcher.clone();
        let consumers = self.state.consumers.clone();
        let id = self.producer_id;
        tokio::spawn(async move {
            matcher
                .on_producer_disconnect(id, consumers.as_ref())
                .await;
        });
    }
}

async fn producer_read_loop(
    state: &RelayState,
    producer_id: u64,
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<()> {
    loop {
        let mut magic = [0u8; 4];
        crate::protocol::read_exact_async(reader, &mut magic).await?;
        process_producer_magic(state, producer_id, reader, magic).await?;
    }
}

async fn process_producer_magic(
    state: &RelayState,
    producer_id: u64,
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    magic: [u8; 4],
) -> Result<()> {
    match magic {
        MAGIC_PRODUCER_READY => {
            state
                .matcher
                .on_producer_ready(producer_id)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
        }
        MAGIC_HEARTBEAT => {
            let hb = read_heartbeat_payload(reader).await?;
            let rematch = state.producers.record_heartbeat(producer_id, hb).await;
            if rematch {
                state.matcher.wake_producer(producer_id).await;
            } else if matches!(
                state.producers.state_of(producer_id).await,
                Some(crate::producers::ProducerState::Ready)
            ) && state.matcher.waiting_count().await > 0
            {
                let _ = state.matcher.try_match().await;
            }
        }
        MAGIC_DELIVERED => {
            let report = read_delivery_report_payload(reader).await?;
            finish_producer_batch(state, producer_id, &report, true, None).await;
        }
        MAGIC_FAILED => {
            let report = read_failure_report_payload(reader).await?;
            warn!(
                producer_id,
                msg_count = report.msg_count,
                code = report.code,
                message = %report.message,
                "producer batch failed"
            );
            finish_producer_batch(
                state,
                producer_id,
                &crate::protocol::DeliveryReport {
                    msg_count: report.msg_count,
                    batch_bytes: report.batch_bytes,
                },
                false,
                Some(report.message.clone()),
            )
            .await;
        }
        _ => bail!("unknown producer magic {:?}", &magic),
    }
    Ok(())
}

async fn finish_producer_batch(
    state: &RelayState,
    producer_id: u64,
    report: &crate::protocol::DeliveryReport,
    ok: bool,
    fail_message: Option<String>,
) {
    let msg_count = report.msg_count as u64;
    let batch_bytes = report.batch_bytes as u64;
    let mut consumer_id = state
        .producers
        .take_pending_consumer(producer_id)
        .await
        .unwrap_or(0);
    if consumer_id == 0 {
        consumer_id = state
            .consumers
            .find_id_by_assigned_producer(producer_id)
            .await
            .unwrap_or(0);
    }

    if consumer_id > 0 {
        if ok {
            state
                .consumers
                .finish_batch(consumer_id, msg_count, batch_bytes, unix_ms(), msg_count == 0)
                .await;
        } else {
            state.consumers.set_unacked(consumer_id, 0).await;
            state.consumers.set_phase(consumer_id, "delivery_failed").await;
        }
        let (producer_state, consumer_state) = if ok {
            ("busy", "processing")
        } else {
            ("busy", "delivery_failed")
        };
        state
            .metrics
            .record_transfer_tick(
                producer_id,
                consumer_id,
                msg_count,
                batch_bytes,
                ok,
                fail_message,
                producer_state,
                consumer_state,
            )
            .await;
    }

    if ok {
        state.producers.record_delivery(producer_id, msg_count).await;
    } else {
        state.producers.record_failure(producer_id).await;
    }
    state.matcher.on_producer_done(producer_id).await;
}

struct ConsumerSession {
    id: u64,
    registry: Arc<ConsumerRegistry>,
    matcher: Arc<Matchmaker>,
}

impl Drop for ConsumerSession {
    fn drop(&mut self) {
        let registry = self.registry.clone();
        let matcher = self.matcher.clone();
        let id = self.id;
        tokio::spawn(async move {
            matcher.cancel_consumer(id).await;
            registry.remove(id).await;
        });
    }
}

async fn handle_consumer(
    state: RelayState,
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    mut write_half: tokio::net::tcp::OwnedWriteHalf,
    addr: SocketAddr,
) -> Result<()> {
    let mut reader = reader;

    let data_addr = match read_register_payload(&mut reader).await {
        Ok(a) => a,
        Err(e) => {
            let err = encode_error(ERR_PROTOCOL, "expected REG!");
            let _ = write_half.write_all(&err).await;
            return Err(e);
        }
    };

    let data_addr: Arc<str> = Arc::from(data_addr);
    let consumer_id = state
        .consumers
        .register(addr.to_string(), Arc::clone(&data_addr))
        .await;
    let _session = ConsumerSession {
        id: consumer_id,
        registry: state.consumers.clone(),
        matcher: state.matcher.clone(),
    };

    info!(%addr, data_addr = %data_addr, consumer_id, "consumer registered");

    loop {
        let ready = match read_consumer_ready_request(&mut reader).await {
            Ok(r) => r,
            Err(e) => {
                let err = encode_error(ERR_PROTOCOL, "expected CRDY");
                let _ = write_half.write_all(&err).await;
                return Err(e);
            }
        };

        state
            .consumers
            .begin_cycle(
                consumer_id,
                ready.max_items,
                &ready.consumer_tag,
                unix_ms(),
            )
            .await;

        state
            .matcher
            .enqueue_consumer(consumer_id, ready, Arc::clone(&data_addr))
            .await;

        if let Err(e) = state.matcher.try_match().await {
            warn!(consumer_id, error = %e, "try_match after CRDY");
        }
    }
}
