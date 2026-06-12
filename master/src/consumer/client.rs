use crate::protocol::{
    encode_consumer_ready, encode_register, parse_batch_payload_items, read_batch_frame, tune_tcp,
};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time;

pub const DEFAULT_MAX_BATCH_SIZE_COUNT: u16 = 32;
pub const DEFAULT_PREFETCH_BATCH_COUNT: usize = 4;
pub const DEFAULT_BATCH_TIMEOUT_SECS: u64 = 10;
pub const DEFAULT_DATA_IDLE_SECS: u64 = 60;
pub const DEFAULT_RECONNECT_DELAY_SECS: u64 = 2;
/// Retry while master restarts — mirrors producer (~50ms loop).
const CONNECT_RETRY_MS: u64 = 100;

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub master: String,
    /// Address advertised in REG! (producer connects here).
    pub data_addr: String,
    /// BATC bind local — default `0.0.0.0:port` (port from `data_addr`); override when needed.
    pub bind_addr: Option<String>,
    pub consumer_tag: String,
    pub max_batch_size_count: u16,
    pub prefetch_batch_count: usize,
    pub batch_timeout_secs: u64,
    pub data_idle_secs: u64,
    pub reconnect: bool,
    pub reconnect_delay_secs: u64,
}

impl ConsumerConfig {
    /// `data_addr` required `host:port` — explicit BATC port, no default.
    pub fn new(master: impl Into<String>, data_addr: impl Into<String>) -> Result<Self> {
        let master = master.into();
        let data_addr = data_addr.into();
        if master.is_empty() {
            bail!("master address required");
        }
        if data_addr.is_empty() {
            bail!("data_addr required (host:port)");
        }
        parse_host_port(&data_addr).context("data_addr must be host:port")?;
        Ok(Self {
            master,
            data_addr,
            bind_addr: None,
            consumer_tag: String::new(),
            max_batch_size_count: DEFAULT_MAX_BATCH_SIZE_COUNT,
            prefetch_batch_count: DEFAULT_PREFETCH_BATCH_COUNT,
            batch_timeout_secs: DEFAULT_BATCH_TIMEOUT_SECS,
            data_idle_secs: DEFAULT_DATA_IDLE_SECS,
            reconnect: true,
            reconnect_delay_secs: DEFAULT_RECONNECT_DELAY_SECS,
        })
    }

    pub fn bind_addr(mut self, addr: impl Into<String>) -> Result<Self> {
        let addr = addr.into();
        parse_host_port(&addr).context("bind_addr must be host:port")?;
        self.bind_addr = Some(addr);
        Ok(self)
    }

    pub fn consumer_tag(mut self, tag: impl Into<String>) -> Self {
        self.consumer_tag = tag.into();
        self
    }

    pub fn max_batch_size_count(mut self, max_items: u16) -> Self {
        self.max_batch_size_count = max_items;
        self
    }

    pub fn prefetch_batch_count(mut self, count: usize) -> Self {
        self.prefetch_batch_count = count.max(1);
        self
    }

    pub fn batch_timeout_secs(mut self, secs: u64) -> Self {
        self.batch_timeout_secs = secs.max(1);
        self
    }

    pub fn data_idle_secs(mut self, secs: u64) -> Self {
        self.data_idle_secs = secs.max(1);
        self
    }

    pub fn reconnect(mut self, on: bool) -> Self {
        self.reconnect = on;
        self
    }

    pub fn reconnect_delay_secs(mut self, secs: u64) -> Self {
        self.reconnect_delay_secs = secs.max(1);
        self
    }

    /// Local BATC socket — `bind_addr` or `0.0.0.0:<port from data_addr>`.
    pub fn resolve_bind(&self) -> Result<(String, u16)> {
        if let Some(bind) = self.bind_addr.as_deref() {
            return parse_host_port(bind);
        }
        let (_, port) = parse_host_port(&self.data_addr)?;
        Ok(("0.0.0.0".to_string(), port))
    }
}

#[derive(Debug, Default)]
pub struct ConsumerStats {
    pub batches_in: AtomicU64,
    pub msgs_in: AtomicU64,
    pub reconnects: AtomicU64,
    pub drops: AtomicU64,
    pub empty_waits: AtomicU64,
    pub stale_batches: AtomicU64,
}

/// `asyncio.Event` — same contract as Python (`set` / `clear` / `wait`).
struct EventGate {
    raised: AtomicBool,
    notify: Notify,
}

impl EventGate {
    fn new(raised: bool) -> Self {
        Self {
            raised: AtomicBool::new(raised),
            notify: Notify::new(),
        }
    }

    fn set(&self) {
        self.raised.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    fn clear(&self) {
        self.raised.store(false, Ordering::Release);
    }

    async fn wait(&self) {
        loop {
            if self.raised.load(Ordering::Acquire) {
                return;
            }
            self.notify.notified().await;
        }
    }

    async fn wait_and_clear(&self) {
        self.wait().await;
        self.clear();
    }
}

struct DataPlane {
    accept_batch: AtomicBool,
    batch_ready: EventGate,
    pending: Mutex<Option<Vec<Bytes>>>,
    stale_batches: AtomicU64,
}

impl DataPlane {
    fn new() -> Self {
        Self {
            accept_batch: AtomicBool::new(false),
            batch_ready: EventGate::new(false),
            pending: Mutex::new(None),
            stale_batches: AtomicU64::new(0),
        }
    }

    fn set_accept_batch(&self, on: bool) {
        self.accept_batch.store(on, Ordering::Release);
    }

    fn accept_batch(&self) -> bool {
        self.accept_batch.load(Ordering::Acquire)
    }

    async fn store_batch(&self, items: Vec<Bytes>) {
        *self.pending.lock().await = Some(items);
        self.batch_ready.set();
    }
}

struct Shared {
    config: ConsumerConfig,
    stats: Arc<ConsumerStats>,
    data: Arc<DataPlane>,
    prefetch: Arc<EventGate>,
    stop: Arc<AtomicBool>,
    out_tx: mpsc::Sender<Option<Vec<Bytes>>>,
}

fn parse_host_port(addr: &str) -> Result<(String, u16)> {
    let (host, port_s) = addr
        .rsplit_once(':')
        .context("address must be host:port")?;
    if host.is_empty() {
        bail!("address host required");
    }
    let port: u16 = port_s.parse().context("invalid port")?;
    Ok((host.to_string(), port))
}

fn parse_master_addr(addr: &str) -> Result<(String, u16)> {
    parse_host_port(addr).or_else(|_| {
        Ok((addr.to_string(), 9750))
    })
}

/// Consumer client — [`consume`](Self::consume) iterates batches; background handles REG!/CRDY/BATC.
pub struct Consumer {
    rx: mpsc::Receiver<Option<Vec<Bytes>>>,
    prefetch: Arc<EventGate>,
    stats: Arc<ConsumerStats>,
    stop: Arc<AtomicBool>,
    _data: JoinHandle<()>,
    _control: JoinHandle<()>,
}

impl Consumer {
    pub async fn connect(config: ConsumerConfig) -> Result<Self> {
        let prefetch = config.prefetch_batch_count.max(1);
        let (out_tx, rx) = mpsc::channel(prefetch);
        let stats = Arc::new(ConsumerStats::default());
        let data = Arc::new(DataPlane::new());
        let stop = Arc::new(AtomicBool::new(false));
        let prefetch = Arc::new(EventGate::new(true));

        let shared = Arc::new(Shared {
            config,
            stats: stats.clone(),
            data: data.clone(),
            prefetch: prefetch.clone(),
            stop: stop.clone(),
            out_tx,
        });

        let data_task = {
            let shared = shared.clone();
            tokio::spawn(async move { run_data_plane(shared).await })
        };
        let control_task = {
            let shared = shared.clone();
            tokio::spawn(async move { run_control_plane(shared).await })
        };

        Ok(Self {
            rx,
            prefetch,
            stats,
            stop,
            _data: data_task,
            _control: control_task,
        })
    }

    pub fn stats(&self) -> &ConsumerStats {
        &self.stats
    }

    /// Iterates batches — after each handler, releases next CRDY (same as Python `consume()`).
    pub async fn consume<F, Fut>(&mut self, mut handler: F) -> Result<()>
    where
        F: FnMut(Vec<Bytes>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        while let Some(batch) = self.pull_batch().await {
            handler(batch).await?;
            self.release_prefetch_slot();
        }
        Ok(())
    }

    async fn pull_batch(&mut self) -> Option<Vec<Bytes>> {
        match self.rx.recv().await {
            Some(Some(batch)) => Some(batch),
            _ => None,
        }
    }

    fn release_prefetch_slot(&self) {
        self.prefetch.set();
    }

    pub fn close(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.close();
    }
}

async fn run_data_plane(shared: Arc<Shared>) {
    let (host, port) = match shared.config.resolve_bind() {
        Ok(v) => v,
        Err(_) => return,
    };
    let listener = match TcpListener::bind((host.as_str(), port)).await {
        Ok(l) => l,
        Err(_) => return,
    };

    loop {
        if shared.stop.load(Ordering::Acquire) {
            break;
        }
        let accept = listener.accept().await;
        let Ok((stream, _)) = accept else {
            continue;
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            serve_data_connection(shared, stream).await;
        });
    }
}

/// One task per BATC connection — same as Python `asyncio.start_server`.
async fn serve_data_connection(shared: Arc<Shared>, mut stream: TcpStream) {
    tune_tcp(&stream);
    let idle = Duration::from_secs(shared.config.data_idle_secs);
    loop {
        if shared.stop.load(Ordering::Acquire) {
            break;
        }
        let read = time::timeout(idle, read_batch_frame(&mut stream)).await;
        let Ok(Ok(payload)) = read else {
            break;
        };
        let Ok(items) = parse_batch_payload_items(Bytes::from(payload)) else {
            break;
        };
        if !shared.data.accept_batch() {
            shared
                .data
                .stale_batches
                .fetch_add(1, Ordering::Relaxed);
            shared.stats.stale_batches.store(
                shared.data.stale_batches.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            continue;
        }
        shared.data.store_batch(items).await;
    }
}

fn connect_retry_interval(config: &ConsumerConfig) -> Duration {
    Duration::from_millis(
        CONNECT_RETRY_MS.min(config.reconnect_delay_secs.saturating_mul(1000)),
    )
}

fn spawn_session_watchdog(mut read: OwnedReadHalf, dead: Arc<Notify>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = [0u8; 1];
        match read.read(&mut buf).await {
            Ok(0) | Err(_) => dead.notify_one(),
            Ok(_) => dead.notify_one(),
        }
    })
}

async fn run_control_plane(shared: Arc<Shared>) {
    let retry = connect_retry_interval(&shared.config);
    let (master_host, master_port) = match parse_master_addr(&shared.config.master) {
        Ok(v) => v,
        Err(_) => return,
    };

    while !shared.stop.load(Ordering::Acquire) {
        let mut stream = loop {
            match TcpStream::connect((master_host.as_str(), master_port)).await {
                Ok(s) => break s,
                Err(_) => {
                    shared.stats.drops.fetch_add(1, Ordering::Relaxed);
                    if !shared.config.reconnect || shared.stop.load(Ordering::Acquire) {
                        let _ = shared.out_tx.send(None).await;
                        return;
                    }
                    time::sleep(retry).await;
                }
            }
        };
        tune_tcp(&stream);

        let reg = match encode_register(&shared.config.data_addr) {
            Ok(f) => f,
            Err(_) => {
                shared.stats.drops.fetch_add(1, Ordering::Relaxed);
                if !shared.config.reconnect || shared.stop.load(Ordering::Acquire) {
                    break;
                }
                time::sleep(retry).await;
                continue;
            }
        };
        if stream.write_all(&reg).await.is_err() || stream.flush().await.is_err() {
            shared.stats.drops.fetch_add(1, Ordering::Relaxed);
            if !shared.config.reconnect || shared.stop.load(Ordering::Acquire) {
                break;
            }
            time::sleep(retry).await;
            shared.stats.reconnects.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        let (read_half, write_half) = stream.into_split();
        let session_dead = Arc::new(Notify::new());
        let watchdog = spawn_session_watchdog(read_half, session_dead.clone());

        if run_control_session(shared.clone(), write_half, session_dead)
            .await
            .is_err()
        {
            shared.stats.drops.fetch_add(1, Ordering::Relaxed);
        }
        watchdog.abort();

        if !shared.config.reconnect || shared.stop.load(Ordering::Acquire) {
            break;
        }
        shared.prefetch.set();
        time::sleep(retry).await;
        shared.stats.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    let _ = shared.out_tx.send(None).await;
}

/// Mirrors `ConsumerRuntime._session` / `fetch_loop` in `_consumer.py`.
async fn run_control_session(
    shared: Arc<Shared>,
    mut stream: OwnedWriteHalf,
    session_dead: Arc<Notify>,
) -> Result<()> {
    loop {
        if shared.stop.load(Ordering::Acquire) {
            break;
        }

        tokio::select! {
            _ = shared.prefetch.wait_and_clear() => {}
            _ = session_dead.notified() => {
                bail!("master control disconnected");
            }
        }
        if shared.stop.load(Ordering::Acquire) {
            break;
        }

        shared.data.batch_ready.clear();
        *shared.data.pending.lock().await = None;

        let frame = encode_consumer_ready(
            shared.config.max_batch_size_count,
            &shared.config.consumer_tag,
        )?;
        stream.write_all(&frame).await?;
        stream.flush().await?;
        shared.data.set_accept_batch(true);

        let batc = tokio::select! {
            biased;
            _ = session_dead.notified() => {
                shared.data.set_accept_batch(false);
                bail!("master control disconnected");
            }
            _ = shared.data.batch_ready.wait() => true,
            _ = time::sleep(Duration::from_secs(shared.config.batch_timeout_secs)) => false,
        };
        shared.data.set_accept_batch(false);

        if !batc {
            shared.stats.empty_waits.fetch_add(1, Ordering::Relaxed);
            shared.prefetch.set();
            continue;
        }

        shared.stats.stale_batches.store(
            shared.data.stale_batches.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        let batch = shared.data.pending.lock().await.take().unwrap_or_default();
        if batch.is_empty() {
            shared.stats.empty_waits.fetch_add(1, Ordering::Relaxed);
            shared.prefetch.set();
            continue;
        }

        shared.stats.batches_in.fetch_add(1, Ordering::Relaxed);
        shared
            .stats
            .msgs_in
            .fetch_add(batch.len() as u64, Ordering::Relaxed);

        // Blocks when queue full — next CRDY only after `consume()` → prefetch.set()
        if shared.out_tx.send(Some(batch)).await.is_err() {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod bind_tests {
    use super::ConsumerConfig;

    #[test]
    fn resolve_bind_defaults_to_all_interfaces() {
        let cfg = ConsumerConfig::new("127.0.0.1:9750", "192.168.1.10:9760").unwrap();
        let (host, port) = cfg.resolve_bind().unwrap();
        assert_eq!(host, "0.0.0.0");
        assert_eq!(port, 9760);
    }

    #[test]
    fn resolve_bind_honors_explicit_bind_addr() {
        let cfg = ConsumerConfig::new("127.0.0.1:9750", "127.0.0.1:9760")
            .unwrap()
            .bind_addr("0.0.0.0:9761")
            .unwrap();
        let (host, port) = cfg.resolve_bind().unwrap();
        assert_eq!(host, "0.0.0.0");
        assert_eq!(port, 9761);
    }
}
