//! Keep-alive pool by `host:port` — one live BATC TCP connection per destination.

use anyhow::Result;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time;

use crate::protocol::tune_tcp;

pub const DEFAULT_DELIVERY_IDLE_SECS: u64 = 60;
const CONNECT_ATTEMPTS: u32 = 4;
const CONNECT_BACKOFF_MS: u64 = 5;

struct PooledConn {
    stream: TcpStream,
    idle_since: Instant,
}

/// One live BATC connection per destination (TCP keep-alive by host:port).
pub struct DeliveryPool {
    conns: HashMap<String, PooledConn>,
    idle_timeout: Duration,
}

impl DeliveryPool {
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            conns: HashMap::new(),
            idle_timeout,
        }
    }

    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    fn evict_stale(&mut self) {
        self.conns.retain(|_, c| c.idle_since.elapsed() < self.idle_timeout);
    }

    /// Removes invalid connection (BATC I/O error).
    pub fn invalidate(&mut self, addr: &str) {
        self.conns.remove(addr);
    }

    /// Takes connection from pool or `None` if expired/absent.
    pub fn take(&mut self, addr: &str) -> Option<TcpStream> {
        self.evict_stale();
        let Some(entry) = self.conns.remove(addr) else {
            return None;
        };
        if entry.idle_since.elapsed() >= self.idle_timeout {
            return None;
        }
        Some(entry.stream)
    }

    /// Returns live connection to pool (replaces previous).
    pub fn put(&mut self, addr: impl Into<String>, stream: TcpStream) {
        self.evict_stale();
        self.conns.insert(
            addr.into(),
            PooledConn {
                stream,
                idle_since: Instant::now(),
            },
        );
    }

    pub async fn get_or_connect(&mut self, addr: &str) -> Result<TcpStream> {
        if let Some(stream) = self.take(addr) {
            return Ok(stream);
        }
        connect_fresh(addr).await
    }
}

async fn connect_fresh(addr: &str) -> Result<TcpStream> {
    let mut last = None;
    for attempt in 0..CONNECT_ATTEMPTS {
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                tune_tcp(&stream);
                return Ok(stream);
            }
            Err(e) => {
                let wrapped =
                    anyhow::Error::new(e).context(format!("connect consumer data {addr}"));
                let retryable = wrapped
                    .chain()
                    .any(|cause| cause.downcast_ref::<std::io::Error>().is_some());
                last = Some(wrapped);
                if !retryable || attempt + 1 >= CONNECT_ATTEMPTS {
                    break;
                }
                time::sleep(Duration::from_millis(
                    CONNECT_BACKOFF_MS * (1 << attempt.min(3)),
                ))
                .await;
            }
        }
    }
    Err(last.unwrap())
}
