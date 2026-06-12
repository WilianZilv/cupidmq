use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize)]
pub struct HistoryPoint {
    pub ts: u64,
    pub producer_backlog: u64,
    pub producer_pending: u64,
    pub producer_ring: u64,
    pub consumer_inflight: u64,
    pub transfer_per_sec: f64,
    pub batches_per_sec: f64,
    pub bytes_per_sec: f64,
    pub delivery_failures_per_sec: f64,
    pub drops_per_sec: f64,
    pub drops_total: u64,
    pub producers_ready: u64,
    pub producers_busy: u64,
    pub consumers_waiting: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistoryResponse {
    pub interval_ms: u64,
    pub points: Vec<HistoryPoint>,
}

#[derive(Default, Clone, Copy)]
struct RateCounters {
    ts: u64,
    transfers_total: u64,
    transfer_bytes_total: u64,
    batches_total: u64,
    delivery_failures_total: u64,
    drops_total: u64,
}

pub struct MetricsHistory {
    points: Mutex<Vec<HistoryPoint>>,
    last: Mutex<Option<RateCounters>>,
    cap: usize,
}

impl MetricsHistory {
    pub fn new(cap: usize) -> Self {
        Self {
            points: Mutex::new(Vec::with_capacity(cap.min(256))),
            last: Mutex::new(None),
            cap: cap.max(1),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn record(
        &self,
        ts: u64,
        producer_backlog: u64,
        producer_pending: u64,
        producer_ring: u64,
        consumer_inflight: u64,
        transfers_total: u64,
        transfer_bytes_total: u64,
        batches_total: u64,
        delivery_failures_total: u64,
        drops_total: u64,
        producers_ready: u64,
        producers_busy: u64,
        consumers_waiting: u64,
    ) {
        let (
            transfer_per_sec,
            batches_per_sec,
            bytes_per_sec,
            delivery_failures_per_sec,
            drops_per_sec,
        ) = {
            let mut prev = self.last.lock().await;
            let rates = if let Some(p) = *prev {
                let dt_ms = ts.saturating_sub(p.ts);
                if dt_ms > 0 {
                    let dt = dt_ms as f64 / 1000.0;
                    (
                        rate(transfers_total, p.transfers_total, dt),
                        rate(batches_total, p.batches_total, dt),
                        rate_f64(transfer_bytes_total as f64, p.transfer_bytes_total as f64, dt),
                        rate(delivery_failures_total, p.delivery_failures_total, dt),
                        rate(drops_total, p.drops_total, dt),
                    )
                } else {
                    (0.0, 0.0, 0.0, 0.0, 0.0)
                }
            } else {
                (0.0, 0.0, 0.0, 0.0, 0.0)
            };
            *prev = Some(RateCounters {
                ts,
                transfers_total,
                transfer_bytes_total,
                batches_total,
                delivery_failures_total,
                drops_total,
            });
            rates
        };

        let point = HistoryPoint {
            ts,
            producer_backlog,
            producer_pending,
            producer_ring,
            consumer_inflight,
            transfer_per_sec,
            batches_per_sec,
            bytes_per_sec,
            delivery_failures_per_sec,
            drops_per_sec,
            drops_total,
            producers_ready,
            producers_busy,
            consumers_waiting,
        };

        let mut points = self.points.lock().await;
        points.push(point);
        if points.len() > self.cap {
            let drop = points.len() - self.cap;
            points.drain(0..drop);
        }
    }

    pub async fn list_since(&self, since_ms: u64) -> Vec<HistoryPoint> {
        let points = self.points.lock().await;
        points
            .iter()
            .filter(|p| p.ts >= since_ms)
            .cloned()
            .collect()
    }

    pub async fn list_all(&self) -> Vec<HistoryPoint> {
        self.points.lock().await.clone()
    }

    /// Últimas taxas amostradas — mesma fonte do gráfico /metrics/history.
    pub async fn last_rates(&self) -> (f64, f64, f64) {
        let points = self.points.lock().await;
        if let Some(p) = points.last() {
            (p.transfer_per_sec, p.batches_per_sec, p.bytes_per_sec)
        } else {
            (0.0, 0.0, 0.0)
        }
    }
}

fn rate(now: u64, before: u64, dt_sec: f64) -> f64 {
    now.saturating_sub(before) as f64 / dt_sec
}

fn rate_f64(now: f64, before: f64, dt_sec: f64) -> f64 {
    (now - before).max(0.0) / dt_sec
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ring_buffer_respects_cap() {
        let h = MetricsHistory::new(3);
        for i in 0..5 {
            h.record(
                i * 1000,
                i,
                i / 2,
                i / 2,
                0,
                i * 10,
                i * 1000,
                i,
                0,
                0,
                1,
                0,
                0,
            )
            .await;
        }
        let all = h.list_all().await;
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].ts, 2000);
    }
}
