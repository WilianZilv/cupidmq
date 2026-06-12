use serde::Serialize;
use std::collections::VecDeque;
use tokio::sync::Mutex;

pub const TICK_BUFFER_CAP: usize = 65_536;

#[derive(Debug, Clone, Serialize)]
pub struct TransferTick {
    pub ts_ms: u64,
    pub producer_id: u64,
    pub consumer_id: u64,
    pub msg_count: u64,
    pub batch_bytes: u64,
    /// `assign` | `deliver` | `fail`
    pub phase: String,
    pub ok: bool,
    /// Preenchido em `fail` — mensagem do producer (TCP/timeout).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fail_message: Option<String>,
    pub producer_state: String,
    pub consumer_state: String,
}

pub struct TickBuffer {
    inner: Mutex<VecDeque<TransferTick>>,
    cap: usize,
}

impl TickBuffer {
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(cap.min(256))),
            cap: cap.clamp(1, TICK_BUFFER_CAP),
        }
    }

    pub async fn push(&self, tick: TransferTick) {
        let mut q = self.inner.lock().await;
        if q.len() >= self.cap {
            q.pop_front();
        }
        q.push_back(tick);
    }

    pub async fn drain(&self) -> Vec<TransferTick> {
        let mut q = self.inner.lock().await;
        q.drain(..).collect()
    }

    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn respects_cap() {
        let buf = TickBuffer::new(3);
        for i in 0..5u64 {
            buf.push(TransferTick {
                ts_ms: i,
                producer_id: 1,
                consumer_id: 2,
                msg_count: 1,
                batch_bytes: 10,
                phase: "deliver".into(),
                ok: true,
                fail_message: None,
                producer_state: "busy".into(),
                consumer_state: "processing".into(),
            })
            .await;
        }
        assert_eq!(buf.len().await, 3);
        let drained = buf.drain().await;
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].ts_ms, 2);
    }
}
