use bytes::Bytes;
use std::collections::VecDeque;

use super::QueuedMessage;

struct RingEntry {
    payload: Bytes,
    enqueued_ms: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PushResult {
    pub dropped: u32,
    pub accepted: bool,
}

/// Fila byte-capped com drop oldest — base do ring do server e do producer.
pub struct ByteRing {
    queue: VecDeque<RingEntry>,
    bytes: usize,
    max_bytes: usize,
    pub drops_total: u64,
}

impl ByteRing {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            bytes: 0,
            max_bytes: max_bytes.max(1),
            drops_total: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn byte_len(&self) -> usize {
        self.bytes
    }

    pub fn head_age_ms(&self, now_ms: u64) -> u64 {
        self.queue
            .front()
            .map(|e| now_ms.saturating_sub(e.enqueued_ms))
            .unwrap_or(0)
    }

    /// Push back; drop oldest até caber.
    pub fn push(&mut self, payload: Bytes, enqueued_ms: u64) -> PushResult {
        if payload.is_empty() {
            return PushResult {
                dropped: 0,
                accepted: true,
            };
        }
        if payload.len() > self.max_bytes {
            self.drops_total += 1;
            return PushResult {
                dropped: 1,
                accepted: false,
            };
        }
        let mut dropped = 0u32;
        while self.bytes.saturating_add(payload.len()) > self.max_bytes && !self.queue.is_empty() {
            if self.pop_front_inner().is_some() {
                dropped += 1;
                self.drops_total += 1;
            }
        }

        let len = payload.len();
        self.bytes = self.bytes.saturating_add(len);
        self.queue.push_back(RingEntry {
            payload,
            enqueued_ms,
        });
        PushResult {
            dropped,
            accepted: true,
        }
    }

    pub fn pop_front(&mut self) -> Option<QueuedMessage> {
        self.pop_front_inner()
            .map(|e| QueuedMessage {
                payload: e.payload,
                enqueued_ms: e.enqueued_ms,
            })
    }

    pub fn pop_up_to(&mut self, max_items: usize, now_ms: u64) -> Vec<QueuedMessage> {
        let take = max_items.min(self.queue.len());
        let mut out = Vec::with_capacity(take);
        for _ in 0..take {
            if let Some(entry) = self.pop_front_inner() {
                out.push(QueuedMessage {
                    payload: entry.payload,
                    enqueued_ms: entry.enqueued_ms,
                });
            }
        }
        let _ = now_ms;
        out
    }

    /// Requeue no head; drop newest (back) se estourar cap de bytes.
    pub fn push_front(&mut self, msg: QueuedMessage) -> u32 {
        let len = msg.payload.len();
        self.bytes = self.bytes.saturating_add(len);
        self.queue.push_front(RingEntry {
            payload: msg.payload,
            enqueued_ms: msg.enqueued_ms,
        });

        let mut dropped = 0u32;
        while self.bytes > self.max_bytes {
            if let Some(old) = self.queue.pop_back() {
                self.bytes = self.bytes.saturating_sub(old.payload.len());
                dropped += 1;
                self.drops_total += 1;
            } else {
                break;
            }
        }
        dropped
    }

    fn pop_front_inner(&mut self) -> Option<RingEntry> {
        let entry = self.queue.pop_front()?;
        self.bytes = self.bytes.saturating_sub(entry.payload.len());
        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_oldest_when_full() {
        let mut ring = ByteRing::new(2);
        ring.push(Bytes::from_static(&[1]), 100);
        ring.push(Bytes::from_static(&[2]), 200);
        ring.push(Bytes::from_static(&[3]), 300);
        assert_eq!(ring.drops_total, 1);
        assert_eq!(ring.len(), 2);
        let msg = ring.pop_front().unwrap();
        assert_eq!(&msg.payload[..], &[2]);
    }
}
