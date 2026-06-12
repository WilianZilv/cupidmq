use bytes::Bytes;
use std::time::{Duration, Instant};

/// Accumulates payloads by size; flush on `max_bytes` or timeout.
pub struct BatchAccumulator {
    items: Vec<Bytes>,
    bytes: usize,
    max_bytes: usize,
    flush_timeout: Duration,
    opened_at: Option<Instant>,
}

impl BatchAccumulator {
    pub fn new(max_bytes: usize, flush_timeout: Duration) -> Self {
        Self {
            items: Vec::new(),
            bytes: 0,
            max_bytes: max_bytes.max(1),
            flush_timeout,
            opened_at: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn pending_bytes(&self) -> usize {
        self.bytes
    }

    pub fn pending_items(&self) -> usize {
        self.items.len()
    }

    /// Returns batches ready to send (0–2: prior flush + oversized item).
    pub fn push(&mut self, item: Bytes) -> Vec<Vec<Bytes>> {
        let item_len = item.len();
        if item_len > self.max_bytes {
            let mut out = Vec::new();
            if let Some(batch) = self.take_batch() {
                out.push(batch);
            }
            out.push(vec![item]);
            return out;
        }

        let mut out = Vec::new();
        if self.bytes.saturating_add(item_len) > self.max_bytes {
            if let Some(batch) = self.take_batch() {
                out.push(batch);
            }
        }

        if self.opened_at.is_none() {
            self.opened_at = Some(Instant::now());
        }
        self.bytes = self.bytes.saturating_add(item_len);
        self.items.push(item);
        out
    }

    pub fn flush_if_timeout(&mut self) -> Option<Vec<Bytes>> {
        if self.items.is_empty() {
            return None;
        }
        let opened = self.opened_at?;
        if opened.elapsed() < self.flush_timeout {
            return None;
        }
        self.take_batch()
    }

    pub fn take_batch(&mut self) -> Option<Vec<Bytes>> {
        if self.items.is_empty() {
            return None;
        }
        self.bytes = 0;
        self.opened_at = None;
        Some(std::mem::take(&mut self.items))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_on_size() {
        let mut acc = BatchAccumulator::new(10, Duration::from_secs(60));
        assert!(acc.push(Bytes::from_static(&[1; 6])).is_empty());
        let batches = acc.push(Bytes::from_static(&[2; 6]));
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[0][0].len(), 6);
        assert_eq!(acc.pending_items(), 1);
    }

    #[test]
    fn oversized_item_own_batch() {
        let mut acc = BatchAccumulator::new(10, Duration::from_secs(60));
        acc.push(Bytes::from_static(&[1; 4]));
        let batches = acc.push(Bytes::from_static(&[9; 20]));
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[1][0].len(), 20);
    }
}
