mod byte_ring;

pub use byte_ring::{ByteRing, PushResult};
pub use byte_ring::ByteRing as InnerRing;

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct QueuedMessage {
    pub payload: Bytes,
    pub enqueued_ms: u64,
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
        assert_eq!(msg.enqueued_ms, 200);
    }
}
