//! Cliente producer — ring local, PRDY/ASGN/DELV, TCP BATC → consumer.

mod accumulator;
mod client;
mod delivery_pool;

pub use client::{
    Producer, ProducerConfig, ProducerStats, DEFAULT_DELIVERY_TIMEOUT_SECS,
    DEFAULT_FLUSH_TIMEOUT_MS, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_OUTBOUND_MAX_BYTES, DEFAULT_RECONNECT_DELAY_SECS,
};
pub use delivery_pool::DEFAULT_DELIVERY_IDLE_SECS;
