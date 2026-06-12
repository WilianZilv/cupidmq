//! Cliente consumer — REG!/CRDY + listener BATC.

mod client;

pub use client::{
    Consumer, ConsumerConfig, ConsumerStats, DEFAULT_MAX_BATCH_SIZE_COUNT,
    DEFAULT_BATCH_TIMEOUT_SECS,
    DEFAULT_DATA_IDLE_SECS, DEFAULT_PREFETCH_BATCH_COUNT, DEFAULT_RECONNECT_DELAY_SECS,
};
