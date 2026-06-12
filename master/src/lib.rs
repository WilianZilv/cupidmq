//! CupidMQ — brokerless P2P batch queue (`server` + `client` / `producer` / `consumer`).
//!
//! Dev/stress harness — `examples/cupidmq-producer.rs` is not public API.

pub mod capacity;
pub mod client;
pub mod consumer;
pub mod config;
pub mod consumers;
pub mod metrics;
pub mod metrics_cache;
pub mod metrics_history;
pub mod producer;
pub mod tick_buffer;
pub mod producers;
pub mod protocol;
pub mod ring;
pub mod server;

pub use client::CupidMQ;
pub use producers::{Matchmaker, ProducerRegistry, ProducerRow};

pub use metrics::{Metrics, MetricsSnapshot};
pub use consumer::{
    Consumer, ConsumerConfig, ConsumerStats, DEFAULT_MAX_BATCH_SIZE_COUNT,
    DEFAULT_BATCH_TIMEOUT_SECS,
    DEFAULT_DATA_IDLE_SECS, DEFAULT_PREFETCH_BATCH_COUNT,
};
pub use producer::{
    Producer, ProducerConfig, ProducerStats, DEFAULT_FLUSH_TIMEOUT_MS,
    DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_MAX_BATCH_BYTES, DEFAULT_OUTBOUND_MAX_BYTES,
    DEFAULT_RECONNECT_DELAY_SECS,
};
pub use protocol::{
    encode_assign, encode_batch, encode_batch_payload, encode_consumer_ready, encode_delivered,
    encode_error, encode_failed, encode_producer_ready, encode_register, parse_batch_payload_items,
    read_assign, read_assign_payload, read_batch_frame, read_consumer_ready_request, read_register,
    AssignRequest, DeliveryReport, FailureReport, ReadyRequest, ERR_BUSY, MAGIC_ASSIGN,
    MAGIC_CONSUMER_READY,
};
pub use ring::{ByteRing, PushResult, QueuedMessage};
