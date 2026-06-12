//! CupidMQ client — producer + consumer.

use std::future::Future;

use anyhow::{bail, Result};
use bytes::Bytes;

use crate::consumer::{Consumer, ConsumerConfig, ConsumerStats};
use crate::producer::{Producer, ProducerConfig, ProducerStats};

/// Master client — producer `enqueue` or consumer `consume`.
pub enum CupidMQ {
    Producer(Producer),
    Consumer(Consumer),
}

impl CupidMQ {
    pub async fn connect_producer(config: ProducerConfig) -> Result<Self> {
        Ok(CupidMQ::Producer(Producer::connect(config).await?))
    }

    pub async fn connect_consumer(config: ConsumerConfig) -> Result<Self> {
        Ok(CupidMQ::Consumer(Consumer::connect(config).await?))
    }

    pub fn enqueue(&self, payload: impl Into<Bytes>) -> Result<()> {
        match self {
            CupidMQ::Producer(p) => p.publish_batch(payload),
            CupidMQ::Consumer(_) => bail!("enqueue: not a producer"),
        }
    }

    /// Processes batches — next CRDY only after handler (same as Python `consume()`).
    pub async fn consume<F, Fut>(&mut self, handler: F) -> Result<()>
    where
        F: FnMut(Vec<Bytes>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        match self {
            CupidMQ::Consumer(c) => c.consume(handler).await,
            CupidMQ::Producer(_) => bail!("consume: not a consumer"),
        }
    }

    pub fn producer_stats(&self) -> Result<&ProducerStats> {
        match self {
            CupidMQ::Producer(p) => Ok(p.stats()),
            CupidMQ::Consumer(_) => bail!("stats: not a producer"),
        }
    }

    pub fn consumer_stats(&self) -> Result<&ConsumerStats> {
        match self {
            CupidMQ::Consumer(c) => Ok(c.stats()),
            CupidMQ::Producer(_) => bail!("stats: not a consumer"),
        }
    }
}
