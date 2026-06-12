//! Docker / Compose consumer — config via env (see docker/entrypoint-consumer.sh).

use anyhow::{Context, Result};
use cupidmq::consumer::ConsumerConfig;
use cupidmq::CupidMQ;
use rand::Rng;
use std::env;
use std::time::Duration;

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} required"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let master = env::var("CUPIDMQ_MASTER").unwrap_or_else(|_| "127.0.0.1:9750".into());
    let data_addr = required("CUPIDMQ_DATA_ADDR")?;

    let mut config = ConsumerConfig::new(master, data_addr)?;
    if let Ok(bind) = env::var("CUPIDMQ_BIND_ADDR") {
        config = config.bind_addr(bind)?;
    }
    if let Ok(tag) = env::var("CUPIDMQ_CONSUMER_TAG") {
        config = config.consumer_tag(tag);
    }

    let bind = config.resolve_bind()?;
    eprintln!(
        "docker-consumer: advertise={} bind={}:{}",
        config.data_addr, bind.0, bind.1
    );

    let process_min_ms: u64 = env::var("CUPIDMQ_PROCESS_MS_MIN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let process_max_ms: u64 = env::var("CUPIDMQ_PROCESS_MS_MAX")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16)
        .max(process_min_ms);

    eprintln!(
        "docker-consumer: process {process_min_ms}-{process_max_ms}ms/item",
    );

    let mut client = CupidMQ::connect_consumer(config).await?;

    client
        .consume(move |batch| async move {
            let mut rng = rand::thread_rng();
            for item in &batch {
                let ms = rng.gen_range(process_min_ms..=process_max_ms);
                if ms > 0 {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
                let _ = item.len();
            }
            Ok(())
        })
        .await?;

    Ok(())
}
