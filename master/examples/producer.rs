//! Minimal producer — all tuning knobs (defaults shown).

use anyhow::Result;
use cupidmq::producer::ProducerConfig;
use cupidmq::CupidMQ;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let mb = 1024 * 1024;
    let config = ProducerConfig::new("127.0.0.1:9750") // master control plane (PRDY/ASGN/DELV)
        .max_batch_bytes(64 * mb) // max encoded BATC batch size
        .flush_timeout_ms(100) // flush partial batch after this wait
        .outbound_max_bytes(4096 * mb) // local ring cap — drop oldest when full
        .reconnect_delay_secs(2) // backoff between control-plane reconnects
        .delivery_timeout_secs(8) // timeout for one BATC write+flush
        .delivery_idle_secs(60); // close idle pooled BATC TCP to consumer

    let client = CupidMQ::connect_producer(config).await?;

    for i in 0..10 {
        client.enqueue(format!("hello-{i}"))?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    Ok(())
}
