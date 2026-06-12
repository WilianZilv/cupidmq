//! Minimal consumer — all tuning knobs (defaults shown).

use anyhow::Result;
use cupidmq::consumer::ConsumerConfig;
use cupidmq::CupidMQ;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ConsumerConfig::new(
        "127.0.0.1:9750", // master control (connect)
        "127.0.0.1:9760", // BATC advertised in REG! (bind defaults 0.0.0.0:9760)
    )?
    .consumer_tag("") // optional CRDY tag (metrics/debug; empty → master assigns consumer-{id})
    .max_batch_size_count(32) // max items per batch in CRDY
    .prefetch_batch_count(4) // batches buffered locally before next CRDY
    .batch_timeout_secs(10) // wait for BATC after CRDY before retry
    .data_idle_secs(60) // close idle BATC TCP if no frame
    .reconnect(true) // reconnect control plane after disconnect
    .reconnect_delay_secs(2); // delay between reconnect attempts

    let mut client = CupidMQ::connect_consumer(config).await?;

    client
        .consume(|batch| async move {
            for item in batch {
                println!("{}", String::from_utf8_lossy(&item));
            }
            Ok(())
        })
        .await?;

    Ok(())
}
