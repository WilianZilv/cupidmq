//! Stress harness — synthetic byte payloads via `cupidmq::producer`.

use anyhow::{bail, Result};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use cupidmq::CupidMQ;
use cupidmq::producer::{ProducerConfig, DEFAULT_DELIVERY_IDLE_SECS};
use rand::Rng;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const KB: usize = 1024;
const MAGIC: &[u8; 8] = b"CUPIDMQ\0";
const HEADER_LEN: usize = MAGIC.len() + 8 + 4;

#[derive(Parser, Debug)]
#[command(name = "cupidmq-producer", about = "CupidMQ stress producer")]
struct Args {
    // --- connection ---
    #[arg(long, env = "CUPIDMQ_MASTER", default_value = "127.0.0.1:9750")]
    master: String,
    #[arg(long, env = "CUPIDMQ_PRODUCER_LABEL", default_value = "producer")]
    label: String,
    #[arg(long, default_value_t = 2)]
    reconnect_delay_secs: u64,

    // --- publish rate (0 = idle: connect only) ---
    #[arg(long, default_value_t = 0)]
    rate: u64,
    #[arg(long, default_value_t = 0)]
    rate_min: u64,
    #[arg(long, default_value_t = 0)]
    rate_max: u64,
    #[arg(long, default_value_t = 0)]
    count: u64,
    #[arg(long, default_value_t = 0)]
    duration_secs: u64,
    #[arg(long, default_value_t = 1, env = "CUPIDMQ_PUBLISH_MULT")]
    publish_mult: u64,

    // --- payload ---
    #[arg(long, default_value_t = 1)]
    source_id: u32,
    #[arg(long, default_value_t = 8 * KB as u64)]
    payload_bytes: u64,
    #[arg(long, default_value_t = 0)]
    payload_bytes_min: u64,
    #[arg(long, default_value_t = 0)]
    payload_bytes_max: u64,
    #[arg(long, value_enum, default_value_t = PayloadPattern::Zeros)]
    payload_pattern: PayloadPattern,
    #[arg(long, default_value_t = false)]
    reuse_payload: bool,

    // --- producer tuning ---
    #[arg(long, default_value_t = false)]
    batch_mode: bool,
    #[arg(long, default_value_t = 64)]
    max_batch_size_mb: u64,
    #[arg(long, default_value_t = 100)]
    flush_timeout_ms: u64,
    #[arg(long, default_value_t = 4096)]
    outbound_max_mb: u64,
    #[arg(long, env = "CUPIDMQ_DELIVERY_TIMEOUT_SECS", default_value_t = 8)]
    delivery_timeout_secs: u64,
    #[arg(long, env = "CUPIDMQ_DELIVERY_IDLE_SECS", default_value_t = DEFAULT_DELIVERY_IDLE_SECS)]
    delivery_idle_secs: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PayloadPattern {
    Zeros,
    Random,
    Increment,
}

struct RatePlan {
    idle: bool,
    min: u64,
    max: u64,
}

impl Args {
    fn rate_plan(&self) -> RatePlan {
        let idle = self.rate == 0 && self.rate_min == 0 && self.rate_max == 0;
        if idle {
            return RatePlan {
                idle: true,
                min: 0,
                max: 0,
            };
        }
        if self.rate_min > 0 || self.rate_max > 0 {
            let min = self.rate_min.max(1);
            return RatePlan {
                idle: false,
                min,
                max: self.rate_max.max(min),
            };
        }
        let r = self.rate.max(1);
        RatePlan {
            idle: false,
            min: r,
            max: r,
        }
    }

    fn payload_byte_range(&self) -> (usize, usize) {
        let (lo, hi) = if self.payload_bytes_min > 0 || self.payload_bytes_max > 0 {
            let lo = if self.payload_bytes_min > 0 {
                self.payload_bytes_min as usize
            } else {
                self.payload_bytes as usize
            };
            let hi = if self.payload_bytes_max > 0 {
                self.payload_bytes_max as usize
            } else {
                lo
            };
            (lo, hi)
        } else {
            let n = self.payload_bytes as usize;
            (n, n)
        };
        let lo = lo.min(hi).max(HEADER_LEN);
        let hi = lo.max(hi).max(HEADER_LEN);
        (lo, hi)
    }

    fn producer_config(&self) -> ProducerConfig {
        let mb = 1024 * 1024;
        ProducerConfig::new(&self.master)
            .max_batch_bytes((self.max_batch_size_mb as usize).saturating_mul(mb))
            .flush_timeout_ms(self.flush_timeout_ms)
            .outbound_max_bytes((self.outbound_max_mb as usize).saturating_mul(mb))
            .reconnect_delay_secs(self.reconnect_delay_secs)
            .delivery_timeout_secs(self.delivery_timeout_secs)
            .delivery_idle_secs(self.delivery_idle_secs)
    }
}

fn build_payload(seq: u64, source_id: u32, size: usize, pattern: PayloadPattern) -> Vec<u8> {
    let size = size.max(HEADER_LEN);
    let mut buf = Vec::with_capacity(size);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&source_id.to_le_bytes());

    let body_len = size - HEADER_LEN;
    match pattern {
        PayloadPattern::Zeros => buf.resize(size, 0),
        PayloadPattern::Random => {
            let mut rng = rand::thread_rng();
            buf.extend((0..body_len).map(|_| rng.gen::<u8>()));
        }
        PayloadPattern::Increment => {
            buf.extend((0..body_len).map(|i| (seq as u8).wrapping_add(i as u8)));
        }
    }
    buf
}

fn tick_interval(rng: &mut impl Rng, plan: &RatePlan) -> Duration {
    if plan.min == plan.max {
        return Duration::from_secs_f64(1.0 / plan.min as f64);
    }
    let rate = rng.gen_range(plan.min..=plan.max);
    Duration::from_secs_f64(1.0 / rate as f64)
}

async fn sleep_until(next: &mut Instant, interval: Duration) {
    *next += interval;
    let now = Instant::now();
    if *next > now {
        tokio::time::sleep(*next - now).await;
    } else {
        *next = now;
    }
}

fn next_payload(
    args: &Args,
    seq: u64,
    size_lo: usize,
    size_hi: usize,
    template: &Option<Bytes>,
    rng: &mut impl Rng,
) -> Bytes {
    if let Some(bytes) = template {
        return bytes.clone();
    }
    let size = if size_lo == size_hi {
        size_lo
    } else {
        rng.gen_range(size_lo..=size_hi)
    };
    Bytes::from(build_payload(seq, args.source_id, size, args.payload_pattern))
}

fn publish_mult(
    client: &CupidMQ,
    item: Bytes,
    mult: u64,
    batch_mode: bool,
    label: &str,
) -> Result<u64> {
    let mut sent = 0u64;
    for _ in 0..mult {
        match client.enqueue(item.clone()) {
            Ok(()) => sent += 1,
            Err(e) if batch_mode => return Err(e),
            Err(e) => {
                warn!(label, error = %e, "publish failed");
                sent += 1;
            }
        }
    }
    Ok(sent)
}

async fn run_idle(label: &str) -> Result<()> {
    info!(label, "connected — idle (no publish rate)");
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

async fn run_stress(client: CupidMQ, args: &Args, plan: &RatePlan) -> Result<()> {
    let (size_lo, size_hi) = args.payload_byte_range();
    let mult = args.publish_mult.max(1);
    let fixed_interval = (plan.min == plan.max).then(|| Duration::from_secs_f64(1.0 / plan.min as f64));

    let template = args.reuse_payload.then(|| {
        Bytes::from(build_payload(1, args.source_id, size_lo, args.payload_pattern))
    });

    let started = Instant::now();
    let mut next_tick = Instant::now();
    let mut seq = 0u64;
    let mut ticks = 0u64;
    let mut items_sent = 0u64;
    let mut rng = rand::thread_rng();

    while (args.count == 0 || ticks < args.count)
        && (args.duration_secs == 0 || started.elapsed().as_secs() < args.duration_secs)
    {
        seq += 1;
        ticks += 1;
        let item = next_payload(args, seq, size_lo, size_hi, &template, &mut rng);
        items_sent += publish_mult(&client, item, mult, args.batch_mode, &args.label)?;

        let interval = fixed_interval.unwrap_or_else(|| tick_interval(&mut rng, plan));
        sleep_until(&mut next_tick, interval).await;
    }

    tokio::time::sleep(Duration::from_millis(args.flush_timeout_ms.max(1) + 50)).await;

    let s = client.producer_stats()?;
    info!(
        label = %args.label,
        ticks,
        items_sent,
        publish_mult = mult,
        published_items = s.published_items.load(Ordering::Relaxed),
        published_batches = s.published_batches.load(Ordering::Relaxed),
        reconnects = s.reconnects.load(Ordering::Relaxed),
        delivery_errors = s.delivery_errors.load(Ordering::Relaxed),
        queue_dropped_items = s.queue_dropped_items.load(Ordering::Relaxed),
        elapsed_ms = started.elapsed().as_millis(),
        "done"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let plan = args.rate_plan();
    let (size_lo, size_hi) = args.payload_byte_range();
    if size_lo < HEADER_LEN {
        bail!("payload_bytes must be at least {HEADER_LEN}");
    }

    let client = CupidMQ::connect_producer(args.producer_config()).await?;

    info!(
        label = %args.label,
        master = %args.master,
        rate_min = plan.min,
        rate_max = plan.max,
        idle = plan.idle,
        payload_bytes_min = size_lo,
        payload_bytes_max = size_hi,
        payload_pattern = ?args.payload_pattern,
        source_id = args.source_id,
        batch_mode = args.batch_mode,
        reuse_payload = args.reuse_payload,
        publish_mult = args.publish_mult.max(1),
        "start"
    );

    if plan.idle {
        run_idle(&args.label).await
    } else {
        run_stress(client, &args, &plan).await
    }
}
