//! Consumer capacity — ciclo CRDY → batch → process → CRDY.
//!
//! μ observado = EMA(itens/batch) ÷ EMA(ciclo ms)
//! λ demanda = (transfer/s + max(0, Δbacklog/s)) ÷ N
//! capacity = μ/λ × 100 (>100 = headroom; <100 = atrasado)

use crate::consumers::ConsumerRow;

#[derive(Debug, Clone, Copy)]
pub struct ConsumerCapacityInput {
    pub batch_items: f64,
    pub cycle_ms: f64,
}

const DEFAULT_CYCLE_MS: f64 = 256.0;
const DEFAULT_BATCH_ITEMS: f64 = 32.0;

/// Throughput observado (msg/s) a partir do ciclo real do consumer.
pub fn observed_throughput(batch_items: f64, cycle_ms: f64) -> f64 {
    let items = batch_items.max(0.1);
    let tau = (cycle_ms / 1000.0).max(0.001);
    items / tau
}

/// Demanda de ingestão = drenagem atual + crescimento do backlog producer.
pub fn demand_per_sec(transfer_per_sec: f64, backlog_growth_per_sec: f64) -> f64 {
    if transfer_per_sec <= 0.01 && backlog_growth_per_sec <= 0.01 {
        return 0.0;
    }
    transfer_per_sec + backlog_growth_per_sec.max(0.0)
}

/// Capacity de um consumer vs cota justa da demanda total.
pub fn per_consumer_capacity_model(
    batch_items: f64,
    cycle_ms: f64,
    demand_per_sec: f64,
    consumer_count: usize,
) -> f64 {
    if consumer_count == 0 {
        return 0.0;
    }
    if demand_per_sec <= 0.01 {
        return 100.0;
    }
    let lambda = demand_per_sec / consumer_count as f64;
    let mu = observed_throughput(batch_items, cycle_ms);
    (mu / lambda * 100.0).max(0.0)
}

/// Capacity global = soma dos μ vs demanda total.
pub fn global_capacity_model(inputs: &[ConsumerCapacityInput], demand_per_sec: f64) -> f64 {
    let n = inputs.len();
    if n == 0 {
        return 0.0;
    }
    if demand_per_sec <= 0.01 {
        return 100.0;
    }
    let total_mu: f64 = inputs
        .iter()
        .map(|c| observed_throughput(c.batch_items, c.cycle_ms))
        .sum();
    (total_mu / demand_per_sec * 100.0).max(0.0)
}

/// Fallback quando não há taxa de publish recente.
pub fn compute_consumer_capacity_fallback(consumers: &[ConsumerRow], ready: u64) -> f64 {
    let n = consumers.len();
    if n == 0 {
        return 0.0;
    }
    if ready == 0 {
        return 100.0;
    }
    let busy = consumers.iter().filter(|c| c.unacked > 0).count();
    ((n - busy) as f64 / n as f64 * 100.0).clamp(0.0, 100.0)
}

pub fn effective_cycle_ms(ema_ms: f64) -> f64 {
    if ema_ms > 0.0 {
        ema_ms
    } else {
        DEFAULT_CYCLE_MS
    }
}

pub fn effective_batch_items(ema_items: f64, last_batch_items: u64, max_batch: u16) -> f64 {
    if ema_items > 0.0 {
        ema_items
    } else if last_batch_items > 0 {
        last_batch_items as f64
    } else if max_batch > 0 {
        max_batch as f64
    } else {
        DEFAULT_BATCH_ITEMS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_up_when_mu_exceeds_lambda() {
        // 32 items / 256ms ≈ 125 msg/s; demand 120/s ÷ 2 = 60/s per consumer → ~208%
        let cap = per_consumer_capacity_model(32.0, 256.0, 120.0, 2);
        assert!(cap > 100.0, "cap={cap}");
    }

    #[test]
    fn global_extrapolates_above_100() {
        // 2× 32/256ms ≈ 250 msg/s vs demand 120/s
        let inputs = [
            ConsumerCapacityInput {
                batch_items: 32.0,
                cycle_ms: 256.0,
            },
            ConsumerCapacityInput {
                batch_items: 32.0,
                cycle_ms: 256.0,
            },
        ];
        let g = global_capacity_model(&inputs, 120.0);
        assert!(g > 100.0, "g={g}");
    }

    #[test]
    fn partial_batches_fall_behind() {
        // batch real ~8, ciclo 280ms → μ≈28.6; demand 553/s ÷ 9 ≈ 61.4
        let cap = per_consumer_capacity_model(8.0, 280.0, 553.0, 9);
        assert!(cap > 40.0 && cap < 55.0, "cap={cap}");
    }

    #[test]
    fn global_sums_consumers() {
        let inputs = [
            ConsumerCapacityInput {
                batch_items: 32.0,
                cycle_ms: 256.0,
            },
            ConsumerCapacityInput {
                batch_items: 16.0,
                cycle_ms: 256.0,
            },
        ];
        let g = global_capacity_model(&inputs, 240.0);
        assert!(g > 50.0 && g < 100.0, "g={g}");
    }

    #[test]
    fn demand_includes_backlog_growth() {
        assert!((demand_per_sec(100.0, 50.0) - 150.0).abs() < 0.01);
        assert!((demand_per_sec(100.0, -10.0) - 100.0).abs() < 0.01);
    }

    #[test]
    fn idle_demand_is_full() {
        assert_eq!(per_consumer_capacity_model(32.0, 256.0, 0.0, 1), 100.0);
    }
}
