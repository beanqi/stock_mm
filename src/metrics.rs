//! Acceptance metrics (spec, last section): per-side markouts at 50 ms /
//! 200 ms / 1 s, inventory digestion time, active-exit cost and true equity.

use std::collections::VecDeque;
use std::io::Write;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::strategy::risk::Pnl;
use crate::types::Side;

const HORIZONS_MS: [u64; 3] = [50, 200, 1000];

#[derive(Debug, Clone)]
struct PendingMarkout {
    at: Instant,
    side: Side,
    price: f64,
    size: i64,
    done: [bool; 3],
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MarkoutStats {
    pub count: u64,
    /// Size-weighted mean markout in bps (positive = fill was favourable).
    pub mean_bps: [f64; 3],
    #[serde(skip)]
    sum_bps: [f64; 3],
    #[serde(skip)]
    weight: [f64; 3],
}

impl MarkoutStats {
    fn add(&mut self, h: usize, bps: f64, w: f64) {
        self.sum_bps[h] += bps * w;
        self.weight[h] += w;
        if self.weight[h] > 0.0 {
            self.mean_bps[h] = self.sum_bps[h] / self.weight[h];
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DigestionStats {
    pub episodes: u64,
    pub mean_ms: f64,
    pub max_ms: u64,
    pub active_reduce_episodes: u64,
    #[serde(skip)]
    total_ms: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ExitStats {
    pub ioc_orders: u64,
    pub ioc_contracts: i64,
    /// Σ (decision fair − fill) × sign × size × multiplier, USDT (positive = cost).
    pub cost_usdt: f64,
    pub market_orders: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct FillStats {
    pub maker_fills: u64,
    pub maker_contracts: i64,
    pub taker_fills: u64,
    pub taker_contracts: i64,
}

#[derive(Debug, Serialize)]
pub struct Snapshot {
    pub ts_ms: i64,
    pub uptime_s: u64,
    pub fair: Option<f64>,
    pub beta0_bps: Option<f64>,
    pub beta_t_bps: Option<f64>,
    pub drift_bps: Option<f64>,
    pub delta_bps: f64,
    pub session: String,
    pub basis_status: String,
    pub q_contracts: i64,
    pub q_usdt: f64,
    pub u: f64,
    pub long: i64,
    pub short: i64,
    pub active_orders: usize,
    pub protection_buy: String,
    pub protection_sell: String,
    pub reduce_phase: String,
    pub trust_reasons: Vec<String>,
    pub pnl: Pnl,
    pub equity_change: f64,
    pub available_margin: Option<f64>,
    pub markout_buy: MarkoutStats,
    pub markout_sell: MarkoutStats,
    pub digestion: DigestionStats,
    pub exit: ExitStats,
    pub fills: FillStats,
    pub cycles: u64,
}

#[derive(Debug)]
pub struct Metrics {
    pending: VecDeque<PendingMarkout>,
    pub buy: MarkoutStats,
    pub sell: MarkoutStats,
    pub digestion: DigestionStats,
    pub exit: ExitStats,
    pub fills: FillStats,
    file: Option<std::fs::File>,
    last_snapshot: Option<Instant>,
    interval: Duration,
    started: Instant,
}

impl Metrics {
    pub fn new(path: &str, interval: Duration) -> Self {
        let file = if path.is_empty() {
            None
        } else {
            std::fs::OpenOptions::new().create(true).append(true).open(path).ok()
        };
        Self {
            pending: VecDeque::new(),
            buy: MarkoutStats::default(),
            sell: MarkoutStats::default(),
            digestion: DigestionStats::default(),
            exit: ExitStats::default(),
            fills: FillStats::default(),
            file,
            last_snapshot: None,
            interval,
            started: Instant::now(),
        }
    }

    pub fn on_fill(&mut self, side: Side, price: f64, size: i64, is_maker: bool, at: Instant) {
        if is_maker {
            self.fills.maker_fills += 1;
            self.fills.maker_contracts += size;
        } else {
            self.fills.taker_fills += 1;
            self.fills.taker_contracts += size;
        }
        self.pending.push_back(PendingMarkout { at, side, price, size, done: [false; 3] });
        while self.pending.len() > 10_000 {
            self.pending.pop_front();
        }
    }

    /// Evaluate pending markouts against the current reference mid.
    pub fn on_mid(&mut self, mid: f64, now: Instant) {
        if mid <= 0.0 {
            return;
        }
        for p in self.pending.iter_mut() {
            let el = now.saturating_duration_since(p.at);
            for (h, ms) in HORIZONS_MS.iter().enumerate() {
                if !p.done[h] && el >= Duration::from_millis(*ms) {
                    p.done[h] = true;
                    let bps = match p.side {
                        Side::Buy => 10_000.0 * (mid - p.price) / p.price,
                        Side::Sell => 10_000.0 * (p.price - mid) / p.price,
                    };
                    let stats = match p.side {
                        Side::Buy => &mut self.buy,
                        Side::Sell => &mut self.sell,
                    };
                    if h == 0 {
                        stats.count += 1;
                    }
                    stats.add(h, bps, p.size as f64);
                }
            }
        }
        while let Some(f) = self.pending.front() {
            if f.done.iter().all(|d| *d) {
                self.pending.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn on_digestion_episode(&mut self, dur: Duration, used_active_reduce: bool) {
        let ms = dur.as_millis() as u64;
        self.digestion.episodes += 1;
        self.digestion.total_ms += ms as f64;
        self.digestion.mean_ms = self.digestion.total_ms / self.digestion.episodes as f64;
        self.digestion.max_ms = self.digestion.max_ms.max(ms);
        if used_active_reduce {
            self.digestion.active_reduce_episodes += 1;
        }
    }

    pub fn on_exit_fill(&mut self, side: Side, decision_fair: f64, fill_price: f64, size: i64, multiplier: f64, was_market: bool) {
        self.exit.ioc_contracts += size;
        let sign = match side {
            Side::Sell => 1.0,
            Side::Buy => -1.0,
        };
        self.exit.cost_usdt += (decision_fair - fill_price) * sign * size as f64 * multiplier;
        if was_market {
            self.exit.market_orders += 1;
        } else {
            self.exit.ioc_orders += 1;
        }
    }

    pub fn snapshot_due(&self, now: Instant) -> bool {
        self.last_snapshot.map(|t| now.duration_since(t) >= self.interval).unwrap_or(true)
    }

    pub fn write_snapshot(&mut self, snap: &Snapshot, now: Instant) {
        self.last_snapshot = Some(now);
        if let Some(f) = self.file.as_mut() {
            if let Ok(s) = serde_json::to_string(snap) {
                let _ = writeln!(f, "{s}");
            }
        }
    }

    pub fn uptime(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.started)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markouts_by_side_and_horizon() {
        let t0 = Instant::now();
        let mut m = Metrics::new("", Duration::from_secs(10));
        m.on_fill(Side::Buy, 1000.0, 10, true, t0);
        m.on_fill(Side::Sell, 1000.0, 10, true, t0);
        // 50 ms later mid = 1000.5 → buy +5 bps, sell −5 bps
        m.on_mid(1000.5, t0 + Duration::from_millis(60));
        assert!((m.buy.mean_bps[0] - 5.0).abs() < 1e-9);
        assert!((m.sell.mean_bps[0] + 5.0).abs() < 1e-9);
        assert_eq!(m.buy.mean_bps[1], 0.0);
        // 1 s later mid = 999 → buy −10 bps at both remaining horizons
        m.on_mid(999.0, t0 + Duration::from_millis(1100));
        assert!((m.buy.mean_bps[1] + 10.0).abs() < 1e-9);
        assert!((m.buy.mean_bps[2] + 10.0).abs() < 1e-9);
        assert!(m.pending.is_empty());
        assert_eq!(m.buy.count, 1);
    }

    #[test]
    fn exit_cost_sign() {
        let mut m = Metrics::new("", Duration::from_secs(10));
        // selling 10 contracts at 999.5 when fair was 1000 costs 0.5 * 10 * 0.01 = 0.05 USDT
        m.on_exit_fill(Side::Sell, 1000.0, 999.5, 10, 0.01, false);
        assert!((m.exit.cost_usdt - 0.05).abs() < 1e-9);
        m.on_exit_fill(Side::Buy, 1000.0, 1000.5, 10, 0.01, false);
        assert!((m.exit.cost_usdt - 0.10).abs() < 1e-9);
    }
}
