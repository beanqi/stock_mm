//! Gate aggressive-flow impact detector.
//!
//! Over a short window (100 ms) the taker buy/sell notional is accumulated.
//! If one side's share exceeds `impact_share` **and** the window notional
//! exceeds the `impact_quantile` of historical window notionals, the *quoting
//! side that would be hit next* is flagged: heavy taker buying → our asks are
//! in danger (sell side), heavy taker selling → our bids (buy side).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::config;
use crate::types::Side;
use crate::util::{TimeWindow, quantile};

#[derive(Debug, Clone, Copy)]
pub struct Trade {
    pub at: Instant,
    /// Taker side.
    pub side: Side,
    pub notional: f64,
}

#[derive(Debug, Clone)]
pub struct ImpactDetector {
    cfg: config::Protection,
    recent: VecDeque<Trade>,
    /// Historical window notionals (sampled when a trade arrives).
    history: TimeWindow<f64>,
    last_history_sample: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImpactSignal {
    /// Side of *our quotes* that is endangered.
    pub endangered: Side,
    pub share: f64,
    pub notional: f64,
    pub threshold: f64,
}

impl ImpactDetector {
    pub fn new(cfg: config::Protection) -> Self {
        let history = TimeWindow::new(Duration::from_secs(cfg.impact_history_secs));
        Self { cfg, recent: VecDeque::new(), history, last_history_sample: None }
    }

    pub fn window(&self) -> Duration {
        Duration::from_millis(self.cfg.impact_window_ms)
    }

    pub fn on_trade(&mut self, t: Trade) {
        self.recent.push_back(t);
        self.evict(t.at);
        let (buy, sell) = self.sums();
        let total = buy + sell;
        // Record one history sample per window to build the "normal" distribution.
        let due = self.last_history_sample.map(|s| t.at.duration_since(s) >= self.window()).unwrap_or(true);
        if due && total > 0.0 {
            self.history.push(t.at, total);
            self.last_history_sample = Some(t.at);
        }
    }

    fn evict(&mut self, now: Instant) {
        let w = self.window();
        while let Some(f) = self.recent.front() {
            if now.duration_since(f.at) > w {
                self.recent.pop_front();
            } else {
                break;
            }
        }
    }

    fn sums(&self) -> (f64, f64) {
        let mut buy = 0.0;
        let mut sell = 0.0;
        for t in &self.recent {
            match t.side {
                Side::Buy => buy += t.notional,
                Side::Sell => sell += t.notional,
            }
        }
        (buy, sell)
    }

    /// Evaluate at `now`. Requires a minimal history so a single small print
    /// cannot trigger protection.
    pub fn evaluate(&mut self, now: Instant) -> Option<ImpactSignal> {
        self.evict(now);
        if self.recent.is_empty() || self.history.len() < 20 {
            return None;
        }
        let (buy, sell) = self.sums();
        let total = buy + sell;
        if total <= 0.0 {
            return None;
        }
        let hist: Vec<f64> = self.history.values().collect();
        let threshold = quantile(&hist, self.cfg.impact_quantile)?;
        if total <= threshold {
            return None;
        }
        let buy_share = buy / total;
        if buy_share >= self.cfg.impact_share {
            Some(ImpactSignal { endangered: Side::Sell, share: buy_share, notional: total, threshold })
        } else if (1.0 - buy_share) >= self.cfg.impact_share {
            Some(ImpactSignal { endangered: Side::Buy, share: 1.0 - buy_share, notional: total, threshold })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_prints_do_not_trigger_but_bursts_do() {
        let t0 = Instant::now();
        let mut d = ImpactDetector::new(config::Protection::default());
        // 40 quiet windows of ~100 USDT mixed flow
        for i in 0..40 {
            let at = t0 + Duration::from_millis(i * 150);
            d.on_trade(Trade { at, side: if i % 2 == 0 { Side::Buy } else { Side::Sell }, notional: 100.0 });
        }
        let quiet = t0 + Duration::from_millis(40 * 150);
        assert!(d.evaluate(quiet).is_none());
        // burst: 5000 USDT of taker buys inside 100 ms
        let burst = quiet + Duration::from_secs(1);
        for k in 0..5 {
            d.on_trade(Trade { at: burst + Duration::from_millis(k * 10), side: Side::Buy, notional: 1000.0 });
        }
        let sig = d.evaluate(burst + Duration::from_millis(50)).expect("signal");
        assert_eq!(sig.endangered, Side::Sell);
        assert!(sig.share >= 0.8);
        // after the window passes the signal clears
        assert!(d.evaluate(burst + Duration::from_millis(400)).is_none());
    }

    #[test]
    fn mixed_flow_does_not_trigger() {
        let t0 = Instant::now();
        let mut d = ImpactDetector::new(config::Protection::default());
        for i in 0..40 {
            let at = t0 + Duration::from_millis(i * 150);
            d.on_trade(Trade { at, side: Side::Buy, notional: 100.0 });
        }
        let burst = t0 + Duration::from_secs(10);
        d.on_trade(Trade { at: burst, side: Side::Buy, notional: 3000.0 });
        d.on_trade(Trade { at: burst + Duration::from_millis(5), side: Side::Sell, notional: 2500.0 });
        assert!(d.evaluate(burst + Duration::from_millis(10)).is_none());
    }
}
