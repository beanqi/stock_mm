//! Binance reference price: mid history, short-horizon velocity (v100) and
//! realised-move quantile (V90).

use std::time::{Duration, Instant};

use crate::util::{TimeWindow, quantile};

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // qty / exchange time are carried for logging & future signals
pub struct Bbo {
    pub bid: f64,
    pub ask: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    /// Exchange event time (ms) when available.
    pub exch_ts_ms: i64,
    pub recv: Instant,
}

impl Bbo {
    pub fn mid(&self) -> f64 {
        (self.bid + self.ask) / 2.0
    }
    pub fn is_valid(&self) -> bool {
        self.bid > 0.0 && self.ask > 0.0 && self.ask >= self.bid && self.bid.is_finite() && self.ask.is_finite()
    }
}

#[derive(Debug, Clone)]
pub struct ReferencePrice {
    pub last: Option<Bbo>,
    /// Mid history for velocity (kept ~2 s).
    mids: TimeWindow<f64>,
    /// Absolute 200 ms mid moves in bps over the vol window.
    moves_bps: TimeWindow<f64>,
    vol_step: Duration,
    last_vol_sample: Option<(Instant, f64)>,
    velocity_window: Duration,
}

impl ReferencePrice {
    pub fn new(vol_window: Duration, vol_step: Duration, velocity_window: Duration) -> Self {
        Self {
            last: None,
            mids: TimeWindow::new(Duration::from_secs(2)),
            moves_bps: TimeWindow::new(vol_window),
            vol_step,
            last_vol_sample: None,
            velocity_window,
        }
    }

    pub fn update(&mut self, bbo: Bbo) {
        if !bbo.is_valid() {
            return;
        }
        let mid = bbo.mid();
        let now = bbo.recv;
        self.last = Some(bbo);
        self.mids.push(now, mid);
        match self.last_vol_sample {
            None => self.last_vol_sample = Some((now, mid)),
            Some((t, m)) => {
                if now.duration_since(t) >= self.vol_step {
                    if m > 0.0 {
                        self.moves_bps.push(now, 10_000.0 * ((mid / m).ln()).abs());
                    }
                    self.last_vol_sample = Some((now, mid));
                }
            }
        }
    }

    pub fn mid(&self) -> Option<f64> {
        self.last.map(|b| b.mid())
    }

    pub fn age(&self, now: Instant) -> Option<Duration> {
        self.last.map(|b| now.saturating_duration_since(b.recv))
    }

    /// v100 = 1e4 · ln(M(t) / M(t − window)), using the latest sample at or
    /// before `t − window`. Returns 0 when there is no history.
    pub fn velocity_bps(&self, now: Instant) -> f64 {
        let Some(cur) = self.mid() else { return 0.0 };
        let Some(t0) = now.checked_sub(self.velocity_window) else { return 0.0 };
        match self.mids.at_or_before(t0) {
            Some((_, past)) if *past > 0.0 => 10_000.0 * (cur / past).ln(),
            _ => 0.0,
        }
    }

    /// V_q: quantile of absolute step moves (bps) over the vol window.
    pub fn realised_move_quantile(&self, q: f64) -> Option<f64> {
        let v: Vec<f64> = self.moves_bps.values().collect();
        quantile(&v, q)
    }

    #[cfg(test)]
    pub fn vol_samples(&self) -> usize {
        self.moves_bps.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bbo(bid: f64, ask: f64, at: Instant) -> Bbo {
        Bbo { bid, ask, bid_qty: 1.0, ask_qty: 1.0, exch_ts_ms: 0, recv: at }
    }

    #[test]
    fn velocity_uses_sample_100ms_back() {
        let t0 = Instant::now();
        let mut r = ReferencePrice::new(Duration::from_secs(300), Duration::from_millis(200), Duration::from_millis(100));
        r.update(bbo(999.9, 1000.1, t0));
        r.update(bbo(1000.9, 1001.1, t0 + Duration::from_millis(120)));
        let v = r.velocity_bps(t0 + Duration::from_millis(120));
        // ln(1001/1000)*1e4 ≈ 9.995 bps
        assert!((v - 9.995).abs() < 0.01, "v={v}");
        // no sample old enough -> 0
        assert_eq!(r.velocity_bps(t0 + Duration::from_millis(50)), 0.0);
    }

    #[test]
    fn realised_moves_sampled_every_step() {
        let t0 = Instant::now();
        let mut r = ReferencePrice::new(Duration::from_secs(300), Duration::from_millis(200), Duration::from_millis(100));
        let mut px = 1000.0;
        for i in 0..10 {
            px += if i % 2 == 0 { 0.5 } else { -0.2 };
            r.update(bbo(px - 0.1, px + 0.1, t0 + Duration::from_millis(200 * i)));
        }
        assert_eq!(r.vol_samples(), 9);
        let v90 = r.realised_move_quantile(0.9).unwrap();
        assert!(v90 > 3.0 && v90 < 6.0, "v90={v90}");
    }
}
