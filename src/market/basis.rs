//! Natural basis β₀ between Gate and Binance, per trading session, with the
//! D_stop abnormality guard and a 5‑minute drift monitor.
//!
//! β_t = 1e4 · (M_G / M_B − 1) (bps). β₀ is the median of *valid* β_t samples
//! over the main window (1 h) for the *current* session kind. A sample is
//! valid only when both quotes are fresh and synchronised and the external
//! Gate quote is informative (not self-dominated / too wide).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::config;
use crate::market::session::SessionKind;
use crate::util::{TimeWindow, median, quantile};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasisStatus {
    /// Not enough samples yet in this session.
    WarmingUp,
    Normal,
    /// D > D_stop: β₀ frozen, no new risk.
    Abnormal,
    /// D back under the recovery threshold, waiting for the stability period.
    Recovering,
}

#[derive(Debug, Clone)]
struct SessionStats {
    beta: TimeWindow<f64>,
    /// D = |β_t − β₀| for samples observed while status was Normal.
    d_normal: TimeWindow<f64>,
    beta0: Option<f64>,
    last_sample: Option<Instant>,
}

impl SessionStats {
    fn new(window: Duration) -> Self {
        Self { beta: TimeWindow::new(window), d_normal: TimeWindow::new(window), beta0: None, last_sample: None }
    }
}

#[derive(Debug, Clone)]
pub struct BasisEstimator {
    cfg: config::Pricing,
    per_session: HashMap<SessionKind, SessionStats>,
    drift: TimeWindow<f64>,
    current: SessionKind,
    status: BasisStatus,
    /// Latest β_t regardless of validity (for diagnostics).
    pub last_beta_t: Option<f64>,
    /// Latest D.
    pub last_d: Option<f64>,
    recover_since: Option<Instant>,
    last_beta0_calc: Option<Instant>,
    dstop_cache: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
pub struct BasisInput {
    pub binance_mid: f64,
    pub binance_recv: Instant,
    pub gate_ext_mid: f64,
    pub gate_ext_spread_bps: f64,
    pub gate_recv: Instant,
}

impl BasisEstimator {
    pub fn new(cfg: config::Pricing) -> Self {
        let drift = TimeWindow::new(Duration::from_secs(cfg.drift_window_secs));
        Self {
            cfg,
            per_session: HashMap::new(),
            drift,
            current: SessionKind::Overnight,
            status: BasisStatus::WarmingUp,
            last_beta_t: None,
            last_d: None,
            recover_since: None,
            last_beta0_calc: None,
            dstop_cache: None,
        }
    }

    pub fn set_session(&mut self, k: SessionKind) {
        if k != self.current {
            self.current = k;
            self.drift.clear();
            self.recover_since = None;
            self.status = if self.stats().beta0.is_some() && self.stats().beta.len() >= self.cfg.basis_min_samples {
                BasisStatus::Normal
            } else {
                BasisStatus::WarmingUp
            };
        }
    }

    fn stats(&self) -> &SessionStats {
        // A missing entry is treated as empty; callers create on write.
        self.per_session.get(&self.current).unwrap_or_else(|| {
            // Safe: leaks nothing, only used for read paths on an empty session.
            static EMPTY: std::sync::OnceLock<SessionStats> = std::sync::OnceLock::new();
            EMPTY.get_or_init(|| SessionStats::new(Duration::from_secs(3600)))
        })
    }

    fn stats_mut(&mut self) -> &mut SessionStats {
        let window = Duration::from_secs(self.cfg.basis_window_secs);
        self.per_session.entry(self.current).or_insert_with(|| SessionStats::new(window))
    }

    pub fn status(&self) -> BasisStatus {
        self.status
    }

    pub fn beta0(&self) -> Option<f64> {
        self.stats().beta0
    }

    pub fn samples(&self) -> usize {
        self.stats().beta.len()
    }

    /// Fair price F = M_B · (1 + β₀ / 1e4).
    pub fn fair(&self, binance_mid: f64) -> Option<f64> {
        self.beta0().map(|b| binance_mid * (1.0 + b / 10_000.0))
    }

    /// Current D_stop (bps) given the base half-spread δ (bps).
    pub fn d_stop(&self, delta_bps: f64) -> f64 {
        let floor = self.cfg.dstop_delta_mult * delta_bps;
        match self.dstop_cache {
            Some(q) => floor.max(q),
            None => floor,
        }
    }

    /// 5‑minute drift: median(β over 5 min) − β₀, when both are available.
    pub fn drift_bps(&self) -> Option<f64> {
        let b0 = self.beta0()?;
        let v: Vec<f64> = self.drift.values().collect();
        median(&v).map(|m| m - b0)
    }

    /// Feed a synchronised observation. `delta_bps` is the current base
    /// half-spread, used for the D_stop floor. Returns the resulting status.
    pub fn observe(&mut self, inp: BasisInput, delta_bps: f64, now: Instant) -> BasisStatus {
        if inp.binance_mid <= 0.0 || inp.gate_ext_mid <= 0.0 {
            return self.status;
        }
        let beta_t = 10_000.0 * (inp.gate_ext_mid / inp.binance_mid - 1.0);
        self.last_beta_t = Some(beta_t);

        // Synchronisation / informativeness checks.
        let skew = if inp.binance_recv > inp.gate_recv {
            inp.binance_recv - inp.gate_recv
        } else {
            inp.gate_recv - inp.binance_recv
        };
        let synced = skew <= Duration::from_millis(self.cfg.basis_sync_tolerance_ms);
        let informative = inp.gate_ext_spread_bps.is_finite() && inp.gate_ext_spread_bps <= self.cfg.basis_max_ext_spread_bps;
        let valid = synced && informative;
        // Decimate samples so bursts of updates do not dominate the windows.
        let interval = Duration::from_millis(self.cfg.basis_sample_interval_ms);
        let due = self.stats().last_sample.map(|t| now.duration_since(t) >= interval).unwrap_or(true);

        let b0 = self.beta0();
        if let Some(b0) = b0 {
            let d = (beta_t - b0).abs();
            self.last_d = Some(d);
            let dstop = self.d_stop(delta_bps);
            match self.status {
                BasisStatus::Normal => {
                    if d > dstop {
                        self.status = BasisStatus::Abnormal;
                        self.recover_since = None;
                    } else if valid && due {
                        self.stats_mut().d_normal.push(now, d);
                    }
                }
                BasisStatus::Abnormal | BasisStatus::Recovering => {
                    if d <= dstop * self.cfg.basis_recover_ratio {
                        let since = *self.recover_since.get_or_insert(now);
                        self.status = BasisStatus::Recovering;
                        if now.duration_since(since) >= Duration::from_millis(self.cfg.basis_recover_ms) {
                            self.status = BasisStatus::Normal;
                            self.recover_since = None;
                        }
                    } else {
                        self.status = BasisStatus::Abnormal;
                        self.recover_since = None;
                    }
                }
                BasisStatus::WarmingUp => {}
            }
        }

        // β₀ is frozen while abnormal/recovering; samples still go to the drift
        // monitor for diagnostics.
        if valid && due {
            self.drift.push(now, beta_t);
            let frozen = matches!(self.status, BasisStatus::Abnormal | BasisStatus::Recovering);
            let st = self.stats_mut();
            st.last_sample = Some(now);
            if !frozen {
                st.beta.push(now, beta_t);
            }
        }
        self.maybe_recompute(now);
        self.status
    }

    fn maybe_recompute(&mut self, now: Instant) {
        let due = self.last_beta0_calc.map(|t| now.duration_since(t) >= Duration::from_secs(1)).unwrap_or(true);
        if !due {
            return;
        }
        self.last_beta0_calc = Some(now);
        let frozen = matches!(self.status, BasisStatus::Abnormal | BasisStatus::Recovering);
        let min = self.cfg.basis_min_samples;
        let q = self.cfg.dstop_quantile;
        let st = self.stats_mut();
        st.beta.evict(now);
        st.d_normal.evict(now);
        if !frozen {
            let v: Vec<f64> = st.beta.values().collect();
            if v.len() >= min {
                st.beta0 = median(&v);
            } else if v.is_empty() {
                st.beta0 = None;
            }
        }
        let dv: Vec<f64> = st.d_normal.values().collect();
        let enough_d = dv.len() >= min.max(2);
        let has_beta0 = st.beta0.is_some();
        let samples = st.beta.len();
        self.dstop_cache = if enough_d { quantile(&dv, q) } else { None };
        if self.status == BasisStatus::WarmingUp && has_beta0 && samples >= min {
            self.status = BasisStatus::Normal;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> config::Pricing {
        config::Pricing { basis_min_samples: 5, basis_sample_interval_ms: 0, basis_recover_ms: 1000, ..Default::default() }
    }

    fn inp(bm: f64, gm: f64, at: Instant) -> BasisInput {
        BasisInput { binance_mid: bm, binance_recv: at, gate_ext_mid: gm, gate_ext_spread_bps: 5.0, gate_recv: at }
    }

    #[test]
    fn beta0_is_median_and_fair_applies_it() {
        let t0 = Instant::now();
        let mut b = BasisEstimator::new(cfg());
        b.set_session(SessionKind::Regular);
        // Gate consistently 10 bps above Binance.
        for i in 0..6 {
            b.observe(inp(1000.0, 1001.0, t0 + Duration::from_millis(i * 10)), 2.0, t0 + Duration::from_millis(i * 10));
        }
        // force recompute after 1s
        b.observe(inp(1000.0, 1001.0, t0 + Duration::from_millis(1100)), 2.0, t0 + Duration::from_millis(1100));
        assert_eq!(b.status(), BasisStatus::Normal);
        let b0 = b.beta0().unwrap();
        assert!((b0 - 10.0).abs() < 1e-6, "b0={b0}");
        assert!((b.fair(1000.0).unwrap() - 1001.0).abs() < 1e-9);
    }

    #[test]
    fn abnormal_freezes_and_recovers_after_stability() {
        let t0 = Instant::now();
        let mut b = BasisEstimator::new(cfg());
        b.set_session(SessionKind::Regular);
        let mut t = t0;
        for _ in 0..10 {
            b.observe(inp(1000.0, 1001.0, t), 2.0, t);
            t += Duration::from_millis(150);
        }
        assert_eq!(b.status(), BasisStatus::Normal);
        // Gate jumps 40 bps above: D = 30 > D_stop = max(10, p99) → abnormal
        b.observe(inp(1000.0, 1004.0, t), 2.0, t);
        assert_eq!(b.status(), BasisStatus::Abnormal);
        let frozen_b0 = b.beta0().unwrap();
        // Keep feeding abnormal samples for 2s: β₀ must not move.
        for _ in 0..15 {
            t += Duration::from_millis(150);
            b.observe(inp(1000.0, 1004.0, t), 2.0, t);
        }
        assert_eq!(b.beta0().unwrap(), frozen_b0);
        assert_eq!(b.status(), BasisStatus::Abnormal);
        // Basis returns: needs 1s of stability before Normal.
        t += Duration::from_millis(150);
        b.observe(inp(1000.0, 1001.0, t), 2.0, t);
        assert_eq!(b.status(), BasisStatus::Recovering);
        t += Duration::from_millis(500);
        b.observe(inp(1000.0, 1001.0, t), 2.0, t);
        assert_eq!(b.status(), BasisStatus::Recovering);
        t += Duration::from_millis(600);
        b.observe(inp(1000.0, 1001.0, t), 2.0, t);
        assert_eq!(b.status(), BasisStatus::Normal);
    }

    #[test]
    fn unsynced_or_wide_samples_are_ignored() {
        let t0 = Instant::now();
        let mut b = BasisEstimator::new(cfg());
        b.set_session(SessionKind::Regular);
        let mut i = BasisInput { binance_mid: 1000.0, binance_recv: t0, gate_ext_mid: 1001.0, gate_ext_spread_bps: 5.0, gate_recv: t0 + Duration::from_secs(2) };
        for _ in 0..10 {
            b.observe(i, 2.0, t0 + Duration::from_secs(2));
        }
        assert_eq!(b.samples(), 0);
        i.gate_recv = t0;
        i.gate_ext_spread_bps = 100.0;
        for _ in 0..10 {
            b.observe(i, 2.0, t0);
        }
        assert_eq!(b.samples(), 0);
    }

    #[test]
    fn sessions_are_independent() {
        let t0 = Instant::now();
        let mut b = BasisEstimator::new(cfg());
        b.set_session(SessionKind::Regular);
        for k in 0..10 {
            let t = t0 + Duration::from_millis(k * 200);
            b.observe(inp(1000.0, 1001.0, t), 2.0, t);
        }
        assert!(b.beta0().is_some());
        b.set_session(SessionKind::Post);
        assert_eq!(b.status(), BasisStatus::WarmingUp);
        assert!(b.beta0().is_none());
        b.set_session(SessionKind::Regular);
        assert_eq!(b.status(), BasisStatus::Normal);
    }
}
