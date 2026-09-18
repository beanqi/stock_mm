//! Trust / risk gates evaluated every cycle (spec §4.4, §7 step 2).
//!
//! Anything that makes market data, private state, positions or margin
//! untrustworthy removes the permission to add risk and (when severe) the
//! permission to keep any resting order at all.

use std::time::{Duration, Instant};

use serde::Serialize;

use crate::config;
use crate::market::basis::BasisStatus;
use crate::market::session::SessionKind;
use crate::strategy::inventory::Positions;

#[derive(Debug, Clone, Copy)]
pub struct TrustInputs {
    pub binance_age: Option<Duration>,
    pub gate_bbo_age: Option<Duration>,
    pub gate_book_synced: bool,
    pub private_connected: bool,
    pub private_age: Option<Duration>,
    pub account_age: Option<Duration>,
    pub position_ok: bool,
    /// Exchange-reported available margin (USDT), if known.
    pub available_margin: Option<f64>,
    pub basis: BasisStatus,
    pub session: SessionKind,
    pub session_tradable: bool,
    pub loss_limit_hit: bool,
    pub halted: bool,
    /// |F / mark − 1| in bps when both are known.
    pub mark_deviation_bps: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TrustReport {
    /// Market data on both venues is fresh and the Gate book is consistent.
    pub market_ok: bool,
    /// Private stream and account state are trustworthy.
    pub private_ok: bool,
    pub position_ok: bool,
    pub margin_ok: bool,
    pub basis_ok: bool,
    pub session_ok: bool,
    pub reasons: Vec<String>,
}

impl TrustReport {
    /// Maker reduce orders may rest (we can still see the market and our fills).
    pub fn allow_resting(&self) -> bool {
        self.market_ok && self.private_ok
    }
    /// Risk-adding orders allowed.
    pub fn allow_new_risk(&self) -> bool {
        self.market_ok && self.private_ok && self.position_ok && self.margin_ok && self.basis_ok && self.session_ok
    }
}

pub fn evaluate(cfg: &config::Risk, inp: TrustInputs) -> TrustReport {
    let mut r = TrustReport::default();
    let stale = |age: Option<Duration>, limit_ms: u64| age.map(|a| a > Duration::from_millis(limit_ms)).unwrap_or(true);

    r.market_ok = true;
    if stale(inp.binance_age, cfg.binance_stale_ms) {
        r.market_ok = false;
        r.reasons.push("binance_stale".into());
    }
    if stale(inp.gate_bbo_age, cfg.gate_stale_ms) {
        r.market_ok = false;
        r.reasons.push("gate_stale".into());
    }
    if !inp.gate_book_synced {
        r.market_ok = false;
        r.reasons.push("gate_book_unsynced".into());
    }

    r.private_ok = inp.private_connected && !stale(inp.private_age, cfg.private_stale_ms);
    if !r.private_ok {
        r.reasons.push("private_feed".into());
    }
    if stale(inp.account_age, cfg.account_poll_ms * 5) {
        r.private_ok = false;
        r.reasons.push("account_stale".into());
    }

    r.position_ok = inp.position_ok;
    if !inp.position_ok {
        r.reasons.push("position_mismatch".into());
    }

    r.margin_ok = match inp.available_margin {
        Some(a) => a > cfg.margin_buffer_usdt,
        None => false,
    };
    if !r.margin_ok {
        r.reasons.push("margin".into());
    }

    r.basis_ok = inp.basis == BasisStatus::Normal;
    if !r.basis_ok {
        r.reasons.push(format!("basis_{:?}", inp.basis).to_lowercase());
    }
    if let Some(dev) = inp.mark_deviation_bps {
        if dev.abs() > cfg.mark_deviation_bps {
            r.basis_ok = false;
            r.reasons.push("mark_deviation".into());
        }
    }

    r.session_ok = inp.session_tradable && !matches!(inp.session, SessionKind::Weekend | SessionKind::Blackout);
    if !r.session_ok {
        r.reasons.push(format!("session_{}", inp.session.label()));
    }

    if inp.loss_limit_hit {
        r.reasons.push("loss_limit".into());
        r.basis_ok = false; // no new risk
    }
    if inp.halted {
        r.reasons.push("halted".into());
        r.basis_ok = false;
        r.session_ok = false;
    }
    r
}

/// Compares locally maintained positions with exchange-reported ones.
#[derive(Debug, Clone, Default)]
pub struct PositionReconciler {
    pub exchange: Option<Positions>,
    pub exchange_at: Option<Instant>,
    mismatch_since: Option<Instant>,
}

impl PositionReconciler {
    pub fn on_exchange(&mut self, p: Positions, now: Instant) {
        self.exchange = Some(p);
        self.exchange_at = Some(now);
    }

    /// Returns `true` if positions agree (within tolerance / grace period).
    pub fn check(&mut self, local: Positions, cfg: &config::Risk, now: Instant) -> bool {
        let Some(ex) = self.exchange else {
            // Nothing to compare yet: trust local until the first snapshot arrives.
            return true;
        };
        let tol = cfg.position_mismatch_tolerance;
        let ok = (ex.long - local.long).abs() <= tol && (ex.short - local.short).abs() <= tol;
        if ok {
            self.mismatch_since = None;
            true
        } else {
            let since = *self.mismatch_since.get_or_insert(now);
            now.saturating_duration_since(since) < Duration::from_millis(cfg.position_mismatch_ms)
        }
    }

}

/// True equity tracking: realised + unrealised + fees + funding.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Pnl {
    pub realised: f64,
    pub fees: f64,
    /// Balance changes reported by the exchange that are not fills/fees (funding etc.).
    pub funding: f64,
    pub unrealised: f64,
    pub start_equity: Option<f64>,
}

impl Pnl {
    pub fn equity_change(&self) -> f64 {
        self.realised - self.fees + self.funding + self.unrealised
    }
    pub fn loss_limit_hit(&self, limit: f64) -> bool {
        limit > 0.0 && self.equity_change() <= -limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(now: Instant) -> TrustInputs {
        TrustInputs {
            binance_age: Some(Duration::from_millis(50)),
            gate_bbo_age: Some(Duration::from_millis(50)),
            gate_book_synced: true,
            private_connected: true,
            private_age: Some(Duration::from_millis(100)),
            account_age: Some(Duration::from_millis(500)),
            position_ok: true,
            available_margin: Some(1000.0),
            basis: BasisStatus::Normal,
            session: SessionKind::Regular,
            session_tradable: true,
            loss_limit_hit: false,
            halted: false,
            mark_deviation_bps: Some(1.0),
        }
    }

    #[test]
    fn mark_deviation_blocks_new_risk_only() {
        let mut i = base(Instant::now());
        i.mark_deviation_bps = Some(80.0);
        let r = evaluate(&config::Risk::default(), i);
        assert!(r.allow_resting());
        assert!(!r.allow_new_risk());
        assert!(r.reasons.iter().any(|x| x == "mark_deviation"));
    }

    #[test]
    fn all_good_allows_everything() {
        let r = evaluate(&config::Risk::default(), base(Instant::now()));
        assert!(r.allow_new_risk());
        assert!(r.allow_resting());
        assert!(r.reasons.is_empty());
    }

    #[test]
    fn stale_binance_blocks_resting_and_new_risk() {
        let mut i = base(Instant::now());
        i.binance_age = Some(Duration::from_secs(10));
        let r = evaluate(&config::Risk::default(), i);
        assert!(!r.allow_resting());
        assert!(!r.allow_new_risk());
    }

    #[test]
    fn basis_or_session_only_blocks_new_risk() {
        let mut i = base(Instant::now());
        i.basis = BasisStatus::Abnormal;
        let r = evaluate(&config::Risk::default(), i);
        assert!(r.allow_resting());
        assert!(!r.allow_new_risk());
        let mut i = base(Instant::now());
        i.session = SessionKind::Weekend;
        i.session_tradable = false;
        let r = evaluate(&config::Risk::default(), i);
        assert!(r.allow_resting());
        assert!(!r.allow_new_risk());
    }

    #[test]
    fn reconciler_grace_period() {
        let cfg = config::Risk::default();
        let t0 = Instant::now();
        let mut rc = PositionReconciler::default();
        let local = Positions { long: 10, short: 0, long_entry: 1.0, short_entry: 0.0 };
        assert!(rc.check(local, &cfg, t0));
        rc.on_exchange(Positions { long: 12, short: 0, long_entry: 1.0, short_entry: 0.0 }, t0);
        assert!(rc.check(local, &cfg, t0 + Duration::from_millis(500)));
        assert!(!rc.check(local, &cfg, t0 + Duration::from_millis(2500)));
        rc.on_exchange(local, t0 + Duration::from_secs(3));
        assert!(rc.check(local, &cfg, t0 + Duration::from_secs(3)));
    }
}
