//! Per-side protection state machine driven by the Binance velocity signal
//! and the Gate flow-impact signal.
//!
//! Signal → level (None / Light / Strong) per **endangered quoting side**:
//! - Binance rising fast endangers our asks (sell side) and vice versa.
//! - Gate taker buying endangers our asks.
//!
//! When the signal clears, the side goes through a staged recovery:
//! keep the restriction for `recover_half_ms`, then half size until
//! `recover_full_ms`, then full size.

use std::time::{Duration, Instant};

use crate::config;
use crate::market::impact::ImpactSignal;
use crate::types::Side;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub enum Level {
    None,
    Light,
    Strong,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Phase {
    Normal,
    /// Signal active at `level`.
    Active(Level),
    /// Signal cleared; still applying the last level until `recover_half_ms`.
    Hold(Level),
    /// Half size, normal distance.
    Half,
}

#[derive(Debug, Clone)]
pub struct SideProtection {
    pub phase: Phase,
    /// Instant when the signal last cleared.
    cleared_at: Option<Instant>,
    last_level: Level,
}

impl Default for SideProtection {
    fn default() -> Self {
        Self { phase: Phase::Normal, cleared_at: None, last_level: Level::None }
    }
}

/// How the quoting engine must treat a side right now.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SidePolicy {
    /// Allow risk-adding orders on this side at all.
    pub allow_open: bool,
    /// Multiplier for open-order sizes.
    pub size_mult: f64,
    /// Multiplier for the open-order distance (δ) from the quote centre.
    pub distance_mult: f64,
    /// Follow the fair price on every update (ignore the re-quote threshold).
    pub follow_tight: bool,
    pub level: Level,
}

impl SidePolicy {
    pub const NORMAL: SidePolicy = SidePolicy { allow_open: true, size_mult: 1.0, distance_mult: 1.0, follow_tight: false, level: Level::None };
}

#[derive(Debug, Clone)]
pub struct Protection {
    cfg: config::Protection,
    pub buy: SideProtection,
    pub sell: SideProtection,
    pub last_velocity_bps: f64,
    pub last_impact: Option<ImpactSignal>,
}

impl Protection {
    pub fn new(cfg: config::Protection) -> Self {
        Self { cfg, buy: SideProtection::default(), sell: SideProtection::default(), last_velocity_bps: 0.0, last_impact: None }
    }

    /// Update from the latest signals. `velocity_bps` is v100 (positive = Binance rising),
    /// `delta_bps` the current base half-spread.
    pub fn update(&mut self, velocity_bps: f64, delta_bps: f64, impact: Option<ImpactSignal>, now: Instant) {
        self.last_velocity_bps = velocity_bps;
        self.last_impact = impact;
        let mut sell_level = Level::None;
        let mut buy_level = Level::None;
        let mag = velocity_bps.abs();
        let vel_level = if delta_bps > 0.0 && mag >= self.cfg.strong_velocity_delta * delta_bps {
            Level::Strong
        } else if delta_bps > 0.0 && mag >= self.cfg.light_velocity_delta * delta_bps {
            Level::Light
        } else {
            Level::None
        };
        if vel_level != Level::None {
            if velocity_bps > 0.0 {
                sell_level = vel_level;
            } else {
                buy_level = vel_level;
            }
        }
        if let Some(sig) = impact {
            let lvl = if self.cfg.impact_is_strong { Level::Strong } else { Level::Light };
            match sig.endangered {
                Side::Sell => sell_level = sell_level.max(lvl),
                Side::Buy => buy_level = buy_level.max(lvl),
            }
        }
        let cfg = &self.cfg;
        Self::advance(&mut self.sell, sell_level, cfg, now);
        Self::advance(&mut self.buy, buy_level, cfg, now);
    }

    fn advance(sp: &mut SideProtection, level: Level, cfg: &config::Protection, now: Instant) {
        if level != Level::None {
            sp.phase = Phase::Active(level);
            sp.last_level = level;
            sp.cleared_at = None;
            return;
        }
        match sp.phase {
            Phase::Normal => {}
            Phase::Active(l) => {
                sp.cleared_at = Some(now);
                sp.phase = Phase::Hold(l);
            }
            Phase::Hold(_) | Phase::Half => {
                let since = sp.cleared_at.unwrap_or(now);
                let el = now.saturating_duration_since(since);
                if el >= Duration::from_millis(cfg.recover_full_ms) {
                    sp.phase = Phase::Normal;
                    sp.cleared_at = None;
                } else if el >= Duration::from_millis(cfg.recover_half_ms) {
                    sp.phase = Phase::Half;
                }
            }
        }
    }

    fn side(&self, side: Side) -> &SideProtection {
        match side {
            Side::Buy => &self.buy,
            Side::Sell => &self.sell,
        }
    }

    /// Policy for `side`, taking into account the *other* side's strong state
    /// (strong pressure on one side halves and tightens the other).
    pub fn policy(&self, side: Side) -> SidePolicy {
        let own = self.side(side);
        let other = self.side(side.opposite());
        let mut p = SidePolicy::NORMAL;
        match own.phase {
            Phase::Normal => {}
            Phase::Active(Level::Light) | Phase::Hold(Level::Light) => {
                p.size_mult *= self.cfg.light_size_mult;
                p.distance_mult *= self.cfg.light_distance_mult;
                p.level = Level::Light;
            }
            Phase::Active(Level::Strong) | Phase::Hold(Level::Strong) => {
                p.allow_open = false;
                p.size_mult = 0.0;
                p.level = Level::Strong;
            }
            Phase::Half => p.size_mult *= 0.5,
            Phase::Active(Level::None) | Phase::Hold(Level::None) => {}
        }
        if matches!(other.phase, Phase::Active(Level::Strong) | Phase::Hold(Level::Strong)) {
            p.size_mult = p.size_mult.min(self.cfg.strong_other_side_size_mult);
            p.follow_tight = true;
        }
        p
    }

    /// Strong pressure direction, if any: `Some(Side::Sell)` means Binance is
    /// rising hard (asks endangered), `Some(Side::Buy)` means falling hard.
    pub fn strong_side(&self) -> Option<Side> {
        if matches!(self.sell.phase, Phase::Active(Level::Strong)) {
            Some(Side::Sell)
        } else if matches!(self.buy.phase, Phase::Active(Level::Strong)) {
            Some(Side::Buy)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn velocity_levels_and_staged_recovery() {
        let cfg = config::Protection::default();
        let mut p = Protection::new(cfg);
        let t0 = Instant::now();
        // +2.5 bps with δ = 2 → light on sell side
        p.update(2.5, 2.0, None, t0);
        let s = p.policy(Side::Sell);
        assert_eq!(s.level, Level::Light);
        assert_eq!(s.size_mult, 0.5);
        assert_eq!(s.distance_mult, 1.5);
        assert!(p.policy(Side::Buy) == SidePolicy::NORMAL);
        // +5 bps → strong on sell: no opens, buy side halves and follows tight
        p.update(5.0, 2.0, None, t0 + Duration::from_millis(10));
        assert!(!p.policy(Side::Sell).allow_open);
        let b = p.policy(Side::Buy);
        assert_eq!(b.size_mult, 0.5);
        assert!(b.follow_tight);
        assert_eq!(p.strong_side(), Some(Side::Sell));
        // signal clears: hold restrictive until 500ms, half until 2s, then full
        let tc = t0 + Duration::from_millis(20);
        p.update(0.0, 2.0, None, tc);
        assert!(!p.policy(Side::Sell).allow_open);
        p.update(0.0, 2.0, None, tc + Duration::from_millis(499));
        assert!(!p.policy(Side::Sell).allow_open);
        p.update(0.0, 2.0, None, tc + Duration::from_millis(500));
        let s = p.policy(Side::Sell);
        assert!(s.allow_open);
        assert_eq!(s.size_mult, 0.5);
        assert_eq!(s.distance_mult, 1.0);
        // buy side is also released from the "other side strong" constraint
        assert!(!p.policy(Side::Buy).follow_tight);
        p.update(0.0, 2.0, None, tc + Duration::from_millis(2000));
        assert_eq!(p.policy(Side::Sell), SidePolicy::NORMAL);
        assert_eq!(p.strong_side(), None);
    }

    #[test]
    fn impact_marks_endangered_side() {
        let mut p = Protection::new(config::Protection::default());
        let t0 = Instant::now();
        let sig = ImpactSignal { endangered: Side::Buy, share: 0.9, notional: 1e4, threshold: 5e3 };
        p.update(0.0, 2.0, Some(sig), t0);
        assert_eq!(p.policy(Side::Buy).level, Level::Light);
        assert_eq!(p.policy(Side::Sell).level, Level::None);
    }
}
