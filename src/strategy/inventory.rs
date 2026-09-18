//! Inventory model for Gate dual-position mode.
//!
//! `long` / `short` are non-negative contract counts. Net inventory
//! Q = long − short, converted to USDT notional with the fair price. The band
//! timer implements the "outside ±0.1H for more than 3 s" rule: it starts when
//! |Q| leaves the band and is only cleared once |Q| is back inside **and** no
//! risk-adding orders are pending.

use std::time::{Duration, Instant};

use crate::config;
use crate::types::{ContractMeta, clip};

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Positions {
    pub long: i64,
    pub short: i64,
    pub long_entry: f64,
    pub short_entry: f64,
}

impl Positions {
    pub fn net(&self) -> i64 {
        self.long - self.short
    }
    pub fn gross(&self) -> i64 {
        self.long + self.short
    }
    pub fn is_flat(&self) -> bool {
        self.long == 0 && self.short == 0
    }
    /// Apply a fill in dual mode. `reduce` fills close the opposite side and
    /// can never open a position (a reduce-only fill larger than the locally
    /// known position is clamped; exchange position snapshots reconcile the
    /// rest). Open fills add to their own side. Returns the realised PnL (USDT).
    pub fn apply_fill(&mut self, signed_size: i64, price: f64, reduce: bool, meta: &ContractMeta) -> f64 {
        let qty = signed_size.abs();
        let mult = meta.multiplier_f64();
        if signed_size > 0 {
            if reduce {
                let q = qty.min(self.short);
                if q < qty {
                    tracing::warn!(fill = qty, short = self.short, "reduce-only buy exceeds local short; clamping");
                }
                let pnl = (self.short_entry - price) * q as f64 * mult;
                self.short -= q;
                if self.short == 0 {
                    self.short_entry = 0.0;
                }
                pnl
            } else {
                self.add_long(qty, price);
                0.0
            }
        } else if signed_size < 0 {
            if reduce {
                let q = qty.min(self.long);
                if q < qty {
                    tracing::warn!(fill = qty, long = self.long, "reduce-only sell exceeds local long; clamping");
                }
                let pnl = (price - self.long_entry) * q as f64 * mult;
                self.long -= q;
                if self.long == 0 {
                    self.long_entry = 0.0;
                }
                pnl
            } else {
                self.add_short(qty, price);
                0.0
            }
        } else {
            0.0
        }
    }

    fn add_long(&mut self, qty: i64, price: f64) {
        let tot = self.long + qty;
        if tot > 0 {
            self.long_entry = (self.long_entry * self.long as f64 + price * qty as f64) / tot as f64;
        }
        self.long = tot;
    }

    fn add_short(&mut self, qty: i64, price: f64) {
        let tot = self.short + qty;
        if tot > 0 {
            self.short_entry = (self.short_entry * self.short as f64 + price * qty as f64) / tot as f64;
        }
        self.short = tot;
    }

    /// Unrealised PnL at `mark`.
    pub fn unrealised(&self, mark: f64, meta: &ContractMeta) -> f64 {
        let mult = meta.multiplier_f64();
        (mark - self.long_entry) * self.long as f64 * mult + (self.short_entry - mark) * self.short as f64 * mult
    }
}

/// Pending (not yet final) order quantities that could still change inventory.
#[derive(Debug, Clone, Copy, Default)]
pub struct PendingExposure {
    /// Contracts of risk-adding buy orders (open long), live + in flight.
    pub open_buy: i64,
    /// Contracts of risk-adding sell orders (open short).
    pub open_sell: i64,
    /// Contracts of reduce-only buys (closing short).
    pub reduce_buy: i64,
    /// Contracts of reduce-only sells (closing long).
    pub reduce_sell: i64,
}

#[derive(Debug, Clone)]
pub struct InventoryState {
    pub pos: Positions,
    pub cfg: config::Inventory,
    band_since: Option<Instant>,
    /// Timestamp when |Q| last left the band (for digestion-time metrics).
    pub episode_start: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
pub struct InventorySnapshot {
    pub q_usdt: f64,
    pub u: f64,
    pub in_band: bool,
    /// Time spent outside the band so far, if any.
    pub out_of_band_for: Option<Duration>,
}

impl InventoryState {
    pub fn new(cfg: config::Inventory) -> Self {
        Self { pos: Positions::default(), cfg, band_since: None, episode_start: None }
    }

    pub fn h(&self) -> f64 {
        self.cfg.max_net_usdt
    }

    pub fn q_usdt(&self, fair: f64, meta: &ContractMeta) -> f64 {
        meta.notional(self.pos.net(), fair)
    }

    /// u = clip(Q / H, −1, 1)
    pub fn u(&self, fair: f64, meta: &ContractMeta) -> f64 {
        clip(self.q_usdt(fair, meta) / self.h(), -1.0, 1.0)
    }

    pub fn band_usdt(&self) -> f64 {
        self.cfg.band_ratio * self.h()
    }

    /// Contracts allowed inside the band at `fair`.
    pub fn band_contracts(&self, fair: f64, meta: &ContractMeta) -> i64 {
        meta.contracts_for_notional(self.band_usdt(), fair)
    }

    /// Contracts corresponding to H at `fair`.
    pub fn h_contracts(&self, fair: f64, meta: &ContractMeta) -> i64 {
        meta.contracts_for_notional(self.h(), fair)
    }

    /// Update the band timer. Must be called whenever inventory or pending
    /// exposure changes and on every timer tick.
    pub fn tick(&mut self, fair: f64, meta: &ContractMeta, pending_open_adds: i64, now: Instant) -> InventorySnapshot {
        let q_usdt = self.q_usdt(fair, meta);
        let in_band = q_usdt.abs() <= self.band_usdt();
        if !in_band {
            if self.band_since.is_none() {
                self.band_since = Some(now);
                self.episode_start = Some(now);
            }
        } else if pending_open_adds == 0 {
            // Only end the episode once we are back in the band with no
            // unresolved risk-adding orders.
            self.band_since = None;
        }
        InventorySnapshot {
            q_usdt,
            u: clip(q_usdt / self.h(), -1.0, 1.0),
            in_band,
            out_of_band_for: self.band_since.map(|t| now.saturating_duration_since(t)),
        }
    }

    /// Whether the band timeout has elapsed.
    pub fn band_timed_out(&self, now: Instant) -> bool {
        self.band_since
            .map(|t| now.saturating_duration_since(t) >= Duration::from_millis(self.cfg.band_timeout_ms))
            .unwrap_or(false)
    }

    pub fn clear_episode(&mut self) {
        self.band_since = None;
        self.episode_start = None;
    }

    /// Worst-case net inventory (contracts) if every buy order filled and no sell did,
    /// and symmetrically for sells. Reduce-only orders cannot flip a side.
    pub fn worst_case(&self, pend: PendingExposure) -> (i64, i64) {
        let long_after_reduce = (self.pos.long - pend.reduce_sell).max(0);
        let short_after_reduce = (self.pos.short - pend.reduce_buy).max(0);
        // all buys fill: longs grow by open_buy, shorts shrink by reduce_buy
        let worst_long = (self.pos.long + pend.open_buy) - short_after_reduce;
        // all sells fill: shorts grow by open_sell, longs shrink by reduce_sell
        let worst_short = long_after_reduce - (self.pos.short + pend.open_sell);
        (worst_long, worst_short)
    }

    /// Max additional open-buy contracts allowed by the net cap H and the gross cap.
    pub fn open_buy_capacity(&self, pend: PendingExposure, fair: f64, meta: &ContractMeta) -> i64 {
        let h_c = self.h_contracts(fair, meta);
        let (worst_long, _) = self.worst_case(pend);
        let net_room = h_c - worst_long;
        let gross_c = meta.contracts_for_notional(self.cfg.max_gross_usdt, fair);
        let gross_room = gross_c - (self.pos.gross() + pend.open_buy + pend.open_sell);
        net_room.min(gross_room).max(0)
    }

    pub fn open_sell_capacity(&self, pend: PendingExposure, fair: f64, meta: &ContractMeta) -> i64 {
        let h_c = self.h_contracts(fair, meta);
        let (_, worst_short) = self.worst_case(pend);
        let net_room = h_c + worst_short; // worst_short is negative when short
        let gross_c = meta.contracts_for_notional(self.cfg.max_gross_usdt, fair);
        let gross_room = gross_c - (self.pos.gross() + pend.open_buy + pend.open_sell);
        net_room.min(gross_room).max(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn meta() -> ContractMeta {
        ContractMeta {
            name: "SNDK_USDT".into(),
            tick: Decimal::from_str("0.01").unwrap(),
            multiplier: Decimal::from_str("0.01").unwrap(),
            order_size_min: 1,
            order_size_max: 1_000_000,
            orders_limit: 100,
            price_deviate: 0.05,
            funding_interval_secs: 28800,
            funding_next_apply: 0,
            maker_fee: -0.0001,
            taker_fee: 0.00075,
            in_delisting: false,
        }
    }

    #[test]
    fn dual_mode_fill_mapping() {
        let m = meta();
        let mut p = Positions::default();
        p.apply_fill(10, 1000.0, false, &m); // open long 10
        assert_eq!((p.long, p.short), (10, 0));
        let pnl = p.apply_fill(-4, 1002.0, true, &m); // reduce long 4 at +2
        assert_eq!((p.long, p.short), (6, 0));
        assert!((pnl - 4.0 * 0.01 * 2.0).abs() < 1e-9);
        p.apply_fill(-3, 999.0, false, &m); // open short 3 while long -> both exist
        assert_eq!((p.long, p.short), (6, 3));
        assert_eq!(p.net(), 3);
        assert_eq!(p.gross(), 9);
        let pnl = p.apply_fill(5, 998.0, true, &m); // reduce short 3; the excess 2 is clamped (reduce-only never opens)
        assert_eq!((p.long, p.short), (6, 0));
        assert!((pnl - 3.0 * 0.01 * 1.0).abs() < 1e-9);
    }

    #[test]
    fn u_and_band_timer() {
        let m = meta();
        let cfg = config::Inventory { max_net_usdt: 5000.0, max_gross_usdt: 5000.0, band_ratio: 0.1, band_timeout_ms: 3000, stop_add_u: 0.5 };
        let mut inv = InventoryState::new(cfg);
        let t0 = Instant::now();
        let fair = 1000.0;
        // 250 contracts * 0.01 * 1000 = 2500 USDT = 0.5H
        inv.pos.apply_fill(250, fair, false, &m);
        let s = inv.tick(fair, &m, 0, t0);
        assert!((s.u - 0.5).abs() < 1e-9);
        assert!(!s.in_band);
        assert!(!inv.band_timed_out(t0 + Duration::from_millis(2999)));
        assert!(inv.band_timed_out(t0 + Duration::from_millis(3000)));
        // reduce to 40 contracts = 400 USDT (in band) but with a pending add: timer stays
        inv.pos.apply_fill(-210, fair, true, &m);
        let s = inv.tick(fair, &m, 5, t0 + Duration::from_millis(3100));
        assert!(s.in_band);
        assert!(inv.band_timed_out(t0 + Duration::from_millis(3100)));
        // once no pending adds, the episode ends
        inv.tick(fair, &m, 0, t0 + Duration::from_millis(3200));
        assert!(!inv.band_timed_out(t0 + Duration::from_millis(9000)));
    }

    #[test]
    fn capacity_uses_worst_case_per_side() {
        let m = meta();
        let cfg = config::Inventory { max_net_usdt: 5000.0, max_gross_usdt: 5000.0, band_ratio: 0.1, band_timeout_ms: 3000, stop_add_u: 0.5 };
        let mut inv = InventoryState::new(cfg);
        let fair = 1000.0; // H = 500 contracts
        inv.pos.apply_fill(100, fair, false, &m);
        let pend = PendingExposure { open_buy: 50, open_sell: 30, reduce_buy: 0, reduce_sell: 60 };
        let (wl, ws) = inv.worst_case(pend);
        assert_eq!(wl, 150); // 100 + 50
        assert_eq!(ws, 40 - 30); // (100-60) - 30 = 10
        // net room = 500 - 150 = 350; gross room = 500 - (100 + 50 + 30) = 320
        assert_eq!(inv.open_buy_capacity(pend, fair, &m), 320);
        // sell room: net = 500 + 10 = 510, gross 320 -> 320
        assert_eq!(inv.open_sell_capacity(pend, fair, &m), 320);
    }
}
