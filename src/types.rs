//! Core value types shared across the engine.
//!
//! Prices on the Gate side are represented as *integer tick counts* (`Ticks`)
//! so that quote arithmetic (±1 tick, floor/ceil) is exact. Sizes on Gate are
//! integer contract counts. Statistics are plain `f64` in bps.

use std::fmt;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn sign(self) -> i64 {
        match self {
            Side::Buy => 1,
            Side::Sell => -1,
        }
    }
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Why an order exists: adding inventory or reducing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Purpose {
    Open,
    Reduce,
}

/// Price expressed in whole ticks of the Gate contract.
pub type Ticks = i64;

/// Gate contract metadata required by the engine.
#[derive(Debug, Clone, PartialEq)]
pub struct ContractMeta {
    pub name: String,
    /// Price step (`order_price_round`).
    pub tick: Decimal,
    /// Underlying units per contract (`quanto_multiplier`).
    pub multiplier: Decimal,
    pub order_size_min: i64,
    pub order_size_max: i64,
    /// Max open orders per contract (`orders_limit`).
    pub orders_limit: i64,
    /// Max allowed |price / last - 1| for limit orders (`order_price_deviate`).
    pub price_deviate: f64,
    pub funding_interval_secs: i64,
    pub funding_next_apply: i64,
    /// Contract-level default fee rates (account-level rates override).
    pub maker_fee: f64,
    pub taker_fee: f64,
    pub in_delisting: bool,
}

impl ContractMeta {
    pub fn tick_f64(&self) -> f64 {
        self.tick.to_f64().unwrap_or(0.0)
    }
    pub fn multiplier_f64(&self) -> f64 {
        self.multiplier.to_f64().unwrap_or(0.0)
    }
    /// Notional (USDT) of `contracts` at `price`.
    pub fn notional(&self, contracts: i64, price: f64) -> f64 {
        contracts as f64 * self.multiplier_f64() * price
    }
    /// Contracts whose notional is at most `usdt` at `price` (floored).
    pub fn contracts_for_notional(&self, usdt: f64, price: f64) -> i64 {
        let unit = self.multiplier_f64() * price;
        if unit <= 0.0 || !usdt.is_finite() {
            return 0;
        }
        (usdt / unit + 1e-9).floor().max(0.0) as i64
    }
}

/// Tick conversions bound to a specific tick size.
#[derive(Debug, Clone, Copy)]
pub struct TickGrid {
    tick: f64,
    tick_dec: Decimal,
    scale: u32,
}

impl TickGrid {
    pub fn new(tick: Decimal) -> Self {
        let tick_n = tick.normalize();
        Self {
            tick: tick_n.to_f64().unwrap_or(0.01),
            tick_dec: tick_n,
            scale: tick_n.scale(),
        }
    }

    pub fn tick_size(&self) -> f64 {
        self.tick
    }

    /// Largest tick count whose price is ≤ `px` (with a tiny tolerance for FP noise).
    pub fn floor(&self, px: f64) -> Ticks {
        (px / self.tick + 1e-7).floor() as Ticks
    }

    /// Smallest tick count whose price is ≥ `px`.
    pub fn ceil(&self, px: f64) -> Ticks {
        (px / self.tick - 1e-7).ceil() as Ticks
    }

    pub fn round(&self, px: f64) -> Ticks {
        (px / self.tick).round() as Ticks
    }

    pub fn to_f64(&self, t: Ticks) -> f64 {
        t as f64 * self.tick
    }

    pub fn to_decimal(&self, t: Ticks) -> Decimal {
        (Decimal::from(t) * self.tick_dec).round_dp(self.scale)
    }

    /// Wire representation of a price expressed in ticks.
    pub fn to_string(&self, t: Ticks) -> String {
        self.to_decimal(t).to_string()
    }

    /// Parse an exchange price string to ticks (rounded to nearest).
    pub fn parse(&self, s: &str) -> Option<Ticks> {
        let d: Decimal = s.parse().ok()?;
        let q = (d / self.tick_dec).round();
        q.to_i64()
    }

    /// bps value of one tick at price `px`.
    pub fn tick_bps(&self, px: f64) -> f64 {
        if px <= 0.0 { 0.0 } else { 10_000.0 * self.tick / px }
    }
}

#[inline]
pub fn clip(v: f64, lo: f64, hi: f64) -> f64 {
    v.max(lo).min(hi)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn tick_floor_ceil_exact_on_grid() {
        let g = TickGrid::new(Decimal::from_str("0.01").unwrap());
        assert_eq!(g.floor(999.80), 99980);
        assert_eq!(g.ceil(999.80), 99980);
        assert_eq!(g.floor(999.801), 99980);
        assert_eq!(g.ceil(999.801), 99981);
        assert_eq!(g.floor(1000.0 - 1000.0 * 2.0 / 10_000.0), 99980);
        assert_eq!(g.to_string(99980), "999.80");
        assert_eq!(g.parse("999.80"), Some(99980));
        assert_eq!(g.to_decimal(100029).to_string(), "1000.29");
    }

    #[test]
    fn contracts_for_notional_floors() {
        let m = ContractMeta {
            name: "X".into(),
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
        };
        // 250 USDT at 1546 with 0.01 multiplier -> 16.17 contracts -> 16
        assert_eq!(m.contracts_for_notional(250.0, 1546.0), 16);
        assert!((m.notional(16, 1546.0) - 247.36).abs() < 1e-9);
    }
}
