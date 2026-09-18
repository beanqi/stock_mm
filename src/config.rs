//! Strategy / connectivity configuration.
//!
//! Loaded from a TOML file; API credentials come from environment variables
//! (`GATE_API_KEY`, `GATE_API_SECRET`) or a `.env` file so that secrets never
//! live in the config file.

use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub mode: Mode,
    pub instruments: Instruments,
    pub endpoints: Endpoints,
    pub pricing: Pricing,
    pub quoting: Quoting,
    pub inventory: Inventory,
    pub protection: Protection,
    pub exit: Exit,
    pub risk: Risk,
    pub orders: Orders,
    pub session: Session,
    pub metrics: Metrics,
    pub paper: Paper,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Mode::Paper,
            instruments: Instruments::default(),
            endpoints: Endpoints::default(),
            pricing: Pricing::default(),
            quoting: Quoting::default(),
            inventory: Inventory::default(),
            protection: Protection::default(),
            exit: Exit::default(),
            risk: Risk::default(),
            orders: Orders::default(),
            session: Session::default(),
            metrics: Metrics::default(),
            paper: Paper::default(),
        }
    }
}

/// Paper-mode simulation parameters.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Paper {
    /// Starting balance of the simulated account (USDT).
    pub balance_usdt: f64,
}

impl Default for Paper {
    fn default() -> Self {
        Self { balance_usdt: 10_000.0 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Full market data + private feeds, but orders are simulated locally.
    Paper,
    /// Real orders on Gate.
    Live,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Instruments {
    /// Binance USDⓈ-M symbol, e.g. `SNDKUSDT`.
    pub binance_symbol: String,
    /// Gate USDT futures contract, e.g. `SNDK_USDT`.
    pub gate_contract: String,
    /// Gate settle currency path segment (`usdt`).
    pub gate_settle: String,
}

impl Default for Instruments {
    fn default() -> Self {
        Self {
            binance_symbol: "SNDKUSDT".into(),
            gate_contract: "SNDK_USDT".into(),
            gate_settle: "usdt".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Endpoints {
    pub binance_ws: String,
    pub binance_rest: String,
    pub gate_ws: String,
    pub gate_rest: String,
}

impl Default for Endpoints {
    fn default() -> Self {
        Self {
            binance_ws: "wss://fstream.binance.com/stream".into(),
            binance_rest: "https://fapi.binance.com".into(),
            gate_ws: "wss://fx-ws.gateio.ws/v4/ws/usdt".into(),
            gate_rest: "https://api.gateio.ws".into(),
        }
    }
}

/// Fair-value (F) construction: Binance mid + natural basis.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Pricing {
    /// Main basis window (seconds). Spec: 1 hour.
    pub basis_window_secs: u64,
    /// Drift observation window (seconds). Spec: 5 minutes.
    pub drift_window_secs: u64,
    /// Minimum number of valid basis samples before quoting in a session.
    pub basis_min_samples: usize,
    /// Minimum spacing between basis samples (ms) to avoid over-weighting bursts.
    pub basis_sample_interval_ms: u64,
    /// Max age difference between the Binance and Gate quotes used for a sample (ms).
    pub basis_sync_tolerance_ms: u64,
    /// External Gate spread (bps) above which the Gate quote is considered
    /// self-dominated / uninformative and the sample is skipped.
    pub basis_max_ext_spread_bps: f64,
    /// Multiplier of δ used for the floor of D_stop (spec: 5δ).
    pub dstop_delta_mult: f64,
    /// Quantile of normal-sample D used for D_stop (spec: 0.99).
    pub dstop_quantile: f64,
    /// Basis is considered recovered when D ≤ D_stop × this ratio ...
    pub basis_recover_ratio: f64,
    /// ... continuously for this long (ms).
    pub basis_recover_ms: u64,
    /// Log a drift warning when |β_5m − β_0| exceeds this (bps).
    pub drift_warn_bps: f64,
}

impl Default for Pricing {
    fn default() -> Self {
        Self {
            basis_window_secs: 3600,
            drift_window_secs: 300,
            basis_min_samples: 300,
            basis_sample_interval_ms: 200,
            basis_sync_tolerance_ms: 400,
            basis_max_ext_spread_bps: 15.0,
            dstop_delta_mult: 5.0,
            dstop_quantile: 0.99,
            basis_recover_ratio: 0.5,
            basis_recover_ms: 10_000,
            drift_warn_bps: 3.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Quoting {
    /// δ_config: minimum base half-spread (bps).
    pub min_half_spread_bps: f64,
    /// e: safety margin on top of net maker fee (bps).
    pub fee_buffer_bps: f64,
    /// Window for V90 (seconds). Spec: 5 minutes of 200 ms absolute moves.
    pub vol_window_secs: u64,
    /// Sampling step for V90 (ms). Spec: 200 ms.
    pub vol_step_ms: u64,
    /// Quantile for V90.
    pub vol_quantile: f64,
    /// Layers per side (spec: 3).
    pub layers: usize,
    /// Outer-layer offsets from the inner quote, in multiples of δ (spec: 0.5, 1.25).
    pub layer_offsets_delta: Vec<f64>,
    /// Per-layer base notional as fraction of H (spec: 0.05).
    pub layer_notional_ratio: f64,
    /// Inner-layer re-quote threshold (ticks). Spec: 2.
    pub inner_requote_ticks: i64,
    /// Outer-layer re-quote threshold (ticks).
    pub outer_requote_ticks: i64,
    /// Relative size deviation that triggers an amend (0.3 = 30%).
    pub size_requote_ratio: f64,
    /// |u| at or above which the reducing side may price inside the Gate spread
    /// without the "improve by one tick only" constraint (spec: 0.5).
    pub aggressive_reduce_u: f64,
}

impl Default for Quoting {
    fn default() -> Self {
        Self {
            min_half_spread_bps: 2.0,
            fee_buffer_bps: 0.5,
            vol_window_secs: 300,
            vol_step_ms: 200,
            vol_quantile: 0.90,
            layers: 3,
            layer_offsets_delta: vec![0.5, 1.25],
            layer_notional_ratio: 0.05,
            inner_requote_ticks: 2,
            outer_requote_ticks: 4,
            size_requote_ratio: 0.3,
            aggressive_reduce_u: 0.5,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Inventory {
    /// H: net inventory hard cap in USDT notional.
    pub max_net_usdt: f64,
    /// Gross (long + short) cap in USDT notional (dual mode).
    pub max_gross_usdt: f64,
    /// Target band half-width as fraction of H (spec: 0.1).
    pub band_ratio: f64,
    /// Time allowed outside the band before active reduction (ms). Spec: 3 s.
    pub band_timeout_ms: u64,
    /// |u| at or above which no new risk is added (aggressive reduce only).
    pub stop_add_u: f64,
}

impl Default for Inventory {
    fn default() -> Self {
        Self {
            max_net_usdt: 5_000.0,
            max_gross_usdt: 5_000.0,
            band_ratio: 0.1,
            band_timeout_ms: 3_000,
            stop_add_u: 0.5,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Protection {
    /// v100 velocity threshold for light protection, in multiples of δ (spec: 1).
    pub light_velocity_delta: f64,
    /// v100 velocity threshold for strong protection, in multiples of δ (spec: 2).
    pub strong_velocity_delta: f64,
    /// Velocity lookback (ms). Spec: 100 ms.
    pub velocity_window_ms: u64,
    /// Gate impact window (ms). Spec: 100 ms.
    pub impact_window_ms: u64,
    /// Aggressor share threshold (spec: 0.8).
    pub impact_share: f64,
    /// Window-notional quantile threshold (spec: 0.95).
    pub impact_quantile: f64,
    /// History used for the impact notional quantile (seconds).
    pub impact_history_secs: u64,
    /// Whether a Gate impact signal is treated as strong (true) or light (false).
    pub impact_is_strong: bool,
    /// Light protection: size multiplier and distance multiplier.
    pub light_size_mult: f64,
    pub light_distance_mult: f64,
    /// Under strong protection, opposite side size multiplier (spec: ≤ 0.5).
    pub strong_other_side_size_mult: f64,
    /// After the signal clears: wait this long before restoring half size (ms).
    pub recover_half_ms: u64,
    /// ... and this long before restoring full size (ms).
    pub recover_full_ms: u64,
}

impl Default for Protection {
    fn default() -> Self {
        Self {
            light_velocity_delta: 1.0,
            strong_velocity_delta: 2.0,
            velocity_window_ms: 100,
            impact_window_ms: 100,
            impact_share: 0.8,
            impact_quantile: 0.95,
            impact_history_secs: 1800,
            impact_is_strong: false,
            light_size_mult: 0.5,
            light_distance_mult: 1.5,
            strong_other_side_size_mult: 0.5,
            recover_half_ms: 500,
            recover_full_ms: 2_000,
        }
    }
}

/// Active (IOC) reduction and emergency exit.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Exit {
    /// Max slippage from F accepted by a protected IOC (bps).
    pub ioc_max_slippage_bps: f64,
    /// Minimum interval between successive IOC attempts (ms).
    pub ioc_retry_interval_ms: u64,
    /// Wall-clock budget for the whole active-reduce episode before escalation (ms).
    pub ioc_timeout_ms: u64,
    /// Time to wait for conflicting-order cancels before sending the first IOC (ms).
    pub cancel_wait_ms: u64,
    /// Emergency exit policy after the IOC budget is exhausted.
    pub emergency: Emergency,
    /// Max market-order attempts in the emergency phase.
    pub emergency_max_attempts: u32,
    /// Whether the strategy may resume normal quoting after an escalation.
    pub resume_after_escalation: bool,
    /// On SIGINT/SIGTERM: cancel orders and also flatten the position (true),
    /// or cancel orders only and leave the position (false).
    pub liquidate_on_shutdown: bool,
}

impl Default for Exit {
    fn default() -> Self {
        Self {
            ioc_max_slippage_bps: 15.0,
            ioc_retry_interval_ms: 200,
            ioc_timeout_ms: 5_000,
            cancel_wait_ms: 800,
            emergency: Emergency::MarketThenHalt,
            emergency_max_attempts: 3,
            resume_after_escalation: false,
            liquidate_on_shutdown: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Emergency {
    /// Send reduce-only market orders (exchange-side slippage cap), then halt.
    MarketThenHalt,
    /// Cancel everything and halt, leaving the position for manual handling.
    HaltOnly,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Risk {
    /// Session loss limit (USDT, positive number) on true equity change.
    pub loss_limit_usdt: f64,
    /// Binance feed staleness before it is considered broken (ms).
    pub binance_stale_ms: u64,
    /// Gate public feed staleness (ms).
    pub gate_stale_ms: u64,
    /// Gate private feed staleness (heartbeat) (ms).
    pub private_stale_ms: u64,
    /// Account snapshot poll interval (ms).
    pub account_poll_ms: u64,
    /// Position mismatch tolerance in contracts before distrust.
    pub position_mismatch_tolerance: i64,
    /// Position mismatch must persist this long before distrust (ms).
    pub position_mismatch_ms: u64,
    /// Leverage assumed when estimating order margin.
    pub margin_leverage: f64,
    /// Minimum free margin buffer kept in reserve (USDT).
    pub margin_buffer_usdt: f64,
    /// |F / Gate mark price − 1| above this (bps) is treated as an anomaly: no new risk.
    /// Mark/index prices are sanity checks only – never used as tradable prices.
    pub mark_deviation_bps: f64,
}

impl Default for Risk {
    fn default() -> Self {
        Self {
            loss_limit_usdt: 200.0,
            binance_stale_ms: 5_000,
            gate_stale_ms: 5_000,
            private_stale_ms: 30_000,
            account_poll_ms: 2_000,
            position_mismatch_tolerance: 0,
            position_mismatch_ms: 2_000,
            margin_leverage: 5.0,
            margin_buffer_usdt: 50.0,
            mark_deviation_bps: 50.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Orders {
    /// Client-order-id prefix (Gate requires `t-`).
    pub client_id_prefix: String,
    /// In-flight request timeout before an order status query is issued (ms).
    pub inflight_timeout_ms: u64,
    /// Use native amend (true) or cancel+re-place (false).
    pub use_amend: bool,
    /// Periodic risk timer (ms).
    pub timer_ms: u64,
}

impl Default for Orders {
    fn default() -> Self {
        Self {
            client_id_prefix: "t-mm".into(),
            inflight_timeout_ms: 3_000,
            use_amend: true,
            timer_ms: 50,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Session {
    /// IANA timezone of the underlying's primary market.
    pub timezone: String,
    /// Sessions in which new risk may be added.
    pub tradable: Vec<String>,
    /// Extra full-day blackout dates (YYYY-MM-DD, market timezone), e.g. holidays.
    pub holidays: Vec<String>,
    /// Event windows (`[start, end]` RFC3339) during which no new risk is added.
    pub event_windows: Vec<[String; 2]>,
    /// Force a single session kind regardless of the clock (e.g. "regular").
    /// Empty = use the calendar.
    pub force_session: String,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            timezone: "America/New_York".into(),
            tradable: vec!["regular".into(), "pre".into(), "post".into(), "overnight".into()],
            holidays: vec![],
            event_windows: vec![],
            force_session: String::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Metrics {
    /// Snapshot interval (ms).
    pub snapshot_ms: u64,
    /// JSONL output file (empty = disabled).
    pub file: String,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            snapshot_ms: 10_000,
            file: "metrics.jsonl".into(),
        }
    }
}

/// API credentials, read from the environment.
#[derive(Debug, Clone, Default)]
pub struct Credentials {
    pub gate_key: String,
    pub gate_secret: String,
}

impl Credentials {
    pub fn from_env() -> Self {
        Self {
            gate_key: std::env::var("GATE_API_KEY").unwrap_or_default(),
            gate_secret: std::env::var("GATE_API_SECRET").unwrap_or_default(),
        }
    }

    pub fn is_complete(&self) -> bool {
        !self.gate_key.is_empty() && !self.gate_secret.is_empty()
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read config {}", path.display()))?;
        let cfg: Config = toml::from_str(&raw).context("parse config")?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(self.inventory.max_net_usdt > 0.0, "inventory.max_net_usdt must be > 0");
        anyhow::ensure!(self.inventory.max_gross_usdt > 0.0, "inventory.max_gross_usdt must be > 0");
        anyhow::ensure!(
            self.inventory.band_ratio > 0.0 && self.inventory.band_ratio < 1.0,
            "inventory.band_ratio must be in (0,1)"
        );
        anyhow::ensure!(self.quoting.layers >= 1, "quoting.layers must be >= 1");
        anyhow::ensure!(
            self.quoting.layer_offsets_delta.len() + 1 >= self.quoting.layers,
            "quoting.layer_offsets_delta must have at least layers-1 entries"
        );
        anyhow::ensure!(self.quoting.min_half_spread_bps > 0.0, "quoting.min_half_spread_bps must be > 0");
        anyhow::ensure!(
            self.protection.strong_velocity_delta >= self.protection.light_velocity_delta,
            "protection.strong_velocity_delta must be >= light"
        );
        anyhow::ensure!(
            self.protection.recover_full_ms >= self.protection.recover_half_ms,
            "protection.recover_full_ms must be >= recover_half_ms"
        );
        anyhow::ensure!(self.orders.client_id_prefix.starts_with("t-"), "orders.client_id_prefix must start with 't-'");
        anyhow::ensure!(
            self.pricing.dstop_quantile > 0.0 && self.pricing.dstop_quantile <= 1.0,
            "pricing.dstop_quantile must be in (0,1]"
        );
        Ok(())
    }

    /// Default config serialised as TOML (used by `--print-default-config`).
    pub fn default_toml() -> String {
        toml::to_string_pretty(&Config::default()).expect("serialise default config")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_roundtrips() {
        let s = Config::default_toml();
        let cfg: Config = toml::from_str(&s).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.quoting.layers, 3);
        assert_eq!(cfg.inventory.band_ratio, 0.1);
    }

    #[test]
    fn partial_config_uses_defaults() {
        let cfg: Config = toml::from_str("mode = \"live\"\n[inventory]\nmax_net_usdt = 1000\n").unwrap();
        assert_eq!(cfg.mode, Mode::Live);
        assert_eq!(cfg.inventory.max_net_usdt, 1000.0);
        assert_eq!(cfg.quoting.min_half_spread_bps, 2.0);
    }
}
