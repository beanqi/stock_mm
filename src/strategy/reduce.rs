//! Active inventory reduction with price-protected reduce-only IOCs and a
//! predefined emergency escalation (spec §5.3–5.4).
//!
//! Episode life-cycle:
//! `Idle → Cancelling (conflicting orders) → Executing (IOCs) → Idle | Emergency → Halted`
//!
//! - inventory only decreases on *actual* fills (the engine feeds positions);
//! - each IOC is sized from the Gate depth within the slippage cap and priced at
//!   the worst level it needs to reach;
//! - when the budget expires with inventory left the episode escalates and the
//!   strategy does **not** silently return to normal quoting.

use std::time::{Duration, Instant};

use serde::Serialize;

use crate::config::{self, Emergency};
use crate::market::book::LocalBook;
use crate::strategy::inventory::Positions;
use crate::types::{ContractMeta, Side, TickGrid, Ticks};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ReduceReason {
    BandTimeout,
    StrongAgainstInventory,
    BasisAbnormal,
    HardCap,
    LossLimit,
    Untrusted,
    Shutdown,
}

impl ReduceReason {
    /// Reasons that stop the strategy after the position is flat.
    pub fn is_fatal(self) -> bool {
        matches!(self, ReduceReason::HardCap | ReduceReason::LossLimit | ReduceReason::Shutdown)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Phase {
    Idle,
    Cancelling,
    Executing,
    Emergency,
    Halted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReduceAction {
    Wait,
    /// Cancel every order that adds risk or could self-trade with the exit.
    CancelConflicts,
    SendIoc { side: Side, size: i64, price: Ticks },
    SendMarket { side: Side, size: i64 },
    /// Target reached; resume normal operation.
    Completed,
    /// Escalation exhausted or fatal reason: strategy must stop.
    Halt,
}

#[derive(Debug, Clone, Copy)]
pub struct ReduceInputs<'a> {
    pub now: Instant,
    pub pos: Positions,
    pub fair: f64,
    pub book: &'a LocalBook,
    pub own: &'a [(Side, Ticks, i64)],
    pub grid: TickGrid,
    pub meta: &'a ContractMeta,
    /// True while any non-exit order is still not terminal.
    pub conflicts_pending: bool,
    /// True while an exit order we sent is still not terminal.
    pub exit_inflight: bool,
}

#[derive(Debug, Clone)]
pub struct ActiveReduce {
    cfg: config::Exit,
    pub phase: Phase,
    pub reason: Option<ReduceReason>,
    /// Reduce until |net| ≤ this many contracts.
    pub target_abs: i64,
    started: Option<Instant>,
    phase_since: Option<Instant>,
    last_sent: Option<Instant>,
    pub attempts: u32,
    pub emergency_attempts: u32,
    /// Fair price at decision time (exit-cost metric).
    pub decision_fair: f64,
}

impl ActiveReduce {
    pub fn new(cfg: config::Exit) -> Self {
        Self {
            cfg,
            phase: Phase::Idle,
            reason: None,
            target_abs: 0,
            started: None,
            phase_since: None,
            last_sent: None,
            attempts: 0,
            emergency_attempts: 0,
            decision_fair: 0.0,
        }
    }

    pub fn is_active(&self) -> bool {
        !matches!(self.phase, Phase::Idle)
    }

    pub fn is_halted(&self) -> bool {
        self.phase == Phase::Halted
    }

    /// Start (or upgrade) an episode. A fatal reason overrides a non-fatal
    /// one and forces a full liquidation target.
    pub fn trigger(&mut self, reason: ReduceReason, target_abs: i64, fair: f64, now: Instant) -> bool {
        if self.phase == Phase::Halted {
            return false;
        }
        if self.is_active() {
            let cur_fatal = self.reason.map(|r| r.is_fatal()).unwrap_or(false);
            if reason.is_fatal() && !cur_fatal {
                self.reason = Some(reason);
                self.target_abs = 0;
            } else {
                self.target_abs = self.target_abs.min(target_abs);
            }
            return false;
        }
        self.phase = Phase::Cancelling;
        self.reason = Some(reason);
        self.target_abs = if reason.is_fatal() { 0 } else { target_abs.max(0) };
        self.started = Some(now);
        self.phase_since = Some(now);
        self.last_sent = None;
        self.attempts = 0;
        self.emergency_attempts = 0;
        self.decision_fair = fair;
        true
    }

    /// Reset after a completed episode (called by the engine once it has
    /// decided the strategy may resume).
    pub fn reset(&mut self) {
        if self.phase != Phase::Halted {
            *self = Self { cfg: self.cfg.clone(), ..Self::new(self.cfg.clone()) };
        }
    }

    pub fn elapsed(&self, now: Instant) -> Duration {
        self.started.map(|s| now.saturating_duration_since(s)).unwrap_or_default()
    }

    /// Remaining contracts to reduce on the dominant side.
    pub fn remaining(&self, pos: Positions) -> Option<(Side, i64)> {
        let net = pos.net();
        let excess = net.abs() - self.target_abs;
        if excess <= 0 {
            return None;
        }
        if net > 0 {
            Some((Side::Sell, excess.min(pos.long)))
        } else {
            Some((Side::Buy, excess.min(pos.short)))
        }
    }

    pub fn step(&mut self, inp: ReduceInputs<'_>) -> ReduceAction {
        let now = inp.now;
        match self.phase {
            Phase::Idle => ReduceAction::Wait,
            Phase::Halted => ReduceAction::Halt,
            Phase::Cancelling => {
                let since = self.phase_since.unwrap_or(now);
                let waited = now.saturating_duration_since(since);
                if !inp.conflicts_pending || waited >= Duration::from_millis(self.cfg.cancel_wait_ms) {
                    self.phase = Phase::Executing;
                    self.phase_since = Some(now);
                    return self.step_executing(inp);
                }
                ReduceAction::CancelConflicts
            }
            Phase::Executing => self.step_executing(inp),
            Phase::Emergency => self.step_emergency(inp),
        }
    }

    fn finish_or_halt(&mut self) -> ReduceAction {
        if self.reason.map(|r| r.is_fatal()).unwrap_or(false) {
            self.phase = Phase::Halted;
            ReduceAction::Halt
        } else {
            self.phase = Phase::Idle;
            ReduceAction::Completed
        }
    }

    fn step_executing(&mut self, inp: ReduceInputs<'_>) -> ReduceAction {
        let now = inp.now;
        let Some((side, qty)) = self.remaining(inp.pos) else {
            return self.finish_or_halt();
        };
        if inp.exit_inflight {
            return ReduceAction::Wait;
        }
        if self.elapsed(now) >= Duration::from_millis(self.cfg.ioc_timeout_ms) {
            self.phase = Phase::Emergency;
            self.phase_since = Some(now);
            return self.step_emergency(inp);
        }
        if let Some(t) = self.last_sent {
            if now.saturating_duration_since(t) < Duration::from_millis(self.cfg.ioc_retry_interval_ms) {
                return ReduceAction::Wait;
            }
        }
        // Price cap relative to F.
        let slip = self.cfg.ioc_max_slippage_bps / 10_000.0;
        let cap = match side {
            Side::Sell => inp.grid.floor(inp.fair * (1.0 - slip)),
            Side::Buy => inp.grid.ceil(inp.fair * (1.0 + slip)),
        };
        let qty = qty.min(inp.meta.order_size_max);
        let sweep = inp.book.sweep(side, qty, inp.own, Some(cap));
        if sweep.filled < inp.meta.order_size_min {
            // No acceptable depth right now: try again after the interval.
            self.last_sent = Some(now);
            self.attempts += 1;
            return ReduceAction::Wait;
        }
        let price = sweep.worst_price.unwrap_or(cap);
        self.last_sent = Some(now);
        self.attempts += 1;
        ReduceAction::SendIoc { side, size: sweep.filled, price }
    }

    fn step_emergency(&mut self, inp: ReduceInputs<'_>) -> ReduceAction {
        let now = inp.now;
        let Some((side, qty)) = self.remaining(inp.pos) else {
            return self.finish_after_escalation();
        };
        if inp.exit_inflight {
            return ReduceAction::Wait;
        }
        match self.cfg.emergency {
            Emergency::HaltOnly => {
                self.phase = Phase::Halted;
                ReduceAction::Halt
            }
            Emergency::MarketThenHalt => {
                if self.emergency_attempts >= self.cfg.emergency_max_attempts {
                    self.phase = Phase::Halted;
                    return ReduceAction::Halt;
                }
                if let Some(t) = self.last_sent {
                    if now.saturating_duration_since(t) < Duration::from_millis(self.cfg.ioc_retry_interval_ms) {
                        return ReduceAction::Wait;
                    }
                }
                self.emergency_attempts += 1;
                self.last_sent = Some(now);
                ReduceAction::SendMarket { side, size: qty.min(inp.meta.order_size_max) }
            }
        }
    }

    fn finish_after_escalation(&mut self) -> ReduceAction {
        if self.cfg.resume_after_escalation && !self.reason.map(|r| r.is_fatal()).unwrap_or(false) {
            self.phase = Phase::Idle;
            ReduceAction::Completed
        } else {
            self.phase = Phase::Halted;
            ReduceAction::Halt
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::book::{BookDelta, BookSnapshot, DepthLevel};
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

    fn book(now: Instant) -> LocalBook {
        let mut b = LocalBook::new();
        b.apply_snapshot(
            BookSnapshot {
                id: 1,
                bids: vec![DepthLevel { price: 99990, size: 30 }, DepthLevel { price: 99970, size: 40 }, DepthLevel { price: 99500, size: 500 }],
                asks: vec![DepthLevel { price: 100010, size: 30 }],
            },
            now,
        );
        b.apply_delta(BookDelta { first_id: 2, last_id: 2, bids: vec![], asks: vec![], full: false }, now);
        b
    }

    fn inputs<'a>(now: Instant, pos: Positions, book: &'a LocalBook, meta: &'a ContractMeta, conflicts: bool, inflight: bool) -> ReduceInputs<'a> {
        ReduceInputs {
            now,
            pos,
            fair: 1000.0,
            book,
            own: &[],
            grid: TickGrid::new(meta.tick),
            meta,
            conflicts_pending: conflicts,
            exit_inflight: inflight,
        }
    }

    #[test]
    fn episode_cancels_then_sends_protected_ioc_sized_by_depth() {
        let m = meta();
        let t0 = Instant::now();
        let b = book(t0);
        let mut ar = ActiveReduce::new(config::Exit::default());
        let mut pos = Positions::default();
        pos.apply_fill(100, 1000.0, false, &m); // long 100
        assert!(ar.trigger(ReduceReason::BandTimeout, 50, 1000.0, t0));
        // conflicts pending → keep cancelling
        assert_eq!(ar.step(inputs(t0, pos, &b, &m, true, false)), ReduceAction::CancelConflicts);
        // conflicts gone → IOC for 50, within 15 bps cap (≥ 998.50): 30@999.90 + 20@999.70 → worst 999.70
        let a = ar.step(inputs(t0 + Duration::from_millis(100), pos, &b, &m, false, false));
        assert_eq!(a, ReduceAction::SendIoc { side: Side::Sell, size: 50, price: 99970 });
        // while the exit is in flight, wait
        assert_eq!(ar.step(inputs(t0 + Duration::from_millis(150), pos, &b, &m, false, true)), ReduceAction::Wait);
        // partial fill of 30 → remaining 20; retry interval respected
        pos.apply_fill(-30, 999.9, true, &m);
        assert_eq!(ar.step(inputs(t0 + Duration::from_millis(200), pos, &b, &m, false, false)), ReduceAction::Wait);
        let a = ar.step(inputs(t0 + Duration::from_millis(400), pos, &b, &m, false, false));
        assert_eq!(a, ReduceAction::SendIoc { side: Side::Sell, size: 20, price: 99990 });
        // reaching the target completes the episode (non-fatal reason)
        pos.apply_fill(-20, 999.9, true, &m);
        assert_eq!(ar.step(inputs(t0 + Duration::from_millis(700), pos, &b, &m, false, false)), ReduceAction::Completed);
        assert!(!ar.is_active());
    }

    #[test]
    fn timeout_escalates_to_market_then_halts() {
        let m = meta();
        let t0 = Instant::now();
        let mut b = LocalBook::new();
        // no bids inside the cap → IOCs cannot be sized
        b.apply_snapshot(BookSnapshot { id: 1, bids: vec![DepthLevel { price: 90000, size: 1000 }], asks: vec![] }, t0);
        b.apply_delta(BookDelta { first_id: 2, last_id: 2, bids: vec![], asks: vec![], full: false }, t0);
        let cfg = config::Exit { ioc_timeout_ms: 1000, emergency_max_attempts: 2, ..Default::default() };
        let mut ar = ActiveReduce::new(cfg);
        let mut pos = Positions::default();
        pos.apply_fill(100, 1000.0, false, &m);
        ar.trigger(ReduceReason::BandTimeout, 0, 1000.0, t0);
        assert_eq!(ar.step(inputs(t0, pos, &b, &m, false, false)), ReduceAction::Wait);
        // after the budget: market orders
        let a = ar.step(inputs(t0 + Duration::from_millis(1100), pos, &b, &m, false, false));
        assert_eq!(a, ReduceAction::SendMarket { side: Side::Sell, size: 100 });
        assert_eq!(ar.phase, Phase::Emergency);
        let a = ar.step(inputs(t0 + Duration::from_millis(1400), pos, &b, &m, false, false));
        assert_eq!(a, ReduceAction::SendMarket { side: Side::Sell, size: 100 });
        let a = ar.step(inputs(t0 + Duration::from_millis(1700), pos, &b, &m, false, false));
        assert_eq!(a, ReduceAction::Halt);
        assert!(ar.is_halted());
        // and a halted machine refuses new triggers
        assert!(!ar.trigger(ReduceReason::BandTimeout, 0, 1000.0, t0));
    }

    #[test]
    fn fatal_reason_upgrades_episode_and_halts_when_flat() {
        let m = meta();
        let t0 = Instant::now();
        let b = book(t0);
        let mut ar = ActiveReduce::new(config::Exit::default());
        let mut pos = Positions::default();
        pos.apply_fill(10, 1000.0, false, &m);
        ar.trigger(ReduceReason::BandTimeout, 5, 1000.0, t0);
        ar.trigger(ReduceReason::LossLimit, 5, 1000.0, t0);
        assert_eq!(ar.target_abs, 0);
        pos.apply_fill(-10, 999.9, true, &m);
        assert_eq!(ar.step(inputs(t0 + Duration::from_secs(1), pos, &b, &m, false, false)), ReduceAction::Halt);
    }
}
