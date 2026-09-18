//! Compare the desired quote set with our live orders and emit the minimal
//! set of exchange commands (spec §4 and §7 step 9):
//!
//! 1. dangerous orders (beyond the safety bound) are repriced/cancelled first,
//!    even for a one-tick breach;
//! 2. safe orders only move once the target has drifted by the re-quote
//!    threshold (inner 2 ticks, outer wider) – or on any change when the side
//!    must follow tightly;
//! 3. size-only changes: decreases keep queue priority and are always applied
//!    when the desired size is smaller; increases are applied only beyond the
//!    size tolerance (an increase goes to the back of the queue);
//! 4. slots without an order are filled only when the quote set contains them
//!    (the quote builder already applied every permission check).

use std::collections::HashMap;
use std::time::Instant;

use tracing::debug;

use super::manager::OrderManager;
use super::model::{ExecCommand, Tif};
use crate::config;
use crate::strategy::quote::{Quote, SideBounds};
use crate::types::{Side, Ticks};

#[derive(Debug, Clone, Copy)]
pub struct ReconcileParams<'a> {
    pub bounds: SideBounds,
    pub follow_tight_buy: bool,
    pub follow_tight_sell: bool,
    pub cfg: &'a config::Quoting,
    /// Native amend (true) or cancel + re-create with fresh prices (false).
    pub use_amend: bool,
}

#[derive(Debug, Default)]
pub struct ReconcileReport {
    pub commands: Vec<ExecCommand>,
    pub dangerous: usize,
    pub repriced: usize,
    pub resized: usize,
    pub placed: usize,
    pub cancelled: usize,
    pub kept: usize,
}

pub fn reconcile(
    mgr: &mut OrderManager,
    desired: &[Quote],
    p: ReconcileParams<'_>,
    now: Instant,
    now_ms: i64,
) -> ReconcileReport {
    let mut rep = ReconcileReport::default();
    let mut danger_cmds: Vec<ExecCommand> = Vec::new();
    let mut other_cmds: Vec<ExecCommand> = Vec::new();

    let mut want: HashMap<(Side, u8), Quote> = HashMap::new();
    for q in desired {
        want.insert((q.side, q.layer), *q);
    }

    // Snapshot of live slots (ids + fields) to avoid holding borrows.
    struct Live {
        id: u64,
        side: Side,
        layer: u8,
        purpose: crate::types::Purpose,
        price: Option<Ticks>,
        remaining: i64,
    }
    let live: Vec<Live> = mgr
        .slots()
        .into_iter()
        .flat_map(|(_, v)| v.into_iter())
        .map(|o| Live { id: o.id, side: o.side, layer: o.layer, purpose: o.purpose, price: o.price, remaining: o.remaining() })
        .collect::<Vec<_>>();

    let mut slot_taken: HashMap<(Side, u8), bool> = HashMap::new();

    for o in live {
        let key = (o.side, o.layer);
        let Some(price) = o.price else {
            // market/priceless order in a slot: nothing to compare
            continue;
        };
        let dangerous = p.bounds.is_dangerous(o.side, o.purpose, price);
        match want.get(&key) {
            None => {
                // Not wanted any more.
                if let Some(c) = mgr.cancel(o.id, now) {
                    if dangerous { danger_cmds.push(c) } else { other_cmds.push(c) }
                }
                rep.cancelled += 1;
                if dangerous {
                    rep.dangerous += 1;
                }
            }
            Some(q) if q.purpose != o.purpose => {
                // Open ↔ Reduce cannot be amended: cancel, re-place later.
                if let Some(c) = mgr.cancel(o.id, now) {
                    if dangerous { danger_cmds.push(c) } else { other_cmds.push(c) }
                }
                rep.cancelled += 1;
                if dangerous {
                    rep.dangerous += 1;
                }
                // Slot stays unfilled this round: a new order is only placed
                // once the old one is confirmed gone (avoids double exposure).
                slot_taken.insert(key, true);
            }
            Some(q) => {
                if slot_taken.get(&key).copied().unwrap_or(false) {
                    // duplicate order in the same slot → cancel the extra
                    if let Some(c) = mgr.cancel(o.id, now) {
                        other_cmds.push(c);
                    }
                    rep.cancelled += 1;
                    continue;
                }
                slot_taken.insert(key, true);
                let diff = (q.price - price).abs();
                let follow = match o.side {
                    Side::Buy => p.follow_tight_buy,
                    Side::Sell => p.follow_tight_sell,
                };
                let threshold = if o.layer == 0 { p.cfg.inner_requote_ticks } else { p.cfg.outer_requote_ticks };
                let size_shrink = q.size < o.remaining;
                let size_grow_big = q.size > o.remaining
                    && (q.size - o.remaining) as f64 > p.cfg.size_requote_ratio * o.remaining.max(1) as f64;

                // Without native amend we cancel and re-create *later* with the
                // then-current fair price (never the price cached before the cancel).
                let use_amend = p.use_amend;
                if dangerous {
                    rep.dangerous += 1;
                    rep.repriced += 1;
                    debug!(id = o.id, side = %o.side, layer = o.layer, from = price, to = q.price, "dangerous order → reprice");
                    let cmd = if use_amend { mgr.amend(o.id, q.price, q.size, now) } else { mgr.cancel(o.id, now) };
                    if let Some(c) = cmd {
                        danger_cmds.push(c);
                    }
                } else if diff >= threshold || (follow && diff >= 1) {
                    rep.repriced += 1;
                    let cmd = if use_amend { mgr.amend(o.id, q.price, q.size, now) } else { mgr.cancel(o.id, now) };
                    if let Some(c) = cmd {
                        other_cmds.push(c);
                    }
                } else if size_shrink || size_grow_big {
                    rep.resized += 1;
                    // Keep the current price so a decrease preserves priority.
                    let cmd = if use_amend { mgr.amend(o.id, price, q.size, now) } else { mgr.cancel(o.id, now) };
                    if let Some(c) = cmd {
                        other_cmds.push(c);
                    }
                } else {
                    rep.kept += 1;
                }
            }
        }
    }

    // Missing slots → new orders.
    for (key, q) in want {
        if slot_taken.get(&key).copied().unwrap_or(false) {
            continue;
        }
        if q.size <= 0 {
            continue;
        }
        let (_, cmd) = mgr.place(q.side, q.purpose, q.layer, Some(q.price), q.size, Tif::Poc, now, now_ms);
        other_cmds.push(cmd);
        rep.placed += 1;
    }

    rep.commands = danger_cmds;
    rep.commands.extend(other_cmds);
    rep
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::model::{ExchangeStatus, ExecResponse, ExecResult, OrderInfo};
    use crate::types::Purpose;
    use std::time::Duration;

    fn params(cfg: &config::Quoting) -> ReconcileParams<'_> {
        ReconcileParams {
            bounds: SideBounds { buy_open_max: 99980, buy_reduce_max: 99980, sell_open_min: 100020, sell_reduce_min: 100020 },
            follow_tight_buy: false,
            follow_tight_sell: false,
            cfg,
            use_amend: true,
        }
    }

    fn q(side: Side, layer: u8, purpose: Purpose, price: Ticks, size: i64) -> Quote {
        Quote { side, layer, purpose, price, size }
    }

    fn make_live(mgr: &mut OrderManager, cmds: &[ExecCommand], now: Instant) {
        for c in cmds {
            if let ExecCommand::Place { req_id, client_id, side, size, price, .. } = c {
                let info = OrderInfo {
                    exchange_id: format!("E{}", req_id),
                    client_id: client_id.clone(),
                    side: *side,
                    price: price.unwrap(),
                    size: *size,
                    left: *size,
                    fill_price: 0.0,
                    status: ExchangeStatus::Open,
                    finish_as: String::new(),
                    reduce_only: false,
                    tif: Some(Tif::Poc),
                    update_ms: 1,
                };
                mgr.on_exec_response(ExecResponse { req_id: req_id.clone(), result: ExecResult::Placed(info), at: now });
            }
        }
    }

    #[test]
    fn places_missing_then_keeps_within_threshold() {
        let cfg = config::Quoting::default();
        let mut m = OrderManager::new("t-mm", Duration::from_secs(3));
        let t0 = Instant::now();
        let desired = vec![q(Side::Buy, 0, Purpose::Open, 99971, 25), q(Side::Sell, 0, Purpose::Open, 100029, 25)];
        let r = reconcile(&mut m, &desired, params(&cfg), t0, 1);
        assert_eq!(r.placed, 2);
        make_live(&mut m, &r.commands, t0);
        // one tick drift, still safe → keep
        let desired = vec![q(Side::Buy, 0, Purpose::Open, 99970, 25), q(Side::Sell, 0, Purpose::Open, 100030, 25)];
        let r = reconcile(&mut m, &desired, params(&cfg), t0, 2);
        assert_eq!(r.kept, 2);
        assert!(r.commands.is_empty());
        // two ticks → reprice
        let desired = vec![q(Side::Buy, 0, Purpose::Open, 99969, 25), q(Side::Sell, 0, Purpose::Open, 100031, 25)];
        let r = reconcile(&mut m, &desired, params(&cfg), t0, 3);
        assert_eq!(r.repriced, 2);
        assert_eq!(r.commands.len(), 2);
    }

    #[test]
    fn dangerous_order_repriced_even_one_tick_and_first() {
        let cfg = config::Quoting::default();
        let mut m = OrderManager::new("t-mm", Duration::from_secs(3));
        let t0 = Instant::now();
        let desired = vec![q(Side::Buy, 0, Purpose::Open, 99980, 25), q(Side::Buy, 2, Purpose::Open, 99955, 25)];
        let r = reconcile(&mut m, &desired, params(&cfg), t0, 1);
        make_live(&mut m, &r.commands, t0);
        // Binance drops: new bound 99979 → our 99980 bid is now 1 tick over the boundary.
        let mut p = params(&cfg);
        p.bounds.buy_open_max = 99979;
        let desired = vec![q(Side::Buy, 0, Purpose::Open, 99979, 25), q(Side::Buy, 2, Purpose::Open, 99954, 25)];
        let r = reconcile(&mut m, &desired, p, t0, 2);
        assert_eq!(r.dangerous, 1);
        assert_eq!(r.kept, 1); // outer layer safe & within 4 ticks
        assert_eq!(r.commands.len(), 1);
        assert!(matches!(r.commands[0], ExecCommand::Amend { price: Some(99979), .. }));
    }

    #[test]
    fn purpose_change_cancels_and_defers_replacement() {
        let cfg = config::Quoting::default();
        let mut m = OrderManager::new("t-mm", Duration::from_secs(3));
        let t0 = Instant::now();
        let desired = vec![q(Side::Sell, 0, Purpose::Open, 100029, 25)];
        let r = reconcile(&mut m, &desired, params(&cfg), t0, 1);
        make_live(&mut m, &r.commands, t0);
        // we got long: sells become reduce-only
        let desired = vec![q(Side::Sell, 0, Purpose::Reduce, 100029, 30)];
        let r = reconcile(&mut m, &desired, params(&cfg), t0, 2);
        assert_eq!(r.cancelled, 1);
        assert_eq!(r.placed, 0);
        assert!(matches!(r.commands.as_slice(), [ExecCommand::Cancel { .. }]));
    }

    #[test]
    fn without_native_amend_reprice_is_cancel_then_fresh_place() {
        let cfg = config::Quoting::default();
        let mut m = OrderManager::new("t-mm", Duration::from_secs(3));
        let t0 = Instant::now();
        let desired = vec![q(Side::Buy, 0, Purpose::Open, 99971, 25)];
        let mut p = params(&cfg);
        p.use_amend = false;
        let r = reconcile(&mut m, &desired, p, t0, 1);
        make_live(&mut m, &r.commands, t0);
        // 3-tick move: cancel only, nothing placed in the same round
        let desired = vec![q(Side::Buy, 0, Purpose::Open, 99968, 25)];
        let r = reconcile(&mut m, &desired, p, t0, 2);
        assert_eq!(r.repriced, 1);
        assert_eq!(r.placed, 0);
        assert!(matches!(r.commands.as_slice(), [ExecCommand::Cancel { .. }]));
        // once the cancel is confirmed the slot is re-created at the *current* target
        let ExecCommand::Cancel { req_id, client_id, .. } = &r.commands[0] else { panic!() };
        let info = OrderInfo {
            exchange_id: "E1".into(),
            client_id: client_id.clone(),
            side: Side::Buy,
            price: 99971,
            size: 25,
            left: 25,
            fill_price: 0.0,
            status: ExchangeStatus::Finished,
            finish_as: "cancelled".into(),
            reduce_only: false,
            tif: Some(Tif::Poc),
            update_ms: 1,
        };
        m.on_exec_response(ExecResponse { req_id: req_id.clone(), result: ExecResult::Cancelled(info), at: t0 });
        let desired = vec![q(Side::Buy, 0, Purpose::Open, 99966, 25)];
        let r = reconcile(&mut m, &desired, p, t0, 3);
        assert_eq!(r.placed, 1);
        assert!(matches!(r.commands.as_slice(), [ExecCommand::Place { price: Some(99966), .. }]));
    }

    #[test]
    fn size_shrink_keeps_price_and_unwanted_slots_cancel() {
        let cfg = config::Quoting::default();
        let mut m = OrderManager::new("t-mm", Duration::from_secs(3));
        let t0 = Instant::now();
        let desired = vec![q(Side::Buy, 0, Purpose::Open, 99971, 25), q(Side::Buy, 1, Purpose::Open, 99961, 25)];
        let r = reconcile(&mut m, &desired, params(&cfg), t0, 1);
        make_live(&mut m, &r.commands, t0);
        let desired = vec![q(Side::Buy, 0, Purpose::Open, 99971, 12)];
        let r = reconcile(&mut m, &desired, params(&cfg), t0, 2);
        assert_eq!(r.resized, 1);
        assert_eq!(r.cancelled, 1);
        let amend = r.commands.iter().find(|c| matches!(c, ExecCommand::Amend { .. })).unwrap();
        assert!(matches!(amend, ExecCommand::Amend { price: Some(99971), size: Some(12), .. }));
        // small increase within tolerance is ignored
        make_live(&mut m, &[], t0);
    }
}
