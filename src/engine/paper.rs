//! Paper-trading executor: simulates Gate order handling against the live Gate
//! market data so the whole engine (quoting, reconcile, reduce, metrics) runs
//! end-to-end without sending real orders.
//!
//! Fill model (deliberately simple and slightly optimistic):
//! - a resting order fills when a public trade prints at or through its price,
//!   or when the external best quote moves through it;
//! - IOC/market orders sweep the local book;
//! - post-only orders that would cross are finished with `finish_as = poc`;
//! - reduce-only orders are clamped to the available position.

use std::collections::HashMap;
use std::time::Instant;

use crate::engine::events::{AccountInfo, Event, PositionInfo};
use crate::market::book::LocalBook;
use crate::order::model::{ExchangeStatus, ExecCommand, ExecResponse, ExecResult, OrderInfo, Tif, UserTrade};
use crate::strategy::inventory::Positions;
use crate::types::{ContractMeta, Side, TickGrid, Ticks};
use crate::util::unix_ms;

#[derive(Debug, Clone)]
struct PaperOrder {
    id: String,
    client_id: String,
    side: Side,
    price: Option<Ticks>,
    size: i64,
    left: i64,
    tif: Tif,
    reduce_only: bool,
    fill_notional: f64,
    finished: bool,
    finish_as: String,
}

impl PaperOrder {
    fn info(&self, grid: &TickGrid) -> OrderInfo {
        let filled = self.size - self.left;
        OrderInfo {
            exchange_id: self.id.clone(),
            client_id: self.client_id.clone(),
            side: self.side,
            price: self.price.unwrap_or(0),
            size: self.size,
            left: self.left,
            fill_price: if filled > 0 { self.fill_notional / filled as f64 / grid.tick_size() * grid.tick_size() } else { 0.0 },
            status: if self.finished { ExchangeStatus::Finished } else { ExchangeStatus::Open },
            finish_as: self.finish_as.clone(),
            reduce_only: self.reduce_only,
            tif: Some(self.tif),
            update_ms: unix_ms(),
        }
    }
}

pub struct PaperExchange {
    grid: TickGrid,
    meta: ContractMeta,
    orders: HashMap<String, PaperOrder>,
    by_client: HashMap<String, String>,
    next_id: u64,
    next_trade: u64,
    pub pos: Positions,
    pub balance: f64,
    fee_maker: f64,
    fee_taker: f64,
    ext_bid: Option<Ticks>,
    ext_ask: Option<Ticks>,
    leverage: f64,
}

impl PaperExchange {
    pub fn new(grid: TickGrid, meta: ContractMeta, balance: f64, fee_maker: f64, fee_taker: f64, leverage: f64) -> Self {
        Self {
            grid,
            meta,
            orders: HashMap::new(),
            by_client: HashMap::new(),
            next_id: 1,
            next_trade: 1,
            pos: Positions::default(),
            balance,
            fee_maker,
            fee_taker,
            ext_bid: None,
            ext_ask: None,
            leverage,
        }
    }

    fn resolve(&self, id: &str) -> Option<String> {
        if self.orders.contains_key(id) {
            Some(id.to_string())
        } else {
            self.by_client.get(id).cloned()
        }
    }

    fn fill(&mut self, id: &str, qty: i64, price: Ticks, maker: bool, now: Instant, out: &mut Vec<Event>) {
        let Some(o) = self.orders.get_mut(id) else { return };
        let mut qty = qty.min(o.left);
        if qty <= 0 {
            return;
        }
        // Reduce-only cannot exceed the position it closes.
        if o.reduce_only {
            let avail = match o.side {
                Side::Sell => self.pos.long,
                Side::Buy => self.pos.short,
            };
            qty = qty.min(avail);
            if qty <= 0 {
                o.left = 0;
                o.finished = true;
                o.finish_as = "reduce_only".into();
                let info = o.info(&self.grid);
                out.push(Event::GateOrder(info));
                return;
            }
        }
        let px = self.grid.to_f64(price);
        let notional = self.meta.notional(qty, px);
        let fee = notional * if maker { self.fee_maker } else { self.fee_taker };
        o.left -= qty;
        o.fill_notional += qty as f64 * px;
        let signed = qty * o.side.sign();
        let reduce = o.reduce_only;
        let client_id = o.client_id.clone();
        if o.left == 0 {
            o.finished = true;
            o.finish_as = "filled".into();
        }
        let info = o.info(&self.grid);
        let realised = self.pos.apply_fill(signed, px, reduce, &self.meta);
        self.balance += realised - fee;
        let tid = self.next_trade;
        self.next_trade += 1;
        out.push(Event::GateUserTrade(UserTrade {
            trade_id: format!("pt{tid}"),
            exchange_order_id: id.to_string(),
            client_id,
            signed_size: signed,
            price,
            price_f64: px,
            fee,
            is_maker: maker,
            exch_ms: unix_ms(),
            at: now,
        }));
        out.push(Event::GateOrder(info));
        out.push(Event::GateBalanceChange { change: -fee, kind: "fee".into(), at: now });
    }

    fn sweep_fill(&mut self, id: &str, book: &LocalBook, limit: Option<Ticks>, now: Instant, out: &mut Vec<Event>) {
        let Some(o) = self.orders.get(id).cloned() else { return };
        let sw = book.sweep(o.side, o.left, &[], limit);
        if sw.filled > 0 {
            // Fill at the VWAP rounded to tick (single print for simplicity).
            let px = self.grid.round(sw.vwap_ticks * self.grid.tick_size());
            self.fill(id, sw.filled, px, false, now, out);
        }
        if let Some(o) = self.orders.get_mut(id) {
            if !o.finished {
                o.finished = true;
                o.finish_as = "ioc".into();
                out.push(Event::GateOrder(o.info(&self.grid)));
            }
        }
    }

    pub fn execute(&mut self, cmd: &ExecCommand, book: &LocalBook, now: Instant) -> Vec<Event> {
        let mut out = Vec::new();
        let (req_id, result) = match cmd {
            ExecCommand::Place { req_id, client_id, side, size, price, tif, reduce_only } => {
                let id = format!("P{}", self.next_id);
                self.next_id += 1;
                let mut o = PaperOrder {
                    id: id.clone(),
                    client_id: client_id.clone(),
                    side: *side,
                    price: *price,
                    size: *size,
                    left: *size,
                    tif: *tif,
                    reduce_only: *reduce_only,
                    fill_notional: 0.0,
                    finished: false,
                    finish_as: String::new(),
                };
                if *reduce_only {
                    let avail = match side {
                        Side::Sell => self.pos.long,
                        Side::Buy => self.pos.short,
                    };
                    if avail <= 0 {
                        (req_id.clone(), ExecResult::Error { label: "REDUCE_ONLY_NO_POSITION".into(), message: "no position to reduce".into() })
                    } else {
                        self.orders.insert(id.clone(), o.clone());
                        self.by_client.insert(client_id.clone(), id.clone());
                        self.after_place(&id, book, now, &mut out);
                        (req_id.clone(), ExecResult::Placed(self.orders[&id].info(&self.grid)))
                    }
                } else {
                    // Post-only crossing check against the external quote.
                    let crosses = match (tif, side, price) {
                        (Tif::Poc, Side::Buy, Some(p)) => self.ext_ask.map(|a| *p >= a).unwrap_or(false),
                        (Tif::Poc, Side::Sell, Some(p)) => self.ext_bid.map(|b| *p <= b).unwrap_or(false),
                        _ => false,
                    };
                    if crosses {
                        o.finished = true;
                        o.left = o.size;
                        o.finish_as = "poc".into();
                        (req_id.clone(), ExecResult::Placed(o.info(&self.grid)))
                    } else {
                        self.orders.insert(id.clone(), o);
                        self.by_client.insert(client_id.clone(), id.clone());
                        self.after_place(&id, book, now, &mut out);
                        (req_id.clone(), ExecResult::Placed(self.orders[&id].info(&self.grid)))
                    }
                }
            }
            ExecCommand::Amend { req_id, exchange_id, price, size, .. } => match self.resolve(exchange_id) {
                Some(id) if !self.orders[&id].finished => {
                    let o = self.orders.get_mut(&id).unwrap();
                    if let Some(p) = price {
                        o.price = Some(*p);
                    }
                    if let Some(s) = size {
                        let filled = o.size - o.left;
                        if *s <= filled {
                            o.left = 0;
                            o.finished = true;
                            o.finish_as = "cancelled".into();
                        } else {
                            o.size = *s;
                            o.left = *s - filled;
                        }
                    }
                    let info = o.info(&self.grid);
                    out.push(Event::GateOrder(info.clone()));
                    // A repriced order may now be marketable.
                    self.after_place(&id, book, now, &mut out);
                    (req_id.clone(), ExecResult::Amended(self.orders[&id].info(&self.grid)))
                }
                _ => (req_id.clone(), ExecResult::Error { label: "ORDER_NOT_FOUND".into(), message: "paper: not found or finished".into() }),
            },
            ExecCommand::Cancel { req_id, exchange_id, client_id } => {
                let key = exchange_id.clone().unwrap_or_else(|| client_id.clone());
                match self.resolve(&key) {
                    Some(id) if !self.orders[&id].finished => {
                        let o = self.orders.get_mut(&id).unwrap();
                        o.finished = true;
                        o.finish_as = "cancelled".into();
                        let info = o.info(&self.grid);
                        out.push(Event::GateOrder(info.clone()));
                        (req_id.clone(), ExecResult::Cancelled(info))
                    }
                    _ => (req_id.clone(), ExecResult::Error { label: "ORDER_NOT_FOUND".into(), message: "paper: not found or finished".into() }),
                }
            }
            ExecCommand::Query { req_id, exchange_id, client_id } => {
                let key = exchange_id.clone().unwrap_or_else(|| client_id.clone());
                match self.resolve(&key) {
                    Some(id) => (req_id.clone(), ExecResult::Queried(self.orders[&id].info(&self.grid))),
                    None => (req_id.clone(), ExecResult::Error { label: "ORDER_NOT_FOUND".into(), message: "paper: not found".into() }),
                }
            }
            ExecCommand::CancelAll { req_id } => {
                let ids: Vec<String> = self.orders.values().filter(|o| !o.finished).map(|o| o.id.clone()).collect();
                for id in &ids {
                    let o = self.orders.get_mut(id).unwrap();
                    o.finished = true;
                    o.finish_as = "cancelled".into();
                    out.push(Event::GateOrder(o.info(&self.grid)));
                }
                (req_id.clone(), ExecResult::CancelledAll(ids.len()))
            }
        };
        out.insert(0, Event::Exec(ExecResponse { req_id, result, at: now }));
        self.gc();
        out
    }

    fn after_place(&mut self, id: &str, book: &LocalBook, now: Instant, out: &mut Vec<Event>) {
        let Some(o) = self.orders.get(id).cloned() else { return };
        if o.finished {
            return;
        }
        match (o.tif, o.price) {
            (Tif::Ioc, limit) => self.sweep_fill(id, book, limit, now, out),
            (_, Some(p)) => {
                // Immediately marketable non-post-only order: fill against the book.
                let marketable = match o.side {
                    Side::Buy => self.ext_ask.map(|a| p >= a).unwrap_or(false),
                    Side::Sell => self.ext_bid.map(|b| p <= b).unwrap_or(false),
                };
                if marketable && o.tif != Tif::Poc {
                    self.sweep_fill(id, book, Some(p), now, out);
                }
            }
            _ => {}
        }
    }

    /// External quote moved: orders the market traded through are filled.
    pub fn on_bbo(&mut self, bid: Ticks, ask: Ticks, now: Instant) -> Vec<Event> {
        self.ext_bid = Some(bid);
        self.ext_ask = Some(ask);
        let mut out = Vec::new();
        let ids: Vec<(String, i64, Ticks)> = self
            .orders
            .values()
            .filter(|o| !o.finished)
            .filter_map(|o| {
                let p = o.price?;
                let through = match o.side {
                    Side::Buy => ask <= p,
                    Side::Sell => bid >= p,
                };
                through.then(|| (o.id.clone(), o.left, p))
            })
            .collect();
        for (id, left, p) in ids {
            self.fill(&id, left, p, true, now, &mut out);
        }
        self.gc();
        out
    }

    /// Public trade printed at `price`: resting orders at or beyond it fill.
    pub fn on_trade(&mut self, price: Ticks, signed_size: i64, now: Instant) -> Vec<Event> {
        let mut out = Vec::new();
        let mut budget = signed_size.abs();
        // A taker sell (negative) hits bids; a taker buy hits asks.
        let hit_side = if signed_size < 0 { Side::Buy } else { Side::Sell };
        let mut ids: Vec<(String, Ticks, i64)> = self
            .orders
            .values()
            .filter(|o| !o.finished && o.side == hit_side)
            .filter_map(|o| {
                let p = o.price?;
                let hit = match hit_side {
                    Side::Buy => price <= p,
                    Side::Sell => price >= p,
                };
                hit.then(|| (o.id.clone(), p, o.left))
            })
            .collect();
        // Best-priced orders first.
        ids.sort_by_key(|(_, p, _)| if hit_side == Side::Buy { -*p } else { *p });
        for (id, p, left) in ids {
            if budget <= 0 {
                break;
            }
            let q = left.min(budget);
            self.fill(&id, q, p, true, now, &mut out);
            budget -= q;
        }
        self.gc();
        out
    }

    fn gc(&mut self) {
        if self.orders.len() > 2_000 {
            let dead: Vec<String> = self.orders.values().filter(|o| o.finished).map(|o| o.id.clone()).collect();
            for id in dead {
                if let Some(o) = self.orders.remove(&id) {
                    self.by_client.remove(&o.client_id);
                }
            }
        }
    }

    pub fn positions_event(&self) -> Event {
        Event::GatePositions(vec![
            PositionInfo { mode: "dual_long".into(), size: self.pos.long, entry_price: self.pos.long_entry, update_ms: unix_ms() },
            PositionInfo { mode: "dual_short".into(), size: -self.pos.short, entry_price: self.pos.short_entry, update_ms: unix_ms() },
        ])
    }

    pub fn account_event(&self, mark: f64, now: Instant) -> Event {
        let pos_notional = self.meta.notional(self.pos.gross(), mark);
        let order_notional: f64 = self.orders.values().filter(|o| !o.finished).map(|o| self.meta.notional(o.left, o.price.map(|p| self.grid.to_f64(p)).unwrap_or(mark))).sum();
        let position_margin = pos_notional / self.leverage;
        let order_margin = order_notional / self.leverage;
        let unrealised = self.pos.unrealised(mark, &self.meta);
        Event::GateAccount(AccountInfo {
            user_id: 0,
            total: self.balance + unrealised,
            available: (self.balance + unrealised - position_margin - order_margin).max(0.0),
            unrealised_pnl: unrealised,
            order_margin,
            position_margin,
            in_dual_mode: true,
            at: now,
        })
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
            BookSnapshot { id: 1, bids: vec![DepthLevel { price: 99990, size: 30 }], asks: vec![DepthLevel { price: 100010, size: 30 }] },
            now,
        );
        b.apply_delta(BookDelta { first_id: 2, last_id: 2, bids: vec![], asks: vec![], full: false }, now);
        b
    }

    #[test]
    fn resting_order_fills_on_trade_and_updates_position() {
        let m = meta();
        let g = TickGrid::new(m.tick);
        let now = Instant::now();
        let b = book(now);
        let mut px = PaperExchange::new(g, m.clone(), 10_000.0, -0.0001, 0.00075, 5.0);
        px.on_bbo(99990, 100010, now);
        let ev = px.execute(&ExecCommand::Place { req_id: "p-1".into(), client_id: "t-mm1".into(), side: Side::Buy, size: 10, price: Some(99995), tif: Tif::Poc, reduce_only: false }, &b, now);
        assert!(matches!(ev[0], Event::Exec(ExecResponse { result: ExecResult::Placed(_), .. })));
        // a taker sell prints at 999.90 → our bid at 999.95 is hit
        let ev = px.on_trade(99990, -4, now);
        assert!(ev.iter().any(|e| matches!(e, Event::GateUserTrade(t) if t.signed_size == 4)));
        assert_eq!(px.pos.long, 4);
        // post-only crossing is finished as poc
        let ev = px.execute(&ExecCommand::Place { req_id: "p-2".into(), client_id: "t-mm2".into(), side: Side::Buy, size: 1, price: Some(100010), tif: Tif::Poc, reduce_only: false }, &b, now);
        match &ev[0] {
            Event::Exec(ExecResponse { result: ExecResult::Placed(i), .. }) => {
                assert_eq!(i.status, ExchangeStatus::Finished);
                assert_eq!(i.finish_as, "poc");
            }
            other => panic!("{other:?}"),
        }
        // reduce-only IOC sell sweeps the bid and closes the long
        let ev = px.execute(&ExecCommand::Place { req_id: "p-3".into(), client_id: "t-mm3".into(), side: Side::Sell, size: 4, price: Some(99990), tif: Tif::Ioc, reduce_only: true }, &b, now);
        assert!(ev.iter().any(|e| matches!(e, Event::GateUserTrade(t) if t.signed_size == -4 && !t.is_maker)));
        assert_eq!(px.pos.long, 0);
    }
}
