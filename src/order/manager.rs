//! Local order book of *our* orders with a strict state machine.
//!
//! Rules implemented here (spec §6):
//! - one in-flight request per order; newer intents overwrite older ones and
//!   are sent only after the in-flight request resolves ("latest intent wins");
//! - a failed/timed-out amend or cancel never assumes the order is gone – a
//!   status query is issued instead;
//! - a timed-out new order is never blindly re-sent;
//! - `ack` is not a final result: state only advances on the final response
//!   or on the private order stream;
//! - amend sizes are **total** sizes (filled + desired remaining);
//! - fills are applied idempotently by trade id; cancel confirmations never
//!   erase fills that arrive later.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use tracing::{debug, warn};

use super::model::*;
use crate::strategy::inventory::PendingExposure;
use crate::types::{Purpose, Side, Ticks};

#[derive(Debug)]
pub struct OrderManager {
    next_id: u64,
    next_req: u64,
    prefix: String,
    inflight_timeout: Duration,
    orders: HashMap<u64, LocalOrder>,
    by_client: HashMap<String, u64>,
    by_exchange: HashMap<String, u64>,
    by_req: HashMap<String, u64>,
    seen_trades: HashSet<String>,
    seen_order: VecDeque<String>,
    /// Client ids we saw on the private stream but never created (leaked orders).
    pub foreign: HashMap<String, OrderInfo>,
    /// Orders finished more than this long ago are garbage-collected.
    retention: Duration,
}

/// Commands the manager wants sent after processing an event.
#[derive(Debug, Default)]
pub struct Followups {
    pub commands: Vec<ExecCommand>,
}

impl OrderManager {
    pub fn new(prefix: &str, inflight_timeout: Duration) -> Self {
        Self {
            next_id: 1,
            next_req: 1,
            prefix: prefix.to_string(),
            inflight_timeout,
            orders: HashMap::new(),
            by_client: HashMap::new(),
            by_exchange: HashMap::new(),
            by_req: HashMap::new(),
            seen_trades: HashSet::new(),
            seen_order: VecDeque::new(),
            foreign: HashMap::new(),
            retention: Duration::from_secs(120),
        }
    }

    pub fn is_ours(&self, client_id: &str) -> bool {
        client_id.starts_with(&self.prefix)
    }

    fn new_client_id(&mut self, now_ms: i64) -> String {
        let id = self.next_id;
        // Gate: `text` must start with "t-" and be ≤ 28 chars after the prefix.
        format!("{}{}{}", self.prefix, now_ms % 1_000_000_000, id)
    }

    fn new_req_id(&mut self, kind: &str) -> String {
        let r = self.next_req;
        self.next_req += 1;
        format!("{kind}-{r}")
    }

    pub fn get(&self, id: u64) -> Option<&LocalOrder> {
        self.orders.get(&id)
    }

    pub fn active(&self) -> impl Iterator<Item = &LocalOrder> {
        self.orders.values().filter(|o| !o.state.is_terminal())
    }

    pub fn active_count(&self) -> usize {
        self.active().count()
    }

    /// Non-terminal orders keyed by quote slot (side, layer), excluding exits.
    pub fn slots(&self) -> HashMap<(Side, u8), Vec<&LocalOrder>> {
        let mut m: HashMap<(Side, u8), Vec<&LocalOrder>> = HashMap::new();
        for o in self.active().filter(|o| o.layer != u8::MAX) {
            m.entry((o.side, o.layer)).or_default().push(o);
        }
        m
    }

    /// Quantities that might still trade and are (side, price, remaining) — used to
    /// strip our own orders from the Gate book.
    pub fn own_resting(&self) -> Vec<(Side, Ticks, i64)> {
        self.active()
            .filter_map(|o| o.price.map(|p| (o.side, p, o.remaining())))
            .filter(|(_, _, q)| *q > 0)
            .collect()
    }

    /// Exposure of orders whose quantity we cannot currently control
    /// (in flight). Live orders are excluded since the reconcile step will
    /// re-specify them.
    pub fn uncontrolled_exposure(&self) -> PendingExposure {
        self.uncontrolled_exposure_where(|_| true)
    }

    /// In-flight exposure restricted to orders matching `pred`. The quote
    /// builder uses this to exclude in-flight orders that sit in a quote slot
    /// with the purpose it is about to re-specify (they are *ours to amend*, not
    /// external exposure), so a freshly placed order never starves its own slot.
    pub fn uncontrolled_exposure_where<F: Fn(&LocalOrder) -> bool>(&self, pred: F) -> PendingExposure {
        let mut e = PendingExposure::default();
        for o in self.active().filter(|o| o.state.is_inflight() && pred(o)) {
            // For a pending amend count the larger of old/new sizes.
            let mut q = o.remaining();
            if let Some((_, tot)) = o.amend_target {
                q = q.max((tot - o.filled).max(0));
            }
            match (o.side, o.reduce_only) {
                (Side::Buy, false) => e.open_buy += q,
                (Side::Sell, false) => e.open_sell += q,
                (Side::Buy, true) => e.reduce_buy += q,
                (Side::Sell, true) => e.reduce_sell += q,
            }
        }
        e
    }

    // ----------------------------------------------------------------- intents

    /// Create a new order and return the command to send.
    pub fn place(
        &mut self,
        side: Side,
        purpose: Purpose,
        layer: u8,
        price: Option<Ticks>,
        size: i64,
        tif: Tif,
        now: Instant,
        now_ms: i64,
    ) -> (u64, ExecCommand) {
        let id = self.next_id;
        self.next_id += 1;
        let client_id = self.new_client_id(now_ms);
        let req_id = self.new_req_id("p");
        let reduce_only = purpose == Purpose::Reduce;
        let o = LocalOrder {
            id,
            client_id: client_id.clone(),
            exchange_id: None,
            side,
            purpose,
            layer,
            tif,
            reduce_only,
            price,
            size,
            filled: 0,
            trade_filled: 0,
            state: OrderState::PendingNew,
            intent: None,
            amend_target: None,
            inflight_since: Some(now),
            req_id: Some(req_id.clone()),
            last_update: now,
            done_at: None,
        };
        self.by_client.insert(client_id.clone(), id);
        self.by_req.insert(req_id.clone(), id);
        self.orders.insert(id, o);
        (id, ExecCommand::Place { req_id, client_id, side, size, price, tif, reduce_only })
    }

    /// Request a new price / **remaining** size. Returns a command if it can be
    /// sent now; otherwise the intent is stored.
    pub fn amend(&mut self, id: u64, price: Ticks, remaining: i64, now: Instant) -> Option<ExecCommand> {
        let (state, filled) = {
            let o = self.orders.get(&id)?;
            (o.state, o.filled)
        };
        if state.is_terminal() {
            return None;
        }
        let total = filled + remaining.max(0);
        if total <= filled {
            // Nothing left to rest: that is a cancel.
            return self.cancel(id, now);
        }
        let req_id_new = self.new_req_id("a");
        let o = self.orders.get_mut(&id)?;
        if o.state != OrderState::Live {
            o.intent = Some(Intent::Amend { price, size: remaining });
            return None;
        }
        let Some(ex) = o.exchange_id.clone() else {
            o.intent = Some(Intent::Amend { price, size: remaining });
            return None;
        };
        o.state = OrderState::PendingAmend;
        o.amend_target = Some((price, total));
        o.inflight_since = Some(now);
        o.req_id = Some(req_id_new.clone());
        o.intent = None;
        let cmd = ExecCommand::Amend {
            req_id: req_id_new.clone(),
            exchange_id: ex,
            client_id: o.client_id.clone(),
            price: Some(price),
            size: Some(total),
        };
        self.by_req.insert(req_id_new, id);
        Some(cmd)
    }

    pub fn cancel(&mut self, id: u64, now: Instant) -> Option<ExecCommand> {
        let req_id_new = self.new_req_id("c");
        let o = self.orders.get_mut(&id)?;
        if o.state.is_terminal() {
            return None;
        }
        match o.state {
            OrderState::Live => {
                o.state = OrderState::PendingCancel;
                o.inflight_since = Some(now);
                o.req_id = Some(req_id_new.clone());
                o.intent = None;
                let cmd = ExecCommand::Cancel { req_id: req_id_new.clone(), exchange_id: o.exchange_id.clone(), client_id: o.client_id.clone() };
                self.by_req.insert(req_id_new, id);
                Some(cmd)
            }
            OrderState::PendingCancel => None,
            _ => {
                o.intent = Some(Intent::Cancel);
                None
            }
        }
    }

    /// Cancel orders matching a predicate.
    pub fn cancel_where<F: Fn(&LocalOrder) -> bool>(&mut self, pred: F, now: Instant) -> Vec<ExecCommand> {
        let ids: Vec<u64> = self.active().filter(|o| pred(o)).map(|o| o.id).collect();
        ids.into_iter().filter_map(|id| self.cancel(id, now)).collect()
    }

    // --------------------------------------------------------------- responses

    pub fn on_exec_response(&mut self, resp: ExecResponse) -> Followups {
        let mut f = Followups::default();
        let Some(id) = self.by_req.remove(&resp.req_id) else {
            debug!(req = %resp.req_id, "response for unknown request");
            return f;
        };
        let now = resp.at;
        let Some(o) = self.orders.get_mut(&id) else { return f };
        if o.req_id.as_deref() != Some(resp.req_id.as_str()) {
            // A stale response for a superseded request: still useful for ids.
            if let ExecResult::Placed(info) | ExecResult::Amended(info) | ExecResult::Queried(info) = &resp.result {
                if o.exchange_id.is_none() {
                    o.exchange_id = Some(info.exchange_id.clone());
                    self.by_exchange.insert(info.exchange_id.clone(), id);
                }
            }
            return f;
        }
        o.req_id = None;
        o.inflight_since = None;
        o.last_update = now;
        let prev = o.state;
        match resp.result {
            ExecResult::Placed(info) | ExecResult::Queried(info) | ExecResult::Amended(info) => {
                if o.exchange_id.is_none() {
                    self.by_exchange.insert(info.exchange_id.clone(), id);
                }
                Self::absorb_info(o, &info, now);
            }
            ExecResult::Cancelled(info) => {
                if o.exchange_id.is_none() {
                    self.by_exchange.insert(info.exchange_id.clone(), id);
                }
                Self::absorb_info(o, &info, now);
                if !o.state.is_terminal() {
                    o.state = OrderState::Done(DoneReason::Cancelled);
                    o.done_at = Some(now);
                }
            }
            ExecResult::CancelledAll(_) => {}
            ExecResult::Error { label, message } => {
                warn!(order = o.client_id, %label, %message, state = ?prev, "exchange error");
                match prev {
                    OrderState::PendingNew => {
                        // Placement rejected. Post-only crossing → PostOnlyReject; anything else → Rejected.
                        let reason = if label.contains("POC") || label.contains("POST_ONLY") {
                            DoneReason::PostOnlyReject
                        } else {
                            DoneReason::Rejected
                        };
                        // A timeout-like error ("unknown") must not assume rejection: query instead.
                        if label == "TIMEOUT" || label == "UNKNOWN" {
                            o.state = OrderState::PendingNew;
                            let req = self.new_req_id("q");
                            let o = self.orders.get_mut(&id).unwrap();
                            o.req_id = Some(req.clone());
                            o.inflight_since = Some(now);
                            self.by_req.insert(req.clone(), id);
                            f.commands.push(ExecCommand::Query { req_id: req, exchange_id: o.exchange_id.clone(), client_id: o.client_id.clone() });
                            return f;
                        }
                        o.state = OrderState::Done(reason);
                        o.done_at = Some(now);
                    }
                    OrderState::PendingAmend | OrderState::PendingCancel => {
                        // Do not assume anything: the order may be live, filled or gone. Query.
                        o.amend_target = None;
                        if label == "ORDER_NOT_FOUND" || label == "ORDER_FINISHED" || label == "ORDER_CLOSED" {
                            // Exchange says it is not open; fills (if any) arrive via the stream.
                            o.state = OrderState::Done(DoneReason::Lost);
                            o.done_at = Some(now);
                        } else {
                            o.state = OrderState::Live;
                            let req = self.new_req_id("q");
                            let o = self.orders.get_mut(&id).unwrap();
                            o.req_id = Some(req.clone());
                            o.inflight_since = Some(now);
                            self.by_req.insert(req.clone(), id);
                            f.commands.push(ExecCommand::Query { req_id: req, exchange_id: o.exchange_id.clone(), client_id: o.client_id.clone() });
                            return f;
                        }
                    }
                    _ => {}
                }
            }
        }
        // Apply the latest stored intent, if any.
        self.apply_intent(id, now, &mut f);
        f
    }

    fn absorb_info(o: &mut LocalOrder, info: &OrderInfo, now: Instant) {
        o.exchange_id.get_or_insert_with(|| info.exchange_id.clone());
        o.price = Some(info.price);
        o.size = info.size;
        o.filled = o.filled.max(info.filled());
        o.last_update = now;
        match info.status {
            ExchangeStatus::Open => {
                if !o.state.is_terminal() {
                    o.state = OrderState::Live;
                }
                o.amend_target = None;
            }
            ExchangeStatus::Finished => {
                let reason = match info.finish_as.as_str() {
                    "filled" => DoneReason::Filled,
                    "cancelled" | "auto_deleveraged" | "liquidated" | "reduce_out" | "position_closed" | "stp" => DoneReason::Cancelled,
                    "poc" => DoneReason::PostOnlyReject,
                    "ioc" => DoneReason::IocDone,
                    "reduce_only" => DoneReason::Cancelled,
                    _ => {
                        if info.left == 0 { DoneReason::Filled } else { DoneReason::Cancelled }
                    }
                };
                o.state = OrderState::Done(reason);
                o.done_at = Some(now);
                o.amend_target = None;
                o.intent = None;
            }
        }
    }

    fn apply_intent(&mut self, id: u64, now: Instant, f: &mut Followups) {
        let Some(o) = self.orders.get(&id) else { return };
        if o.state != OrderState::Live {
            return;
        }
        match o.intent {
            Some(Intent::Cancel) => {
                if let Some(c) = self.cancel(id, now) {
                    f.commands.push(c);
                }
            }
            Some(Intent::Amend { price, size }) => {
                let cur = self.orders.get(&id).unwrap();
                if cur.price != Some(price) || cur.remaining() != size {
                    if let Some(c) = self.amend(id, price, size, now) {
                        f.commands.push(c);
                    }
                } else {
                    self.orders.get_mut(&id).unwrap().intent = None;
                }
            }
            None => {}
        }
    }

    /// Private order-stream update.
    pub fn on_order_update(&mut self, info: OrderInfo, now: Instant) -> Followups {
        let mut f = Followups::default();
        let id = self
            .by_exchange
            .get(&info.exchange_id)
            .copied()
            .or_else(|| self.by_client.get(&info.client_id).copied());
        let Some(id) = id else {
            if self.is_ours(&info.client_id) && info.status == ExchangeStatus::Open {
                warn!(client = info.client_id, ex = info.exchange_id, "unknown open order with our prefix – will cancel");
                self.foreign.insert(info.exchange_id.clone(), info);
            }
            return f;
        };
        let o = self.orders.get_mut(&id).unwrap();
        if o.exchange_id.is_none() {
            self.by_exchange.insert(info.exchange_id.clone(), id);
        }
        // Stream updates are authoritative for cumulative quantities but must not
        // regress an in-flight amend's requested size (the stream may lag).
        let inflight_amend = o.state == OrderState::PendingAmend;
        let was_state = o.state;
        let prev_size = o.size;
        Self::absorb_info(o, &info, now);
        if inflight_amend && info.status == ExchangeStatus::Open {
            // keep waiting for the amend response; restore in-flight marker
            o.state = OrderState::PendingAmend;
            if info.update_ms > 0 && o.amend_target.is_none() {
                o.size = prev_size.max(info.size);
            }
        }
        if was_state == OrderState::PendingNew && o.state == OrderState::Live && o.req_id.is_some() {
            // The stream confirmed the order before the API response; keep req
            // mapping so the later response is consumed, but allow intents.
            o.inflight_since = None;
        }
        if o.state == OrderState::Live && o.req_id.is_none() {
            self.apply_intent(id, now, &mut f);
        }
        f
    }

    /// Private user-trade. Returns a de-duplicated fill (if new).
    pub fn on_user_trade(&mut self, t: UserTrade) -> Option<Fill> {
        if !self.seen_trades.insert(t.trade_id.clone()) {
            return None;
        }
        self.seen_order.push_back(t.trade_id.clone());
        while self.seen_order.len() > 50_000 {
            if let Some(old) = self.seen_order.pop_front() {
                self.seen_trades.remove(&old);
            }
        }
        let id = self
            .by_exchange
            .get(&t.exchange_order_id)
            .copied()
            .or_else(|| self.by_client.get(&t.client_id).copied());
        let size = t.signed_size.abs();
        let side = if t.signed_size >= 0 { Side::Buy } else { Side::Sell };
        match id {
            Some(id) => {
                let o = self.orders.get_mut(&id).unwrap();
                o.trade_filled += size;
                o.filled = o.filled.max(o.trade_filled);
                o.last_update = t.at;
                if o.filled >= o.size && o.size > 0 && !o.state.is_terminal() {
                    o.state = OrderState::Done(DoneReason::Filled);
                    o.done_at = Some(t.at);
                }
                Some(Fill {
                    local_id: Some(id),
                    client_id: o.client_id.clone(),
                    side: o.side,
                    purpose: o.purpose,
                    layer: o.layer,
                    reduce_only: o.reduce_only,
                    size,
                    price: t.price,
                    price_f64: t.price_f64,
                    fee: t.fee,
                    is_maker: t.is_maker,
                    tif: o.tif,
                    at: t.at,
                })
            }
            None => {
                warn!(trade = t.trade_id, order = t.exchange_order_id, "fill for unknown order");
                Some(Fill {
                    local_id: None,
                    client_id: t.client_id.clone(),
                    side,
                    purpose: Purpose::Open,
                    layer: u8::MAX,
                    reduce_only: false,
                    size,
                    price: t.price,
                    price_f64: t.price_f64,
                    fee: t.fee,
                    is_maker: t.is_maker,
                    tif: Tif::Gtc,
                    at: t.at,
                })
            }
        }
    }

    /// In-flight requests older than the timeout → status queries. Never re-sends.
    pub fn check_timeouts(&mut self, now: Instant) -> Vec<ExecCommand> {
        let mut cmds = Vec::new();
        let ids: Vec<u64> = self
            .active()
            .filter(|o| {
                o.state.is_inflight()
                    && o.inflight_since.map(|t| now.duration_since(t) >= self.inflight_timeout).unwrap_or(false)
            })
            .map(|o| o.id)
            .collect();
        for id in ids {
            let req = self.new_req_id("q");
            let o = self.orders.get_mut(&id).unwrap();
            warn!(order = o.client_id, state = ?o.state, "in-flight request timed out; querying");
            if let Some(old) = o.req_id.take() {
                self.by_req.remove(&old);
            }
            o.req_id = Some(req.clone());
            o.inflight_since = Some(now);
            // Restore a queryable state: the query response decides.
            if o.state == OrderState::PendingAmend {
                o.amend_target = None;
            }
            cmds.push(ExecCommand::Query { req_id: req.clone(), exchange_id: o.exchange_id.clone(), client_id: o.client_id.clone() });
            self.by_req.insert(req, id);
        }
        cmds
    }

    /// Drop terminal orders after the retention period.
    pub fn gc(&mut self, now: Instant) {
        let dead: Vec<u64> = self
            .orders
            .values()
            .filter(|o| o.state.is_terminal() && o.done_at.map(|t| now.duration_since(t) > self.retention).unwrap_or(false))
            .map(|o| o.id)
            .collect();
        for id in dead {
            if let Some(o) = self.orders.remove(&id) {
                self.by_client.remove(&o.client_id);
                if let Some(ex) = o.exchange_id {
                    self.by_exchange.remove(&ex);
                }
                if let Some(r) = o.req_id {
                    self.by_req.remove(&r);
                }
            }
        }
    }

    /// Adopt a snapshot of open orders from the exchange (startup / resync).
    pub fn adopt_open_orders(&mut self, infos: Vec<OrderInfo>, now: Instant) -> Vec<OrderInfo> {
        let mut unknown = Vec::new();
        for info in infos {
            let id = self.by_exchange.get(&info.exchange_id).copied().or_else(|| self.by_client.get(&info.client_id).copied());
            match id {
                Some(id) => {
                    let o = self.orders.get_mut(&id).unwrap();
                    Self::absorb_info(o, &info, now);
                }
                None => unknown.push(info),
            }
        }
        unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(ex: &str, cid: &str, side: Side, price: Ticks, size: i64, left: i64, status: ExchangeStatus, finish_as: &str) -> OrderInfo {
        OrderInfo {
            exchange_id: ex.into(),
            client_id: cid.into(),
            side,
            price,
            size,
            left,
            fill_price: 0.0,
            status,
            finish_as: finish_as.into(),
            reduce_only: false,
            tif: Some(Tif::Poc),
            update_ms: 1,
        }
    }

    fn mgr() -> OrderManager {
        OrderManager::new("t-mm", Duration::from_secs(3))
    }

    #[test]
    fn place_ack_amend_flow_with_latest_intent() {
        let mut m = mgr();
        let t0 = Instant::now();
        let (id, cmd) = m.place(Side::Buy, Purpose::Open, 0, Some(99971), 25, Tif::Poc, t0, 1);
        let ExecCommand::Place { req_id, client_id, .. } = cmd.clone() else { panic!() };
        // amend while pending → intent stored, no command
        assert!(m.amend(id, 99969, 25, t0).is_none());
        // a later amend overwrites the earlier intent
        assert!(m.amend(id, 99968, 20, t0).is_none());
        let f = m.on_exec_response(ExecResponse {
            req_id,
            result: ExecResult::Placed(info("E1", &client_id, Side::Buy, 99971, 25, 25, ExchangeStatus::Open, "")),
            at: t0,
        });
        // exactly one follow-up: the latest amend, sized as a total
        assert_eq!(f.commands.len(), 1);
        match &f.commands[0] {
            ExecCommand::Amend { price, size, exchange_id, .. } => {
                assert_eq!(*price, Some(99968));
                assert_eq!(*size, Some(20));
                assert_eq!(exchange_id, "E1");
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(m.get(id).unwrap().state, OrderState::PendingAmend);
    }

    #[test]
    fn amend_total_includes_filled() {
        let mut m = mgr();
        let t0 = Instant::now();
        let (id, cmd) = m.place(Side::Sell, Purpose::Open, 0, Some(100029), 8, Tif::Poc, t0, 1);
        let ExecCommand::Place { req_id, client_id, .. } = cmd else { panic!() };
        m.on_exec_response(ExecResponse { req_id, result: ExecResult::Placed(info("E2", &client_id, Side::Sell, 100029, 8, 8, ExchangeStatus::Open, "")), at: t0 });
        // 3 filled via trade
        let fill = m.on_user_trade(UserTrade { trade_id: "T1".into(), exchange_order_id: "E2".into(), client_id: client_id.clone(), signed_size: -3, price: 100029, price_f64: 1000.29, fee: -0.01, is_maker: true, exch_ms: 0, at: t0 }).unwrap();
        assert_eq!(fill.size, 3);
        // duplicate trade id is ignored
        assert!(m.on_user_trade(UserTrade { trade_id: "T1".into(), exchange_order_id: "E2".into(), client_id: client_id.clone(), signed_size: -3, price: 100029, price_f64: 1000.29, fee: 0.0, is_maker: true, exch_ms: 0, at: t0 }).is_none());
        // want 5 remaining → total 8
        let cmd = m.amend(id, 100030, 5, t0).unwrap();
        match cmd {
            ExecCommand::Amend { size, .. } => assert_eq!(size, Some(8)),
            _ => panic!(),
        }
    }

    #[test]
    fn cancel_failure_queries_instead_of_assuming() {
        let mut m = mgr();
        let t0 = Instant::now();
        let (id, cmd) = m.place(Side::Buy, Purpose::Open, 1, Some(99961), 25, Tif::Poc, t0, 1);
        let ExecCommand::Place { req_id, client_id, .. } = cmd else { panic!() };
        m.on_exec_response(ExecResponse { req_id, result: ExecResult::Placed(info("E3", &client_id, Side::Buy, 99961, 25, 25, ExchangeStatus::Open, "")), at: t0 });
        let c = m.cancel(id, t0).unwrap();
        let ExecCommand::Cancel { req_id, .. } = c else { panic!() };
        let f = m.on_exec_response(ExecResponse { req_id, result: ExecResult::Error { label: "SERVER_ERROR".into(), message: "x".into() }, at: t0 });
        assert!(matches!(f.commands.as_slice(), [ExecCommand::Query { .. }]));
        assert!(!m.get(id).unwrap().state.is_terminal());
        // query says finished/cancelled → terminal
        let ExecCommand::Query { req_id, .. } = &f.commands[0] else { panic!() };
        m.on_exec_response(ExecResponse { req_id: req_id.clone(), result: ExecResult::Queried(info("E3", &client_id, Side::Buy, 99961, 25, 25, ExchangeStatus::Finished, "cancelled")), at: t0 });
        assert_eq!(m.get(id).unwrap().state, OrderState::Done(DoneReason::Cancelled));
    }

    #[test]
    fn late_fill_after_cancel_is_still_counted() {
        let mut m = mgr();
        let t0 = Instant::now();
        let (id, cmd) = m.place(Side::Buy, Purpose::Open, 0, Some(99971), 25, Tif::Poc, t0, 1);
        let ExecCommand::Place { req_id, client_id, .. } = cmd else { panic!() };
        m.on_exec_response(ExecResponse { req_id, result: ExecResult::Placed(info("E4", &client_id, Side::Buy, 99971, 25, 25, ExchangeStatus::Open, "")), at: t0 });
        let ExecCommand::Cancel { req_id, .. } = m.cancel(id, t0).unwrap() else { panic!() };
        m.on_exec_response(ExecResponse { req_id, result: ExecResult::Cancelled(info("E4", &client_id, Side::Buy, 99971, 25, 20, ExchangeStatus::Finished, "cancelled")), at: t0 });
        assert!(m.get(id).unwrap().state.is_terminal());
        // the fill that happened before the cancel arrives later
        let f = m.on_user_trade(UserTrade { trade_id: "T9".into(), exchange_order_id: "E4".into(), client_id, signed_size: 5, price: 99971, price_f64: 999.71, fee: 0.0, is_maker: true, exch_ms: 0, at: t0 });
        assert_eq!(f.unwrap().size, 5);
        assert_eq!(m.get(id).unwrap().filled, 5);
    }

    #[test]
    fn timeout_issues_query_not_resend() {
        let mut m = mgr();
        let t0 = Instant::now();
        let (id, _) = m.place(Side::Buy, Purpose::Open, 0, Some(99971), 25, Tif::Poc, t0, 1);
        assert!(m.check_timeouts(t0 + Duration::from_secs(1)).is_empty());
        let cmds = m.check_timeouts(t0 + Duration::from_secs(4));
        assert!(matches!(cmds.as_slice(), [ExecCommand::Query { .. }]));
        assert_eq!(m.get(id).unwrap().state, OrderState::PendingNew);
        assert_eq!(m.uncontrolled_exposure().open_buy, 25);
    }

    #[test]
    fn stream_update_drives_terminal_and_pending_new_to_live() {
        let mut m = mgr();
        let t0 = Instant::now();
        let (id, cmd) = m.place(Side::Sell, Purpose::Reduce, 0, Some(100010), 37, Tif::Poc, t0, 1);
        let ExecCommand::Place { client_id, .. } = cmd else { panic!() };
        m.on_order_update(info("E5", &client_id, Side::Sell, 100010, 37, 37, ExchangeStatus::Open, ""), t0);
        assert_eq!(m.get(id).unwrap().state, OrderState::Live);
        m.on_order_update(info("E5", &client_id, Side::Sell, 100010, 37, 0, ExchangeStatus::Finished, "filled"), t0);
        assert_eq!(m.get(id).unwrap().state, OrderState::Done(DoneReason::Filled));
        assert_eq!(m.get(id).unwrap().filled, 37);
        assert!(m.own_resting().is_empty());
    }
}
