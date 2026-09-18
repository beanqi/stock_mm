//! The single-threaded strategy engine: consumes connector events and runs the
//! nine-step cycle from the specification on every relevant event.
//!
//! ```text
//! 1. update fills / orders / positions / margin
//! 2. trust checks (market, basis, positions, account)
//! 3. inventory, band timer, adverse trend, loss limit → active reduce
//! 4. F = Binance mid × (1 + β₀)
//! 5. δ, inventory skew → quote centre r
//! 6. per-side danger → normal / half / wider / no-open
//! 7. inner quotes from the external Gate book, outer layers, open/reduce mapping
//! 8. clip by pending orders, net, gross, margin
//! 9. reconcile with live orders (danger first, keep valid, threshold re-quote, fill gaps)
//! ```

pub mod events;
pub mod paper;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::{Config, Mode};
use crate::exchange::gate_rest::GateRest;
use crate::exchange::gate_trade::TradeCommand;
use crate::market::basis::{BasisEstimator, BasisInput, BasisStatus};
use crate::market::book::{BookStatus, LocalBook};
use crate::market::impact::{ImpactDetector, Trade};
use crate::market::reference::ReferencePrice;
use crate::market::session::{SessionCalendar, SessionKind};
use crate::metrics::{Metrics, Snapshot};
use crate::order::manager::OrderManager;
use crate::order::model::{ExecCommand, Fill, Tif};
use crate::order::reconcile::{ReconcileParams, reconcile};
use crate::strategy::inventory::{InventoryState, Positions};
use crate::strategy::protection::Protection;
use crate::strategy::quote::{QuoteContext, build_quotes, half_spread_bps, side_purposes};
use crate::strategy::reduce::{ActiveReduce, ReduceAction, ReduceInputs, ReduceReason};
use crate::strategy::risk::{self, Pnl, PositionReconciler, TrustInputs, TrustReport};
use crate::types::{ContractMeta, Purpose, Side, TickGrid, Ticks};
use crate::util::unix_ms;
use events::{AccountInfo, Event, Feed, PositionInfo};
use paper::PaperExchange;

#[derive(Debug, Clone, Copy)]
struct GateBboState {
    bid: Ticks,
    ask: Ticks,
    at: Instant,
}

pub enum Executor {
    Live(mpsc::Sender<TradeCommand>),
    Paper(PaperExchange),
}

pub struct Engine {
    cfg: Config,
    meta: ContractMeta,
    grid: TickGrid,
    fee_maker: f64,
    // market
    reference: ReferencePrice,
    gate_bbo: Option<GateBboState>,
    /// Latest Gate (mark, index, funding rate) – sanity checks only.
    gate_ticker: Option<(f64, f64, f64, Instant)>,
    book: LocalBook,
    impact: ImpactDetector,
    basis: BasisEstimator,
    calendar: SessionCalendar,
    // strategy
    inventory: InventoryState,
    protection: Protection,
    reduce: ActiveReduce,
    reconciler: PositionReconciler,
    pnl: Pnl,
    // orders / account
    orders: OrderManager,
    account: Option<AccountInfo>,
    feeds: HashMap<Feed, (bool, Instant)>,
    private_hb: Option<Instant>,
    /// Local ids of exit (IOC / market) orders → `true` when it was a market order.
    exit_orders: HashMap<u64, bool>,
    last_open_orders_sync: Option<Instant>,
    // infra
    exec: Executor,
    rest: Arc<GateRest>,
    tx: mpsc::Sender<Event>,
    metrics: Metrics,
    queue: VecDeque<Event>,
    // state flags
    halted: bool,
    shutting_down: bool,
    shutdown_since: Option<Instant>,
    finished: bool,
    dual_mode: bool,
    last_delta_bps: f64,
    last_fair: Option<f64>,
    last_trust: TrustReport,
    last_account_poll: Option<Instant>,
    last_resnapshot: Option<Instant>,
    last_status_log: Option<Instant>,
    episode_used_reduce: bool,
    cycles: u64,
    /// Test hook: when set, replaces `Instant::now()`.
    clock: Option<Instant>,
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: Config,
        meta: ContractMeta,
        fee_maker: f64,
        calendar: SessionCalendar,
        exec: Executor,
        rest: Arc<GateRest>,
        tx: mpsc::Sender<Event>,
        dual_mode: bool,
        initial_positions: Positions,
    ) -> Self {
        let grid = TickGrid::new(meta.tick);
        let reference = ReferencePrice::new(
            Duration::from_secs(cfg.quoting.vol_window_secs),
            Duration::from_millis(cfg.quoting.vol_step_ms),
            Duration::from_millis(cfg.protection.velocity_window_ms),
        );
        let mut inventory = InventoryState::new(cfg.inventory.clone());
        inventory.pos = initial_positions;
        let metrics = Metrics::new(&cfg.metrics.file, Duration::from_millis(cfg.metrics.snapshot_ms));
        Self {
            grid,
            fee_maker,
            reference,
            gate_bbo: None,
            gate_ticker: None,
            book: LocalBook::new(),
            impact: ImpactDetector::new(cfg.protection.clone()),
            basis: BasisEstimator::new(cfg.pricing.clone()),
            calendar,
            inventory,
            protection: Protection::new(cfg.protection.clone()),
            reduce: ActiveReduce::new(cfg.exit.clone()),
            reconciler: PositionReconciler::default(),
            pnl: Pnl::default(),
            orders: OrderManager::new(&cfg.orders.client_id_prefix, Duration::from_millis(cfg.orders.inflight_timeout_ms)),
            account: None,
            feeds: HashMap::new(),
            private_hb: None,
            exit_orders: HashMap::new(),
            last_open_orders_sync: None,
            exec,
            rest,
            tx,
            metrics,
            queue: VecDeque::new(),
            halted: false,
            shutting_down: false,
            shutdown_since: None,
            finished: false,
            dual_mode,
            last_delta_bps: cfg.quoting.min_half_spread_bps,
            last_fair: None,
            last_trust: TrustReport::default(),
            last_account_poll: None,
            last_resnapshot: None,
            last_status_log: None,
            episode_used_reduce: false,
            cycles: 0,
            clock: None,
            meta,
            cfg,
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    #[cfg(test)]
    pub fn set_clock(&mut self, at: Instant) {
        self.clock = Some(at);
    }

    fn now(&self) -> Instant {
        self.clock.unwrap_or_else(Instant::now)
    }

    // ------------------------------------------------------------------ input

    pub fn handle(&mut self, ev: Event) {
        self.queue.push_back(ev);
        let mut budget = 10_000usize;
        while let Some(ev) = self.queue.pop_front() {
            let run_cycle = self.apply_event(ev);
            if run_cycle {
                let now = self.now();
                self.cycle(now);
            }
            budget -= 1;
            if budget == 0 {
                error!(pending = self.queue.len(), "event feedback loop detected; dropping queued internal events");
                self.queue.clear();
                break;
            }
        }
    }

    /// Step 1: update state. Returns whether a strategy cycle should follow.
    fn apply_event(&mut self, ev: Event) -> bool {
        let now = self.now();
        match ev {
            Event::BinanceBbo(b) => {
                self.reference.update(b);
                if let Some(f) = self.last_fair {
                    self.metrics.on_mid(f, now);
                }
                true
            }
            Event::BinanceTrade { .. } => false,
            Event::GateBbo { bid, ask, at, .. } => {
                self.gate_bbo = Some(GateBboState { bid, ask, at });
                if let Executor::Paper(p) = &mut self.exec {
                    let evs = p.on_bbo(bid, ask, at);
                    self.queue.extend(evs);
                }
                true
            }
            Event::GateBookSnapshot(s) => {
                self.book.apply_snapshot(s, now);
                debug!(status = ?self.book.status(), "gate book snapshot applied");
                false
            }
            Event::GateBookDelta(d) => {
                self.book.apply_delta(d, now);
                if self.book.status() == BookStatus::Broken {
                    self.request_resnapshot(now);
                }
                false
            }
            Event::GateTrade { price, price_f64, signed_size, at, .. } => {
                let side = if signed_size > 0 { Side::Buy } else { Side::Sell };
                let notional = self.meta.notional(signed_size.abs(), price_f64);
                self.impact.on_trade(Trade { at, side, notional });
                if let Executor::Paper(p) = &mut self.exec {
                    let evs = p.on_trade(price, signed_size, at);
                    self.queue.extend(evs);
                }
                true
            }
            Event::GateTicker { mark, index, funding_rate, at } => {
                self.gate_ticker = Some((mark, index, funding_rate, at));
                false
            }
            Event::GateOrder(info) => {
                let f = self.orders.on_order_update(info, now);
                self.dispatch(f.commands, now);
                true
            }
            Event::GateUserTrade(t) => {
                if let Some(fill) = self.orders.on_user_trade(t) {
                    self.on_fill(fill, now);
                }
                true
            }
            Event::GatePositions(list) => {
                let p = positions_from_exchange(&list, self.dual_mode);
                self.reconciler.on_exchange(p, now);
                self.maybe_adopt_exchange_positions(p, now);
                true
            }
            Event::GateBalanceChange { change, kind, .. } => {
                if kind == "fund" || kind == "funding" {
                    self.pnl.funding += change;
                }
                false
            }
            Event::GateAccount(a) => {
                if self.pnl.start_equity.is_none() {
                    self.pnl.start_equity = Some(a.total);
                }
                if a.in_dual_mode != self.dual_mode {
                    warn!(account = a.in_dual_mode, engine = self.dual_mode, "position mode changed on the account");
                    self.dual_mode = a.in_dual_mode;
                }
                self.account = Some(a);
                true
            }
            Event::GateOpenOrders(list) => {
                let unknown = self.orders.adopt_open_orders(list, now);
                for o in unknown {
                    if self.orders.is_ours(&o.client_id) {
                        warn!(ex = o.exchange_id, client = o.client_id, "leaked order with our prefix – cancelling");
                        let req = format!("x-{}-{}", unix_ms(), o.exchange_id);
                        self.send(ExecCommand::Cancel { req_id: req, exchange_id: Some(o.exchange_id.clone()), client_id: o.client_id.clone() }, o.side, now);
                    } else {
                        warn!(ex = o.exchange_id, client = o.client_id, "foreign open order on the contract (left untouched)");
                    }
                }
                true
            }
            Event::Exec(resp) => {
                let f = self.orders.on_exec_response(resp);
                self.dispatch(f.commands, now);
                true
            }
            Event::FeedStatus { feed, connected, at } => {
                info!(feed = feed.label(), connected, "feed status");
                self.feeds.insert(feed, (connected, at));
                if feed == Feed::GatePublic && connected {
                    self.book.reset();
                }
                if feed == Feed::GatePrivate && connected {
                    self.private_hb = Some(at);
                }
                true
            }
            Event::PrivateHeartbeat(at) => {
                self.private_hb = Some(at);
                false
            }
            Event::Timer(at) => {
                self.on_timer(at);
                true
            }
            Event::Shutdown => {
                if !self.shutting_down {
                    info!("shutdown requested: cancelling orders{}", if self.cfg.exit.liquidate_on_shutdown { " and flattening" } else { "" });
                    self.shutting_down = true;
                    self.shutdown_since = Some(now);
                    if self.cfg.exit.liquidate_on_shutdown && !self.inventory.pos.is_flat() {
                        self.reduce.trigger(ReduceReason::Shutdown, 0, self.last_fair.unwrap_or(0.0), now);
                    }
                }
                true
            }
        }
    }

    fn on_fill(&mut self, fill: Fill, now: Instant) {
        let signed = fill.size * fill.side.sign();
        let realised = self.inventory.pos.apply_fill(signed, fill.price_f64, fill.reduce_only, &self.meta);
        self.pnl.realised += realised;
        self.pnl.fees += fill.fee;
        self.metrics.on_fill(fill.side, fill.price_f64, fill.size, fill.is_maker, fill.at);
        if let Some(was_market) = fill.local_id.and_then(|id| self.exit_orders.get(&id).copied()) {
            self.metrics.on_exit_fill(fill.side, self.reduce.decision_fair, fill.price_f64, fill.size, self.meta.multiplier_f64(), was_market);
        }
        info!(
            side = %fill.side,
            purpose = ?fill.purpose,
            layer = fill.layer,
            size = fill.size,
            price = fill.price_f64,
            maker = fill.is_maker,
            fee = fill.fee,
            long = self.inventory.pos.long,
            short = self.inventory.pos.short,
            realised,
            "fill"
        );
        let _ = now;
    }

    fn maybe_adopt_exchange_positions(&mut self, p: Positions, now: Instant) {
        // Adopt the exchange view when nothing of ours is in flight and the
        // local view has been wrong for longer than the grace period.
        let local = self.inventory.pos;
        if (local.long, local.short) == (p.long, p.short) {
            return;
        }
        let quiet = self.orders.active().all(|o| !o.state.is_inflight());
        if quiet && !self.reconciler.check(local, &self.cfg.risk, now) {
            warn!(?local, exchange = ?p, "adopting exchange positions after persistent mismatch");
            self.inventory.pos = p;
        }
    }

    fn on_timer(&mut self, now: Instant) {
        // Account / position polling (live) or synthetic snapshots (paper).
        let due = self.last_account_poll.map(|t| now.duration_since(t) >= Duration::from_millis(self.cfg.risk.account_poll_ms)).unwrap_or(true);
        if due {
            self.last_account_poll = Some(now);
            match &self.exec {
                Executor::Paper(p) => {
                    let mark = self.last_fair.or_else(|| self.gate_mid()).unwrap_or(0.0);
                    let acc = p.account_event(mark, now);
                    let pos = p.positions_event();
                    self.queue.push_back(acc);
                    self.queue.push_back(pos);
                }
                Executor::Live(_) => {
                    let rest = self.rest.clone();
                    let tx = self.tx.clone();
                    let contract = self.cfg.instruments.gate_contract.clone();
                    let dual = self.dual_mode;
                    tokio::spawn(async move {
                        match rest.account().await {
                            Ok(a) => {
                                let _ = tx.send(Event::GateAccount(a)).await;
                            }
                            Err(e) => warn!(error = %e, "account poll failed"),
                        }
                        match rest.positions(&contract, dual).await {
                            Ok(p) => {
                                let _ = tx.send(Event::GatePositions(p)).await;
                            }
                            Err(e) => warn!(error = %e, "position poll failed"),
                        }
                    });
                }
            }
        }
        // Periodic open-order resync (live): catches leaked / lost orders.
        if let Executor::Live(_) = &self.exec {
            let due = self.last_open_orders_sync.map(|t| now.duration_since(t) >= Duration::from_secs(30)).unwrap_or(true);
            if due {
                self.last_open_orders_sync = Some(now);
                let rest = self.rest.clone();
                let tx = self.tx.clone();
                let contract = self.cfg.instruments.gate_contract.clone();
                let grid = self.grid;
                tokio::spawn(async move {
                    match rest.open_orders(&contract, &grid).await {
                        Ok(list) => {
                            let _ = tx.send(Event::GateOpenOrders(list)).await;
                        }
                        Err(e) => warn!(error = %e, "open-order resync failed"),
                    }
                });
            }
        }
        // In-flight timeouts → queries.
        let cmds = self.orders.check_timeouts(now);
        self.dispatch(cmds, now);
        self.orders.gc(now);
        self.exit_orders.retain(|id, _| self.orders.get(*id).is_some());
        // Leaked orders with our prefix.
        let foreign: Vec<_> = self.orders.foreign.drain().collect();
        for (ex, info) in foreign {
            let req = format!("x-{}", unix_ms());
            self.send(ExecCommand::Cancel { req_id: req, exchange_id: Some(ex), client_id: info.client_id.clone() }, info.side, now);
        }
        // Metrics snapshot.
        if self.metrics.snapshot_due(now) {
            self.write_snapshot(now);
        }
        // Shutdown completion.
        if self.shutting_down {
            let no_orders = self.orders.active_count() == 0;
            let flat_ok = !self.cfg.exit.liquidate_on_shutdown || self.inventory.pos.is_flat() || self.reduce.is_halted();
            let timeout = self.shutdown_since.map(|t| now.duration_since(t) > Duration::from_secs(15)).unwrap_or(false);
            if (no_orders && flat_ok && !self.reduce.is_active()) || timeout {
                if timeout {
                    warn!(active = self.orders.active_count(), "shutdown timeout; exiting with residual state");
                }
                self.write_snapshot(now);
                self.finished = true;
            }
        }
    }

    fn request_resnapshot(&mut self, now: Instant) {
        let due = self.last_resnapshot.map(|t| now.duration_since(t) >= Duration::from_secs(2)).unwrap_or(true);
        if !due {
            return;
        }
        self.last_resnapshot = Some(now);
        warn!("gate book broken; requesting a new snapshot");
        self.book.reset();
        let rest = self.rest.clone();
        let tx = self.tx.clone();
        let contract = self.cfg.instruments.gate_contract.clone();
        let grid = self.grid;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            match rest.order_book(&contract, crate::exchange::gate_public::BOOK_LEVELS, &grid).await {
                Ok(s) => {
                    let _ = tx.send(Event::GateBookSnapshot(s)).await;
                }
                Err(e) => warn!(error = %e, "resnapshot failed"),
            }
        });
    }

    // --------------------------------------------------------------- helpers

    fn gate_mid(&self) -> Option<f64> {
        self.gate_bbo.map(|b| (self.grid.to_f64(b.bid) + self.grid.to_f64(b.ask)) / 2.0)
    }

    /// External Gate BBO (own orders removed): prefer the L2 book, fall back to the ticker.
    fn external_bbo(&self) -> (Option<Ticks>, Option<Ticks>) {
        let own = self.orders.own_resting();
        if self.book.is_synced() {
            let (b, a) = self.book.external_bbo(&own);
            let mut bid = b.map(|x| x.0);
            let mut ask = a.map(|x| x.0);
            if let Some(t) = self.gate_bbo {
                // If the ticker is tighter than the (possibly truncated) book, and it is
                // not one of our own levels, use it.
                let own_at = |side: Side, p: Ticks| own.iter().any(|(s, q, _)| *s == side && *q == p);
                if !own_at(Side::Buy, t.bid) && bid.map(|b| t.bid > b).unwrap_or(true) {
                    bid = Some(t.bid);
                }
                if !own_at(Side::Sell, t.ask) && ask.map(|a| t.ask < a).unwrap_or(true) {
                    ask = Some(t.ask);
                }
            }
            (bid, ask)
        } else {
            self.gate_bbo.map(|t| (Some(t.bid), Some(t.ask))).unwrap_or((None, None))
        }
    }

    fn feed_connected(&self, f: Feed) -> bool {
        self.feeds.get(&f).map(|(c, _)| *c).unwrap_or(false)
    }

    fn send(&mut self, cmd: ExecCommand, side: Side, now: Instant) {
        match &mut self.exec {
            Executor::Live(tx) => {
                if let Err(e) = tx.try_send(TradeCommand { cmd: cmd.clone(), side }) {
                    error!(error = %e, ?cmd, "trade channel full/closed; resolving as UNKNOWN");
                    self.queue.push_back(Event::Exec(crate::order::model::ExecResponse {
                        req_id: cmd.req_id().to_string(),
                        result: crate::order::model::ExecResult::Error { label: "UNKNOWN".into(), message: "channel".into() },
                        at: now,
                    }));
                }
            }
            Executor::Paper(p) => {
                let evs = p.execute(&cmd, &self.book, now);
                self.queue.extend(evs);
            }
        }
    }

    fn dispatch(&mut self, cmds: Vec<ExecCommand>, now: Instant) {
        for c in cmds {
            let side = self.side_for(&c);
            self.send(c, side, now);
        }
    }

    fn side_for(&self, cmd: &ExecCommand) -> Side {
        match cmd {
            ExecCommand::Place { side, .. } => *side,
            ExecCommand::Amend { client_id, .. } | ExecCommand::Cancel { client_id, .. } | ExecCommand::Query { client_id, .. } => {
                self.orders.active().find(|o| &o.client_id == client_id).map(|o| o.side).unwrap_or(Side::Buy)
            }
            ExecCommand::CancelAll { .. } => Side::Buy,
        }
    }

    fn cancel_non_exit(&mut self, now: Instant, why: &str) {
        let exits = self.exit_orders.clone();
        let cmds = self.orders.cancel_where(|o| !exits.contains_key(&o.id), now);
        if !cmds.is_empty() {
            info!(n = cmds.len(), why, "cancelling resting orders");
        }
        self.dispatch(cmds, now);
    }

    fn cancel_open_purpose(&mut self, now: Instant, why: &str) {
        let cmds = self.orders.cancel_where(|o| o.purpose == Purpose::Open, now);
        if !cmds.is_empty() {
            info!(n = cmds.len(), why, "cancelling risk-adding orders");
        }
        self.dispatch(cmds, now);
    }

    fn exit_inflight(&self) -> bool {
        self.exit_orders.keys().any(|id| self.orders.get(*id).map(|o| !o.state.is_terminal()).unwrap_or(false))
    }

    // ----------------------------------------------------------------- cycle

    fn cycle(&mut self, now: Instant) {
        self.cycles += 1;
        if self.finished {
            return;
        }

        // Session.
        let session = self.calendar.classify(chrono::Utc::now());
        self.basis.set_session(session);

        // Step 4 (part): basis observation needs both quotes.
        let binance_mid = self.reference.mid();
        let (ext_bid, ext_ask) = self.external_bbo();
        let gate_recv = self.gate_bbo.map(|b| b.at);
        if let (Some(bm), Some(bb), Some(eb), Some(ea), Some(gr)) = (binance_mid, self.reference.last, ext_bid, ext_ask, gate_recv) {
            let gm = (self.grid.to_f64(eb) + self.grid.to_f64(ea)) / 2.0;
            let spread_bps = 10_000.0 * (self.grid.to_f64(ea) - self.grid.to_f64(eb)) / gm;
            self.basis.observe(
                BasisInput { binance_mid: bm, binance_recv: bb.recv, gate_ext_mid: gm, gate_ext_spread_bps: spread_bps, gate_recv: gr },
                self.last_delta_bps,
                now,
            );
        }
        let fair = binance_mid.and_then(|m| self.basis.fair(m));
        self.last_fair = fair;

        // Step 5 (part): δ.
        if let Some(f) = fair {
            let v = self.reference.realised_move_quantile(self.cfg.quoting.vol_quantile);
            self.last_delta_bps = half_spread_bps(&self.cfg.quoting, self.fee_maker, v, self.grid.tick_bps(f));
        }
        let delta = self.last_delta_bps;

        // Step 2: trust.
        let local_pos = self.inventory.pos;
        let position_ok = self.reconciler.check(local_pos, &self.cfg.risk, now);
        let mark = fair.or_else(|| self.gate_mid());
        if let Some(m) = mark {
            self.pnl.unrealised = local_pos.unrealised(m, &self.meta);
        }
        let loss_hit = self.pnl.loss_limit_hit(self.cfg.risk.loss_limit_usdt);
        let private_ok = match self.exec {
            Executor::Paper(_) => true,
            Executor::Live(_) => self.feed_connected(Feed::GatePrivate),
        };
        let trust = risk::evaluate(
            &self.cfg.risk,
            TrustInputs {
                binance_age: if self.feed_connected(Feed::Binance) { self.reference.age(now) } else { None },
                gate_bbo_age: if self.feed_connected(Feed::GatePublic) { self.gate_bbo.map(|b| now.saturating_duration_since(b.at)) } else { None },
                gate_book_synced: self.book.is_synced(),
                private_connected: private_ok,
                private_age: match self.exec {
                    Executor::Paper(_) => Some(Duration::ZERO),
                    Executor::Live(_) => self.private_hb.map(|t| now.saturating_duration_since(t)),
                },
                account_age: self.account.as_ref().map(|a| now.saturating_duration_since(a.at)),
                position_ok,
                available_margin: self.account.as_ref().map(|a| a.available),
                basis: self.basis.status(),
                session,
                session_tradable: self.calendar.is_tradable(session) && self.basis.samples() >= self.cfg.pricing.basis_min_samples,
                loss_limit_hit: loss_hit,
                halted: self.halted,
                mark_deviation_bps: match (fair, self.gate_ticker) {
                    (Some(f), Some((mark, _, _, at))) if mark > 0.0 && now.saturating_duration_since(at) < Duration::from_secs(30) => {
                        Some(10_000.0 * (f / mark - 1.0))
                    }
                    _ => None,
                },
            },
        );
        if trust.reasons != self.last_trust.reasons {
            info!(reasons = ?trust.reasons, "trust state changed");
        }
        self.last_trust = trust.clone();

        // Step 3: inventory / timers / limits.
        let Some(f) = fair else {
            // Cannot price quotes: nothing may rest. Inventory risk is still
            // managed using the Gate mid as the only available reference.
            self.cancel_non_exit(now, "no fair price");
            if let Some(gm) = self.gate_mid() {
                let inflight = self.orders.uncontrolled_exposure();
                let snap = self.inventory.tick(gm, &self.meta, inflight.open_buy + inflight.open_sell, now);
                let band_c = self.inventory.band_contracts(gm, &self.meta);
                if self.reduce.is_active() {
                    self.run_reduce(gm, now);
                } else if !snap.in_band && self.inventory.band_timed_out(now) && self.book.is_synced() && private_ok {
                    if self.reduce.trigger(ReduceReason::Untrusted, band_c, gm, now) {
                        warn!(q_usdt = snap.q_usdt, "reference unavailable with inventory outside band: reducing on Gate mid");
                    }
                }
            }
            self.log_status(now, session, None, delta, &trust);
            return;
        };
        // "Pending adds" = risk-adding orders whose outcome is not yet known (in flight).
        let inflight = self.orders.uncontrolled_exposure();
        let pending_adds = inflight.open_buy + inflight.open_sell;
        let snap = self.inventory.tick(f, &self.meta, pending_adds, now);
        let band_c = self.inventory.band_contracts(f, &self.meta);

        if self.halted {
            self.cancel_non_exit(now, "halted");
            if self.reduce.is_active() {
                self.run_reduce(f, now);
            }
            self.log_status(now, session, Some(f), delta, &trust);
            return;
        }

        if loss_hit && !self.reduce.is_halted() && self.reduce.trigger(ReduceReason::LossLimit, 0, f, now) {
            error!(equity_change = self.pnl.equity_change(), "loss limit hit: liquidating and stopping");
        }
        if snap.q_usdt.abs() >= self.inventory.h() && self.reduce.trigger(ReduceReason::HardCap, 0, f, now) {
            error!(q_usdt = snap.q_usdt, "hard inventory cap reached: liquidating and stopping");
        }
        if !snap.in_band && self.inventory.band_timed_out(now) {
            if self.reduce.trigger(ReduceReason::BandTimeout, band_c, f, now) {
                warn!(q_usdt = snap.q_usdt, for_ms = ?snap.out_of_band_for, "inventory outside band too long: active reduce");
                self.episode_used_reduce = true;
            }
        } else if snap.in_band && self.inventory.episode_start.is_some() && !self.reduce.is_active() && pending_adds == 0 {
            // Back in band with no unresolved adds: the digestion episode ends.
            if let Some(start) = self.inventory.episode_start {
                self.metrics.on_digestion_episode(now.saturating_duration_since(start), self.episode_used_reduce);
            }
            self.inventory.clear_episode();
            self.episode_used_reduce = false;
        }

        // Step 6 (part): signals & protection.
        let velocity = self.reference.velocity_bps(now);
        let impact = self.impact.evaluate(now);
        self.protection.update(velocity, delta, impact, now);
        if let Some(strong) = self.protection.strong_side() {
            // Spec §4.3: with inventory *against* a strong move we do not wait for
            // an ideal maker fill – flatten the adverse side actively.
            let against = match strong {
                Side::Sell => local_pos.short > 0 && local_pos.net() < 0, // rising hard while net short
                Side::Buy => local_pos.long > 0 && local_pos.net() > 0,   // falling hard while net long
            };
            if against && self.reduce.trigger(ReduceReason::StrongAgainstInventory, 0, f, now) {
                warn!(velocity, long = local_pos.long, short = local_pos.short, "strong move against inventory: active reduce");
                self.episode_used_reduce = true;
            }
        }
        if self.basis.status() == BasisStatus::Abnormal && !local_pos.is_flat() && !snap.in_band {
            if self.reduce.trigger(ReduceReason::BasisAbnormal, band_c, f, now) {
                warn!(beta_t = ?self.basis.last_beta_t, beta0 = ?self.basis.beta0(), d = ?self.basis.last_d, "basis abnormal with inventory: active reduce");
                self.episode_used_reduce = true;
            }
        }

        // Step 2 consequences: untrusted market/private → nothing rests.
        if !trust.allow_resting() {
            self.cancel_non_exit(now, "untrusted state");
            if !local_pos.is_flat() && self.book.is_synced() && self.gate_bbo.is_some() && private_ok {
                self.reduce.trigger(ReduceReason::Untrusted, band_c, f, now);
            } else {
                self.log_status(now, session, Some(f), delta, &trust);
                return;
            }
        }

        // Active reduce takes over the cycle.
        if self.reduce.is_active() {
            self.run_reduce(f, now);
            self.log_status(now, session, Some(f), delta, &trust);
            return;
        }

        if !trust.allow_new_risk() {
            self.cancel_open_purpose(now, "new risk not allowed");
        }

        // Steps 5–9: quotes.
        let allow_new_risk = trust.allow_new_risk() && !self.shutting_down;
        // In-flight orders that sit in a quote slot with the purpose we are about
        // to re-specify are re-specifiable; everything else in flight (exits,
        // orders being cancelled after a purpose flip) is external exposure.
        let (buy_purpose, sell_purpose) = side_purposes(local_pos);
        let uncontrolled = self.orders.uncontrolled_exposure_where(|o| {
            let slot_purpose = match o.side {
                Side::Buy => buy_purpose,
                Side::Sell => sell_purpose,
            };
            o.layer == u8::MAX || o.purpose != slot_purpose
        });
        let ctx = QuoteContext {
            fair: f,
            delta_bps: delta,
            u: snap.u,
            ext_bid,
            ext_ask,
            grid: self.grid,
            meta: &self.meta,
            pos: local_pos,
            uncontrolled,
            buy_policy: self.protection.policy(Side::Buy),
            sell_policy: self.protection.policy(Side::Sell),
            allow_new_risk,
            quoting: &self.cfg.quoting,
            inventory: &self.inventory,
        };
        let (mut quotes, set) = build_quotes(&ctx);
        if self.shutting_down {
            // Nothing may rest while shutting down (flattening, if requested,
            // is handled by the active-reduce path above).
            quotes.clear();
        }
        // Margin clipping for risk-adding orders (rough: notional / leverage).
        if let Some(acc) = &self.account {
            let mut free = acc.available - self.cfg.risk.margin_buffer_usdt;
            for q in quotes.iter_mut().filter(|q| q.purpose == Purpose::Open) {
                let need = self.meta.notional(q.size, f) / self.cfg.risk.margin_leverage;
                if need > free {
                    let afford = self.meta.contracts_for_notional(free.max(0.0) * self.cfg.risk.margin_leverage, f);
                    q.size = afford.min(q.size);
                }
                free -= self.meta.notional(q.size, f) / self.cfg.risk.margin_leverage;
            }
            quotes.retain(|q| q.size >= self.meta.order_size_min);
        }
        // Respect the exchange open-order limit.
        let max_orders = (self.meta.orders_limit.max(2) as usize).saturating_sub(2);
        if quotes.len() > max_orders {
            quotes.truncate(max_orders);
        }
        let params = ReconcileParams {
            bounds: set.bounds,
            follow_tight_buy: ctx.buy_policy.follow_tight,
            follow_tight_sell: ctx.sell_policy.follow_tight,
            cfg: &self.cfg.quoting,
            use_amend: self.cfg.orders.use_amend,
        };
        let rep = reconcile(&mut self.orders, &quotes, params, now, unix_ms());
        if !rep.commands.is_empty() {
            debug!(
                dangerous = rep.dangerous,
                repriced = rep.repriced,
                resized = rep.resized,
                placed = rep.placed,
                cancelled = rep.cancelled,
                kept = rep.kept,
                fair = f,
                centre = set.centre,
                delta,
                u = snap.u,
                inner_bid = ?set.inner_bid.map(|t| self.grid.to_f64(t)),
                inner_ask = ?set.inner_ask.map(|t| self.grid.to_f64(t)),
                "reconcile"
            );
        }
        self.dispatch(rep.commands, now);
        self.log_status(now, session, Some(f), delta, &trust);
    }

    fn run_reduce(&mut self, fair: f64, now: Instant) {
        let own = self.orders.own_resting();
        let exits = self.exit_orders.clone();
        let conflicts_pending = self.orders.active().any(|o| !exits.contains_key(&o.id));
        let exit_inflight = self.exit_inflight();
        let action = self.reduce.step(ReduceInputs {
            now,
            pos: self.inventory.pos,
            fair,
            book: &self.book,
            own: &own,
            grid: self.grid,
            meta: &self.meta,
            conflicts_pending,
            exit_inflight,
        });
        match action {
            ReduceAction::Wait => {}
            ReduceAction::CancelConflicts => self.cancel_non_exit(now, "active reduce"),
            ReduceAction::SendIoc { side, size, price } => {
                let (id, cmd) = self.orders.place(side, Purpose::Reduce, u8::MAX, Some(price), size, Tif::Ioc, now, unix_ms());
                self.exit_orders.insert(id, false);
                info!(side = %side, size, price = self.grid.to_f64(price), reason = ?self.reduce.reason, attempt = self.reduce.attempts, "reduce IOC");
                self.send(cmd, side, now);
            }
            ReduceAction::SendMarket { side, size } => {
                let (id, cmd) = self.orders.place(side, Purpose::Reduce, u8::MAX, None, size, Tif::Ioc, now, unix_ms());
                self.exit_orders.insert(id, true);
                error!(side = %side, size, attempt = self.reduce.emergency_attempts, "EMERGENCY market reduce");
                self.send(cmd, side, now);
            }
            ReduceAction::Completed => {
                info!(long = self.inventory.pos.long, short = self.inventory.pos.short, elapsed = ?self.reduce.elapsed(now), "active reduce completed");
                self.reduce.reset();
            }
            ReduceAction::Halt => {
                if !self.halted {
                    error!(reason = ?self.reduce.reason, long = self.inventory.pos.long, short = self.inventory.pos.short, "STRATEGY HALTED – manual intervention required");
                    self.halted = true;
                    self.cancel_non_exit(now, "halt");
                    // Belt and braces: exchange-side cancel-all as well.
                    self.send(ExecCommand::CancelAll { req_id: format!("ca-{}", unix_ms()) }, Side::Buy, now);
                    self.write_snapshot(now);
                }
            }
        }
    }

    fn log_status(&mut self, now: Instant, session: SessionKind, fair: Option<f64>, delta: f64, trust: &TrustReport) {
        let due = self.last_status_log.map(|t| now.duration_since(t) >= Duration::from_secs(5)).unwrap_or(true);
        if !due {
            return;
        }
        self.last_status_log = Some(now);
        let (eb, ea) = self.external_bbo();
        info!(
            session = session.label(),
            basis = ?self.basis.status(),
            samples = self.basis.samples(),
            beta0 = ?self.basis.beta0().map(|b| (b * 100.0).round() / 100.0),
            beta_t = ?self.basis.last_beta_t.map(|b| (b * 100.0).round() / 100.0),
            drift = ?self.basis.drift_bps().map(|b| (b * 100.0).round() / 100.0),
            fair = ?fair.map(|f| (f * 1000.0).round() / 1000.0),
            delta_bps = (delta * 100.0).round() / 100.0,
            binance = ?self.reference.mid(),
            gate_ext = ?(eb.map(|t| self.grid.to_f64(t)), ea.map(|t| self.grid.to_f64(t))),
            gate_mark = ?self.gate_ticker.map(|t| t.0),
            book = ?self.book.status(),
            long = self.inventory.pos.long,
            short = self.inventory.pos.short,
            gross_usdt = ?fair.map(|f| (self.meta.notional(self.inventory.pos.gross(), f) * 100.0).round() / 100.0),
            orders = self.orders.active_count(),
            prot = ?(self.protection.policy(Side::Buy).level, self.protection.policy(Side::Sell).level),
            reduce = ?self.reduce.phase,
            equity = (self.pnl.equity_change() * 100.0).round() / 100.0,
            available = ?self.account.as_ref().map(|a| a.available),
            trust = ?trust.reasons,
            "status"
        );
        if let Some(d) = self.basis.drift_bps() {
            if d.abs() > self.cfg.pricing.drift_warn_bps {
                warn!(drift_bps = d, "5-minute basis drifting away from β₀");
            }
        }
    }

    fn write_snapshot(&mut self, now: Instant) {
        let session = self.calendar.classify(chrono::Utc::now());
        let f = self.last_fair;
        let (q_c, q_usdt, u) = match f {
            Some(f) => (self.inventory.pos.net(), self.inventory.q_usdt(f, &self.meta), self.inventory.u(f, &self.meta)),
            None => (self.inventory.pos.net(), 0.0, 0.0),
        };
        let snap = Snapshot {
            ts_ms: unix_ms(),
            uptime_s: self.metrics.uptime(now).as_secs(),
            fair: f,
            beta0_bps: self.basis.beta0(),
            beta_t_bps: self.basis.last_beta_t,
            drift_bps: self.basis.drift_bps(),
            delta_bps: self.last_delta_bps,
            session: session.label().to_string(),
            basis_status: format!("{:?}", self.basis.status()),
            q_contracts: q_c,
            q_usdt,
            u,
            long: self.inventory.pos.long,
            short: self.inventory.pos.short,
            active_orders: self.orders.active_count(),
            protection_buy: format!("{:?}", self.protection.buy.phase),
            protection_sell: format!("{:?}", self.protection.sell.phase),
            reduce_phase: format!("{:?}", self.reduce.phase),
            trust_reasons: self.last_trust.reasons.clone(),
            pnl: self.pnl.clone(),
            equity_change: self.pnl.equity_change(),
            available_margin: self.account.as_ref().map(|a| a.available),
            markout_buy: self.metrics.buy.clone(),
            markout_sell: self.metrics.sell.clone(),
            digestion: self.metrics.digestion.clone(),
            exit: self.metrics.exit.clone(),
            fills: self.metrics.fills.clone(),
            cycles: self.cycles,
        };
        self.metrics.write_snapshot(&snap, now);
    }
}

/// Convert Gate position records to the dual-mode `Positions` view.
pub fn positions_from_exchange(list: &[PositionInfo], dual: bool) -> Positions {
    let mut p = Positions::default();
    for i in list {
        match i.mode.as_str() {
            "dual_long" => {
                p.long = i.size.max(0);
                p.long_entry = i.entry_price;
            }
            "dual_short" => {
                p.short = i.size.abs();
                p.short_entry = i.entry_price;
            }
            _ => {
                if i.size > 0 {
                    p.long = i.size;
                    p.long_entry = i.entry_price;
                } else if i.size < 0 {
                    p.short = -i.size;
                    p.short_entry = i.entry_price;
                }
            }
        }
    }
    let _ = dual;
    p
}

/// Periodic timer task feeding the engine.
pub async fn run_timer(period: Duration, tx: mpsc::Sender<Event>) {
    let mut iv = tokio::time::interval(period);
    loop {
        iv.tick().await;
        if tx.send(Event::Timer(Instant::now())).await.is_err() {
            return;
        }
    }
}

pub fn mode_label(m: Mode) -> &'static str {
    match m {
        Mode::Paper => "paper",
        Mode::Live => "live",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::book::{BookDelta, BookSnapshot, DepthLevel};
    use crate::market::reference::Bbo;
    use crate::order::model::OrderState;
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

    fn test_engine() -> (Engine, mpsc::Receiver<Event>) {
        let mut cfg = Config::default();
        cfg.mode = Mode::Paper;
        cfg.pricing.basis_min_samples = 5;
        cfg.pricing.basis_sample_interval_ms = 0;
        cfg.session.force_session = "regular".into();
        cfg.metrics.file = String::new();
        cfg.inventory.max_net_usdt = 5000.0;
        cfg.inventory.max_gross_usdt = 5000.0;
        let m = meta();
        let grid = TickGrid::new(m.tick);
        let calendar = SessionCalendar::from_config(&cfg.session).unwrap();
        let rest = Arc::new(GateRest::new("http://127.0.0.1:1", "usdt", "", ""));
        let (tx, rx) = mpsc::channel(1024);
        let exec = Executor::Paper(PaperExchange::new(grid, m.clone(), 10_000.0, -0.0001, 0.00075, 5.0));
        (Engine::new(cfg, m, -0.0001, calendar, exec, rest, tx, true, Positions::default()), rx)
    }

    fn bbo(bid: f64, ask: f64, at: Instant) -> Event {
        Event::BinanceBbo(Bbo { bid, ask, bid_qty: 1.0, ask_qty: 1.0, exch_ts_ms: 0, recv: at })
    }

    fn gate_bbo(bid: Ticks, ask: Ticks, at: Instant) -> Event {
        Event::GateBbo { bid, bid_size: 10, ask, ask_size: 10, exch_ms: 0, at }
    }

    /// Advance the clock and feed one round of synchronised market data.
    fn step(eng: &mut Engine, at: Instant) {
        eng.set_clock(at);
        eng.handle(Event::Timer(at));
        eng.handle(gate_bbo(99980, 100020, at));
        eng.handle(bbo(999.9, 1000.1, at));
    }

    #[test]
    fn end_to_end_quote_fill_reduce_and_band_timeout() {
        let _ = tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).with_test_writer().try_init();
        let (mut eng, _rx) = test_engine();
        let t0 = Instant::now();
        eng.set_clock(t0);
        eng.handle(Event::FeedStatus { feed: Feed::Binance, connected: true, at: t0 });
        eng.handle(Event::FeedStatus { feed: Feed::GatePublic, connected: true, at: t0 });
        eng.handle(Event::GateBookSnapshot(BookSnapshot {
            id: 1,
            bids: vec![DepthLevel { price: 99980, size: 50 }, DepthLevel { price: 99970, size: 200 }],
            asks: vec![DepthLevel { price: 100020, size: 50 }, DepthLevel { price: 100030, size: 200 }],
        }));
        eng.handle(Event::GateBookDelta(BookDelta { first_id: 2, last_id: 2, bids: vec![], asks: vec![], full: false }));
        assert!(eng.book.is_synced());

        // Warm-up: β₀ needs ≥5 samples and a recompute (≥1 s apart).
        for i in 0..8 {
            step(&mut eng, t0 + Duration::from_millis(200 * i));
        }
        assert_eq!(eng.basis.status(), BasisStatus::Normal, "trust={:?}", eng.last_trust.reasons);
        assert!(eng.last_trust.reasons.is_empty(), "trust={:?}", eng.last_trust.reasons);
        // Six live post-only orders: F = 1000, δ = 2 bps → 999.80 / 1000.20 inner (Gate at 999.80/1000.20 → improve by a tick is not possible past the limit).
        assert_eq!(eng.orders.active_count(), 6);
        let inner_bid = eng.orders.active().filter(|o| o.side == Side::Buy && o.layer == 0).next().unwrap();
        assert_eq!(inner_bid.price, Some(99980));
        assert_eq!(inner_bid.purpose, Purpose::Open);
        assert_eq!(inner_bid.size, 25); // 0.05H = 250 USDT → 25 contracts
        assert!(eng.orders.active().all(|o| o.state == OrderState::Live));

        // A taker sell prints at our bid → 25 contracts long, sells flip to reduce-only.
        let t1 = t0 + Duration::from_millis(1700);
        eng.set_clock(t1);
        eng.handle(Event::GateTrade { price: 99980, price_f64: 999.8, signed_size: -25, trade_id: 1, at: t1 });
        assert_eq!(eng.inventory.pos.long, 25);
        step(&mut eng, t1 + Duration::from_millis(50));
        step(&mut eng, t1 + Duration::from_millis(100));
        let sells: Vec<_> = eng.orders.active().filter(|o| o.side == Side::Sell).collect();
        assert!(!sells.is_empty());
        assert!(sells.iter().all(|o| o.purpose == Purpose::Reduce && o.reduce_only), "{sells:?}");
        assert!(sells.iter().map(|o| o.remaining()).sum::<i64>() <= 25);
        // Buys are still risk-adding but smaller (u = 0.05 → 25 × 0.95 = 23).
        let buys: Vec<_> = eng.orders.active().filter(|o| o.side == Side::Buy).collect();
        assert!(buys.iter().all(|o| o.purpose == Purpose::Open));

        // More fills push us beyond the ±0.1H band (50 contracts): long 25 + 23 + 23 = 71.
        let t2 = t1 + Duration::from_millis(300);
        eng.set_clock(t2);
        eng.handle(Event::GateTrade { price: 99950, price_f64: 999.5, signed_size: -200, trade_id: 2, at: t2 });
        assert!(eng.inventory.pos.long > 50, "long={}", eng.inventory.pos.long);
        let long_before = eng.inventory.pos.long;
        step(&mut eng, t2 + Duration::from_millis(50));
        assert!(!eng.reduce.is_active());
        // 3 s outside the band → active reduce → IOC to the band edge.
        for i in 1..=8 {
            step(&mut eng, t2 + Duration::from_millis(50 + 450 * i));
        }
        assert!(eng.inventory.pos.long < long_before, "long={}", eng.inventory.pos.long);
        assert!(eng.inventory.pos.long <= 50, "long={}", eng.inventory.pos.long);
        assert!(eng.metrics.exit.ioc_orders >= 1);
        assert!(eng.metrics.fills.taker_contracts > 0);
        assert!(!eng.reduce.is_halted());
        // After completion the engine resumes quoting.
        for i in 1..=3 {
            step(&mut eng, t2 + Duration::from_millis(5000 + 200 * i));
        }
        assert!(!eng.reduce.is_active());
        assert!(eng.orders.active_count() > 0);
    }

    #[test]
    fn stale_binance_cancels_everything_and_shutdown_finishes() {
        let (mut eng, _rx) = test_engine();
        let t0 = Instant::now();
        eng.set_clock(t0);
        eng.handle(Event::FeedStatus { feed: Feed::Binance, connected: true, at: t0 });
        eng.handle(Event::FeedStatus { feed: Feed::GatePublic, connected: true, at: t0 });
        eng.handle(Event::GateBookSnapshot(BookSnapshot { id: 1, bids: vec![DepthLevel { price: 99980, size: 50 }], asks: vec![DepthLevel { price: 100020, size: 50 }] }));
        eng.handle(Event::GateBookDelta(BookDelta { first_id: 2, last_id: 2, bids: vec![], asks: vec![], full: false }));
        for i in 0..8 {
            step(&mut eng, t0 + Duration::from_millis(200 * i));
        }
        assert_eq!(eng.orders.active_count(), 6);
        // Binance goes silent for 6 s (stale threshold 5 s): every order is cancelled.
        let t1 = t0 + Duration::from_millis(1400 + 6000);
        eng.set_clock(t1);
        eng.handle(Event::Timer(t1));
        eng.handle(gate_bbo(99980, 100020, t1));
        assert!(eng.last_trust.reasons.iter().any(|r| r == "binance_stale"));
        assert_eq!(eng.orders.active_count(), 0);
        // Shutdown with nothing resting finishes on the next timer.
        eng.handle(Event::Shutdown);
        eng.handle(Event::Timer(t1 + Duration::from_millis(100)));
        assert!(eng.is_finished());
    }

    #[test]
    fn positions_from_dual_and_single() {
        let dual = vec![
            PositionInfo { mode: "dual_long".into(), size: 12, entry_price: 1500.0, update_ms: 0 },
            PositionInfo { mode: "dual_short".into(), size: -3, entry_price: 1502.0, update_ms: 0 },
        ];
        let p = positions_from_exchange(&dual, true);
        assert_eq!((p.long, p.short), (12, 3));
        let single = vec![PositionInfo { mode: "single".into(), size: -7, entry_price: 1500.0, update_ms: 0 }];
        let p = positions_from_exchange(&single, false);
        assert_eq!((p.long, p.short), (0, 7));
    }
}
