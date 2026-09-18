//! Events flowing from connectors into the single-threaded engine loop.

use std::time::Instant;

use crate::market::book::{BookDelta, BookSnapshot};
use crate::market::reference::Bbo;
use crate::order::model::{ExecResponse, OrderInfo, UserTrade};
use crate::types::Ticks;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Feed {
    Binance,
    GatePublic,
    GatePrivate,
    GateTrade,
}

impl Feed {
    pub fn label(self) -> &'static str {
        match self {
            Feed::Binance => "binance",
            Feed::GatePublic => "gate_public",
            Feed::GatePrivate => "gate_private",
            Feed::GateTrade => "gate_trade",
        }
    }
}

/// Position record from Gate (`futures.positions` or REST), normalised.
#[derive(Debug, Clone, PartialEq)]
pub struct PositionInfo {
    /// `single`, `dual_long`, `dual_short`.
    pub mode: String,
    /// Signed contracts.
    pub size: i64,
    pub entry_price: f64,
    pub update_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AccountInfo {
    pub user_id: i64,
    pub total: f64,
    pub available: f64,
    pub unrealised_pnl: f64,
    pub order_margin: f64,
    pub position_margin: f64,
    pub in_dual_mode: bool,
    pub at: Instant,
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // some payload fields are informational (logging / future signals)
pub enum Event {
    BinanceBbo(Bbo),
    BinanceTrade { price: f64, qty: f64, buyer_is_maker: bool, at: Instant },
    GateBbo { bid: Ticks, bid_size: i64, ask: Ticks, ask_size: i64, exch_ms: i64, at: Instant },
    GateBookSnapshot(BookSnapshot),
    GateBookDelta(BookDelta),
    /// Public trade. `signed_size` > 0 = taker buy.
    GateTrade { price: Ticks, price_f64: f64, signed_size: i64, trade_id: u64, at: Instant },
    /// Mark / index price and funding rate (`futures.tickers`), used for sanity checks only.
    GateTicker { mark: f64, index: f64, funding_rate: f64, at: Instant },
    GateOrder(OrderInfo),
    GateUserTrade(UserTrade),
    GatePositions(Vec<PositionInfo>),
    /// Balance change from `futures.balances` (`type`: fee, fund, pnl, …).
    GateBalanceChange { change: f64, kind: String, at: Instant },
    GateAccount(AccountInfo),
    /// Open-order snapshot from REST (startup / resync).
    GateOpenOrders(Vec<OrderInfo>),
    Exec(ExecResponse),
    FeedStatus { feed: Feed, connected: bool, at: Instant },
    /// Private-stream heartbeat (any message received).
    PrivateHeartbeat(Instant),
    Timer(Instant),
    Shutdown,
}
