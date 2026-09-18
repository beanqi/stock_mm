//! Order-side data types shared by the engine, the order manager and the
//! exchange executors (live Gate WS API or the paper simulator).

use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::types::{Purpose, Side, Ticks};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tif {
    /// Post-only (Gate `poc`).
    Poc,
    Ioc,
    Gtc,
}

impl Tif {
    pub fn as_gate(self) -> &'static str {
        match self {
            Tif::Poc => "poc",
            Tif::Ioc => "ioc",
            Tif::Gtc => "gtc",
        }
    }
}

/// Commands sent from the engine to an executor.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecCommand {
    Place {
        req_id: String,
        client_id: String,
        side: Side,
        /// Absolute contracts.
        size: i64,
        /// `None` = market order (price "0").
        price: Option<Ticks>,
        tif: Tif,
        reduce_only: bool,
    },
    Amend {
        req_id: String,
        exchange_id: String,
        client_id: String,
        price: Option<Ticks>,
        /// New **total** absolute size (including the filled part), per Gate semantics.
        size: Option<i64>,
    },
    Cancel {
        req_id: String,
        exchange_id: Option<String>,
        client_id: String,
    },
    Query {
        req_id: String,
        exchange_id: Option<String>,
        client_id: String,
    },
    /// Cancel every open order on the contract (REST).
    CancelAll { req_id: String },
}

impl ExecCommand {
    pub fn req_id(&self) -> &str {
        match self {
            ExecCommand::Place { req_id, .. }
            | ExecCommand::Amend { req_id, .. }
            | ExecCommand::Cancel { req_id, .. }
            | ExecCommand::Query { req_id, .. }
            | ExecCommand::CancelAll { req_id } => req_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ExchangeStatus {
    Open,
    Finished,
}

/// Normalised exchange order record.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderInfo {
    pub exchange_id: String,
    pub client_id: String,
    pub side: Side,
    pub price: Ticks,
    /// Absolute total size.
    pub size: i64,
    /// Absolute remaining size.
    pub left: i64,
    /// Average fill price (0 when none).
    pub fill_price: f64,
    pub status: ExchangeStatus,
    /// Gate `finish_as` (`filled`, `cancelled`, `ioc`, `poc`, `reduce_only`, …).
    pub finish_as: String,
    pub reduce_only: bool,
    pub tif: Option<Tif>,
    pub update_ms: i64,
}

impl OrderInfo {
    pub fn filled(&self) -> i64 {
        (self.size - self.left).max(0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecResult {
    Placed(OrderInfo),
    Amended(OrderInfo),
    Cancelled(OrderInfo),
    Queried(OrderInfo),
    CancelledAll(usize),
    /// Exchange rejected the request. `label` is the Gate error label
    /// (`ORDER_NOT_FOUND`, `ORDER_FINISHED`, `POC_FILL_IMMEDIATELY`, …).
    Error { label: String, message: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecResponse {
    pub req_id: String,
    pub result: ExecResult,
    pub at: Instant,
}

/// A fill from the private user-trades stream.
#[derive(Debug, Clone, PartialEq)]
pub struct UserTrade {
    pub trade_id: String,
    pub exchange_order_id: String,
    pub client_id: String,
    /// Signed contracts (+ buy, − sell).
    pub signed_size: i64,
    pub price: Ticks,
    pub price_f64: f64,
    /// Fee in settle currency (positive = paid, negative = rebate).
    pub fee: f64,
    pub is_maker: bool,
    pub exch_ms: i64,
    pub at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DoneReason {
    Filled,
    Cancelled,
    /// Post-only would have crossed; exchange rejected.
    PostOnlyReject,
    /// IOC finished (possibly partially filled).
    IocDone,
    Rejected,
    /// Exchange says it does not exist / already finished; local state unknown.
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OrderState {
    PendingNew,
    Live,
    PendingAmend,
    PendingCancel,
    Done(DoneReason),
}

impl OrderState {
    pub fn is_terminal(self) -> bool {
        matches!(self, OrderState::Done(_))
    }
    pub fn is_inflight(self) -> bool {
        matches!(self, OrderState::PendingNew | OrderState::PendingAmend | OrderState::PendingCancel)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intent {
    Amend { price: Ticks, size: i64 },
    Cancel,
}

#[derive(Debug, Clone)]
pub struct LocalOrder {
    pub id: u64,
    pub client_id: String,
    pub exchange_id: Option<String>,
    pub side: Side,
    pub purpose: Purpose,
    /// Quote layer (0 = inner). `u8::MAX` for exits / untracked.
    pub layer: u8,
    pub tif: Tif,
    pub reduce_only: bool,
    /// Requested/confirmed price (ticks). `None` for market orders.
    pub price: Option<Ticks>,
    /// Total absolute size (requested or confirmed).
    pub size: i64,
    /// Filled absolute size: max(cumulative reported by order updates, Σ de-duplicated trades).
    pub filled: i64,
    /// Σ of de-duplicated user trades attributed to this order.
    pub trade_filled: i64,
    pub state: OrderState,
    pub intent: Option<Intent>,
    /// The pending amend target, so a successful ack can be applied.
    pub amend_target: Option<(Ticks, i64)>,
    pub inflight_since: Option<Instant>,
    pub req_id: Option<String>,
    pub last_update: Instant,
    /// Set when the exchange said it was finished but we may still receive fills.
    pub done_at: Option<Instant>,
}

impl LocalOrder {
    pub fn remaining(&self) -> i64 {
        (self.size - self.filled).max(0)
    }
}

/// Fill event produced by the manager after de-duplication.
#[derive(Debug, Clone, PartialEq)]
pub struct Fill {
    pub local_id: Option<u64>,
    pub client_id: String,
    pub side: Side,
    pub purpose: Purpose,
    pub layer: u8,
    pub reduce_only: bool,
    pub size: i64,
    pub price: Ticks,
    pub price_f64: f64,
    pub fee: f64,
    pub is_maker: bool,
    pub tif: Tif,
    pub at: Instant,
}
