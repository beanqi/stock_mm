//! Gate futures REST v4 client (signed) – metadata, account/position snapshots,
//! order-book snapshot, order queries and REST fallbacks for order actions.

use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use reqwest::Method;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tracing::debug;

use super::gate_types::*;
use crate::engine::events::{AccountInfo, PositionInfo};
use crate::market::book::{BookSnapshot, DepthLevel};
use crate::order::model::{ExecCommand, ExecResult, OrderInfo, Tif};
use crate::types::{ContractMeta, Side, TickGrid};
use crate::util::{hmac_sha512_hex, sha512_hex, unix_secs};

#[derive(Clone)]
pub struct GateRest {
    base: String,
    settle: String,
    key: String,
    secret: String,
    client: reqwest::Client,
}

/// Structured error carrying Gate's `label`.
#[derive(Debug, thiserror::Error)]
#[error("gate {status}: {label}: {message}")]
pub struct GateError {
    pub status: u16,
    pub label: String,
    pub message: String,
}

impl GateRest {
    pub fn new(base: &str, settle: &str, key: &str, secret: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            settle: settle.to_string(),
            key: key.to_string(),
            secret: secret.to_string(),
            client: reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("reqwest client"),
        }
    }

    pub fn has_credentials(&self) -> bool {
        !self.key.is_empty() && !self.secret.is_empty()
    }

    fn path(&self, suffix: &str) -> String {
        format!("/api/v4/futures/{}{}", self.settle, suffix)
    }

    async fn request(&self, method: Method, path: &str, query: &str, body: Option<&Value>, signed: bool) -> Result<Value> {
        let url = if query.is_empty() { format!("{}{}", self.base, path) } else { format!("{}{}?{}", self.base, path, query) };
        let body_str = body.map(|b| b.to_string()).unwrap_or_default();
        let mut req = self.client.request(method.clone(), &url).header("Accept", "application/json").header("Content-Type", "application/json");
        if signed {
            if !self.has_credentials() {
                bail!("gate credentials missing");
            }
            let ts = unix_secs().to_string();
            let payload = format!("{}\n{}\n{}\n{}\n{}", method.as_str(), path, query, sha512_hex(&body_str), ts);
            let sign = hmac_sha512_hex(&self.secret, &payload);
            req = req.header("KEY", &self.key).header("Timestamp", ts).header("SIGN", sign);
        }
        if body.is_some() {
            req = req.body(body_str);
        }
        let resp = req.send().await.with_context(|| format!("gate {method} {path}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            let label = v.get("label").and_then(|l| l.as_str()).unwrap_or("HTTP_ERROR").to_string();
            let message = v.get("message").and_then(|l| l.as_str()).unwrap_or(&text).to_string();
            return Err(GateError { status: status.as_u16(), label, message }.into());
        }
        serde_json::from_str(&text).with_context(|| format!("parse gate response for {path}: {text}"))
    }

    // -------------------------------------------------------------- metadata

    pub async fn contract(&self, contract: &str) -> Result<ContractMeta> {
        let v = self.request(Method::GET, &self.path(&format!("/contracts/{contract}")), "", None, false).await?;
        let c: GateContract = serde_json::from_value(v).context("parse contract")?;
        let tick = Decimal::from_str(&c.order_price_round).context("order_price_round")?;
        let mult = Decimal::from_str(&c.quanto_multiplier).context("quanto_multiplier")?;
        if tick <= Decimal::ZERO || mult <= Decimal::ZERO {
            bail!("invalid contract metadata tick={tick} mult={mult}");
        }
        Ok(ContractMeta {
            name: c.name,
            tick,
            multiplier: mult,
            order_size_min: c.order_size_min.max(1),
            order_size_max: if c.order_size_max > 0 { c.order_size_max } else { i64::MAX / 4 },
            orders_limit: c.orders_limit,
            price_deviate: c.order_price_deviate,
            funding_interval_secs: c.funding_interval,
            funding_next_apply: c.funding_next_apply,
            maker_fee: c.maker_fee_rate,
            taker_fee: c.taker_fee_rate,
            in_delisting: c.in_delisting || c.status != "trading",
        })
    }

    /// Account-level fee rates for the contract (falls back to contract defaults).
    pub async fn fee(&self, contract: &str) -> Result<(f64, f64)> {
        let v = self.request(Method::GET, &self.path("/fee"), &format!("contract={contract}"), None, true).await?;
        let f: GateFee = match v.get(contract) {
            Some(x) => serde_json::from_value(x.clone()).context("parse fee")?,
            None => serde_json::from_value(v).context("parse fee")?,
        };
        Ok((f.maker_fee, f.taker_fee))
    }

    pub async fn account(&self) -> Result<AccountInfo> {
        let v = self.request(Method::GET, &self.path("/accounts"), "", None, true).await?;
        let a: GateAccount = serde_json::from_value(v).context("parse account")?;
        Ok(AccountInfo {
            user_id: a.user,
            total: a.total,
            available: a.available,
            unrealised_pnl: a.unrealised_pnl,
            order_margin: a.order_margin,
            position_margin: a.position_margin,
            in_dual_mode: a.in_dual_mode,
            at: Instant::now(),
        })
    }

    pub async fn positions(&self, contract: &str, dual: bool) -> Result<Vec<PositionInfo>> {
        let path = if dual { self.path(&format!("/dual_comp/positions/{contract}")) } else { self.path(&format!("/positions/{contract}")) };
        let v = self.request(Method::GET, &path, "", None, true).await?;
        let list: Vec<GatePosition> = match v {
            Value::Array(_) => serde_json::from_value(v).context("parse positions")?,
            other => vec![serde_json::from_value(other).context("parse position")?],
        };
        Ok(list.iter().map(|p| p.to_info()).collect())
    }

    pub async fn order_book(&self, contract: &str, limit: u32, grid: &TickGrid) -> Result<BookSnapshot> {
        let q = format!("contract={contract}&limit={limit}&with_id=true");
        let v = self.request(Method::GET, &self.path("/order_book"), &q, None, false).await?;
        let b: GateBookRest = serde_json::from_value(v).context("parse order_book")?;
        Ok(BookSnapshot {
            id: b.id.max(0) as u64,
            bids: b.bids.iter().map(|l| DepthLevel { price: grid.round(l.p), size: l.s }).collect(),
            asks: b.asks.iter().map(|l| DepthLevel { price: grid.round(l.p), size: l.s }).collect(),
        })
    }

    // ---------------------------------------------------------------- orders

    pub async fn open_orders(&self, contract: &str, grid: &TickGrid) -> Result<Vec<OrderInfo>> {
        let q = format!("contract={contract}&status=open&limit=100");
        let v = self.request(Method::GET, &self.path("/orders"), &q, None, true).await?;
        let list: Vec<GateOrder> = serde_json::from_value(v).context("parse orders")?;
        Ok(list.iter().map(|o| o.to_info(grid)).collect())
    }

    pub async fn get_order(&self, id_or_text: &str, grid: &TickGrid) -> Result<OrderInfo> {
        let v = self.request(Method::GET, &self.path(&format!("/orders/{id_or_text}")), "", None, true).await?;
        let o: GateOrder = serde_json::from_value(v).context("parse order")?;
        Ok(o.to_info(grid))
    }

    pub async fn cancel_all(&self, contract: &str) -> Result<usize> {
        let v = self.request(Method::DELETE, &self.path("/orders"), &format!("contract={contract}"), None, true).await?;
        Ok(v.as_array().map(|a| a.len()).unwrap_or(0))
    }

    pub async fn place(&self, contract: &str, side: Side, size: i64, price: Option<&str>, tif: Tif, reduce_only: bool, text: &str, grid: &TickGrid) -> Result<OrderInfo> {
        let signed = size * side.sign();
        let mut body = json!({
            "contract": contract,
            "size": signed,
            "price": price.unwrap_or("0"),
            "tif": tif.as_gate(),
            "text": text,
        });
        if reduce_only {
            body["reduce_only"] = json!(true);
        }
        let v = self.request(Method::POST, &self.path("/orders"), "", Some(&body), true).await?;
        let o: GateOrder = serde_json::from_value(v).context("parse placed order")?;
        Ok(o.to_info(grid))
    }

    pub async fn amend(&self, order_id: &str, price: Option<&str>, size_signed: Option<i64>, grid: &TickGrid) -> Result<OrderInfo> {
        let mut body = json!({});
        if let Some(p) = price {
            body["price"] = json!(p);
        }
        if let Some(s) = size_signed {
            body["size"] = json!(s);
        }
        let v = self.request(Method::PUT, &self.path(&format!("/orders/{order_id}")), "", Some(&body), true).await?;
        let o: GateOrder = serde_json::from_value(v).context("parse amended order")?;
        Ok(o.to_info(grid))
    }

    pub async fn cancel(&self, id_or_text: &str, grid: &TickGrid) -> Result<OrderInfo> {
        let v = self.request(Method::DELETE, &self.path(&format!("/orders/{id_or_text}")), "", None, true).await?;
        let o: GateOrder = serde_json::from_value(v).context("parse cancelled order")?;
        Ok(o.to_info(grid))
    }

    /// Execute an engine command over REST (fallback path). Side signs are
    /// derived from the local order for amends.
    pub async fn execute(&self, contract: &str, cmd: &ExecCommand, side_for_amend: Option<Side>, grid: &TickGrid) -> ExecResult {
        let res: Result<ExecResult> = async {
            match cmd {
                ExecCommand::Place { client_id, side, size, price, tif, reduce_only, .. } => {
                    let p = price.map(|t| grid.to_string(t));
                    let info = self.place(contract, *side, *size, p.as_deref(), *tif, *reduce_only, client_id, grid).await?;
                    Ok(ExecResult::Placed(info))
                }
                ExecCommand::Amend { exchange_id, price, size, .. } => {
                    let p = price.map(|t| grid.to_string(t));
                    let sign = side_for_amend.map(|s| s.sign()).unwrap_or(1);
                    let info = self.amend(exchange_id, p.as_deref(), size.map(|s| s * sign), grid).await?;
                    Ok(ExecResult::Amended(info))
                }
                ExecCommand::Cancel { exchange_id, client_id, .. } => {
                    let id = exchange_id.clone().unwrap_or_else(|| client_id.clone());
                    let info = self.cancel(&id, grid).await?;
                    Ok(ExecResult::Cancelled(info))
                }
                ExecCommand::Query { exchange_id, client_id, .. } => {
                    let id = exchange_id.clone().unwrap_or_else(|| client_id.clone());
                    let info = self.get_order(&id, grid).await?;
                    Ok(ExecResult::Queried(info))
                }
                ExecCommand::CancelAll { .. } => Ok(ExecResult::CancelledAll(self.cancel_all(contract).await?)),
            }
        }
        .await;
        match res {
            Ok(r) => r,
            Err(e) => {
                debug!(error = %e, "gate rest exec error");
                match e.downcast_ref::<GateError>() {
                    Some(g) => ExecResult::Error { label: g.label.clone(), message: g.message.clone() },
                    None => {
                        let msg = e.to_string();
                        let label = if msg.contains("timed out") || msg.contains("timeout") { "TIMEOUT" } else { "UNKNOWN" };
                        ExecResult::Error { label: label.into(), message: msg }
                    }
                }
            }
        }
    }
}

