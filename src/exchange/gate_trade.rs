//! Gate futures WebSocket trading API (`futures.login`, `futures.order_place`,
//! `futures.order_amend`, `futures.order_cancel`) with REST fallback.
//!
//! - `ack` frames are *not* treated as final results; only the frame carrying
//!   `data.result` / `data.errs` resolves a request.
//! - When the socket drops with requests outstanding, every outstanding request
//!   is resolved as `UNKNOWN` so the order manager queries instead of assuming.
//! - Queries and cancel-all always go through REST.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use super::gate_rest::GateRest;
use super::gate_types::GateOrder;
use super::ws;
use crate::engine::events::{Event, Feed};
use crate::order::model::{ExecCommand, ExecResponse, ExecResult};
use crate::types::{Side, TickGrid};
use crate::util::{Backoff, hmac_sha512_hex, unix_secs};

/// A command plus the side of the underlying order (needed to sign amend sizes).
#[derive(Debug, Clone)]
pub struct TradeCommand {
    pub cmd: ExecCommand,
    pub side: Side,
}

pub struct GateTradeWs {
    pub ws_url: String,
    pub contract: String,
    pub grid: TickGrid,
    pub key: String,
    pub secret: String,
    pub rest: Arc<GateRest>,
}

const LOGIN_REQ: &str = "login-1";
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

impl GateTradeWs {
    fn login_request(&self) -> Value {
        let t = unix_secs();
        let sign = hmac_sha512_hex(&self.secret, &format!("api\nfutures.login\n\n{t}"));
        json!({
            "time": t,
            "channel": "futures.login",
            "event": "api",
            "payload": {
                "api_key": self.key,
                "headers": {"X-Gate-Channel-Id": "stock_mm"},
                "signature": sign,
                "timestamp": t.to_string(),
                "req_id": LOGIN_REQ
            }
        })
    }

    fn api_request(&self, cmd: &ExecCommand, side: Side) -> Option<(String, Value)> {
        let (channel, param) = match cmd {
            ExecCommand::Place { client_id, side, size, price, tif, reduce_only, .. } => {
                let mut p = json!({
                    "contract": self.contract,
                    "size": size * side.sign(),
                    "price": price.map(|t| self.grid.to_string(t)).unwrap_or_else(|| "0".into()),
                    "tif": tif.as_gate(),
                    "text": client_id,
                });
                if *reduce_only {
                    p["reduce_only"] = json!(true);
                }
                ("futures.order_place", p)
            }
            ExecCommand::Amend { exchange_id, price, size, .. } => {
                let mut p = json!({"order_id": exchange_id});
                if let Some(t) = price {
                    p["price"] = json!(self.grid.to_string(*t));
                }
                if let Some(s) = size {
                    p["size"] = json!(s * side.sign());
                }
                ("futures.order_amend", p)
            }
            ExecCommand::Cancel { exchange_id, client_id, .. } => {
                let id = exchange_id.clone().unwrap_or_else(|| client_id.clone());
                ("futures.order_cancel", json!({"order_id": id}))
            }
            ExecCommand::Query { .. } | ExecCommand::CancelAll { .. } => return None,
        };
        let req_id = cmd.req_id().to_string();
        Some((
            req_id.clone(),
            json!({"time": unix_secs(), "channel": channel, "event": "api", "payload": {"req_id": req_id, "req_param": param}}),
        ))
    }

    fn spawn_rest(&self, tc: TradeCommand, tx: mpsc::Sender<Event>) {
        let rest = self.rest.clone();
        let contract = self.contract.clone();
        let grid = self.grid;
        tokio::spawn(async move {
            let result = rest.execute(&contract, &tc.cmd, Some(tc.side), &grid).await;
            let _ = tx.send(Event::Exec(ExecResponse { req_id: tc.cmd.req_id().to_string(), result, at: Instant::now() })).await;
        });
    }

    pub async fn run(self, mut cmd_rx: mpsc::Receiver<TradeCommand>, tx: mpsc::Sender<Event>) {
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
        loop {
            info!(url = %self.ws_url, "gate trade: connecting");
            let conn = ws::connect(&self.ws_url).await;
            let (mut sink, mut source) = match conn {
                Ok(c) => c,
                Err(e) => {
                    warn!(error = %e, "gate trade: connect failed; serving commands over REST meanwhile");
                    // Drain commands over REST until the backoff expires.
                    let until = tokio::time::Instant::now() + backoff.next();
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep_until(until) => break,
                            c = cmd_rx.recv() => match c {
                                Some(tc) => self.spawn_rest(tc, tx.clone()),
                                None => return,
                            }
                        }
                    }
                    continue;
                }
            };
            // Login.
            if ws::send_text(&mut sink, self.login_request().to_string()).await.is_err() {
                tokio::time::sleep(backoff.next()).await;
                continue;
            }
            let mut logged_in = false;
            let login_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while !logged_in {
                let msg = tokio::select! {
                    _ = tokio::time::sleep_until(login_deadline) => { warn!("gate trade: login timeout"); break; }
                    m = source.next() => m,
                    c = cmd_rx.recv() => {
                        // Not logged in yet: serve over REST.
                        match c { Some(tc) => { self.spawn_rest(tc, tx.clone()); continue; } None => return }
                    }
                };
                match msg {
                    Some(Ok(m)) => {
                        if let Some(text) = ws::message_text(&m) {
                            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                if v.get("request_id").and_then(|r| r.as_str()) == Some(LOGIN_REQ) {
                                    let status = header_status(&v);
                                    if status == 200 {
                                        logged_in = true;
                                        info!("gate trade: logged in");
                                    } else {
                                        warn!(%text, "gate trade: login rejected");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    _ => break,
                }
            }
            if !logged_in {
                tokio::time::sleep(backoff.next()).await;
                continue;
            }
            backoff.reset();
            let _ = tx.send(Event::FeedStatus { feed: Feed::GateTrade, connected: true, at: Instant::now() }).await;

            let mut pending: HashMap<String, (TradeCommand, Instant)> = HashMap::new();
            let mut ping = tokio::time::interval(Duration::from_secs(15));
            ping.tick().await;
            let mut sweep = tokio::time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    _ = ping.tick() => {
                        let req = json!({"time": unix_secs(), "channel": "futures.ping"});
                        if ws::send_text(&mut sink, req.to_string()).await.is_err() { break; }
                    }
                    _ = sweep.tick() => {
                        // Requests without a final response: resolve as UNKNOWN → manager queries.
                        let now = Instant::now();
                        let expired: Vec<String> = pending.iter().filter(|(_, (_, t))| now.duration_since(*t) > RESPONSE_TIMEOUT).map(|(k, _)| k.clone()).collect();
                        for k in expired {
                            if let Some((tc, _)) = pending.remove(&k) {
                                warn!(req = %k, "gate trade: no response; resolving as UNKNOWN");
                                let _ = tx.send(Event::Exec(ExecResponse { req_id: tc.cmd.req_id().to_string(), result: ExecResult::Error { label: "UNKNOWN".into(), message: "ws response timeout".into() }, at: now })).await;
                            }
                        }
                    }
                    c = cmd_rx.recv() => {
                        let Some(tc) = c else { return };
                        match self.api_request(&tc.cmd, tc.side) {
                            None => self.spawn_rest(tc, tx.clone()),
                            Some((req_id, payload)) => {
                                if ws::send_text(&mut sink, payload.to_string()).await.is_err() {
                                    warn!("gate trade: send failed; falling back to REST");
                                    self.spawn_rest(tc, tx.clone());
                                    break;
                                }
                                pending.insert(req_id, (tc, Instant::now()));
                            }
                        }
                    }
                    msg = source.next() => {
                        match msg {
                            Some(Ok(Message::Ping(p))) => {
                                use futures_util::SinkExt;
                                let _ = sink.send(Message::Pong(p)).await;
                            }
                            Some(Ok(Message::Close(_))) | None => { warn!("gate trade: closed"); break; }
                            Some(Ok(m)) => {
                                if let Some(text) = ws::message_text(&m) {
                                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                        if let Some((req_id, result)) = parse_api_response(&v, &self.grid) {
                                            if let Some((tc, _)) = pending.remove(&req_id) {
                                                let result = match (result, &tc.cmd) {
                                                    (ExecResult::Placed(i), ExecCommand::Amend { .. }) => ExecResult::Amended(i),
                                                    (ExecResult::Placed(i), ExecCommand::Cancel { .. }) => ExecResult::Cancelled(i),
                                                    (r, _) => r,
                                                };
                                                let _ = tx.send(Event::Exec(ExecResponse { req_id, result, at: Instant::now() })).await;
                                            }
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => { warn!(error = %e, "gate trade: ws error"); break; }
                        }
                    }
                }
            }
            let _ = tx.send(Event::FeedStatus { feed: Feed::GateTrade, connected: false, at: Instant::now() }).await;
            // Outstanding requests have unknown outcomes.
            for (req_id, (_, _)) in pending.drain() {
                let _ = tx
                    .send(Event::Exec(ExecResponse { req_id, result: ExecResult::Error { label: "UNKNOWN".into(), message: "ws disconnected".into() }, at: Instant::now() }))
                    .await;
            }
            tokio::time::sleep(backoff.next()).await;
        }
    }
}

fn header_status(v: &Value) -> u16 {
    v.get("header")
        .and_then(|h| h.get("status"))
        .and_then(|s| match s {
            Value::String(s) => s.parse().ok(),
            Value::Number(n) => n.as_u64().map(|x| x as u16),
            _ => None,
        })
        .unwrap_or(0)
}

/// Parse a WS API response. Returns `None` for acks and unrelated frames.
pub fn parse_api_response(v: &Value, grid: &TickGrid) -> Option<(String, ExecResult)> {
    let req_id = v.get("request_id").and_then(|r| r.as_str())?.to_string();
    if v.get("ack").and_then(|a| a.as_bool()).unwrap_or(false) {
        return None; // request acknowledged, not final
    }
    let data = v.get("data")?;
    if let Some(errs) = data.get("errs").filter(|e| !e.is_null()) {
        let label = errs.get("label").and_then(|l| l.as_str()).unwrap_or("ERROR").to_string();
        let message = errs.get("message").and_then(|l| l.as_str()).unwrap_or("").to_string();
        return Some((req_id, ExecResult::Error { label, message }));
    }
    let status = header_status(v);
    let result = data.get("result")?;
    if status != 200 && status != 0 {
        let label = result.get("label").and_then(|l| l.as_str()).unwrap_or("ERROR").to_string();
        let message = result.get("message").and_then(|l| l.as_str()).unwrap_or("").to_string();
        return Some((req_id, ExecResult::Error { label, message }));
    }
    let order: GateOrder = serde_json::from_value(result.clone()).ok()?;
    // The caller re-labels according to the command type.
    Some((req_id, ExecResult::Placed(order.to_info(grid))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn ack_is_not_final_and_errors_are_labelled() {
        let g = TickGrid::new(Decimal::from_str("0.01").unwrap());
        let ack: Value = serde_json::from_str(r#"{"request_id":"p-1","ack":true,"header":{"status":"200","channel":"futures.order_place","event":"api"}}"#).unwrap();
        assert!(parse_api_response(&ack, &g).is_none());
        let err: Value = serde_json::from_str(r#"{"request_id":"p-1","header":{"status":"400","channel":"futures.order_place","event":"api"},"data":{"errs":{"label":"POC_FILL_IMMEDIATELY","message":"..."}}}"#).unwrap();
        match parse_api_response(&err, &g) {
            Some((id, ExecResult::Error { label, .. })) => {
                assert_eq!(id, "p-1");
                assert_eq!(label, "POC_FILL_IMMEDIATELY");
            }
            other => panic!("{other:?}"),
        }
        let ok: Value = serde_json::from_str(r#"{"request_id":"p-2","header":{"status":"200","channel":"futures.order_place","event":"api"},"data":{"result":{"id":123,"text":"t-mm1","size":5,"left":5,"price":"1500.10","status":"open","tif":"poc"}}}"#).unwrap();
        match parse_api_response(&ok, &g) {
            Some((_, ExecResult::Placed(i))) => {
                assert_eq!(i.exchange_id, "123");
                assert_eq!(i.price, 150010);
            }
            other => panic!("{other:?}"),
        }
    }
}
