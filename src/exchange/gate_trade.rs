//! Gate futures WebSocket trading API (`futures.login`, `futures.order_place`,
//! `futures.order_amend`, `futures.order_cancel`) with REST fallback.
//!
//! - `ack` frames are *not* treated as final results; only the frame carrying
//!   `data.result` / `data.errs` resolves a request.
//! - When the socket drops with requests outstanding, every outstanding request
//!   is resolved as `UNKNOWN` so the order manager queries instead of assuming.
//! - Queries and cancel-all always go through REST.
//! - Place/amend/cancel are load-balanced across several WS sessions, each
//!   pinned to a distinct resolved IP of `gate_ws`, so Gate's per-server
//!   rate limits are not concentrated on a single backend.

use std::collections::HashMap;
use std::net::IpAddr;
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

#[derive(Clone)]
pub struct GateTradeWs {
    pub ws_url: String,
    pub contract: String,
    pub grid: TickGrid,
    pub key: String,
    pub secret: String,
    pub rest: Arc<GateRest>,
    /// Distinct Gate WS backends to pin. `1` keeps a single hostname connection.
    pub pool_size: usize,
}

const LOGIN_REQ: &str = "login-1";
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const RESOLVE_LOOKUPS: usize = 8;

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
            ExecCommand::Place {
                client_id,
                side,
                size,
                price,
                tif,
                reduce_only,
                ..
            } => {
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
            ExecCommand::Amend {
                exchange_id,
                price,
                size,
                ..
            } => {
                let mut p = json!({"order_id": exchange_id});
                if let Some(t) = price {
                    p["price"] = json!(self.grid.to_string(*t));
                }
                if let Some(s) = size {
                    p["size"] = json!(s * side.sign());
                }
                ("futures.order_amend", p)
            }
            ExecCommand::Cancel {
                exchange_id,
                client_id,
                ..
            } => {
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
            let _ = tx
                .send(Event::Exec(ExecResponse {
                    req_id: tc.cmd.req_id().to_string(),
                    result,
                    at: Instant::now(),
                }))
                .await;
        });
    }

    pub async fn run(self, cmd_rx: mpsc::Receiver<TradeCommand>, tx: mpsc::Sender<Event>) {
        if self.pool_size <= 1 {
            self.run_session(None, 0, cmd_rx, tx, None).await;
            return;
        }
        let ips = match ws::parse_ws_authority(&self.ws_url) {
            Ok((host, port, true)) => {
                let ips =
                    ws::resolve_unique_ips(&host, port, self.pool_size, RESOLVE_LOOKUPS).await;
                info!(host = %host, want = self.pool_size, n = ips.len(), ?ips, "gate trade: resolved WS backends");
                ips
            }
            Ok((host, _, false)) => {
                warn!(host = %host, "gate trade: non-TLS ws url cannot pin IPs");
                Vec::new()
            }
            Err(e) => {
                warn!(error = %e, "gate trade: bad ws url; using hostname connection");
                Vec::new()
            }
        };
        if ips.is_empty() {
            warn!("gate trade: no distinct IPs; falling back to a single hostname connection");
            self.run_session(None, 0, cmd_rx, tx, None).await;
            return;
        }
        if ips.len() < self.pool_size {
            warn!(
                got = ips.len(),
                want = self.pool_size,
                "gate trade: fewer backends than pool_size"
            );
        }
        self.run_pool(ips, cmd_rx, tx).await;
    }

    async fn run_pool(
        self,
        ips: Vec<IpAddr>,
        mut cmd_rx: mpsc::Receiver<TradeCommand>,
        tx: mpsc::Sender<Event>,
    ) {
        let n = ips.len();
        let (status_tx, mut status_rx) = mpsc::channel::<(usize, bool)>(n * 4);
        let mut senders = Vec::with_capacity(n);
        for (slot, ip) in ips.into_iter().enumerate() {
            let (wtx, wrx) = mpsc::channel(512);
            senders.push(wtx);
            let this = self.clone();
            let ev = tx.clone();
            let st = status_tx.clone();
            tokio::spawn(async move {
                this.run_session(Some(ip), slot, wrx, ev, Some(st)).await;
            });
        }
        drop(status_tx);

        let mut live = vec![false; n];
        let mut live_n = 0usize;
        let mut any_live = false;
        let mut next = 0usize;
        let mut status_open = true;
        loop {
            tokio::select! {
                c = cmd_rx.recv() => {
                    let Some(tc) = c else { return };
                    dispatch_cmd(&self, &senders, &live, &mut next, tc, &tx);
                }
                s = status_rx.recv(), if status_open => {
                    let Some((slot, connected)) = s else {
                        status_open = false;
                        continue;
                    };
                    if slot >= n || live[slot] == connected {
                        continue;
                    }
                    live[slot] = connected;
                    if connected {
                        live_n += 1;
                    } else {
                        live_n = live_n.saturating_sub(1);
                    }
                    let now_any = live_n > 0;
                    if now_any != any_live {
                        any_live = now_any;
                        let _ = tx.send(Event::FeedStatus { feed: Feed::GateTrade, connected: any_live, at: Instant::now() }).await;
                    }
                    info!(slot, connected, live = live_n, n, "gate trade: pool member");
                }
            }
        }
    }

    async fn run_session(
        &self,
        pin: Option<IpAddr>,
        slot: usize,
        mut cmd_rx: mpsc::Receiver<TradeCommand>,
        tx: mpsc::Sender<Event>,
        status_tx: Option<mpsc::Sender<(usize, bool)>>,
    ) {
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
        let publish_status = |connected: bool,
                              status_tx: &Option<mpsc::Sender<(usize, bool)>>,
                              tx: &mpsc::Sender<Event>| {
            if let Some(st) = status_tx {
                let _ = st.try_send((slot, connected));
            } else {
                let _ = tx.try_send(Event::FeedStatus {
                    feed: Feed::GateTrade,
                    connected,
                    at: Instant::now(),
                });
            }
        };
        loop {
            info!(slot, ip = ?pin, url = %self.ws_url, "gate trade: connecting");
            let conn = match pin {
                Some(ip) => ws::connect_via_ip(&self.ws_url, ip).await,
                None => ws::connect(&self.ws_url).await,
            };
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
            if ws::send_text(&mut sink, self.login_request().to_string())
                .await
                .is_err()
            {
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
                                        info!(slot, ip = ?pin, "gate trade: logged in");
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
            publish_status(true, &status_tx, &tx);

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
            publish_status(false, &status_tx, &tx);
            // Outstanding requests have unknown outcomes.
            for (req_id, (_, _)) in pending.drain() {
                let _ = tx
                    .send(Event::Exec(ExecResponse {
                        req_id,
                        result: ExecResult::Error {
                            label: "UNKNOWN".into(),
                            message: "ws disconnected".into(),
                        },
                        at: Instant::now(),
                    }))
                    .await;
            }
            tokio::time::sleep(backoff.next()).await;
        }
    }
}

fn dispatch_cmd(
    trade: &GateTradeWs,
    senders: &[mpsc::Sender<TradeCommand>],
    live: &[bool],
    next: &mut usize,
    tc: TradeCommand,
    tx: &mpsc::Sender<Event>,
) {
    let n = senders.len();
    if n == 0 {
        trade.spawn_rest(tc, tx.clone());
        return;
    }
    for k in 0..n {
        let i = (*next + k) % n;
        if !live[i] {
            continue;
        }
        match senders[i].try_send(tc.clone()) {
            Ok(()) => {
                *next = (i + 1) % n;
                return;
            }
            Err(_) => continue,
        }
    }
    *next = (*next + 1) % n;
    trade.spawn_rest(tc, tx.clone());
}

/// First live slot at or after `start`, wrapping around. `None` if every slot is down.
#[cfg(test)]
fn next_live_slot(live: &[bool], start: usize) -> Option<usize> {
    let n = live.len();
    if n == 0 {
        return None;
    }
    (0..n).map(|k| (start + k) % n).find(|&i| live[i])
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
        let label = errs
            .get("label")
            .and_then(|l| l.as_str())
            .unwrap_or("ERROR")
            .to_string();
        let message = errs
            .get("message")
            .and_then(|l| l.as_str())
            .unwrap_or("")
            .to_string();
        return Some((req_id, ExecResult::Error { label, message }));
    }
    let status = header_status(v);
    let result = data.get("result")?;
    if status != 200 && status != 0 {
        let label = result
            .get("label")
            .and_then(|l| l.as_str())
            .unwrap_or("ERROR")
            .to_string();
        let message = result
            .get("message")
            .and_then(|l| l.as_str())
            .unwrap_or("")
            .to_string();
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

    #[test]
    fn round_robin_skips_dead_slots() {
        assert_eq!(next_live_slot(&[true, true, true], 0), Some(0));
        assert_eq!(next_live_slot(&[false, true, false], 0), Some(1));
        assert_eq!(next_live_slot(&[false, false, true], 0), Some(2));
        assert_eq!(next_live_slot(&[true, false, true], 1), Some(2));
        assert_eq!(next_live_slot(&[true, false, false], 1), Some(0));
        assert_eq!(next_live_slot(&[false, false, false], 0), None);
        assert_eq!(next_live_slot(&[], 0), None);
    }
}
