//! Gate futures private WebSocket streams: orders, user trades, positions and
//! balance changes. Each subscription carries its own HMAC signature.

use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use super::gate_types::{GateOrder, GatePosition, GateUserTrade};
use super::ws;
use crate::engine::events::{Event, Feed};
use crate::types::TickGrid;
use crate::util::{Backoff, hmac_sha512_hex, unix_secs};

pub struct GatePrivate {
    pub ws_url: String,
    pub contract: String,
    pub grid: TickGrid,
    pub key: String,
    pub secret: String,
    pub user_id: i64,
}

impl GatePrivate {
    fn sub_request(&self, channel: &str, payload: Value) -> Value {
        let t = unix_secs();
        let msg = format!("channel={channel}&event=subscribe&time={t}");
        json!({
            "time": t,
            "channel": channel,
            "event": "subscribe",
            "payload": payload,
            "auth": {"method": "api_key", "KEY": self.key, "SIGN": hmac_sha512_hex(&self.secret, &msg)}
        })
    }

    pub async fn run(self, tx: mpsc::Sender<Event>) {
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
        loop {
            info!(url = %self.ws_url, "gate private: connecting");
            match ws::connect(&self.ws_url).await {
                Ok((mut sink, mut source)) => {
                    backoff.reset();
                    let uid = self.user_id.to_string();
                    let channels = ["futures.orders", "futures.usertrades", "futures.positions", "futures.balances"];
                    let mut ok = true;
                    for ch in channels {
                        let payload = if ch == "futures.balances" { json!([uid]) } else { json!([uid, self.contract]) };
                        let req = self.sub_request(ch, payload);
                        if let Err(e) = ws::send_text(&mut sink, req.to_string()).await {
                            warn!(error = %e, "gate private: subscribe failed");
                            ok = false;
                            break;
                        }
                    }
                    if ok {
                        let _ = tx.send(Event::FeedStatus { feed: Feed::GatePrivate, connected: true, at: Instant::now() }).await;
                        let mut ping = tokio::time::interval(Duration::from_secs(10));
                        ping.tick().await;
                        let idle = Duration::from_secs(40);
                        loop {
                            tokio::select! {
                                _ = ping.tick() => {
                                    let req = json!({"time": unix_secs(), "channel": "futures.ping"});
                                    if ws::send_text(&mut sink, req.to_string()).await.is_err() { break; }
                                }
                                msg = tokio::time::timeout(idle, source.next()) => {
                                    match msg {
                                        Ok(Some(Ok(Message::Ping(p)))) => {
                                            use futures_util::SinkExt;
                                            let _ = sink.send(Message::Pong(p)).await;
                                            let _ = tx.send(Event::PrivateHeartbeat(Instant::now())).await;
                                        }
                                        Ok(Some(Ok(Message::Close(_)))) | Ok(None) => { warn!("gate private: closed"); break; }
                                        Ok(Some(Ok(m))) => {
                                            let now = Instant::now();
                                            let _ = tx.send(Event::PrivateHeartbeat(now)).await;
                                            if let Some(text) = ws::message_text(&m) {
                                                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                                    for ev in parse_private(&v, &self.grid, now) {
                                                        if tx.send(ev).await.is_err() { return; }
                                                    }
                                                }
                                            }
                                        }
                                        Ok(Some(Err(e))) => { warn!(error = %e, "gate private: ws error"); break; }
                                        Err(_) => { warn!("gate private: idle timeout"); break; }
                                    }
                                }
                            }
                        }
                    }
                    let _ = tx.send(Event::FeedStatus { feed: Feed::GatePrivate, connected: false, at: Instant::now() }).await;
                }
                Err(e) => warn!(error = %e, "gate private: connect failed"),
            }
            tokio::time::sleep(backoff.next()).await;
        }
    }
}

pub fn parse_private(v: &Value, grid: &TickGrid, now: Instant) -> Vec<Event> {
    let channel = v.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    let event = v.get("event").and_then(|c| c.as_str()).unwrap_or("");
    if event == "subscribe" {
        if let Some(err) = v.get("error") {
            if !err.is_null() {
                warn!(%channel, %err, "gate private: subscribe error");
            }
        } else {
            info!(%channel, "gate private: subscribed");
        }
        return vec![];
    }
    if event != "update" {
        return vec![];
    }
    let Some(result) = v.get("result") else { return vec![] };
    let items: Vec<Value> = match result {
        Value::Array(a) => a.clone(),
        other => vec![other.clone()],
    };
    match channel {
        "futures.orders" => items
            .into_iter()
            .filter_map(|o| serde_json::from_value::<GateOrder>(o).ok())
            .map(|o| Event::GateOrder(o.to_info(grid)))
            .collect(),
        "futures.usertrades" => items
            .into_iter()
            .filter_map(|t| serde_json::from_value::<GateUserTrade>(t).ok())
            .map(|t| Event::GateUserTrade(t.to_user_trade(grid, now)))
            .collect(),
        "futures.positions" => {
            let list: Vec<_> = items
                .into_iter()
                .filter_map(|p| serde_json::from_value::<GatePosition>(p).ok())
                .map(|p| p.to_info())
                .collect();
            if list.is_empty() { vec![] } else { vec![Event::GatePositions(list)] }
        }
        "futures.balances" => items
            .into_iter()
            .filter_map(|b| {
                let change = b.get("change").and_then(|c| match c {
                    Value::Number(n) => n.as_f64(),
                    Value::String(s) => s.parse().ok(),
                    _ => None,
                })?;
                let kind = b.get("type").and_then(|t| t.as_str()).unwrap_or("").to_string();
                Some(Event::GateBalanceChange { change, kind, at: now })
            })
            .collect(),
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn parses_usertrade_and_position() {
        let g = TickGrid::new(Decimal::from_str("0.01").unwrap());
        let v: Value = serde_json::from_str(r#"{"channel":"futures.usertrades","event":"update","result":[{"id":"3335259","create_time":1,"create_time_ms":1000,"contract":"SNDK_USDT","order_id":"15724","size":-10,"price":"1500.5","role":"maker","text":"t-mm1","fee":"-0.001","point_fee":"0"}]}"#).unwrap();
        match parse_private(&v, &g, Instant::now()).pop() {
            Some(Event::GateUserTrade(t)) => {
                assert_eq!(t.trade_id, "3335259");
                assert_eq!(t.signed_size, -10);
                assert!(t.is_maker);
                assert_eq!(t.price, 150050);
            }
            other => panic!("{other:?}"),
        }
        let v: Value = serde_json::from_str(r#"{"channel":"futures.positions","event":"update","result":[{"contract":"SNDK_USDT","mode":"dual_long","size":12,"entry_price":"1500.1","time_ms":1000},{"contract":"SNDK_USDT","mode":"dual_short","size":-3,"entry_price":"1502","time_ms":1000}]}"#).unwrap();
        match parse_private(&v, &g, Instant::now()).pop() {
            Some(Event::GatePositions(p)) => {
                assert_eq!(p.len(), 2);
                assert_eq!(p[0].mode, "dual_long");
                assert_eq!(p[1].size, -3);
            }
            other => panic!("{other:?}"),
        }
    }
}
