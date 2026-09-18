//! Gate futures public WebSocket: `futures.book_ticker`, `futures.order_book_update`
//! and `futures.trades`, plus the REST depth snapshot needed to seed the book.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use super::gate_rest::GateRest;
use super::gate_types::GateDepthLevel;
use super::ws;
use crate::engine::events::{Event, Feed};
use crate::market::book::{BookDelta, DepthLevel};
use crate::types::TickGrid;
use crate::util::{Backoff, unix_secs};

pub const BOOK_LEVELS: u32 = 20;
const BOOK_INTERVAL: &str = "100ms";

pub struct GatePublic {
    pub ws_url: String,
    pub contract: String,
    pub grid: TickGrid,
    pub rest: Arc<GateRest>,
}

impl GatePublic {
    pub async fn run(self, tx: mpsc::Sender<Event>) {
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
        loop {
            info!(url = %self.ws_url, "gate public: connecting");
            match ws::connect(&self.ws_url).await {
                Ok((mut sink, mut source)) => {
                    backoff.reset();
                    let subs = [
                        ("futures.book_ticker", json!([self.contract])),
                        ("futures.trades", json!([self.contract])),
                        ("futures.order_book_update", json!([self.contract, BOOK_INTERVAL, BOOK_LEVELS.to_string()])),
                        ("futures.tickers", json!([self.contract])),
                    ];
                    let mut ok = true;
                    for (ch, payload) in subs {
                        let req = json!({"time": unix_secs(), "channel": ch, "event": "subscribe", "payload": payload});
                        if let Err(e) = ws::send_text(&mut sink, req.to_string()).await {
                            warn!(error = %e, "gate public: subscribe failed");
                            ok = false;
                            break;
                        }
                    }
                    if ok {
                        let _ = tx.send(Event::FeedStatus { feed: Feed::GatePublic, connected: true, at: Instant::now() }).await;
                        // Seed the local book after subscribing so buffered deltas can be replayed.
                        self.spawn_snapshot(tx.clone());
                        let mut ping = tokio::time::interval(Duration::from_secs(15));
                        ping.tick().await;
                        let idle = Duration::from_secs(45);
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
                                        }
                                        Ok(Some(Ok(Message::Close(_)))) | Ok(None) => { warn!("gate public: closed"); break; }
                                        Ok(Some(Ok(m))) => {
                                            if let Some(text) = ws::message_text(&m) {
                                                for ev in self.parse(&text) {
                                                    if tx.send(ev).await.is_err() { return; }
                                                }
                                            }
                                        }
                                        Ok(Some(Err(e))) => { warn!(error = %e, "gate public: ws error"); break; }
                                        Err(_) => { warn!("gate public: idle timeout"); break; }
                                    }
                                }
                            }
                        }
                    }
                    let _ = tx.send(Event::FeedStatus { feed: Feed::GatePublic, connected: false, at: Instant::now() }).await;
                }
                Err(e) => warn!(error = %e, "gate public: connect failed"),
            }
            tokio::time::sleep(backoff.next()).await;
        }
    }

    pub fn spawn_snapshot(&self, tx: mpsc::Sender<Event>) {
        let rest = self.rest.clone();
        let contract = self.contract.clone();
        let grid = self.grid;
        tokio::spawn(async move {
            // Small delay lets the first deltas arrive so the snapshot id is covered.
            tokio::time::sleep(Duration::from_millis(300)).await;
            match rest.order_book(&contract, BOOK_LEVELS, &grid).await {
                Ok(snap) => {
                    let _ = tx.send(Event::GateBookSnapshot(snap)).await;
                }
                Err(e) => warn!(error = %e, "gate public: depth snapshot failed"),
            }
        });
    }

    fn parse(&self, text: &str) -> Vec<Event> {
        let Ok(v) = serde_json::from_str::<Value>(text) else { return vec![] };
        parse_public(&v, &self.grid, Instant::now())
    }
}

pub fn parse_public(v: &Value, grid: &TickGrid, now: Instant) -> Vec<Event> {
    let channel = v.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    let event = v.get("event").and_then(|c| c.as_str()).unwrap_or("");
    if event != "update" && event != "all" {
        if let Some(err) = v.get("error") {
            if !err.is_null() {
                warn!(%channel, %err, "gate public: error");
            }
        }
        return vec![];
    }
    let Some(result) = v.get("result") else { return vec![] };
    match channel {
        "futures.book_ticker" => {
            let bid = result.get("b").and_then(|x| x.as_str()).and_then(|s| grid.parse(s));
            let ask = result.get("a").and_then(|x| x.as_str()).and_then(|s| grid.parse(s));
            let (Some(bid), Some(ask)) = (bid, ask) else { return vec![] };
            if bid <= 0 || ask <= 0 {
                return vec![];
            }
            let bid_size = result.get("B").and_then(|x| x.as_i64()).unwrap_or(0);
            let ask_size = result.get("A").and_then(|x| x.as_i64()).unwrap_or(0);
            let exch_ms = result.get("t").and_then(|x| x.as_i64()).unwrap_or(0);
            vec![Event::GateBbo { bid, bid_size, ask, ask_size, exch_ms, at: now }]
        }
        "futures.trades" => {
            let Some(arr) = result.as_array() else { return vec![] };
            arr.iter()
                .filter_map(|t| {
                    let price_f64: f64 = t.get("price")?.as_str()?.parse().ok()?;
                    let signed_size = t.get("size")?.as_i64()?;
                    let trade_id = t.get("id").and_then(|x| x.as_u64()).unwrap_or(0);
                    Some(Event::GateTrade { price: grid.round(price_f64), price_f64, signed_size, trade_id, at: now })
                })
                .collect()
        }
        "futures.order_book_update" => {
            let first_id = result.get("U").and_then(|x| x.as_u64()).unwrap_or(0);
            let last_id = result.get("u").and_then(|x| x.as_u64()).unwrap_or(0);
            let full = result.get("full").and_then(|x| x.as_bool()).unwrap_or(false);
            let conv = |key: &str| -> Vec<DepthLevel> {
                result
                    .get(key)
                    .and_then(|x| serde_json::from_value::<Vec<GateDepthLevel>>(x.clone()).ok())
                    .unwrap_or_default()
                    .into_iter()
                    .map(|l| DepthLevel { price: grid.round(l.p), size: l.s })
                    .collect()
            };
            vec![Event::GateBookDelta(BookDelta { first_id, last_id, bids: conv("b"), asks: conv("a"), full })]
        }
        "futures.tickers" => {
            let items: Vec<&Value> = match result {
                Value::Array(a) => a.iter().collect(),
                other => vec![other],
            };
            items
                .into_iter()
                .filter_map(|t| {
                    let f = |k: &str| t.get(k).and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok());
                    let mark = f("mark_price")?;
                    let index = f("index_price").unwrap_or(mark);
                    let funding_rate = f("funding_rate").unwrap_or(0.0);
                    Some(Event::GateTicker { mark, index, funding_rate, at: now })
                })
                .collect()
        }
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn grid() -> TickGrid {
        TickGrid::new(Decimal::from_str("0.01").unwrap())
    }

    #[test]
    fn parses_book_ticker() {
        let v: Value = serde_json::from_str(r#"{"time":1,"channel":"futures.book_ticker","event":"update","result":{"t":1000,"u":5,"s":"SNDK_USDT","b":"1545.1","B":12,"a":"1545.3","A":5}}"#).unwrap();
        let ev = parse_public(&v, &grid(), Instant::now());
        assert!(matches!(ev.as_slice(), [Event::GateBbo { bid: 154510, ask: 154530, bid_size: 12, ask_size: 5, .. }]));
    }

    #[test]
    fn parses_trades_and_deltas() {
        let v: Value = serde_json::from_str(r#"{"channel":"futures.trades","event":"update","result":[{"size":-108,"id":27753479,"create_time":1,"create_time_ms":1000,"price":"1545.2","contract":"SNDK_USDT"}]}"#).unwrap();
        let ev = parse_public(&v, &grid(), Instant::now());
        assert!(matches!(ev.as_slice(), [Event::GateTrade { price: 154520, signed_size: -108, trade_id: 27753479, .. }]));
        let v: Value = serde_json::from_str(r#"{"channel":"futures.order_book_update","event":"update","result":{"t":1,"s":"SNDK_USDT","U":10,"u":12,"b":[{"p":"1545.1","s":3}],"a":[{"p":"1545.3","s":0}]}}"#).unwrap();
        match parse_public(&v, &grid(), Instant::now()).pop() {
            Some(Event::GateBookDelta(d)) => {
                assert_eq!((d.first_id, d.last_id), (10, 12));
                assert_eq!(d.bids[0].price, 154510);
                assert_eq!(d.asks[0].size, 0);
            }
            other => panic!("{other:?}"),
        }
    }
}
