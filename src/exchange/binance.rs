//! Binance USDⓈ-M futures: symbol metadata (REST) and the public
//! `bookTicker` + `aggTrade` streams (WS). Reference only – no trading.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use super::ws;
use crate::engine::events::{Event, Feed};
use crate::market::reference::Bbo;
use crate::util::{Backoff, unix_ms};

#[derive(Debug, Clone)]
pub struct BinanceSymbolMeta {
    pub symbol: String,
    pub contract_type: String,
    pub status: String,
    pub tick_size: String,
    pub step_size: String,
    pub min_qty: String,
    pub min_notional: String,
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
struct SymbolInfo {
    symbol: String,
    #[serde(rename = "contractType", default)]
    contract_type: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    filters: Vec<serde_json::Value>,
}

pub async fn fetch_symbol_meta(rest: &str, symbol: &str) -> Result<BinanceSymbolMeta> {
    let url = format!("{rest}/fapi/v1/exchangeInfo?symbol={symbol}");
    let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
    let resp = client.get(&url).send().await.context("binance exchangeInfo")?;
    if !resp.status().is_success() {
        // Some deployments reject the symbol filter; fall back to the full list.
        let url = format!("{rest}/fapi/v1/exchangeInfo");
        let full: ExchangeInfo = client.get(&url).send().await?.error_for_status()?.json().await?;
        return pick(full, symbol);
    }
    let info: ExchangeInfo = resp.json().await.context("parse exchangeInfo")?;
    pick(info, symbol)
}

fn pick(info: ExchangeInfo, symbol: &str) -> Result<BinanceSymbolMeta> {
    let s = info
        .symbols
        .into_iter()
        .find(|s| s.symbol.eq_ignore_ascii_case(symbol))
        .ok_or_else(|| anyhow!("binance symbol {symbol} not found"))?;
    let mut meta = BinanceSymbolMeta {
        symbol: s.symbol,
        contract_type: s.contract_type,
        status: s.status,
        tick_size: String::new(),
        step_size: String::new(),
        min_qty: String::new(),
        min_notional: String::new(),
    };
    for f in s.filters {
        match f.get("filterType").and_then(|v| v.as_str()) {
            Some("PRICE_FILTER") => meta.tick_size = f.get("tickSize").and_then(|v| v.as_str()).unwrap_or("").into(),
            Some("LOT_SIZE") => {
                meta.step_size = f.get("stepSize").and_then(|v| v.as_str()).unwrap_or("").into();
                meta.min_qty = f.get("minQty").and_then(|v| v.as_str()).unwrap_or("").into();
            }
            Some("MIN_NOTIONAL") => meta.min_notional = f.get("notional").and_then(|v| v.as_str()).unwrap_or("").into(),
            _ => {}
        }
    }
    Ok(meta)
}

#[derive(Deserialize)]
struct Combined<'a> {
    #[serde(borrow)]
    stream: &'a str,
    data: serde_json::Value,
}

/// Run the Binance public stream forever (with reconnects).
pub async fn run_public(ws_url: String, symbol: String, tx: mpsc::Sender<Event>) {
    let lower = symbol.to_lowercase();
    let url = format!("{ws_url}?streams={lower}@bookTicker/{lower}@aggTrade");
    let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
    loop {
        info!(%url, "binance: connecting");
        match ws::connect(&url).await {
            Ok((mut sink, mut source)) => {
                let _ = tx.send(Event::FeedStatus { feed: Feed::Binance, connected: true, at: Instant::now() }).await;
                backoff.reset();
                // Binance drops connections after 24h; also disconnect on silence.
                let idle = Duration::from_secs(60);
                loop {
                    let msg = match tokio::time::timeout(idle, source.next()).await {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => {
                            warn!(error = %e, "binance: ws error");
                            break;
                        }
                        Ok(None) => {
                            warn!("binance: ws closed");
                            break;
                        }
                        Err(_) => {
                            warn!("binance: idle timeout");
                            break;
                        }
                    };
                    match msg {
                        Message::Ping(p) => {
                            use futures_util::SinkExt;
                            let _ = sink.send(Message::Pong(p)).await;
                        }
                        Message::Close(_) => break,
                        other => {
                            if let Some(text) = ws::message_text(&other) {
                                if let Some(ev) = parse_message(&text) {
                                    if tx.send(ev).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
                let _ = tx.send(Event::FeedStatus { feed: Feed::Binance, connected: false, at: Instant::now() }).await;
            }
            Err(e) => warn!(error = %e, "binance: connect failed"),
        }
        tokio::time::sleep(backoff.next()).await;
    }
}

fn parse_message(text: &str) -> Option<Event> {
    let c: Combined = serde_json::from_str(text).ok()?;
    let now = Instant::now();
    if c.stream.ends_with("@bookTicker") {
        let d = &c.data;
        let bid: f64 = d.get("b")?.as_str()?.parse().ok()?;
        let ask: f64 = d.get("a")?.as_str()?.parse().ok()?;
        let bid_qty: f64 = d.get("B").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let ask_qty: f64 = d.get("A").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let ts = d.get("E").and_then(|v| v.as_i64()).or_else(|| d.get("T").and_then(|v| v.as_i64())).unwrap_or_else(unix_ms);
        Some(Event::BinanceBbo(Bbo { bid, ask, bid_qty, ask_qty, exch_ts_ms: ts, recv: now }))
    } else if c.stream.ends_with("@aggTrade") {
        let d = &c.data;
        let price: f64 = d.get("p")?.as_str()?.parse().ok()?;
        let qty: f64 = d.get("q")?.as_str()?.parse().ok()?;
        let m = d.get("m").and_then(|v| v.as_bool()).unwrap_or(false);
        Some(Event::BinanceTrade { price, qty, buyer_is_maker: m, at: now })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_book_ticker_and_agg_trade() {
        let t = r#"{"stream":"sndkusdt@bookTicker","data":{"e":"bookTicker","u":1,"s":"SNDKUSDT","b":"1545.10","B":"3.2","a":"1545.30","A":"1.1","T":1,"E":2}}"#;
        match parse_message(t) {
            Some(Event::BinanceBbo(b)) => {
                assert_eq!(b.bid, 1545.10);
                assert_eq!(b.ask, 1545.30);
                assert_eq!(b.exch_ts_ms, 2);
            }
            other => panic!("{other:?}"),
        }
        let t = r#"{"stream":"sndkusdt@aggTrade","data":{"e":"aggTrade","a":5,"s":"SNDKUSDT","p":"1545.2","q":"0.5","m":true,"T":3}}"#;
        assert!(matches!(parse_message(t), Some(Event::BinanceTrade { buyer_is_maker: true, .. })));
    }
}
