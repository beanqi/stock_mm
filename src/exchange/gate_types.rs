//! Gate futures JSON shapes shared by REST, private streams and the WS API.

use std::time::Instant;

use serde::Deserialize;

use crate::engine::events::PositionInfo;
use crate::order::model::{ExchangeStatus, OrderInfo, Tif, UserTrade};
use crate::types::{Side, TickGrid};

fn de_i64_lossy<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    let v = serde_json::Value::deserialize(d)?;
    Ok(match v {
        serde_json::Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)).unwrap_or(0),
        serde_json::Value::String(s) => s.parse::<f64>().map(|f| f as i64).unwrap_or(0),
        _ => 0,
    })
}

fn de_f64_lossy<'de, D: serde::Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    let v = serde_json::Value::deserialize(d)?;
    Ok(match v {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    })
}

fn de_string_lossy<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    let v = serde_json::Value::deserialize(d)?;
    Ok(match v {
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s,
        _ => String::new(),
    })
}

/// Raw Gate futures order (REST + WS share the shape).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct GateOrder {
    #[serde(deserialize_with = "de_string_lossy")]
    pub id: String,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub contract: String,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub size: i64,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub left: i64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub price: f64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub fill_price: f64,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub finish_as: String,
    #[serde(default)]
    pub is_reduce_only: bool,
    #[serde(default)]
    pub tif: String,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub update_time: i64,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub update_time_ms: i64,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub create_time_ms: i64,
}

impl GateOrder {
    pub fn to_info(&self, grid: &TickGrid) -> OrderInfo {
        let side = if self.size >= 0 { Side::Buy } else { Side::Sell };
        let tif = match self.tif.as_str() {
            "poc" => Some(Tif::Poc),
            "ioc" => Some(Tif::Ioc),
            "gtc" => Some(Tif::Gtc),
            _ => None,
        };
        let update_ms = if self.update_time_ms > 0 { self.update_time_ms } else { self.update_time * 1000 };
        OrderInfo {
            exchange_id: self.id.clone(),
            client_id: self.text.clone(),
            side,
            price: grid.round(self.price),
            size: self.size.abs(),
            left: self.left.abs(),
            fill_price: self.fill_price,
            status: if self.status == "finished" { ExchangeStatus::Finished } else { ExchangeStatus::Open },
            finish_as: self.finish_as.clone(),
            reduce_only: self.is_reduce_only,
            tif,
            update_ms,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct GateUserTrade {
    #[serde(deserialize_with = "de_string_lossy")]
    pub id: String,
    #[serde(deserialize_with = "de_string_lossy", default)]
    pub order_id: String,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub contract: String,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub size: i64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub price: f64,
    #[serde(default)]
    pub role: String,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub fee: f64,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub create_time_ms: i64,
}

impl GateUserTrade {
    pub fn to_user_trade(&self, grid: &TickGrid, at: Instant) -> UserTrade {
        UserTrade {
            trade_id: self.id.clone(),
            exchange_order_id: self.order_id.clone(),
            client_id: self.text.clone(),
            signed_size: self.size,
            price: grid.round(self.price),
            price_f64: self.price,
            fee: self.fee,
            is_maker: self.role == "maker",
            exch_ms: self.create_time_ms,
            at,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct GatePosition {
    #[serde(default)]
    pub contract: String,
    #[serde(default)]
    pub mode: String,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub size: i64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub entry_price: f64,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub update_time: i64,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub time_ms: i64,
}

impl GatePosition {
    pub fn to_info(&self) -> PositionInfo {
        PositionInfo {
            mode: if self.mode.is_empty() { "single".into() } else { self.mode.clone() },
            size: self.size,
            entry_price: self.entry_price,
            update_ms: if self.time_ms > 0 { self.time_ms } else { self.update_time * 1000 },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GateAccount {
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub user: i64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub total: f64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub available: f64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub unrealised_pnl: f64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub order_margin: f64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub position_margin: f64,
    #[serde(default)]
    pub in_dual_mode: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct GateContract {
    pub name: String,
    #[serde(default)]
    pub order_price_round: String,
    #[serde(default)]
    pub quanto_multiplier: String,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub order_size_min: i64,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub order_size_max: i64,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub orders_limit: i64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub order_price_deviate: f64,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub funding_interval: i64,
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub funding_next_apply: i64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub maker_fee_rate: f64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub taker_fee_rate: f64,
    #[serde(default)]
    pub in_delisting: bool,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub contract_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GateDepthLevel {
    #[serde(deserialize_with = "de_f64_lossy")]
    pub p: f64,
    #[serde(deserialize_with = "de_i64_lossy")]
    pub s: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GateBookRest {
    #[serde(deserialize_with = "de_i64_lossy", default)]
    pub id: i64,
    #[serde(default)]
    pub asks: Vec<GateDepthLevel>,
    #[serde(default)]
    pub bids: Vec<GateDepthLevel>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GateFee {
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub taker_fee: f64,
    #[serde(deserialize_with = "de_f64_lossy", default)]
    pub maker_fee: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn parses_order_with_mixed_number_types() {
        let j = r#"{"id":15724,"text":"t-mm1","contract":"SNDK_USDT","size":-10,"left":-4,"price":"1500.25","fill_price":"1500.25","status":"open","finish_as":"","is_reduce_only":true,"tif":"poc","update_time":1,"update_time_ms":1000}"#;
        let o: GateOrder = serde_json::from_str(j).unwrap();
        let g = TickGrid::new(Decimal::from_str("0.01").unwrap());
        let i = o.to_info(&g);
        assert_eq!(i.exchange_id, "15724");
        assert_eq!(i.side, Side::Sell);
        assert_eq!(i.size, 10);
        assert_eq!(i.left, 4);
        assert_eq!(i.filled(), 6);
        assert_eq!(i.price, 150025);
        assert!(i.reduce_only);
        assert_eq!(i.tif, Some(Tif::Poc));
    }

    #[test]
    fn parses_contract() {
        let j = r#"{"name":"SNDK_USDT","order_price_round":"0.01","quanto_multiplier":"0.01","order_size_min":1,"order_size_max":1000000,"orders_limit":100,"order_price_deviate":"0.05","funding_interval":28800,"funding_next_apply":1789660800,"maker_fee_rate":"-0.0001","taker_fee_rate":"0.00075","in_delisting":false,"status":"trading","contract_type":"stocks"}"#;
        let c: GateContract = serde_json::from_str(j).unwrap();
        assert_eq!(c.maker_fee_rate, -0.0001);
        assert_eq!(c.order_size_min, 1);
    }
}
