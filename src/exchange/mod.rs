//! Exchange connectivity: Binance (reference data only) and Gate (data,
//! private streams, trading API, REST).

pub mod binance;
pub mod gate_private;
pub mod gate_public;
pub mod gate_rest;
pub mod gate_trade;
pub mod gate_types;
pub mod ws;
