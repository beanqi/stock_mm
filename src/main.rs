//! stock_mm — Binance-referenced, Gate-executed market maker for SNDK perpetuals.
//!
//! Usage:
//!   stock_mm [--config config.toml] [--paper | --live] [--print-default-config]
//!
//! Credentials come from `GATE_API_KEY` / `GATE_API_SECRET` (or a `.env` file).

mod config;
mod engine;
mod exchange;
mod market;
mod metrics;
mod order;
mod strategy;
mod types;
mod util;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use config::{Config, Credentials, Mode};
use engine::events::Event;
use engine::{Engine, Executor};
use exchange::gate_rest::GateRest;
use strategy::inventory::Positions;
use types::TickGrid;

struct Args {
    config: PathBuf,
    mode: Option<Mode>,
    print_default: bool,
}

fn parse_args() -> Result<Args> {
    let mut args = Args { config: PathBuf::from("config.toml"), mode: None, print_default: false };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config" | "-c" => args.config = PathBuf::from(it.next().context("--config needs a path")?),
            "--paper" => args.mode = Some(Mode::Paper),
            "--live" => args.mode = Some(Mode::Live),
            "--print-default-config" => args.print_default = true,
            "-h" | "--help" => {
                println!("usage: stock_mm [--config config.toml] [--paper|--live] [--print-default-config]");
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}"),
        }
    }
    Ok(args)
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,stock_mm=info"));
    tracing_subscriber::fmt().with_env_filter(filter).with_target(false).with_thread_ids(false).init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    init_logging();
    let args = parse_args()?;
    if args.print_default {
        print!("{}", Config::default_toml());
        return Ok(());
    }
    let mut cfg = if args.config.exists() {
        Config::load(&args.config)?
    } else {
        warn!(path = %args.config.display(), "config file not found; using defaults");
        Config::default()
    };
    if let Some(m) = args.mode {
        cfg.mode = m;
    }
    let creds = Credentials::from_env();
    if cfg.mode == Mode::Live && !creds.is_complete() {
        bail!("live mode requires GATE_API_KEY and GATE_API_SECRET");
    }
    info!(mode = engine::mode_label(cfg.mode), binance = cfg.instruments.binance_symbol, gate = cfg.instruments.gate_contract, "starting stock_mm");

    // ---------------------------------------------------------- metadata
    let rest = Arc::new(GateRest::new(&cfg.endpoints.gate_rest, &cfg.instruments.gate_settle, &creds.gate_key, &creds.gate_secret));
    let meta = rest.contract(&cfg.instruments.gate_contract).await.context("gate contract metadata")?;
    if meta.in_delisting {
        bail!("gate contract {} is not trading", meta.name);
    }
    let grid = TickGrid::new(meta.tick);
    info!(
        tick = %meta.tick,
        multiplier = %meta.multiplier,
        size_min = meta.order_size_min,
        size_max = meta.order_size_max,
        orders_limit = meta.orders_limit,
        maker_fee_default = meta.maker_fee,
        taker_fee_default = meta.taker_fee,
        funding_next = meta.funding_next_apply,
        "gate contract"
    );
    match exchange::binance::fetch_symbol_meta(&cfg.endpoints.binance_rest, &cfg.instruments.binance_symbol).await {
        Ok(b) => {
            info!(symbol = b.symbol, contract_type = b.contract_type, status = b.status, tick = b.tick_size, step = b.step_size, min_notional = b.min_notional, "binance symbol");
            if b.status != "TRADING" {
                bail!("binance symbol {} is not trading ({})", b.symbol, b.status);
            }
            if b.tick_size.parse::<f64>().ok() != Some(meta.tick_f64()) {
                warn!(binance_tick = b.tick_size, gate_tick = %meta.tick, "tick sizes differ between venues (expected; Gate tick is used for quoting)");
            }
        }
        Err(e) => bail!("binance symbol metadata: {e}"),
    }

    // ------------------------------------------------- account & positions
    let (fee_maker, fee_taker, dual_mode, user_id, initial_positions) = match cfg.mode {
        Mode::Live => {
            let (mk, tk) = match rest.fee(&cfg.instruments.gate_contract).await {
                Ok(f) => f,
                Err(e) => {
                    warn!(error = %e, "account fee query failed; using contract defaults");
                    (meta.maker_fee, meta.taker_fee)
                }
            };
            let acc = rest.account().await.context("gate account")?;
            info!(user = acc.user_id, total = acc.total, available = acc.available, dual = acc.in_dual_mode, "gate account");
            let n = rest.cancel_all(&cfg.instruments.gate_contract).await.context("cancel existing orders")?;
            if n > 0 {
                warn!(n, "cancelled pre-existing open orders on the contract");
            }
            let pos_list = rest.positions(&cfg.instruments.gate_contract, acc.in_dual_mode).await.context("gate positions")?;
            let pos = engine::positions_from_exchange(&pos_list, acc.in_dual_mode);
            if !pos.is_flat() {
                warn!(long = pos.long, short = pos.short, "starting with an existing position");
            }
            (mk, tk, acc.in_dual_mode, acc.user_id, pos)
        }
        Mode::Paper => (meta.maker_fee, meta.taker_fee, true, 0, Positions::default()),
    };
    info!(fee_maker, fee_taker, dual_mode, "effective fee rates / position mode");

    // ------------------------------------------------------------ engine
    let calendar = market::session::SessionCalendar::from_config(&cfg.session)?;
    let (tx, mut rx) = mpsc::channel::<Event>(65_536);
    let executor = match cfg.mode {
        Mode::Paper => Executor::Paper(engine::paper::PaperExchange::new(
            grid,
            meta.clone(),
            cfg.paper.balance_usdt,
            fee_maker,
            fee_taker,
            cfg.risk.margin_leverage,
        )),
        Mode::Live => {
            let (ctx, crx) = mpsc::channel::<exchange::gate_trade::TradeCommand>(4_096);
            let trade = exchange::gate_trade::GateTradeWs {
                ws_url: cfg.endpoints.gate_ws.clone(),
                contract: cfg.instruments.gate_contract.clone(),
                grid,
                key: creds.gate_key.clone(),
                secret: creds.gate_secret.clone(),
                rest: rest.clone(),
            };
            tokio::spawn(trade.run(crx, tx.clone()));
            let private = exchange::gate_private::GatePrivate {
                ws_url: cfg.endpoints.gate_ws.clone(),
                contract: cfg.instruments.gate_contract.clone(),
                grid,
                key: creds.gate_key.clone(),
                secret: creds.gate_secret.clone(),
                user_id,
            };
            tokio::spawn(private.run(tx.clone()));
            Executor::Live(ctx)
        }
    };
    let mut eng = Engine::new(cfg.clone(), meta.clone(), fee_maker, calendar, executor, rest.clone(), tx.clone(), dual_mode, initial_positions);

    // --------------------------------------------------------- connectors
    tokio::spawn(exchange::binance::run_public(cfg.endpoints.binance_ws.clone(), cfg.instruments.binance_symbol.clone(), tx.clone()));
    let public = exchange::gate_public::GatePublic {
        ws_url: cfg.endpoints.gate_ws.clone(),
        contract: cfg.instruments.gate_contract.clone(),
        grid,
        rest: rest.clone(),
    };
    tokio::spawn(public.run(tx.clone()));
    tokio::spawn(engine::run_timer(Duration::from_millis(cfg.orders.timer_ms), tx.clone()));

    // Signals → shutdown event.
    {
        let tx = tx.clone();
        tokio::spawn(async move {
            let ctrl_c = tokio::signal::ctrl_c();
            #[cfg(unix)]
            {
                let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("sigterm handler");
                tokio::select! {
                    _ = ctrl_c => {},
                    _ = term.recv() => {},
                }
            }
            #[cfg(not(unix))]
            {
                let _ = ctrl_c.await;
            }
            let _ = tx.send(Event::Shutdown).await;
        });
    }

    // ------------------------------------------------------------- loop
    while let Some(ev) = rx.recv().await {
        eng.handle(ev);
        if eng.is_finished() {
            break;
        }
    }
    if cfg.mode == Mode::Live {
        // Final safety net: make sure nothing rests on the contract.
        match rest.cancel_all(&cfg.instruments.gate_contract).await {
            Ok(n) => info!(n, "final cancel-all"),
            Err(e) => error!(error = %e, "final cancel-all failed"),
        }
        match rest.positions(&cfg.instruments.gate_contract, dual_mode).await {
            Ok(p) => {
                let pos = engine::positions_from_exchange(&p, dual_mode);
                info!(long = pos.long, short = pos.short, "final positions on exchange");
            }
            Err(e) => warn!(error = %e, "final position query failed"),
        }
    }
    info!("stock_mm stopped");
    Ok(())
}
