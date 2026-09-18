//! Quote generation: fair price → inventory-skewed centre → Gate-book-aware
//! layered POST_ONLY quotes with dual-mode open/reduce mapping and clipping.
//!
//! All prices in the output are Gate ticks; sizes are contracts.

use crate::config;
use crate::strategy::inventory::{InventoryState, PendingExposure, Positions};
use crate::strategy::protection::SidePolicy;
use crate::types::{ContractMeta, Purpose, Side, TickGrid, Ticks};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quote {
    pub side: Side,
    pub layer: u8,
    pub purpose: Purpose,
    pub price: Ticks,
    pub size: i64,
}

/// Safety boundaries used for the "dangerous order" check. An order is
/// dangerous when it is priced *beyond* its bound (buy above / sell below).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SideBounds {
    pub buy_open_max: Ticks,
    pub buy_reduce_max: Ticks,
    pub sell_open_min: Ticks,
    pub sell_reduce_min: Ticks,
}

impl SideBounds {
    pub fn bound_for(&self, side: Side, purpose: Purpose) -> Ticks {
        match (side, purpose) {
            (Side::Buy, Purpose::Open) => self.buy_open_max,
            (Side::Buy, Purpose::Reduce) => self.buy_reduce_max,
            (Side::Sell, Purpose::Open) => self.sell_open_min,
            (Side::Sell, Purpose::Reduce) => self.sell_reduce_min,
        }
    }

    /// True when an order at `price` has crossed its safety boundary.
    pub fn is_dangerous(&self, side: Side, purpose: Purpose, price: Ticks) -> bool {
        let b = self.bound_for(side, purpose);
        match side {
            Side::Buy => price > b,
            Side::Sell => price < b,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuoteSet {
    pub centre: f64,
    pub fair: f64,
    pub delta_bps: f64,
    pub u: f64,
    pub bounds: SideBounds,
    pub inner_bid: Option<Ticks>,
    pub inner_ask: Option<Ticks>,
}

#[derive(Debug, Clone, Copy)]
pub struct QuoteContext<'a> {
    pub fair: f64,
    pub delta_bps: f64,
    pub u: f64,
    /// External (own orders excluded) Gate best bid / ask in ticks.
    pub ext_bid: Option<Ticks>,
    pub ext_ask: Option<Ticks>,
    pub grid: TickGrid,
    pub meta: &'a ContractMeta,
    pub pos: Positions,
    /// Exposure of orders whose final quantity is not under our control right now
    /// (in flight / pending cancel / pending amend).
    pub uncontrolled: PendingExposure,
    pub buy_policy: SidePolicy,
    pub sell_policy: SidePolicy,
    /// Global permission to add risk (feeds trusted, basis normal, session ok, not reducing/halted).
    pub allow_new_risk: bool,
    pub quoting: &'a config::Quoting,
    pub inventory: &'a InventoryState,
}

/// Compute the half-spread δ (bps):
/// δ = max(δ_cfg, f_m + e, V_q, 1e4·tick/F)
pub fn half_spread_bps(cfg: &config::Quoting, maker_fee_rate: f64, v_quantile_bps: Option<f64>, tick_bps: f64) -> f64 {
    let fee_term = maker_fee_rate * 10_000.0 + cfg.fee_buffer_bps;
    let mut d = cfg.min_half_spread_bps.max(fee_term).max(tick_bps);
    if let Some(v) = v_quantile_bps {
        d = d.max(v);
    }
    d
}

pub fn quote_centre(fair: f64, delta_bps: f64, u: f64) -> f64 {
    fair * (1.0 - delta_bps * u / 10_000.0)
}

/// Dual-mode purpose of each quoting side: while short, buys reduce the short;
/// while long, sells reduce the long; otherwise both sides add inventory.
pub fn side_purposes(pos: Positions) -> (Purpose, Purpose) {
    let buy = if pos.short > 0 { Purpose::Reduce } else { Purpose::Open };
    let sell = if pos.long > 0 { Purpose::Reduce } else { Purpose::Open };
    (buy, sell)
}

/// Build the full desired quote set. Returns the quotes and the diagnostics/bounds.
pub fn build_quotes(ctx: &QuoteContext<'_>) -> (Vec<Quote>, QuoteSet) {
    let g = ctx.grid;
    let f = ctx.fair;
    let delta = ctx.delta_bps;
    let r = quote_centre(f, delta, ctx.u);
    let dist = |mult: f64| f * delta * mult / 10_000.0;

    // Base (reduce) limits and protection-widened open limits.
    let b_limit_reduce = r - dist(1.0);
    let a_limit_reduce = r + dist(1.0);
    let b_limit_open = r - dist(ctx.buy_policy.distance_mult);
    let a_limit_open = r + dist(ctx.sell_policy.distance_mult);

    let bounds = SideBounds {
        buy_open_max: g.floor(b_limit_open),
        buy_reduce_max: g.floor(b_limit_reduce),
        sell_open_min: g.ceil(a_limit_open),
        sell_reduce_min: g.ceil(a_limit_reduce),
    };

    let aggressive = ctx.u.abs() >= ctx.quoting.aggressive_reduce_u;
    let pos = ctx.pos;

    // Dual-mode purpose selection per side.
    let (buy_purpose, sell_purpose) = side_purposes(pos);

    // Inner prices.
    let inner_bid = inner_buy(&g, b_limit_open, b_limit_reduce, buy_purpose, aggressive && ctx.u < 0.0, ctx.ext_bid, ctx.ext_ask);
    let inner_ask = inner_sell(&g, a_limit_open, a_limit_reduce, sell_purpose, aggressive && ctx.u > 0.0, ctx.ext_bid, ctx.ext_ask);

    // Layer prices.
    let mut buy_prices = layer_prices(&g, inner_bid, f, delta, ctx.quoting, Side::Buy);
    let mut sell_prices = layer_prices(&g, inner_ask, f, delta, ctx.quoting, Side::Sell);
    // Never let a bid meet/cross an ask of our own.
    if let (Some(b0), Some(a0)) = (buy_prices.first().copied(), sell_prices.first().copied()) {
        if b0 >= a0 {
            // Push both apart symmetrically around the centre.
            let c = g.round(r);
            buy_prices = buy_prices.into_iter().map(|p| p.min(c - 1)).collect();
            sell_prices = sell_prices.into_iter().map(|p| p.max(c + 1)).collect();
        }
    }

    // Sizes.
    let v = ctx.quoting.layer_notional_ratio * ctx.inventory.h();
    let buy_notional = v * (1.0 - ctx.u).max(0.0) * ctx.buy_policy.size_mult;
    let sell_notional = v * (1.0 + ctx.u).max(0.0) * ctx.sell_policy.size_mult;
    let buy_base = ctx.meta.contracts_for_notional(buy_notional, f).min(ctx.meta.order_size_max);
    let sell_base = ctx.meta.contracts_for_notional(sell_notional, f).min(ctx.meta.order_size_max);
    // Reduce orders are not shrunk by protection size multipliers: use the raw inventory skew.
    let buy_reduce_base = ctx.meta.contracts_for_notional(v * (1.0 - ctx.u).max(0.0), f).min(ctx.meta.order_size_max);
    let sell_reduce_base = ctx.meta.contracts_for_notional(v * (1.0 + ctx.u).max(0.0), f).min(ctx.meta.order_size_max);

    let mut out = Vec::with_capacity(buy_prices.len() + sell_prices.len());

    // Buy side.
    match buy_purpose {
        Purpose::Reduce => {
            let mut budget = (pos.short - ctx.uncontrolled.reduce_buy).max(0);
            for (i, p) in buy_prices.iter().enumerate() {
                let s = buy_reduce_base.min(budget);
                if s >= ctx.meta.order_size_min {
                    out.push(Quote { side: Side::Buy, layer: i as u8, purpose: Purpose::Reduce, price: *p, size: s });
                    budget -= s;
                }
            }
        }
        Purpose::Open => {
            if ctx.allow_new_risk && ctx.buy_policy.allow_open && ctx.u < ctx.inventory.cfg.stop_add_u {
                let mut budget = ctx.inventory.open_buy_capacity(ctx.uncontrolled, f, ctx.meta);
                for (i, p) in buy_prices.iter().enumerate() {
                    let s = buy_base.min(budget);
                    if s >= ctx.meta.order_size_min {
                        out.push(Quote { side: Side::Buy, layer: i as u8, purpose: Purpose::Open, price: *p, size: s });
                        budget -= s;
                    }
                }
            }
        }
    }

    // Sell side.
    match sell_purpose {
        Purpose::Reduce => {
            let mut budget = (pos.long - ctx.uncontrolled.reduce_sell).max(0);
            for (i, p) in sell_prices.iter().enumerate() {
                let s = sell_reduce_base.min(budget);
                if s >= ctx.meta.order_size_min {
                    out.push(Quote { side: Side::Sell, layer: i as u8, purpose: Purpose::Reduce, price: *p, size: s });
                    budget -= s;
                }
            }
        }
        Purpose::Open => {
            if ctx.allow_new_risk && ctx.sell_policy.allow_open && -ctx.u < ctx.inventory.cfg.stop_add_u {
                let mut budget = ctx.inventory.open_sell_capacity(ctx.uncontrolled, f, ctx.meta);
                for (i, p) in sell_prices.iter().enumerate() {
                    let s = sell_base.min(budget);
                    if s >= ctx.meta.order_size_min {
                        out.push(Quote { side: Side::Sell, layer: i as u8, purpose: Purpose::Open, price: *p, size: s });
                        budget -= s;
                    }
                }
            }
        }
    }

    let set = QuoteSet {
        centre: r,
        fair: f,
        delta_bps: delta,
        u: ctx.u,
        bounds,
        inner_bid: buy_prices.first().copied(),
        inner_ask: sell_prices.first().copied(),
    };
    (out, set)
}

/// B₀ = floor_tick[min(B_limit, G_bid + tick, G_ask − tick)]; in aggressive
/// reduce mode the `G_bid + tick` term is dropped so the bid may sit anywhere
/// inside the Gate spread down to the limit.
fn inner_buy(
    g: &TickGrid,
    b_limit_open: f64,
    b_limit_reduce: f64,
    purpose: Purpose,
    aggressive: bool,
    ext_bid: Option<Ticks>,
    ext_ask: Option<Ticks>,
) -> Ticks {
    let limit = match purpose {
        Purpose::Open => g.floor(b_limit_open),
        Purpose::Reduce => g.floor(b_limit_reduce),
    };
    let mut p = limit;
    if !(purpose == Purpose::Reduce && aggressive) {
        if let Some(b) = ext_bid {
            p = p.min(b + 1);
        }
    }
    if let Some(a) = ext_ask {
        p = p.min(a - 1);
    }
    p
}

fn inner_sell(
    g: &TickGrid,
    a_limit_open: f64,
    a_limit_reduce: f64,
    purpose: Purpose,
    aggressive: bool,
    ext_bid: Option<Ticks>,
    ext_ask: Option<Ticks>,
) -> Ticks {
    let limit = match purpose {
        Purpose::Open => g.ceil(a_limit_open),
        Purpose::Reduce => g.ceil(a_limit_reduce),
    };
    let mut p = limit;
    if !(purpose == Purpose::Reduce && aggressive) {
        if let Some(a) = ext_ask {
            p = p.max(a - 1);
        }
    }
    if let Some(b) = ext_bid {
        p = p.max(b + 1);
    }
    p
}

/// Inner price plus outer layers at `offsets × δ` from the inner price;
/// price levels that collapse onto the same tick are merged (kept once).
fn layer_prices(g: &TickGrid, inner: Ticks, fair: f64, delta_bps: f64, cfg: &config::Quoting, side: Side) -> Vec<Ticks> {
    let mut v = vec![inner];
    let inner_px = g.to_f64(inner);
    for off in cfg.layer_offsets_delta.iter().take(cfg.layers.saturating_sub(1)) {
        let d = fair * delta_bps * off / 10_000.0;
        let p = match side {
            Side::Buy => g.floor(inner_px - d),
            Side::Sell => g.ceil(inner_px + d),
        };
        if !v.contains(&p) {
            v.push(p);
        }
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn meta() -> ContractMeta {
        ContractMeta {
            name: "SNDK_USDT".into(),
            tick: Decimal::from_str("0.01").unwrap(),
            multiplier: Decimal::from_str("0.01").unwrap(),
            order_size_min: 1,
            order_size_max: 1_000_000,
            orders_limit: 100,
            price_deviate: 0.05,
            funding_interval_secs: 28800,
            funding_next_apply: 0,
            maker_fee: -0.0001,
            taker_fee: 0.00075,
            in_delisting: false,
        }
    }

    fn inv(h: f64) -> InventoryState {
        InventoryState::new(config::Inventory { max_net_usdt: h, max_gross_usdt: h, band_ratio: 0.1, band_timeout_ms: 3000, stop_add_u: 0.5 })
    }

    struct Fix {
        meta: ContractMeta,
        inv: InventoryState,
        q: config::Quoting,
    }

    impl Fix {
        fn new() -> Self {
            Self { meta: meta(), inv: inv(5000.0), q: config::Quoting::default() }
        }
        fn ctx(&self, fair: f64, u: f64, bid: Option<Ticks>, ask: Option<Ticks>) -> QuoteContext<'_> {
            QuoteContext {
                fair,
                delta_bps: 2.0,
                u,
                ext_bid: bid,
                ext_ask: ask,
                grid: TickGrid::new(self.meta.tick),
                meta: &self.meta,
                pos: self.inv.pos,
                uncontrolled: PendingExposure::default(),
                buy_policy: SidePolicy::NORMAL,
                sell_policy: SidePolicy::NORMAL,
                allow_new_risk: true,
                quoting: &self.q,
                inventory: &self.inv,
            }
        }
    }

    #[test]
    fn half_spread_takes_max_of_terms() {
        let q = config::Quoting::default(); // min 2 bps, buffer 0.5
        assert_eq!(half_spread_bps(&q, -0.0001, None, 0.065), 2.0); // fee term = -1+0.5 = -0.5
        assert_eq!(half_spread_bps(&q, 0.0002, None, 0.065), 2.5); // 2 + 0.5
        assert_eq!(half_spread_bps(&q, -0.0001, Some(3.7), 0.065), 3.7);
        assert_eq!(half_spread_bps(&q, -0.0001, None, 6.0), 6.0);
    }

    #[test]
    fn centre_skews_with_inventory() {
        assert!((quote_centre(1000.0, 2.0, 0.0) - 1000.0).abs() < 1e-9);
        assert!((quote_centre(1000.0, 2.0, 0.5) - 999.9).abs() < 1e-9);
        assert!((quote_centre(1000.0, 2.0, -0.5) - 1000.1).abs() < 1e-9);
    }

    #[test]
    fn inner_quotes_improve_gate_by_one_tick_within_limits() {
        // Spec example: F=1000, theoretical 999.80/1000.20, Gate 999.70/1000.30 → 999.71/1000.29
        let fx = Fix::new();
        let (quotes, set) = build_quotes(&fx.ctx(1000.0, 0.0, Some(99970), Some(100030)));
        assert_eq!(set.inner_bid, Some(99971));
        assert_eq!(set.inner_ask, Some(100029));
        assert_eq!(set.bounds.buy_open_max, 99980);
        assert_eq!(set.bounds.sell_open_min, 100020);
        let buys: Vec<_> = quotes.iter().filter(|q| q.side == Side::Buy).collect();
        let sells: Vec<_> = quotes.iter().filter(|q| q.side == Side::Sell).collect();
        assert_eq!(buys.len(), 3);
        assert_eq!(sells.len(), 3);
        // outer layers: 0.5δ = 0.10, 1.25δ = 0.25 below/above inner
        assert_eq!(buys[1].price, 99961);
        assert_eq!(buys[2].price, 99946);
        assert_eq!(sells[1].price, 100039);
        assert_eq!(sells[2].price, 100054);
        // v = 0.05 * 5000 = 250 USDT → 25 contracts at 1000 with 0.01 multiplier
        assert!(buys.iter().all(|q| q.size == 25 && q.purpose == Purpose::Open));
        assert!(sells.iter().all(|q| q.size == 25 && q.purpose == Purpose::Open));
    }

    #[test]
    fn does_not_chase_gate_beyond_limit() {
        // Gate bid at 1000.10 but our theoretical bid is 999.80 → stay at 999.80
        let fx = Fix::new();
        let (_, set) = build_quotes(&fx.ctx(1000.0, 0.0, Some(100010), Some(100030)));
        assert_eq!(set.inner_bid, Some(99980));
        // Gate ask at 999.90 (below our theoretical bid): bid improves Gate's bid by
        // one tick and stays below the ask (post-only safe); ask stays at A_limit.
        let (_, set) = build_quotes(&fx.ctx(1000.0, 0.0, Some(99970), Some(99990)));
        assert_eq!(set.inner_bid, Some(99971));
        assert_eq!(set.inner_ask, Some(100020));
        // Gate locked one tick wide below our limit: the bid cannot improve the
        // Gate bid (it would cross the ask) so it is capped one tick under the ask.
        let (_, set) = build_quotes(&fx.ctx(1000.0, 0.0, Some(99973), Some(99974)));
        assert_eq!(set.inner_bid, Some(99973));
        // A tight Gate market above our limit never pulls the bid past B_limit.
        let (_, set) = build_quotes(&fx.ctx(1000.0, 0.0, Some(99985), Some(99986)));
        assert_eq!(set.inner_bid, Some(99980));
    }

    #[test]
    fn long_inventory_maps_sells_to_reduce_and_shrinks_buys() {
        let mut fx = Fix::new();
        // long 250 contracts at F=1000 → Q = 2500 = 0.5H
        fx.inv.pos.apply_fill(250, 1000.0, false, &fx.meta);
        let u = fx.inv.u(1000.0, &fx.meta);
        assert!((u - 0.5).abs() < 1e-9);
        let (quotes, set) = build_quotes(&fx.ctx(1000.0, u, Some(99970), Some(100030)));
        // centre 999.90 → A_limit 1000.10; aggressive reduce: ceil(max(1000.10, Gbid+tick=999.71)) = 1000.10
        assert_eq!(set.inner_ask, Some(100010));
        let sells: Vec<_> = quotes.iter().filter(|q| q.side == Side::Sell).collect();
        assert!(sells.iter().all(|q| q.purpose == Purpose::Reduce));
        // sell size = 25 * 1.5 = 37 per layer, capped by long 250 in total
        assert_eq!(sells[0].size, 37);
        assert_eq!(sells.iter().map(|q| q.size).sum::<i64>(), 111);
        // buys: u >= stop_add_u → no adds
        assert!(quotes.iter().all(|q| q.side == Side::Sell));
    }

    #[test]
    fn moderate_long_keeps_reduced_buys() {
        let mut fx = Fix::new();
        fx.inv.pos.apply_fill(100, 1000.0, false, &fx.meta); // 0.2H
        let u = fx.inv.u(1000.0, &fx.meta);
        let (quotes, _) = build_quotes(&fx.ctx(1000.0, u, Some(99970), Some(100030)));
        let buys: Vec<_> = quotes.iter().filter(|q| q.side == Side::Buy).collect();
        assert!(!buys.is_empty());
        assert!(buys.iter().all(|q| q.purpose == Purpose::Open && q.size == 20)); // 25*(1-0.2)
        let sells: Vec<_> = quotes.iter().filter(|q| q.side == Side::Sell).collect();
        assert!(sells.iter().all(|q| q.purpose == Purpose::Reduce && q.size == 30)); // 25*1.2
        // total reduce ≤ long
        assert!(sells.iter().map(|q| q.size).sum::<i64>() <= 100);
    }

    #[test]
    fn open_orders_respect_capacity_and_min_size() {
        let mut fx = Fix::new();
        fx.inv.pos.apply_fill(470, 1000.0, false, &fx.meta); // 0.94H → u ≥ stop_add → no buys
        let u = fx.inv.u(1000.0, &fx.meta);
        let (quotes, _) = build_quotes(&fx.ctx(1000.0, u, Some(99970), Some(100030)));
        assert!(quotes.iter().all(|q| q.side == Side::Sell));

        // flat but with uncontrolled in-flight buys nearly filling H: capacity clips
        let fx = Fix::new();
        let mut c = fx.ctx(1000.0, 0.0, Some(99970), Some(100030));
        c.uncontrolled.open_buy = 490; // H = 500 contracts → 10 left
        let (quotes, _) = build_quotes(&c);
        let buys: Vec<_> = quotes.iter().filter(|q| q.side == Side::Buy).collect();
        assert_eq!(buys.len(), 1);
        assert_eq!(buys[0].size, 10);
    }

    #[test]
    fn protection_policies_apply_to_open_side_only() {
        let mut fx = Fix::new();
        fx.inv.pos.apply_fill(100, 1000.0, false, &fx.meta);
        let u = fx.inv.u(1000.0, &fx.meta);
        let mut c = fx.ctx(1000.0, u, Some(99970), Some(100030));
        // strong up-pressure: sell opens forbidden, buys halved & tight
        c.sell_policy = SidePolicy { allow_open: false, size_mult: 0.0, distance_mult: 1.0, follow_tight: false, level: crate::strategy::protection::Level::Strong };
        c.buy_policy = SidePolicy { allow_open: true, size_mult: 0.5, distance_mult: 1.0, follow_tight: true, level: crate::strategy::protection::Level::None };
        let (quotes, _) = build_quotes(&c);
        // reduce sells remain (we are long) even though sell opens are forbidden
        assert!(quotes.iter().any(|q| q.side == Side::Sell && q.purpose == Purpose::Reduce));
        let buys: Vec<_> = quotes.iter().filter(|q| q.side == Side::Buy).collect();
        assert!(buys.iter().all(|q| q.size == 10)); // 25 * 0.8 * 0.5
        // light protection widens the open distance: bound moves
        let mut c2 = fx.ctx(1000.0, 0.0, Some(99900), Some(100100));
        c2.buy_policy = SidePolicy { allow_open: true, size_mult: 0.5, distance_mult: 1.5, follow_tight: false, level: crate::strategy::protection::Level::Light };
        let (_, set) = build_quotes(&c2);
        assert_eq!(set.bounds.buy_open_max, 99970); // 1000 - 0.30
        assert_eq!(set.bounds.buy_reduce_max, 99980);
    }

    #[test]
    fn danger_check_semantics() {
        let b = SideBounds { buy_open_max: 99980, buy_reduce_max: 99980, sell_open_min: 100020, sell_reduce_min: 100020 };
        assert!(b.is_dangerous(Side::Buy, Purpose::Open, 99981));
        assert!(!b.is_dangerous(Side::Buy, Purpose::Open, 99980));
        assert!(b.is_dangerous(Side::Sell, Purpose::Open, 100019));
        assert!(!b.is_dangerous(Side::Sell, Purpose::Reduce, 100020));
    }
}
