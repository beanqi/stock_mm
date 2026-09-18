//! Local Gate order book (L2) with own-order exclusion and sweep-cost estimation.
//!
//! Prices are stored as ticks, sizes as contracts. The book is maintained from
//! a REST snapshot followed by `futures.order_book_update` deltas; sequence
//! gaps mark the book as unreliable until the next snapshot.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::types::{Side, Ticks};

#[derive(Debug, Clone, Default)]
pub struct DepthLevel {
    pub price: Ticks,
    pub size: i64,
}

#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub id: u64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

#[derive(Debug, Clone)]
pub struct BookDelta {
    pub first_id: u64,
    pub last_id: u64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
    /// Gate `full = true`: the message is a complete snapshot and must replace the book.
    pub full: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookStatus {
    /// No snapshot yet.
    Empty,
    /// Snapshot applied but the first matching delta has not arrived.
    AwaitingDelta,
    Synced,
    /// Sequence gap: must be re-snapshotted.
    Broken,
}

#[derive(Debug, Clone)]
pub struct LocalBook {
    bids: BTreeMap<Ticks, i64>,
    asks: BTreeMap<Ticks, i64>,
    last_id: u64,
    status: BookStatus,
    pub last_update: Option<Instant>,
    /// Buffered deltas received before the snapshot.
    pending: Vec<BookDelta>,
}

impl Default for LocalBook {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_id: 0,
            status: BookStatus::Empty,
            last_update: None,
            pending: Vec::new(),
        }
    }

    pub fn status(&self) -> BookStatus {
        self.status
    }

    pub fn is_synced(&self) -> bool {
        self.status == BookStatus::Synced
    }

    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_id = 0;
        self.status = BookStatus::Empty;
        self.pending.clear();
    }

    pub fn apply_snapshot(&mut self, snap: BookSnapshot, now: Instant) {
        self.bids.clear();
        self.asks.clear();
        for l in snap.bids {
            if l.size > 0 {
                self.bids.insert(l.price, l.size);
            }
        }
        for l in snap.asks {
            if l.size > 0 {
                self.asks.insert(l.price, l.size);
            }
        }
        self.last_id = snap.id;
        self.status = BookStatus::AwaitingDelta;
        self.last_update = Some(now);
        let pending = std::mem::take(&mut self.pending);
        for d in pending {
            self.apply_delta(d, now);
            if self.status == BookStatus::Broken {
                break;
            }
        }
    }

    /// Apply a delta following Gate's sequencing rules:
    /// - before snapshot: buffer
    /// - after snapshot: drop `u < id+1`; first must satisfy `U <= id+1 <= u`
    /// - afterwards require `U == last_u + 1`
    pub fn apply_delta(&mut self, d: BookDelta, now: Instant) {
        if d.full {
            // A full push replaces the book and re-anchors the sequence.
            let snap = BookSnapshot { id: d.last_id, bids: d.bids, asks: d.asks };
            self.pending.clear();
            self.apply_snapshot(snap, now);
            self.status = BookStatus::Synced;
            return;
        }
        match self.status {
            BookStatus::Empty => {
                self.pending.push(d);
                if self.pending.len() > 2_000 {
                    self.pending.remove(0);
                }
                return;
            }
            BookStatus::Broken => return,
            BookStatus::AwaitingDelta => {
                let next = self.last_id + 1;
                if d.last_id < next {
                    return; // stale
                }
                if d.first_id > next {
                    self.status = BookStatus::Broken;
                    return;
                }
                self.status = BookStatus::Synced;
            }
            BookStatus::Synced => {
                if d.first_id != self.last_id + 1 {
                    if d.last_id <= self.last_id {
                        return; // duplicate
                    }
                    self.status = BookStatus::Broken;
                    return;
                }
            }
        }
        for l in d.bids {
            if l.size <= 0 {
                self.bids.remove(&l.price);
            } else {
                self.bids.insert(l.price, l.size);
            }
        }
        for l in d.asks {
            if l.size <= 0 {
                self.asks.remove(&l.price);
            } else {
                self.asks.insert(l.price, l.size);
            }
        }
        self.last_id = d.last_id;
        self.last_update = Some(now);
    }

    #[cfg(test)]
    pub fn best_bid(&self) -> Option<(Ticks, i64)> {
        self.bids.iter().next_back().map(|(p, s)| (*p, *s))
    }

    #[cfg(test)]
    pub fn best_ask(&self) -> Option<(Ticks, i64)> {
        self.asks.iter().next().map(|(p, s)| (*p, *s))
    }

    /// Best bid/ask after removing our own resting orders (`own` = (side, price, remaining)).
    pub fn external_bbo(&self, own: &[(Side, Ticks, i64)]) -> (Option<(Ticks, i64)>, Option<(Ticks, i64)>) {
        let bid = self
            .bids
            .iter()
            .rev()
            .map(|(p, s)| (*p, *s - own_at(own, Side::Buy, *p)))
            .find(|(_, s)| *s > 0);
        let ask = self
            .asks
            .iter()
            .map(|(p, s)| (*p, *s - own_at(own, Side::Sell, *p)))
            .find(|(_, s)| *s > 0);
        (bid, ask)
    }

    /// Cost of sweeping `qty` contracts against the book on the side that
    /// an order of `side` would hit (buy hits asks). Own orders are excluded.
    /// Returns `(filled_qty, vwap_ticks, worst_price_ticks)`; `filled_qty` may be
    /// less than `qty` if depth (within `limit` if given) is insufficient.
    pub fn sweep(&self, side: Side, qty: i64, own: &[(Side, Ticks, i64)], limit: Option<Ticks>) -> Sweep {
        let mut remaining = qty.max(0);
        let mut filled = 0i64;
        let mut cost = 0f64;
        let mut worst: Option<Ticks> = None;
        let levels: Box<dyn Iterator<Item = (&Ticks, &i64)>> = match side {
            Side::Buy => Box::new(self.asks.iter()),
            Side::Sell => Box::new(self.bids.iter().rev()),
        };
        for (p, s) in levels {
            if remaining == 0 {
                break;
            }
            if let Some(lim) = limit {
                let beyond = match side {
                    Side::Buy => *p > lim,
                    Side::Sell => *p < lim,
                };
                if beyond {
                    break;
                }
            }
            let avail = *s - own_at(own, side.opposite(), *p);
            if avail <= 0 {
                continue;
            }
            let take = avail.min(remaining);
            filled += take;
            remaining -= take;
            cost += take as f64 * *p as f64;
            worst = Some(*p);
        }
        Sweep {
            filled,
            vwap_ticks: if filled > 0 { cost / filled as f64 } else { 0.0 },
            worst_price: worst,
        }
    }

}

fn own_at(own: &[(Side, Ticks, i64)], side: Side, price: Ticks) -> i64 {
    own.iter().filter(|(s, p, _)| *s == side && *p == price).map(|(_, _, q)| *q).sum()
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sweep {
    pub filled: i64,
    pub vwap_ticks: f64,
    pub worst_price: Option<Ticks>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lvl(p: Ticks, s: i64) -> DepthLevel {
        DepthLevel { price: p, size: s }
    }

    fn synced_book() -> LocalBook {
        let mut b = LocalBook::new();
        let now = Instant::now();
        b.apply_snapshot(
            BookSnapshot {
                id: 100,
                bids: vec![lvl(99970, 10), lvl(99960, 20)],
                asks: vec![lvl(100030, 5), lvl(100040, 50)],
            },
            now,
        );
        b.apply_delta(BookDelta { first_id: 99, last_id: 101, bids: vec![lvl(99950, 7)], asks: vec![], full: false }, now);
        assert!(b.is_synced());
        b
    }

    #[test]
    fn sequencing_rules() {
        let now = Instant::now();
        let mut b = LocalBook::new();
        // delta before snapshot is buffered
        b.apply_delta(BookDelta { first_id: 101, last_id: 101, bids: vec![lvl(99980, 3)], asks: vec![], full: false }, now);
        b.apply_snapshot(BookSnapshot { id: 100, bids: vec![lvl(99970, 10)], asks: vec![lvl(100030, 5)] }, now);
        assert!(b.is_synced());
        assert_eq!(b.best_bid(), Some((99980, 3)));
        // gap breaks the book
        b.apply_delta(BookDelta { first_id: 105, last_id: 106, bids: vec![], asks: vec![], full: false }, now);
        assert_eq!(b.status(), BookStatus::Broken);
    }

    #[test]
    fn full_push_replaces_book_and_reanchors() {
        let now = Instant::now();
        let mut b = synced_book();
        b.apply_delta(BookDelta { first_id: 500, last_id: 520, bids: vec![lvl(99900, 1)], asks: vec![lvl(100100, 2)], full: true }, now);
        assert!(b.is_synced());
        assert_eq!(b.best_bid(), Some((99900, 1)));
        assert_eq!(b.best_ask(), Some((100100, 2)));
        // sequence continues from the full push id
        b.apply_delta(BookDelta { first_id: 521, last_id: 521, bids: vec![lvl(99910, 3)], asks: vec![], full: false }, now);
        assert!(b.is_synced());
        assert_eq!(b.best_bid(), Some((99910, 3)));
    }

    #[test]
    fn stale_first_delta_dropped_then_synced() {
        let now = Instant::now();
        let mut b = LocalBook::new();
        b.apply_snapshot(BookSnapshot { id: 100, bids: vec![], asks: vec![] }, now);
        b.apply_delta(BookDelta { first_id: 90, last_id: 95, bids: vec![lvl(1, 1)], asks: vec![], full: false }, now);
        assert_eq!(b.status(), BookStatus::AwaitingDelta);
        assert!(b.best_bid().is_none());
        b.apply_delta(BookDelta { first_id: 96, last_id: 101, bids: vec![lvl(2, 1)], asks: vec![], full: false }, now);
        assert!(b.is_synced());
    }

    #[test]
    fn external_bbo_excludes_own() {
        let b = synced_book();
        let own = vec![(Side::Buy, 99970, 10), (Side::Sell, 100030, 2)];
        let (bid, ask) = b.external_bbo(&own);
        assert_eq!(bid, Some((99960, 20)));
        assert_eq!(ask, Some((100030, 3)));
    }

    #[test]
    fn sweep_respects_limit_and_own() {
        let b = synced_book();
        let own = vec![(Side::Buy, 99970, 4)];
        // sell 20 hits bids: 99970 (10-4=6), 99960 (14 of 20)
        let s = b.sweep(Side::Sell, 20, &own, None);
        assert_eq!(s.filled, 20);
        assert_eq!(s.worst_price, Some(99960));
        assert!((s.vwap_ticks - (6.0 * 99970.0 + 14.0 * 99960.0) / 20.0).abs() < 1e-9);
        // with a price floor at 99970 only 6 are available
        let s = b.sweep(Side::Sell, 20, &own, Some(99970));
        assert_eq!(s.filled, 6);
        // buy sweeps asks
        let s = b.sweep(Side::Buy, 7, &[], None);
        assert_eq!(s.filled, 7);
        assert_eq!(s.worst_price, Some(100040));
    }
}
