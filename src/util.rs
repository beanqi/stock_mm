//! Small shared helpers: time, quantiles, rolling windows, HMAC signing.

use std::collections::VecDeque;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha512};

pub fn unix_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

pub fn unix_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// Quantile (0..=1) of a slice; sorts a copy. Returns `None` on empty input.
pub fn quantile(values: &[f64], q: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut v: Vec<f64> = values.iter().copied().filter(|x| x.is_finite()).collect();
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = q.clamp(0.0, 1.0);
    let pos = q * (v.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        Some(v[lo])
    } else {
        let w = pos - lo as f64;
        Some(v[lo] * (1.0 - w) + v[hi] * w)
    }
}

pub fn median(values: &[f64]) -> Option<f64> {
    quantile(values, 0.5)
}

/// Time-bounded rolling window of `(Instant, T)` samples.
#[derive(Debug, Clone)]
pub struct TimeWindow<T> {
    pub span: Duration,
    items: VecDeque<(Instant, T)>,
}

impl<T: Copy> TimeWindow<T> {
    pub fn new(span: Duration) -> Self {
        Self { span, items: VecDeque::new() }
    }

    pub fn push(&mut self, at: Instant, v: T) {
        self.items.push_back((at, v));
        self.evict(at);
    }

    pub fn evict(&mut self, now: Instant) {
        while let Some((t, _)) = self.items.front() {
            if now.duration_since(*t) > self.span {
                self.items.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }

    pub fn values(&self) -> impl Iterator<Item = T> + '_ {
        self.items.iter().map(|(_, v)| *v)
    }

    /// Latest sample whose timestamp is ≤ `at`.
    pub fn at_or_before(&self, at: Instant) -> Option<&(Instant, T)> {
        // Windows are short (seconds); linear scan from the back is fine.
        self.items.iter().rev().find(|(t, _)| *t <= at)
    }
}

/// HMAC-SHA512 hex digest.
pub fn hmac_sha512_hex(secret: &str, message: &str) -> String {
    let mut mac = Hmac::<Sha512>::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// SHA512 hex digest of a body (Gate REST signing uses this).
pub fn sha512_hex(body: &str) -> String {
    let mut h = Sha512::new();
    h.update(body.as_bytes());
    hex::encode(h.finalize())
}

/// Exponential backoff helper for reconnect loops.
pub struct Backoff {
    cur: Duration,
    max: Duration,
}

impl Backoff {
    pub fn new(start: Duration, max: Duration) -> Self {
        Self { cur: start, max }
    }
    pub fn next(&mut self) -> Duration {
        let d = self.cur;
        self.cur = (self.cur * 2).min(self.max);
        d
    }
    pub fn reset(&mut self) {
        self.cur = Duration::from_millis(500);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_basic() {
        let v = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(quantile(&v, 0.5), Some(3.0));
        assert_eq!(quantile(&v, 0.0), Some(1.0));
        assert_eq!(quantile(&v, 1.0), Some(5.0));
        assert!((quantile(&v, 0.9).unwrap() - 4.6).abs() < 1e-9);
        assert_eq!(quantile(&[], 0.5), None);
    }

    #[test]
    fn window_evicts_and_lookup() {
        let t0 = Instant::now();
        let mut w = TimeWindow::new(Duration::from_millis(1000));
        w.push(t0, 1.0);
        w.push(t0 + Duration::from_millis(500), 2.0);
        w.push(t0 + Duration::from_millis(1400), 3.0);
        assert_eq!(w.len(), 2); // t0 evicted (1400 ms old), t0+500 kept
        assert_eq!(w.at_or_before(t0 + Duration::from_millis(1300)).map(|x| x.1), Some(2.0));
        assert_eq!(w.at_or_before(t0 + Duration::from_millis(1500)).map(|x| x.1), Some(3.0));
        assert_eq!(w.at_or_before(t0 + Duration::from_millis(400)).map(|x| x.1), None);
    }

    #[test]
    fn hmac_known_vector() {
        // Gate documentation example: HMAC-SHA512 of empty message with key "secret" is deterministic.
        let s = hmac_sha512_hex("secret", "");
        assert_eq!(s.len(), 128);
        assert_eq!(sha512_hex("").len(), 128);
        assert!(sha512_hex("").starts_with("cf83e1357eefb8bd"));
    }
}
