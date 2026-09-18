//! Trading-session classification for the underlying equity.
//!
//! Binance stock perpetuals compute their index differently in regular hours,
//! pre/post market, overnight and when no external quote exists, so basis and
//! volatility statistics are kept per session and new risk is only added in
//! sessions that are explicitly enabled and have enough samples.

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Utc, Weekday};
use chrono_tz::Tz;

use crate::config;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionKind {
    Regular,
    Pre,
    Post,
    Overnight,
    Weekend,
    /// Holiday or explicit event blackout.
    Blackout,
}

impl SessionKind {
    pub fn label(self) -> &'static str {
        match self {
            SessionKind::Regular => "regular",
            SessionKind::Pre => "pre",
            SessionKind::Post => "post",
            SessionKind::Overnight => "overnight",
            SessionKind::Weekend => "weekend",
            SessionKind::Blackout => "blackout",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SessionCalendar {
    tz: Tz,
    tradable: Vec<SessionKind>,
    holidays: Vec<NaiveDate>,
    event_windows: Vec<(DateTime<Utc>, DateTime<Utc>)>,
    force: Option<SessionKind>,
}

fn parse_kind(s: &str) -> anyhow::Result<SessionKind> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "regular" => SessionKind::Regular,
        "pre" => SessionKind::Pre,
        "post" => SessionKind::Post,
        "overnight" => SessionKind::Overnight,
        "weekend" => SessionKind::Weekend,
        "blackout" => SessionKind::Blackout,
        other => anyhow::bail!("unknown session {other}"),
    })
}

impl SessionCalendar {
    pub fn from_config(cfg: &config::Session) -> anyhow::Result<Self> {
        let tz: Tz = cfg.timezone.parse().map_err(|e| anyhow::anyhow!("bad timezone {}: {e}", cfg.timezone))?;
        let mut tradable = Vec::new();
        for s in &cfg.tradable {
            tradable.push(parse_kind(s)?);
        }
        let force = if cfg.force_session.is_empty() { None } else { Some(parse_kind(&cfg.force_session)?) };
        let mut holidays = Vec::new();
        for h in &cfg.holidays {
            holidays.push(NaiveDate::parse_from_str(h, "%Y-%m-%d").map_err(|e| anyhow::anyhow!("bad holiday {h}: {e}"))?);
        }
        let mut event_windows = Vec::new();
        for [a, b] in &cfg.event_windows {
            let start = DateTime::parse_from_rfc3339(a).map_err(|e| anyhow::anyhow!("bad event start {a}: {e}"))?;
            let end = DateTime::parse_from_rfc3339(b).map_err(|e| anyhow::anyhow!("bad event end {b}: {e}"))?;
            event_windows.push((start.with_timezone(&Utc), end.with_timezone(&Utc)));
        }
        Ok(Self { tz, tradable, holidays, event_windows, force })
    }

    pub fn classify(&self, at: DateTime<Utc>) -> SessionKind {
        if let Some(f) = self.force {
            return f;
        }
        if self.event_windows.iter().any(|(s, e)| at >= *s && at < *e) {
            return SessionKind::Blackout;
        }
        let local = self.tz.from_utc_datetime(&at.naive_utc());
        let date = local.date_naive();
        // A holiday blacks out the whole local day.
        if self.holidays.contains(&date) {
            return SessionKind::Blackout;
        }
        let wd = local.weekday();
        let t = local.time();
        // Overnight session belongs to the *following* trading day; Friday
        // 20:00 → Monday 04:00 is treated as weekend.
        let is_weekend = match wd {
            Weekday::Sat => true,
            Weekday::Sun => true,
            Weekday::Fri => t >= NaiveTime::from_hms_opt(20, 0, 0).unwrap(),
            Weekday::Mon => t < NaiveTime::from_hms_opt(4, 0, 0).unwrap(),
            _ => false,
        };
        if is_weekend {
            return SessionKind::Weekend;
        }
        let h = |hh: u32, mm: u32| NaiveTime::from_hms_opt(hh, mm, 0).unwrap();
        if t >= h(9, 30) && t < h(16, 0) {
            SessionKind::Regular
        } else if t >= h(4, 0) && t < h(9, 30) {
            SessionKind::Pre
        } else if t >= h(16, 0) && t < h(20, 0) {
            SessionKind::Post
        } else {
            SessionKind::Overnight
        }
    }

    /// Whether new risk may be added in this session (subject to sample checks).
    pub fn is_tradable(&self, k: SessionKind) -> bool {
        self.tradable.contains(&k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cal() -> SessionCalendar {
        SessionCalendar::from_config(&config::Session::default()).unwrap()
    }

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn classifies_us_sessions() {
        let c = cal();
        // Wed 2026-09-16 14:00 UTC = 10:00 EDT → regular
        assert_eq!(c.classify(utc("2026-09-16T14:00:00Z")), SessionKind::Regular);
        // 08:00 EDT → pre
        assert_eq!(c.classify(utc("2026-09-16T12:00:00Z")), SessionKind::Pre);
        // 17:00 EDT → post
        assert_eq!(c.classify(utc("2026-09-16T21:00:00Z")), SessionKind::Post);
        // 02:00 EDT Thursday → overnight
        assert_eq!(c.classify(utc("2026-09-17T06:00:00Z")), SessionKind::Overnight);
        // Saturday → weekend
        assert_eq!(c.classify(utc("2026-09-19T15:00:00Z")), SessionKind::Weekend);
        // Friday 21:00 EDT → weekend
        assert_eq!(c.classify(utc("2026-09-19T01:00:00Z")), SessionKind::Weekend);
        assert!(!c.is_tradable(SessionKind::Weekend));
        assert!(c.is_tradable(SessionKind::Regular));
    }

    #[test]
    fn holidays_and_events_blackout() {
        let mut cfg = config::Session::default();
        cfg.holidays = vec!["2026-09-16".into()];
        cfg.event_windows = vec![["2026-09-17T10:00:00Z".into(), "2026-09-17T11:00:00Z".into()]];
        let c = SessionCalendar::from_config(&cfg).unwrap();
        assert_eq!(c.classify(utc("2026-09-16T14:00:00Z")), SessionKind::Blackout);
        assert_eq!(c.classify(utc("2026-09-17T10:30:00Z")), SessionKind::Blackout);
        assert_eq!(c.classify(utc("2026-09-17T11:30:00Z")), SessionKind::Pre);
    }
}
