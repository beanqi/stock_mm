//! Shared WebSocket plumbing.

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config, connect_async,
};
use tracing::warn;

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type WsSink = SplitSink<WsStream, Message>;
pub type WsSource = SplitStream<WsStream>;

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RESOLVE_GAP: Duration = Duration::from_millis(80);

pub async fn connect(url: &str) -> Result<(WsSink, WsSource)> {
    let (ws, _resp) = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(url))
        .await
        .context("ws connect timeout")?
        .with_context(|| format!("ws connect {url}"))?;
    let (sink, source) = ws.split();
    Ok((sink, source))
}

/// Connect to `url` but pin the TCP session to `ip`.
///
/// TLS SNI and the HTTP Host header stay on the original hostname so the
/// certificate still validates; only the socket destination is overridden.
pub async fn connect_via_ip(url: &str, ip: IpAddr) -> Result<(WsSink, WsSource)> {
    let (_, port, tls) = parse_ws_authority(url)?;
    anyhow::ensure!(
        tls,
        "connect_via_ip requires wss (TLS SNI must stay on the hostname)"
    );
    let request = url
        .into_client_request()
        .with_context(|| format!("ws request {url}"))?;
    let addr = SocketAddr::new(ip, port);
    let handshake = async {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("tcp {addr}"))?;
        let _ = stream.set_nodelay(true);
        let (ws, _) = client_async_tls_with_config(request, stream, None, None)
            .await
            .with_context(|| format!("ws handshake {url} via {ip}"))?;
        Ok::<_, anyhow::Error>(ws)
    };
    let ws = tokio::time::timeout(CONNECT_TIMEOUT, handshake)
        .await
        .context("ws connect timeout")??;
    let (sink, source) = ws.split();
    Ok((sink, source))
}

/// Host, port and whether the URL is `wss`.
pub fn parse_ws_authority(url: &str) -> Result<(String, u16, bool)> {
    let (rest, tls) = if let Some(r) = url.strip_prefix("wss://") {
        (r, true)
    } else if let Some(r) = url.strip_prefix("ws://") {
        (r, false)
    } else {
        anyhow::bail!("unsupported ws url: {url}");
    };
    let authority = rest.split(['/', '?']).next().unwrap_or(rest);
    let default_port = if tls { 443 } else { 80 };
    let (host, port) = if let Some(inner) = authority.strip_prefix('[') {
        let (host, rest) = inner.split_once(']').context("bad ipv6 ws url")?;
        let port = match rest.strip_prefix(':') {
            Some(p) if !p.is_empty() => p.parse().context("bad ws port")?,
            _ => default_port,
        };
        (host.to_string(), port)
    } else if let Some((h, p)) = authority.rsplit_once(':') {
        if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) {
            (h.to_string(), p.parse().context("bad ws port")?)
        } else {
            (authority.to_string(), default_port)
        }
    } else {
        (authority.to_string(), default_port)
    };
    anyhow::ensure!(!host.is_empty(), "empty ws host");
    Ok((host, port, tls))
}

/// Resolve `host` repeatedly and collect up to `want` distinct IPs.
///
/// DNS answers are often rotated; several lookups (with a short gap) usually
/// surface more backends than a single `getaddrinfo` call. IPv4 is preferred
/// when enough addresses are available.
pub async fn resolve_unique_ips(
    host: &str,
    port: u16,
    want: usize,
    max_lookups: usize,
) -> Vec<IpAddr> {
    if want == 0 {
        return Vec::new();
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return vec![ip];
    }
    let mut seen = HashSet::new();
    let mut ips = Vec::new();
    for attempt in 0..max_lookups {
        match tokio::net::lookup_host((host, port)).await {
            Ok(addrs) => {
                for addr in addrs {
                    if seen.insert(addr.ip()) {
                        ips.push(addr.ip());
                    }
                }
            }
            Err(e) => warn!(host, port, attempt, error = %e, "dns lookup failed"),
        }
        if ips.len() >= want {
            break;
        }
        if attempt + 1 < max_lookups {
            tokio::time::sleep(RESOLVE_GAP).await;
        }
    }
    finalize_ips(ips, want)
}

pub fn finalize_ips(ips: Vec<IpAddr>, want: usize) -> Vec<IpAddr> {
    let mut v4 = Vec::new();
    let mut v6 = Vec::new();
    for ip in ips {
        if ip.is_ipv4() {
            v4.push(ip);
        } else {
            v6.push(ip);
        }
    }
    if v4.len() >= want {
        v4.truncate(want);
        v4
    } else {
        v4.extend(v6);
        v4.truncate(want);
        v4
    }
}

pub async fn send_text(sink: &mut WsSink, text: String) -> Result<()> {
    sink.send(Message::Text(text.into()))
        .await
        .context("ws send")
}

/// Extract UTF-8 text from a message (text or binary frames).
pub fn message_text(msg: &Message) -> Option<String> {
    match msg {
        Message::Text(t) => Some(t.to_string()),
        Message::Binary(b) => String::from_utf8(b.to_vec()).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn parses_wss_host_and_default_port() {
        let (host, port, tls) = parse_ws_authority("wss://fx-ws.gateio.ws/v4/ws/usdt").unwrap();
        assert_eq!(host, "fx-ws.gateio.ws");
        assert_eq!(port, 443);
        assert!(tls);
    }

    #[test]
    fn parses_explicit_port_and_ipv6() {
        let (host, port, tls) = parse_ws_authority("ws://example.com:9001/foo").unwrap();
        assert_eq!((host, port, tls), ("example.com".into(), 9001, false));
        let (host, port, tls) = parse_ws_authority("wss://[2001:db8::1]:8443/v4").unwrap();
        assert_eq!(host, "2001:db8::1");
        assert_eq!(port, 8443);
        assert!(tls);
    }

    #[test]
    fn finalize_prefers_ipv4_when_enough() {
        let ips = vec![
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(1, 1, 1, 2)),
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        ];
        let out = finalize_ips(ips, 2);
        assert_eq!(
            out,
            vec![
                IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
                IpAddr::V4(Ipv4Addr::new(1, 1, 1, 2))
            ]
        );
    }
}
