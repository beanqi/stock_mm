//! Shared WebSocket plumbing.

use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type WsSink = SplitSink<WsStream, Message>;
pub type WsSource = SplitStream<WsStream>;

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn connect(url: &str) -> Result<(WsSink, WsSource)> {
    let (ws, _resp) = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(url))
        .await
        .context("ws connect timeout")?
        .with_context(|| format!("ws connect {url}"))?;
    let (sink, source) = ws.split();
    Ok((sink, source))
}

pub async fn send_text(sink: &mut WsSink, text: String) -> Result<()> {
    sink.send(Message::Text(text.into())).await.context("ws send")
}

/// Extract UTF-8 text from a message (text or binary frames).
pub fn message_text(msg: &Message) -> Option<String> {
    match msg {
        Message::Text(t) => Some(t.to_string()),
        Message::Binary(b) => String::from_utf8(b.to_vec()).ok(),
        _ => None,
    }
}
