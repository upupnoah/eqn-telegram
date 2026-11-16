use crate::{
    api::{Api, Cursor, NewsPage, Story},
    store::Store,
};
use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::HashSet,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

pub struct Worker {
    pub api: Api,
    pub store: Store,
    pub stop: CancellationToken,
    pub wake: Notify,
    pub connected: AtomicBool,
    pub last_sync: AtomicI64,
    pub last_pong: AtomicI64,
}

pub async fn collect_news<F, Fut>(cursor: Cursor, mut fetch: F) -> Result<(Vec<Story>, Cursor)>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: Future<Output = Result<NewsPage>>,
{
    let mut before = None;
    let mut visited = HashSet::new();
    let mut items = Vec::new();
    let mut newest = cursor;
    loop {
        let page = fetch(before).await?;
        let mut reached = page.items.is_empty();
        for item in page.items {
            let key = item.cursor()?;
            if key > cursor {
                newest = newest.max(key);
                items.push(item);
            } else {
                reached = true;
            }
        }
        if reached || page.next_cursor.is_none() {
            break;
        }
        before = page.next_cursor;
        if !visited.insert(before.clone()) {
            bail!("Repeated pagination cursor");
        }
    }
    items.sort_by_key(|item| item.cursor().expect("validated timestamp"));
    items.dedup_by_key(|item| item.no);
    Ok((items, newest))
}

impl Worker {
    pub fn new(api: Api, store: Store) -> Self {
        Self {
            api,
            store,
            stop: CancellationToken::new(),
            wake: Notify::new(),
            connected: AtomicBool::new(false),
            last_sync: AtomicI64::new(0),
            last_pong: AtomicI64::new(0),
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        if self.store.get::<Cursor>("cursor")?.is_none() {
            let page = self.api.news("all", None, None, 1).await?;
            let cursor = page
                .items
                .first()
                .map(Story::cursor)
                .transpose()?
                .unwrap_or(Cursor(0, 0));
            self.store.set("cursor", &cursor)?;
            tracing::info!("Initial news cursor established");
        }
        Ok(())
    }

    async fn sync_once(&self) -> Result<usize> {
        let cursor = self
            .store
            .get::<Cursor>("cursor")?
            .context("Missing news cursor")?;
        let (items, newest) = collect_news(cursor, |before| {
            let api = self.api.clone();
            async move { api.news("all", None, before.as_deref(), 50).await }
        })
        .await?;
        self.store.commit(&items, Some(newest))?;
        self.last_sync
            .store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
        Ok(items.len())
    }

    pub async fn ingest(self: Arc<Self>) -> Result<()> {
        while !self.stop.is_cancelled() {
            match self.sync_once().await {
                Ok(count) if count > 0 => tracing::info!(count, "Queued new stories"),
                Ok(_) => {}
                Err(error) => tracing::warn!(%error, "News sync will retry"),
            }
            tokio::select! {
                _ = self.stop.cancelled() => break,
                _ = self.wake.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(10)) => {},
            }
        }
        Ok(())
    }

    pub async fn deliver(self: Arc<Self>) -> Result<()> {
        let mut delay = 2;
        while !self.stop.is_cancelled() {
            if !self.store.get::<bool>("paused")?.unwrap_or(false)
                && let Some(item) = self.store.pending()?
            {
                // Finish and persist a successful send before responding to shutdown.
                match self.api.send_story(&item).await {
                    Ok(message_id) => {
                        self.store.acknowledge(item.no, message_id)?;
                        tracing::info!(story = item.no, message_id, "Story delivered");
                        delay = 2;
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(story=item.no, %error, "Delivery remains queued");
                        tokio::select! {
                            _ = self.stop.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_secs(delay)) => {},
                        }
                        delay = (delay * 2).min(60);
                    }
                }
            }
            tokio::select! {
                _ = self.stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(250)) => {},
            }
        }
        Ok(())
    }

    async fn listen_connection(&self) -> Result<()> {
        let (mut socket, _) = tokio::time::timeout(
            Duration::from_secs(20),
            connect_async("wss://api.eqn.news/public/ws"),
        )
        .await??;
        self.connected.store(true, Ordering::Relaxed);
        self.wake.notify_one();
        tracing::info!("EquationNews WebSocket connected");
        let mut ping = tokio::time::interval(Duration::from_secs(30));
        let mut last_received = Instant::now();
        loop {
            tokio::select! {
                _ = self.stop.cancelled() => {
                    let _ = tokio::time::timeout(Duration::from_secs(2), socket.close(None)).await;
                    return Ok(());
                },
                _ = ping.tick() => {
                    if last_received.elapsed() > Duration::from_secs(90) { bail!("WebSocket heartbeat timeout"); }
                    tokio::time::timeout(Duration::from_secs(10), socket.send(Message::text("ping"))).await??;
                },
                message = socket.next() => {
                    match message.context("WebSocket closed")?? {
                        Message::Text(text) => {
                            last_received = Instant::now();
                            if text == "pong" {
                                if self.last_pong.swap(chrono::Utc::now().timestamp(), Ordering::Relaxed) == 0 {
                                    tracing::info!("EquationNews heartbeat verified");
                                }
                            } else {
                                let event: serde_json::Value = serde_json::from_str(&text)?;
                                if event.get("news_title").is_some() {
                                    self.wake.notify_one();
                                    tracing::info!("WebSocket triggered news sync");
                                }
                            }
                        },
                        Message::Close(_) => bail!("WebSocket closed by server"),
                        _ => {},
                    }
                }
            }
        }
    }

    pub async fn listen(self: Arc<Self>) -> Result<()> {
        let mut delay = 1;
        while !self.stop.is_cancelled() {
            let started = Instant::now();
            if let Err(error) = self.listen_connection().await {
                tracing::warn!(%error, "WebSocket reconnecting");
            }
            self.connected.store(false, Ordering::Relaxed);
            if started.elapsed() > Duration::from_secs(60) {
                delay = 1;
            }
            let jitter = chrono::Utc::now().timestamp_subsec_millis() as u64;
            tokio::select! {
                _ = self.stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(delay * 1000 + jitter)) => {},
            }
            delay = (delay * 2).min(30);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn page(ids: &[i64], cursor: Option<&str>) -> NewsPage {
        serde_json::from_value(json!({"items": ids.iter().map(|id| json!({
            "no": id, "time": "2026-11-17T00:00:00Z", "url": "https://eqn.news/main/1"
        })).collect::<Vec<_>>(), "next_cursor": cursor}))
        .unwrap()
    }

    #[tokio::test]
    async fn catch_up_spans_multiple_pages() {
        let cursor = page(&[1], None).items[0].cursor().unwrap();
        let (items, newest) = collect_news(cursor, |before| {
            std::future::ready(Ok(match before.as_deref() {
                None => page(&[5, 4], Some("older")),
                Some("older") => page(&[3, 2], Some("oldest")),
                _ => page(&[1], None),
            }))
        })
        .await
        .unwrap();
        assert_eq!(
            items.iter().map(|item| item.no).collect::<Vec<_>>(),
            [2, 3, 4, 5]
        );
        assert_eq!(newest.1, 5);
    }

    #[tokio::test]
    async fn failed_page_prevents_partial_result() {
        let result = collect_news(Cursor(0, 0), |before| {
            std::future::ready(match before {
                None => Ok(page(&[5], Some("older"))),
                Some(_) => Err(anyhow::anyhow!("temporary failure")),
            })
        })
        .await;
        assert!(result.is_err());
    }
}
