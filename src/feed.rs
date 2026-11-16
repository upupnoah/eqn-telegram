use crate::{
    api::{Api, Cursor, HttpFailure, NewsPage, Story},
    store::Store,
};
use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt, stream::FuturesUnordered};
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

pub const CHECK_SECONDS: u64 = 2;

pub struct Worker {
    pub api: Api,
    pub store: Store,
    pub stop: CancellationToken,
    pub wake: Notify,
    pub delivery_ready: Notify,
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
            delivery_ready: Notify::new(),
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
        if self.store.get::<Cursor>("overlap_floor")?.is_none() {
            let cursor = self
                .store
                .get::<Cursor>("cursor")?
                .context("Missing news cursor")?;
            self.store.set("overlap_floor", &cursor)?;
        }
        Ok(())
    }

    async fn sync_once(&self) -> Result<usize> {
        let cursor = self
            .store
            .get::<Cursor>("cursor")?
            .context("Missing news cursor")?;
        // Re-read a short overlap so a late-indexed item behind the watermark is still seen.
        let initial = self
            .store
            .get::<Cursor>("overlap_floor")?
            .context("Missing overlap floor")?;
        let since = Cursor(chrono::Utc::now().timestamp_millis() - 60_000, 0)
            .min(cursor)
            .max(initial);
        let (items, newest) = collect_news(since, |before| {
            let api = self.api.clone();
            async move { api.news("all", None, before.as_deref(), 12).await }
        })
        .await?;
        let added = if items.is_empty() {
            0
        } else {
            self.store
                .commit(&items, (newest > cursor).then_some(newest))?
        };
        if added > 0 {
            self.delivery_ready.notify_one();
        }
        self.last_sync
            .store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
        Ok(added)
    }

    pub async fn ingest(self: Arc<Self>) -> Result<()> {
        let mut cleanup = Instant::now();
        let mut delay = CHECK_SECONDS;
        let mut follow_up = false;
        loop {
            if self.stop.is_cancelled() {
                break;
            }
            if Instant::now() >= cleanup {
                self.store.prune()?;
                cleanup = Instant::now() + Duration::from_secs(3600);
            }
            let result = tokio::select! {
                _ = self.stop.cancelled() => break,
                result = self.sync_once() => result,
            };
            let failed = result.is_err();
            match result {
                Ok(count) => {
                    if count > 0 {
                        tracing::info!(count, "Queued new stories");
                    }
                    delay = CHECK_SECONDS;
                }
                Err(error) => {
                    delay = (delay * 2).min(60);
                    tracing::warn!(%error, delay, "News sync will retry");
                }
            }
            // The WebSocket can lead the Chinese index. Check once more after a short delay.
            let interval = if follow_up && !failed {
                Duration::from_millis(400)
            } else {
                Duration::from_secs(delay)
            };
            follow_up = tokio::select! {
                _ = self.stop.cancelled() => break,
                _ = self.wake.notified(), if !failed => true,
                _ = tokio::time::sleep(interval) => false,
            };
        }
        Ok(())
    }

    async fn send_one(&self, chat_id: i64, item: Story) -> Result<i64> {
        let started = Instant::now();
        match self.api.for_chat(chat_id).send_story(&item).await {
            Ok(message_id) => {
                self.store.acknowledge(chat_id, item.no, message_id)?;
                let source_age_ms = chrono::Utc::now().timestamp_millis() - item.cursor()?.0;
                tracing::info!(
                    story = item.no,
                    send_ms = started.elapsed().as_millis(),
                    source_age_ms,
                    "Story delivered"
                );
            }
            Err(error) => {
                let http = error.downcast_ref::<HttpFailure>();
                if http.is_some_and(|e| e.status == 403) {
                    self.store.set_paused(chat_id, true)?;
                    tracing::warn!("Telegram refused delivery; subscription paused");
                } else if http.is_some_and(|e| e.status == 400) {
                    self.store.quarantine(chat_id, item.no)?;
                    tracing::warn!(
                        story = item.no,
                        "Telegram rejected message; saved for manual retry"
                    );
                } else {
                    self.store
                        .defer(chat_id, item.no, http.and_then(|e| e.retry_after))?;
                    tracing::warn!(story=item.no, %error, "Recipient delivery deferred");
                }
            }
        }
        Ok(chat_id)
    }

    pub async fn deliver(self: Arc<Self>) -> Result<()> {
        let mut busy = Vec::new();
        let mut sends = FuturesUnordered::new();
        loop {
            while busy.len() < 16 && !self.stop.is_cancelled() {
                let Some((chat_id, item)) = self.store.pending(&busy)? else {
                    break;
                };
                busy.push(chat_id);
                sends.push(self.send_one(chat_id, item));
            }
            if self.stop.is_cancelled() {
                while let Some(sent) = sends.next().await {
                    sent?;
                }
                break;
            }
            tokio::select! {
                result = sends.next(), if !sends.is_empty() => {
                    let done = result.expect("in-flight delivery")?;
                    busy.retain(|id| *id != done);
                },
                _ = self.stop.cancelled() => {},
                _ = self.delivery_ready.notified() => {},
                // Also recover due retries or state written by another process.
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
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
