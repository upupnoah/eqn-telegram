mod api;
mod feed;
mod store;

use anyhow::{Context, Result, bail};
use api::{Api, CHANNELS, HttpFailure, clip, escape};
use feed::Worker;
use fs2::FileExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    env, fs,
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use store::Store;
use tokio::task::JoinSet;

#[derive(Default, Deserialize)]
struct Config {
    #[serde(default)]
    token: String,
    #[serde(default)]
    owner_id: i64,
    #[serde(default)]
    allowed_usernames: Vec<String>,
}

fn configuration() -> Result<(Config, PathBuf)> {
    let state_dir = env::var_os("EQN_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env::var_os("HOME").unwrap_or_default()).join(".config/eqn-telegram")
        });
    let config_path = env::var_os("EQN_CONFIG_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| state_dir.join("config.json"));
    let mut config: Config = if config_path.exists() {
        serde_json::from_slice(&fs::read(config_path)?)?
    } else {
        Config::default()
    };
    if let Ok(token) = env::var("TELEGRAM_BOT_TOKEN") {
        config.token = token;
    }
    if let Ok(owner) = env::var("TELEGRAM_OWNER_ID") {
        config.owner_id = owner.parse()?;
    }
    if config.token.is_empty() || config.owner_id == 0 {
        bail!("Telegram configuration is incomplete");
    }
    fs::create_dir_all(&state_dir)?;
    Ok((config, state_dir))
}

async fn menu(api: &Api, panel: Option<i64>) -> Result<()> {
    api.panel(panel, "<b>Noah 快讯</b>\n市场新闻、交易所公告与行情异动，覆盖方程式新闻 12 个频道。\n\n按下方按钮浏览快讯，或发送 <code>/search BTC</code> 搜索近 30 天的报道。".into(), json!([
        [{"text":"最新快讯","callback_data":"news:all"},{"text":"按频道浏览","callback_data":"channels"}],
        [{"text":"我的推送","callback_data":"status"}]
    ])).await?;
    Ok(())
}

async fn show_news(
    api: &Api,
    panel: Option<i64>,
    channel: &str,
    query: Option<&str>,
) -> Result<()> {
    let page = api.news(channel, query, None, 3).await?;
    let heading = match query {
        Some(query) => format!("搜索 · {}", escape(&clip(query, 60))),
        None => CHANNELS
            .iter()
            .find(|(id, _)| *id == channel)
            .map(|(_, name)| format!("{name} · 最新快讯"))
            .unwrap_or_else(|| "最新快讯".into()),
    };
    let mut text = format!("<b>{heading}</b>");
    let mut links = Vec::new();
    for (index, story) in page.items.iter().enumerate() {
        let time = chrono::DateTime::parse_from_rfc3339(&story.time)?
            .with_timezone(&chrono::FixedOffset::east_opt(7 * 3600).unwrap());
        text.push_str(&format!(
            "\n\n<i>{} · {} UTC+7</i>\n{}",
            story.label(),
            time.format("%m/%d %H:%M"),
            escape(&clip(story.headline(), 180))
        ));
        if let Some(url) = story.article_url() {
            links.push(json!({"text":format!("第 {} 条全文",index+1),"url":url}));
        }
    }
    if page.items.is_empty() {
        text.push_str("\n\n暂无匹配快讯，可以换个关键词或浏览其他频道。");
    }
    let mut buttons = Vec::new();
    if !links.is_empty() {
        buttons.push(links);
    }
    buttons.push(vec![json!({"text":"浏览频道","callback_data":"channels"})]);
    api.panel(panel, text, json!(buttons)).await?;
    Ok(())
}

fn elapsed(ms: i64) -> String {
    if ms < 1000 {
        format!("{ms} 毫秒")
    } else {
        format!("{:.1} 秒", ms as f64 / 1000.)
    }
}

async fn handle(worker: &Worker, update: &Value) -> Result<()> {
    let callback = &update["callback_query"];
    let (message, actor, command) = if callback.is_object() {
        (
            &callback["message"],
            &callback["from"],
            callback["data"].as_str().unwrap_or(""),
        )
    } else {
        let message = &update["message"];
        (
            message,
            &message["from"],
            message["text"].as_str().unwrap_or("").trim(),
        )
    };
    let Some(chat_id) = actor["id"].as_i64() else {
        return Ok(());
    };
    if message["chat"]["id"].as_i64() != Some(chat_id)
        || message["chat"]["type"].as_str() != Some("private")
        || !worker
            .store
            .authorize(chat_id, actor["username"].as_str())?
    {
        return Ok(());
    }
    let api = worker.api.for_chat(chat_id);
    if callback.is_object() {
        api.telegram(
            "answerCallbackQuery",
            json!({"callback_query_id":callback["id"]}),
        )
        .await?;
    }
    let panel = callback
        .get("message")
        .and_then(|m| m["message_id"].as_i64());
    match command {
        "/start" | "/help" | "menu" => menu(&api, panel).await?,
        "/latest" => show_news(&api, panel, "all", None).await?,
        "/channels" | "channels" => {
            let buttons = CHANNELS
                .iter()
                .map(|(id, label)| json!({"text":label,"callback_data":format!("news:{id}")}))
                .collect::<Vec<_>>();
            let mut rows = buttons
                .chunks(3)
                .map(|row| row.to_vec())
                .collect::<Vec<_>>();
            rows.push(vec![json!({"text":"返回菜单","callback_data":"menu"})]);
            api.panel(
                panel,
                "<b>频道目录</b>\n选择频道，查看最近三条快讯。".into(),
                json!(rows),
            )
            .await?;
        }
        "/pause" | "pause" => {
            worker.store.set_paused(chat_id, true)?;
            api.panel(
                panel,
                "<b>推送已暂停</b>\n新快讯会保留，恢复后将自动补发。".into(),
                json!([[{"text":"恢复推送","callback_data":"resume"}]]),
            )
            .await?;
        }
        "/resume" | "resume" => {
            worker.store.set_paused(chat_id, false)?;
            worker.delivery_ready.notify_one();
            api.panel(
                panel,
                "<b>推送已恢复</b>\n待发快讯将自动补发。".into(),
                json!([[{"text":"查看状态","callback_data":"status"}]]),
            )
            .await?;
        }
        "/status" | "status" => {
            let (pending, sent, failed) = worker.store.counts(chat_id)?;
            let paused = worker.store.paused(chat_id)?;
            let mode = if paused { "已暂停" } else { "已开启" };
            let connected = if worker.connected.load(Ordering::Relaxed) {
                "已连接"
            } else {
                "正在重连"
            };
            let stamp = worker.last_sync.load(Ordering::Relaxed);
            let age = chrono::Utc::now().timestamp().saturating_sub(stamp).max(0);
            let sync = if stamp == 0 {
                "正在首次同步".to_owned()
            } else if age > 15 {
                format!("已延迟 · {age} 秒前同步")
            } else {
                format!("{age} 秒前同步")
            };
            let mut text = format!(
                "<b>我的推送 · {mode}</b>\n待发 {pending} 条　·　近 30 天已发 {sent} 条\n\n<b>数据连接</b>\n主频道　{connected}\n全频道　{sync}"
            );
            if let Some((queue, total)) = worker.store.recent_latency(chat_id)? {
                text.push_str(&format!(
                    "\n\n<b>最近一次投递</b>\n入队至确认　{}\n发布至确认　{}",
                    elapsed(queue),
                    elapsed(total)
                ));
            }
            let action = if paused {
                ("恢复推送", "resume")
            } else {
                ("暂停推送", "pause")
            };
            let mut buttons = vec![vec![
                json!({"text":action.0,"callback_data":action.1}),
                json!({"text":"刷新状态","callback_data":"status"}),
            ]];
            if failed > 0 {
                text.push_str(&format!(
                    "\n\n有 {failed} 条消息被 Telegram 拒收，可手动重试。"
                ));
                buttons.push(vec![
                    json!({"text":"重试失败消息","callback_data":"retry_failed"}),
                ]);
            }
            api.panel(panel, text, json!(buttons)).await?;
        }
        "retry_failed" => {
            let count = worker.store.retry_failed(chat_id)?;
            worker.delivery_ready.notify_one();
            api.panel(
                panel,
                format!("已将 {count} 条快讯放回待发队列。"),
                Value::Null,
            )
            .await?;
        }
        "/search" => {
            api.panel(
                panel,
                "<b>搜索快讯</b>\n在命令后加上关键词，例如 <code>/search BTC</code>。".into(),
                Value::Null,
            )
            .await?;
        }
        _ => {
            if let Some(channel) = command.strip_prefix("news:") {
                if channel == "all" || CHANNELS.iter().any(|(id, _)| *id == channel) {
                    show_news(&api, panel, channel, None).await?;
                }
            } else if let Some(query) = command.strip_prefix("/search ") {
                let query = query.chars().take(200).collect::<String>();
                show_news(&api, panel, "all", Some(&query)).await?;
            } else {
                menu(&api, panel).await?;
            }
        }
    }
    Ok(())
}

async fn commands(worker: Arc<Worker>) -> Result<()> {
    let mut offset = worker.store.get::<i64>("telegram_offset")?.unwrap_or(0);
    let mut delay = 2;
    loop {
        let result = tokio::select! {
            _ = worker.stop.cancelled() => return Ok(()),
            result = worker.api.telegram("getUpdates", json!({"offset":offset,"timeout":25,"allowed_updates":["message","callback_query"]})) => result,
        };
        match result {
            Ok(updates) => {
                for update in updates.as_array().context("Invalid Telegram updates")? {
                    if let Err(error) = handle(&worker, update).await {
                        tracing::warn!(%error, "Command failed");
                        let message = update
                            .get("message")
                            .unwrap_or(&update["callback_query"]["message"]);
                        if let Some(chat_id) = message["chat"]["id"].as_i64()
                            && message["chat"]["type"] == "private"
                            && worker.store.authorize(chat_id, None)?
                        {
                            let _ = worker
                                .api
                                .for_chat(chat_id)
                                .send("这次查询未完成，请稍后重试。".into(), Value::Null)
                                .await;
                        }
                    }
                    offset = update["update_id"].as_i64().context("Missing update ID")? + 1;
                    worker.store.set("telegram_offset", &offset)?;
                }
                delay = 2;
            }
            Err(error) => {
                let wait = error
                    .downcast_ref::<HttpFailure>()
                    .and_then(|e| e.retry_after)
                    .unwrap_or(delay)
                    .max(delay)
                    .min(86400);
                tracing::warn!(%error, wait, "Telegram polling will retry");
                tokio::select! {
                    _ = worker.stop.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_secs(wait)) => {},
                }
                delay = (delay * 2).min(60);
            }
        }
    }
}

async fn configure(api: &Api) -> Result<()> {
    let commands = [
        ("latest", "查看最新三条快讯"),
        ("channels", "浏览 12 个频道"),
        ("search", "搜索关键词，例如 /search BTC"),
        ("pause", "暂停自动推送"),
        ("resume", "恢复并补发快讯"),
        ("status", "查看运行状态"),
        ("help", "显示查询菜单"),
    ];
    api.telegram("setMyCommands", json!({"commands":commands.iter().map(|(command,description)| json!({"command":command,"description":description})).collect::<Vec<_>>()})).await?;
    api.telegram("setMyDescription", json!({"description":"市场新闻、交易所公告与行情异动。订阅方程式新闻 12 个频道的中文快讯，按频道浏览，或用关键词搜索近 30 天的报道。"})).await?;
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! { result = tokio::signal::ctrl_c() => result?, _ = terminate.recv() => {} }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_ansi(false).init();
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let (config, state_dir) = configuration()?;
    let api = Api::new(config.token, config.owner_id)?;
    let mode = env::args().nth(1).unwrap_or_else(|| "run".into());
    if mode == "check" {
        let me = api.telegram("getMe", json!({})).await?;
        println!("Telegram bot verified: {}", me["username"]);
        for (channel, _) in CHANNELS {
            let page = api.news(channel, None, None, 1).await?;
            println!("{channel}: {} item(s)", page.items.len());
        }
        return Ok(());
    }
    if mode == "configure" {
        return configure(&api).await;
    }
    let store = Store::open(&state_dir.join("delivery-rust.sqlite3"))?;
    store.configure_recipients(config.owner_id, &config.allowed_usernames)?;
    if mode == "access" {
        for (username, activated) in store.invitations()? {
            println!(
                "@{username}: {}",
                if activated {
                    "activated"
                } else {
                    "awaiting Start"
                }
            );
        }
        return Ok(());
    }
    if mode == "smoke" {
        let mut page = api.news("main", None, None, 1).await?;
        let mut item = page.items.pop().context("No current story")?;
        item.deployment_test = true;
        let message_id = api.send_story(&item).await?;
        println!("Owner preview confirmed by Telegram: {message_id}");
        return Ok(());
    }
    if mode != "run" {
        bail!("Supported commands: run, check, configure, smoke, access");
    }
    let lock = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(state_dir.join("worker.lock"))?;
    lock.try_lock_exclusive()
        .context("Another worker owns this state directory")?;
    if store.get::<i64>("telegram_offset")?.is_none() && state_dir.join("offset.json").exists() {
        let offset: i64 = serde_json::from_slice(&fs::read(state_dir.join("offset.json"))?)?;
        store.set("telegram_offset", &offset)?;
    }
    let worker = Arc::new(Worker::new(api, store));
    worker.initialize().await?;
    let mut tasks = JoinSet::new();
    tasks.spawn(worker.clone().ingest());
    tasks.spawn(worker.clone().listen());
    tasks.spawn(worker.clone().deliver());
    tasks.spawn(commands(worker.clone()));
    tracing::info!("Rust realtime worker started; all 12 channels enabled");
    let result = tokio::select! {
        signal = shutdown_signal() => signal,
        task = tasks.join_next() => match task {
            Some(Ok(Err(error))) => Err(error.context("Background worker failed")),
            Some(Err(error)) => Err(error.into()),
            _ => Err(anyhow::anyhow!("Background worker exited unexpectedly")),
        }
    };
    worker.stop.cancel();
    let drain = async {
        while let Some(task) = tasks.join_next().await {
            task??;
        }
        Ok::<_, anyhow::Error>(())
    };
    match tokio::time::timeout(Duration::from_secs(45), drain).await {
        Ok(done) => done?,
        Err(_) => {
            tracing::warn!("Shutdown grace period elapsed; unacknowledged items remain queued");
            tasks.shutdown().await;
        }
    }
    drop(lock);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn group_messages_cannot_claim_an_invitation() {
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        store
            .configure_recipients(100, &["guest_user".into()])
            .unwrap();
        let api = Api::new("unused".into(), 100).unwrap();
        assert_eq!(api.for_chat(200).owner, 200);
        assert_eq!(api.owner, 100);
        let worker = Worker::new(api, store);
        handle(
            &worker,
            &json!({"message": {"from": {"id":200,"username":"guest_user"},
            "chat":{"id":-1,"type":"group"},"text":"/start"}}),
        )
        .await
        .unwrap();
        assert_eq!(
            worker.store.invitations().unwrap(),
            vec![("guest_user".into(), false)]
        );
        handle(
            &worker,
            &json!({"message": {"from": {"id":300,"username":"someone_else"},
            "chat":{"id":300,"type":"private"},"text":"/start"}}),
        )
        .await
        .unwrap();
        assert!(!worker.store.authorize(300, None).unwrap());
    }
}
