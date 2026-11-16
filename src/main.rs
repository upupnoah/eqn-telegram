mod api;
mod feed;
mod store;

use anyhow::{Context, Result, bail};
use api::{Api, CHANNELS};
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

async fn menu(api: &Api) -> Result<()> {
    api.send("<b>Noah 新闻助手 · Rust 实时推送</b>\n\n已接入方程式新闻 12 个频道。\n主频道由 WebSocket 触发，其余频道每 10 秒检查。\n\n/latest 最新三条\n/channels 浏览频道\n/search 关键词\n/pause 暂停推送\n/resume 恢复并补发\n/status 运行状态\n\n时间显示为越南时间 UTC+7。".into(), json!([
        [{"text":"📰 最新快讯","callback_data":"news:all"},{"text":"📂 12 个频道","callback_data":"channels"}],
        [{"text":"主频道","callback_data":"news:main"},{"text":"币安公告","callback_data":"news:binance"},{"text":"韩国交易所","callback_data":"news:korea"}]
    ])).await?;
    Ok(())
}

async fn show_news(api: &Api, channel: &str, query: Option<&str>) -> Result<()> {
    let page = api.news(channel, query, None, 3).await?;
    if page.items.is_empty() {
        api.send("该频道或关键词暂时没有可用快讯。".into(), Value::Null)
            .await?;
    }
    for item in page.items {
        api.send_story(&item).await?;
    }
    Ok(())
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
    if actor["id"].as_i64() != Some(worker.api.owner)
        || message["chat"]["id"].as_i64() != Some(worker.api.owner)
    {
        return Ok(());
    }
    if callback.is_object() {
        worker
            .api
            .telegram(
                "answerCallbackQuery",
                json!({"callback_query_id":callback["id"]}),
            )
            .await?;
    }
    match command {
        "/start" | "/help" => menu(&worker.api).await?,
        "/latest" => show_news(&worker.api, "all", None).await?,
        "/channels" | "channels" => {
            let buttons = CHANNELS
                .iter()
                .map(|(id, label)| json!({"text":label,"callback_data":format!("news:{id}")}))
                .collect::<Vec<_>>();
            let rows = buttons
                .chunks(3)
                .map(|row| row.to_vec())
                .collect::<Vec<_>>();
            worker
                .api
                .send("选择要查看的频道".into(), json!(rows))
                .await?;
        }
        "/pause" => {
            worker.store.set("paused", &true)?;
            worker
                .api
                .send(
                    "自动推送已暂停。发送 /resume 可恢复并补发积压快讯。".into(),
                    Value::Null,
                )
                .await?;
        }
        "/resume" => {
            worker.store.set("paused", &false)?;
            worker
                .api
                .send("自动推送已恢复，正在补发积压快讯。".into(), Value::Null)
                .await?;
        }
        "/status" => {
            let (pending, sent) = worker.store.counts()?;
            let mode = if worker.store.get::<bool>("paused")?.unwrap_or(false) {
                "已暂停，恢复后补发"
            } else {
                "推送已开启"
            };
            let connected = if worker.connected.load(Ordering::Relaxed) {
                "已连接"
            } else {
                "重连中，定时检查继续运行"
            };
            let timestamp = worker.last_sync.load(Ordering::Relaxed);
            let last_sync = chrono::DateTime::from_timestamp(timestamp, 0)
                .context("Invalid sync timestamp")?
                .with_timezone(&chrono::FixedOffset::east_opt(7 * 3600).unwrap())
                .format("%m月%d日 %H:%M:%S");
            worker.api.send(format!("<b>Rust 实时推送状态</b>\n\n{mode}\n主频道 WebSocket {connected}\n12 个频道每 10 秒同步\n最近同步 {last_sync} UTC+7\n待发送 {pending} 条，近 30 天已发送 {sent} 条"), Value::Null).await?;
        }
        _ => {
            if let Some(channel) = command.strip_prefix("news:") {
                if channel == "all" || CHANNELS.iter().any(|(id, _)| *id == channel) {
                    show_news(&worker.api, channel, None).await?;
                }
            } else if let Some(query) = command.strip_prefix("/search ") {
                let query = query.chars().take(200).collect::<String>();
                show_news(&worker.api, "all", Some(&query)).await?;
            } else {
                menu(&worker.api).await?;
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
                        worker
                            .api
                            .send("这次查询未完成，请稍后重试。".into(), Value::Null)
                            .await?;
                    }
                    offset = update["update_id"].as_i64().context("Missing update ID")? + 1;
                    worker.store.set("telegram_offset", &offset)?;
                }
                delay = 2;
            }
            Err(error) => {
                tracing::warn!(%error, "Telegram polling will retry");
                tokio::select! {
                    _ = worker.stop.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_secs(delay)) => {},
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
    api.telegram("setMyDescription", json!({"description":"个人中文新闻助手 · Rust 实时推送版。接入方程式新闻 12 个频道，支持最新快讯、关键词查询、暂停和恢复。"})).await?;
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
    if mode == "smoke" {
        let mut page = api.news("main", None, None, 1).await?;
        let mut item = page.items.pop().context("No current story")?;
        item.deployment_test = true;
        store.commit(&[item], None)?;
        println!("Deployment verification queued");
        return Ok(());
    }
    if mode != "run" {
        bail!("Supported commands: run, check, configure, smoke");
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
        task = tasks.join_next() => {
            task.context("No background worker")??.context("Background worker failed")?;
            Err(anyhow::anyhow!("Background worker exited unexpectedly"))
        }
    };
    worker.stop.cancel();
    while let Some(task) = tasks.join_next().await {
        task??;
    }
    drop(lock);
    result
}
