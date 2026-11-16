use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, FixedOffset};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::Instant};

pub const CHANNELS: [(&str, &str); 12] = [
    ("main", "主频道"),
    ("tradfi", "传统财经"),
    ("binance", "币安公告"),
    ("korea", "韩国交易所"),
    ("pricechange", "价格异动"),
    ("oi", "持仓异动"),
    ("major", "主流交易所"),
    ("tier2", "二线交易所"),
    ("tier3", "三线交易所"),
    ("alpha", "Alpha"),
    ("cz", "CZ 与何一"),
    ("earn", "理财"),
];

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Story {
    pub no: i64,
    pub time: String,
    #[serde(default)]
    pub channels: Vec<String>,
    pub source: Option<String>,
    pub zh: Option<String>,
    pub en: Option<String>,
    #[serde(default)]
    pub coins: Vec<String>,
    pub url: String,
    #[serde(default)]
    pub deployment_test: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, Ord, PartialEq, PartialOrd)]
pub struct Cursor(pub i64, pub i64);

impl Story {
    pub fn cursor(&self) -> Result<Cursor> {
        Ok(Cursor(
            DateTime::parse_from_rfc3339(&self.time)?.timestamp_millis(),
            self.no,
        ))
    }

    pub fn formatted(&self) -> Result<String> {
        let offset = FixedOffset::east_opt(7 * 3600).unwrap();
        let time = DateTime::parse_from_rfc3339(&self.time)?.with_timezone(&offset);
        let labels = self
            .channels
            .iter()
            .map(|key| {
                CHANNELS
                    .iter()
                    .find(|(id, _)| id == key)
                    .map(|(_, name)| *name)
                    .unwrap_or(key)
            })
            .collect::<Vec<_>>()
            .join(" · ");
        let headline = self
            .zh
            .as_deref()
            .filter(|s| !s.is_empty())
            .or(self.en.as_deref())
            .unwrap_or("暂无中文内容");
        let mut body = headline.chars().take(1700).collect::<String>();
        if headline.chars().count() > 1700 {
            body.push_str("…（全文见链接）");
        }
        let mut text = format!(
            "<b>{}</b> · {} UTC+7\n\n{}",
            escape(&labels),
            time.format("%m月%d日 %H:%M"),
            escape(&body)
        );
        if !self.coins.is_empty() {
            let coins = self
                .coins
                .iter()
                .take(10)
                .map(|coin| format!("${coin}"))
                .collect::<Vec<_>>()
                .join(" ");
            text.push_str(&format!(
                "\n\n{}",
                escape(&coins.chars().take(120).collect::<String>())
            ));
        }
        let source = self
            .source
            .as_deref()
            .unwrap_or("EquationNews")
            .chars()
            .take(80)
            .collect::<String>();
        text.push_str(&format!("\n\n来源 {}", escape(&source)));
        if self.deployment_test {
            text = format!("<b>GCP 上线验证 · 最近一条真实快讯</b>\n\n{text}");
        }
        Ok(text)
    }
}

pub fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[derive(Deserialize)]
pub struct NewsPage {
    pub items: Vec<Story>,
    pub next_cursor: Option<String>,
}

#[derive(Clone)]
pub struct Api {
    client: Client,
    token: String,
    pub owner: i64,
    send_after: Arc<Mutex<Instant>>,
    news_after: Arc<Mutex<Instant>>,
}

impl Api {
    pub fn new(token: String, owner: i64) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(40))
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("eqn-telegram/0.1")
            .build()
            .context("HTTP client initialization failed")?;
        Ok(Self {
            client,
            token,
            owner,
            send_after: Arc::new(Mutex::new(Instant::now())),
            news_after: Arc::new(Mutex::new(Instant::now())),
        })
    }

    async fn post(&self, url: &str, body: &Value) -> Result<Value> {
        // Reqwest error URLs may contain the bot token. Never propagate those URLs.
        let response = self
            .client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|e| anyhow!("HTTP transport failed (timeout={})", e.is_timeout()))?;
        let status = response.status();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(2);
        let body = response.json::<Value>().await;
        if status.as_u16() == 429 {
            let wait = body
                .as_ref()
                .ok()
                .and_then(|value| value["parameters"]["retry_after"].as_u64())
                .unwrap_or(retry_after);
            tokio::time::sleep(Duration::from_secs(wait)).await;
            bail!("HTTP rate limit");
        }
        let body = body.map_err(|_| anyhow!("Invalid JSON response, HTTP {status}"))?;
        if !status.is_success() {
            if let Some(wait) = body["parameters"]["retry_after"].as_u64() {
                tokio::time::sleep(Duration::from_secs(wait)).await;
            }
            bail!("HTTP status {status}");
        }
        Ok(body)
    }

    pub async fn telegram(&self, method: &str, body: Value) -> Result<Value> {
        let result = self
            .post(
                &format!("https://api.telegram.org/bot{}/{method}", self.token),
                &body,
            )
            .await?;
        if result["ok"] != true {
            bail!("Telegram API error {}", result["error_code"]);
        }
        Ok(result["result"].clone())
    }

    pub async fn news(
        &self,
        channel: &str,
        query: Option<&str>,
        before: Option<&str>,
        limit: usize,
    ) -> Result<NewsPage> {
        let mut next = self.news_after.lock().await;
        tokio::time::sleep_until(*next).await;
        let mut args = json!({"channel": channel, "limit": limit});
        if let Some(query) = query {
            args["query"] = json!(query);
        }
        if let Some(before) = before {
            args["before"] = json!(before);
        }
        let result = self
            .post(
                "https://api.eqn.news/public/mcp",
                &json!({
                    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": "list_news", "arguments": args}
                }),
            )
            .await;
        *next = Instant::now() + Duration::from_millis(250);
        let result = result?;
        if !result["error"].is_null() || result["result"]["isError"] == true {
            bail!("EquationNews API error");
        }
        let result = &result["result"];
        let data = if result["structuredContent"].is_object() {
            result["structuredContent"].clone()
        } else {
            serde_json::from_str(
                result["content"][0]["text"]
                    .as_str()
                    .context("Missing news content")?,
            )?
        };
        serde_json::from_value(data).context("Invalid news schema")
    }

    pub async fn send(&self, text: String, keyboard: Value) -> Result<i64> {
        let mut next = self.send_after.lock().await;
        tokio::time::sleep_until(*next).await;
        let mut body = json!({"chat_id": self.owner, "text": text, "parse_mode": "HTML",
            "link_preview_options": {"is_disabled": true}});
        if !keyboard.is_null() {
            body["reply_markup"] = json!({"inline_keyboard": keyboard});
        }
        let result = self.telegram("sendMessage", body).await;
        *next = Instant::now() + Duration::from_millis(1100);
        result?["message_id"]
            .as_i64()
            .context("Missing Telegram message ID")
    }

    pub async fn send_story(&self, item: &Story) -> Result<i64> {
        self.send(
            item.formatted()?,
            json!([[{"text": "查看全文 ↗", "url": item.url}]]),
        )
        .await
    }
}
