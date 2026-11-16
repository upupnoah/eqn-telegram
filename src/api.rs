use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, FixedOffset};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashMap, fmt, sync::Arc, time::Duration};
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
    pub source_url: Option<String>,
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

    pub fn label(&self) -> String {
        let mut labels = self
            .channels
            .iter()
            .filter(|c| c.as_str() != "main")
            .filter_map(|key| {
                CHANNELS
                    .iter()
                    .find(|(id, _)| id == key)
                    .map(|(_, label)| *label)
            })
            .take(2)
            .collect::<Vec<_>>();
        if labels.is_empty() {
            labels.push("综合快讯");
        }
        labels.join(" · ")
    }

    fn body(&self) -> &str {
        [self.zh.as_deref(), self.en.as_deref()]
            .into_iter()
            .flatten()
            .map(str::trim)
            .find(|text| !text.is_empty())
            .unwrap_or("查看原文了解详情")
    }

    pub fn headline(&self) -> &str {
        self.body()
            .lines()
            .next()
            .unwrap_or_default()
            .trim_matches('*')
            .trim()
    }

    pub fn formatted(&self) -> Result<String> {
        let time = DateTime::parse_from_rfc3339(&self.time)?
            .with_timezone(&FixedOffset::east_opt(7 * 3600).unwrap());
        let mut text = format!(
            "<i>{}</i>\n<b>{}</b>",
            self.label(),
            escape(&clip(self.headline(), 230))
        );
        let remaining = self
            .body()
            .split_once('\n')
            .map(|(_, rest)| rest.trim())
            .unwrap_or("");
        // Keep the source's full first line when it exceeds the headline allowance.
        let extra = self.headline().chars().skip(230).collect::<String>();
        let detail = [extra.trim(), remaining]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n");
        if !detail.is_empty() {
            text.push_str(&format!("\n\n{}", escape(&clip(&detail, 980))));
        }
        let coins = self
            .coins
            .iter()
            .take(6)
            .map(|c| c.trim_start_matches('$'))
            .filter(|c| {
                !c.is_empty() && c.len() <= 20 && c.bytes().all(|b| b.is_ascii_alphanumeric())
            })
            .map(|c| format!("${}", escape(c)))
            .collect::<Vec<_>>();
        if !coins.is_empty() {
            text.push_str(&format!("\n\n{}", coins.join("  ")));
        }
        let source = self
            .source
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("EquationNews");
        text.push_str(&format!(
            "\n\n<i>{} · {} UTC+7</i>",
            escape(&clip(source, 64)),
            time.format("%m/%d %H:%M:%S")
        ));
        if self.deployment_test {
            text = format!("<b>版式预览</b>\n\n{text}");
        }
        Ok(text)
    }

    pub fn article_url(&self) -> Option<&str> {
        web_url(&self.url).map(|_| self.url.as_str())
    }

    pub fn keyboard(&self) -> Value {
        let mut buttons = Vec::new();
        if self.article_url().is_some() {
            buttons.push(json!({"text":"阅读全文","url":self.url}));
        }
        if let Some(url) = self
            .source_url
            .as_deref()
            .filter(|s| *s != self.url && web_url(s).is_some())
        {
            buttons.push(json!({"text":"查看来源","url":url}));
        }
        if buttons.is_empty() {
            Value::Null
        } else {
            json!([buttons])
        }
    }
}

fn web_url(s: &str) -> Option<reqwest::Url> {
    reqwest::Url::parse(s).ok().filter(|u| {
        matches!(u.scheme(), "http" | "https")
            && u.host_str().is_some()
            && u.username().is_empty()
            && u.password().is_none()
            && s.len() <= 2048
    })
}

pub fn clip(s: &str, n: usize) -> String {
    let mut chars = s.chars();
    let mut result: String = chars.by_ref().take(n).collect();
    if chars.next().is_some() {
        result.push('…');
    }
    result
}

#[derive(Debug)]
pub struct HttpFailure {
    pub status: u16,
    pub retry_after: Option<u64>,
    unchanged: bool,
}
impl fmt::Display for HttpFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HTTP status {}", self.status)
    }
}
impl std::error::Error for HttpFailure {}

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
    chat_after: Arc<Mutex<HashMap<i64, Arc<Mutex<Instant>>>>>,
    news_after: Arc<Mutex<Instant>>,
}

impl Api {
    pub fn for_chat(&self, chat_id: i64) -> Self {
        Self {
            owner: chat_id,
            ..self.clone()
        }
    }

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
            chat_after: Arc::default(),
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
            .and_then(|v| v.parse::<u64>().ok());
        let body = response.json::<Value>().await;
        if !status.is_success() {
            let wait = body
                .as_ref()
                .ok()
                .and_then(|v| v["parameters"]["retry_after"].as_u64())
                .or(retry_after);
            return Err(HttpFailure {
                status: status.as_u16(),
                retry_after: wait,
                unchanged: body
                    .as_ref()
                    .ok()
                    .and_then(|v| v["description"].as_str())
                    .is_some_and(|v| v.contains("message is not modified")),
            }
            .into());
        }
        let body = body.map_err(|_| anyhow!("Invalid JSON response, HTTP {status}"))?;
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
        let cooldown = result
            .as_ref()
            .err()
            .and_then(|e| e.downcast_ref::<HttpFailure>())
            .filter(|e| e.status == 429)
            .map(|e| Duration::from_secs(e.retry_after.unwrap_or(2).clamp(1, 86400)))
            .unwrap_or(Duration::from_millis(250));
        *next = Instant::now() + cooldown;
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
        self.panel(None, text, keyboard).await
    }

    pub async fn panel(&self, message: Option<i64>, text: String, keyboard: Value) -> Result<i64> {
        let lane = self
            .chat_after
            .lock()
            .await
            .entry(self.owner)
            .or_insert_with(|| Arc::new(Mutex::new(Instant::now())))
            .clone();
        let mut next = lane.lock().await;
        tokio::time::sleep_until(*next).await;
        {
            let mut global = self.send_after.lock().await;
            tokio::time::sleep_until(*global).await;
            *global = Instant::now() + Duration::from_millis(40);
        }
        *next = Instant::now() + Duration::from_millis(1050);
        let mut body = json!({"chat_id": self.owner, "text": text, "parse_mode": "HTML",
            "link_preview_options": {"is_disabled": true}});
        if !keyboard.is_null() {
            body["reply_markup"] = json!({"inline_keyboard": keyboard});
        }
        let method = if let Some(id) = message {
            body["message_id"] = json!(id);
            "editMessageText"
        } else {
            "sendMessage"
        };
        let result = self.telegram(method, body).await;
        if let Some(id) = message
            && result
                .as_ref()
                .err()
                .and_then(|e| e.downcast_ref::<HttpFailure>())
                .is_some_and(|e| e.unchanged)
        {
            return Ok(id);
        }
        if let Some(error) = result
            .as_ref()
            .err()
            .and_then(|e| e.downcast_ref::<HttpFailure>())
            && error.status == 429
        {
            let until = Instant::now()
                + Duration::from_secs(error.retry_after.unwrap_or(2).clamp(1, 86400));
            *next = (*next).max(until);
            let mut global = self.send_after.lock().await;
            *global = (*global).max(until);
        }
        result?["message_id"]
            .as_i64()
            .context("Missing Telegram message ID")
    }

    pub async fn send_story(&self, item: &Story) -> Result<i64> {
        self.send(item.formatted()?, item.keyboard()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Story {
        serde_json::from_value(json!({"no":1,"time":"2026-11-17T01:02:03Z",
            "channels":["main","binance"],"zh":"*新公告 <ETH>\n\n涉及 A&B","url":"https://eqn.news/main/1",
            "source_url":"javascript:alert(1)","coins":["ETH","<bad>"]})).unwrap()
    }

    #[test]
    fn telegram_card_escapes_feed_text_and_validates_buttons() {
        let mut s = sample();
        let text = s.formatted().unwrap();
        assert!(text.contains("<i>币安公告</i>\n<b>新公告 &lt;ETH&gt;</b>"));
        assert!(text.contains("涉及 A&amp;B") && text.contains("$ETH") && !text.contains("<bad>"));
        assert_eq!(s.keyboard()[0].as_array().unwrap().len(), 1);
        s.url = "tg://user?id=1".into();
        assert_eq!(s.keyboard(), Value::Null);
    }

    #[test]
    fn long_unicode_news_fits_telegram_and_empty_translation_falls_back() {
        let mut s = sample();
        s.zh = Some("  ".into());
        s.en = Some("🚀".repeat(6000));
        let text = s.formatted().unwrap();
        assert!(text.contains('🚀') && text.contains('…'));
        assert!(text.encode_utf16().count() < 4096);
    }
}
