use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::warn;

use crate::redact::{redact_tokens, truncate_str};

use super::{FetchError, MessagePage, SlackMessage};

const MAX_RETRIES: u32 = 5;
const PAGE_SIZE: u32 = 200;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const SLACK_API_BASE: &str = "https://slack.com/api";

#[derive(Deserialize)]
struct MessagesResponse {
    ok: bool,
    error: Option<String>,
    #[serde(default)]
    messages: Vec<ApiMessage>,
    has_more: Option<bool>,
    response_metadata: Option<ResponseMetadata>,
}

#[derive(Deserialize)]
struct ChannelInfoResponse {
    ok: bool,
    error: Option<String>,
    channel: Option<ChannelInfo>,
}

#[derive(Deserialize)]
struct ChannelInfo {
    name: String,
}

#[derive(Deserialize)]
struct ApiMessage {
    ts: Option<String>,
    user: Option<String>,
    text: Option<String>,
    thread_ts: Option<String>,
    reply_count: Option<u32>,
}

#[derive(Deserialize)]
struct ResponseMetadata {
    next_cursor: Option<String>,
}

impl MessagesResponse {
    fn next_cursor(&mut self) -> Option<String> {
        self.response_metadata
            .take()
            .and_then(|m| m.next_cursor)
            .filter(|c| !c.is_empty())
    }
}

impl ApiMessage {
    fn into_domain(self) -> Option<SlackMessage> {
        Some(SlackMessage {
            ts: self.ts?,
            // Bot messages and integrations may omit the user field
            user: self.user.unwrap_or_else(|| "unknown".into()),
            text: self.text.unwrap_or_default(),
            thread_ts: self.thread_ts,
            reply_count: self.reply_count.unwrap_or(0),
        })
    }
}

#[derive(Clone, Copy)]
enum Tier {
    Two,   // 20 req/min → 3s between requests
    Three, // ~50 req/min → 1s between requests
}

impl Tier {
    const fn interval(self) -> Duration {
        match self {
            Self::Two => Duration::from_millis(3000),
            Self::Three => Duration::from_millis(1000),
        }
    }
}

pub struct SlackClient {
    http: Client,
    token: String,
    base_url: String,
    last_tier2: Mutex<Option<Instant>>,
    last_tier3: Mutex<Option<Instant>>,
}

impl std::fmt::Debug for SlackClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlackClient")
            .field("base_url", &self.base_url)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl SlackClient {
    pub(crate) fn new(token: String) -> Self {
        Self::with_base_url(token, SLACK_API_BASE.to_string())
            .expect("SLACK_API_BASE is a valid URL")
    }

    pub(crate) fn with_base_url(token: String, base_url: String) -> Result<Self, FetchError> {
        reqwest::Url::parse(&base_url)
            .map_err(|e| FetchError::Api(format!("Invalid base URL: {e}")))?;
        Ok(Self {
            http: Client::new(),
            token,
            base_url,
            last_tier2: Mutex::new(None),
            last_tier3: Mutex::new(None),
        })
    }

    pub fn from_env() -> Result<Self, FetchError> {
        Self::from_env_with(|k| std::env::var(k))
    }

    pub(crate) fn from_env_with(
        get_var: impl Fn(&str) -> Result<String, std::env::VarError>,
    ) -> Result<Self, FetchError> {
        let token = get_var("SLACK_TOKEN")
            .or_else(|_| get_var("SLACK_BOT_TOKEN"))
            .map_err(|_| FetchError::TokenNotSet)?;
        if token.is_empty() {
            return Err(FetchError::TokenNotSet);
        }
        if !token.starts_with("xoxb-") && !token.starts_with("xoxp-") {
            return Err(FetchError::InvalidToken);
        }
        Ok(Self::new(token))
    }

    pub async fn fetch_history(
        &self,
        channel_id: &str,
        oldest: Option<&str>,
        latest: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<MessagePage, FetchError> {
        let mut url = build_url(&self.base_url, "conversations.history", &[
            ("channel", channel_id),
            ("limit", &PAGE_SIZE.to_string()),
        ]);
        if let Some(v) = oldest { url.query_pairs_mut().append_pair("oldest", v); }
        if let Some(v) = latest { url.query_pairs_mut().append_pair("latest", v); }
        if let Some(v) = cursor { url.query_pairs_mut().append_pair("cursor", v); }

        let mut resp: MessagesResponse = self.api_get(url.as_str(), Some(Tier::Three)).await?;
        check_ok(resp.ok, &resp.error)?;

        let next_cursor = resp.next_cursor();
        Ok(MessagePage {
            messages: resp
                .messages
                .into_iter()
                .filter_map(ApiMessage::into_domain)
                .collect(),
            next_cursor,
        })
    }

    pub async fn fetch_replies(
        &self,
        channel_id: &str,
        thread_ts: &str,
    ) -> Result<Vec<SlackMessage>, FetchError> {
        let mut all = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let mut url = build_url(&self.base_url, "conversations.replies", &[
                ("channel", channel_id),
                ("ts", thread_ts),
                ("limit", &PAGE_SIZE.to_string()),
            ]);
            if let Some(ref c) = cursor {
                url.query_pairs_mut().append_pair("cursor", c);
            }

            let mut resp: MessagesResponse = self.api_get(url.as_str(), Some(Tier::Two)).await?;
            check_ok(resp.ok, &resp.error)?;

            let has_more = resp.has_more == Some(true);
            cursor = resp.next_cursor();
            all.extend(resp.messages.into_iter().filter_map(ApiMessage::into_domain));

            if cursor.is_none() || !has_more {
                break;
            }
        }

        Ok(all)
    }

    pub async fn fetch_channel_name(
        &self,
        channel_id: &str,
    ) -> Result<String, FetchError> {
        let url = build_url(&self.base_url, "conversations.info", &[
            ("channel", channel_id),
        ]);
        let resp: ChannelInfoResponse = self.api_get(url.as_str(), Some(Tier::Three)).await?;
        check_ok(resp.ok, &resp.error)?;
        resp.channel
            .map(|c| c.name)
            .ok_or_else(|| FetchError::Api("missing channel info".into()))
    }

    async fn throttle(&self, tier: Tier) {
        let mut last = match tier {
            Tier::Two => self.last_tier2.lock().await,
            Tier::Three => self.last_tier3.lock().await,
        };
        if let Some(prev) = *last {
            let remaining = tier.interval().saturating_sub(prev.elapsed());
            if !remaining.is_zero() {
                tokio::time::sleep(remaining).await;
            }
        }
        *last = Some(Instant::now());
    }

    async fn api_get<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        tier: Option<Tier>,
    ) -> Result<T, FetchError> {
        for attempt in 0..=MAX_RETRIES {
            if let Some(t) = tier {
                self.throttle(t).await;
            }

            let auth = {
                let mut v = reqwest::header::HeaderValue::from_str(
                    &format!("Bearer {}", self.token),
                ).map_err(|_| FetchError::Api("invalid token format".into()))?;
                v.set_sensitive(true);
                v
            };
            let resp = self
                .http
                .get(url)
                .header(reqwest::header::AUTHORIZATION, auth)
                .timeout(REQUEST_TIMEOUT)
                .send()
                .await?;

            let status = resp.status().as_u16();
            if (200..300).contains(&status) {
                return resp.json::<T>().await.map_err(Into::into);
            }

            let retry_after = resp.headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            match retry_wait(status, retry_after.as_deref(), attempt) {
                Some(wait) if attempt < MAX_RETRIES => {
                    warn!(attempt, status, wait_ms = wait.as_millis() as u64, "Retrying Slack API");
                    tokio::time::sleep(wait).await;
                }
                Some(_) => return Err(FetchError::MaxRetries(MAX_RETRIES)),
                None => {
                    let body = resp.text().await.unwrap_or_default();
                    let safe_body = redact_tokens(&body);
                    return Err(FetchError::Api(format!(
                        "HTTP {status}: {}",
                        truncate_str(&safe_body, 500)
                    )));
                }
            }
        }

        Err(FetchError::MaxRetries(MAX_RETRIES))
    }
}

fn build_url(base: &str, endpoint: &str, params: &[(&str, &str)]) -> reqwest::Url {
    let raw = format!("{base}/{endpoint}");
    let mut url = reqwest::Url::parse(&raw).expect("valid base URL");
    for (k, v) in params {
        url.query_pairs_mut().append_pair(k, v);
    }
    url
}

fn check_ok(ok: bool, error: &Option<String>) -> Result<(), FetchError> {
    if !ok {
        return Err(FetchError::Api(
            error.clone().unwrap_or_else(|| "unknown error".into()),
        ));
    }
    Ok(())
}

fn retry_wait(status: u16, retry_after: Option<&str>, attempt: u32) -> Option<Duration> {
    match status {
        429 => {
            let secs = retry_after
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(5);
            Some(Duration::from_secs(secs))
        }
        500 | 502 | 503 => Some(Duration::from_millis(500 * 2u64.pow(attempt))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn test_client(server: &MockServer) -> SlackClient {
        SlackClient::with_base_url("xoxb-test-token".into(), server.uri()).unwrap()
    }

    #[tokio::test]
    async fn fetch_history_parses_messages() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/conversations.history"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "messages": [
                    {"ts": "1.0", "user": "U1", "text": "hello", "reply_count": 0},
                    {"ts": "2.0", "user": "U2", "text": "world", "thread_ts": "1.0", "reply_count": 3}
                ],
                "has_more": false
            })))
            .mount(&server)
            .await;

        let page = test_client(&server)
            .await
            .fetch_history("C123", None, None, None)
            .await
            .unwrap();
        assert_eq!(page.messages.len(), 2);
        assert_eq!(page.messages[0].ts, "1.0");
        assert_eq!(page.messages[0].text, "hello");
        assert_eq!(page.messages[1].reply_count, 3);
        assert!(page.next_cursor.is_none());
    }

    #[tokio::test]
    async fn fetch_history_returns_cursor() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/conversations.history"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "messages": [{"ts": "1.0", "user": "U1", "text": "hi"}],
                "has_more": true,
                "response_metadata": {"next_cursor": "abc123"}
            })))
            .mount(&server)
            .await;

        let page = test_client(&server)
            .await
            .fetch_history("C123", None, None, None)
            .await
            .unwrap();
        assert_eq!(page.next_cursor.as_deref(), Some("abc123"));
    }

    #[tokio::test]
    async fn fetch_channel_name_success() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/conversations.info"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "channel": {"id": "C123", "name": "general"}
            })))
            .mount(&server)
            .await;

        let name = test_client(&server)
            .await
            .fetch_channel_name("C123")
            .await
            .unwrap();
        assert_eq!(name, "general");
    }

    #[tokio::test]
    async fn api_error_returns_fetch_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/conversations.history"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": false,
                "error": "channel_not_found"
            })))
            .mount(&server)
            .await;

        let err = test_client(&server)
            .await
            .fetch_history("C123", None, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("channel_not_found"));
    }

    #[tokio::test]
    async fn rate_limit_retries_on_429() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/conversations.info"))
            .respond_with(
                ResponseTemplate::new(429).insert_header("retry-after", "1"),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/conversations.info"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "channel": {"id": "C1", "name": "dev"}
            })))
            .mount(&server)
            .await;

        let name = test_client(&server)
            .await
            .fetch_channel_name("C1")
            .await
            .unwrap();
        assert_eq!(name, "dev");
    }

    #[test]
    fn from_env_missing_token() {
        let err = SlackClient::from_env_with(|_| Err(std::env::VarError::NotPresent)).unwrap_err();
        assert!(matches!(err, FetchError::TokenNotSet));
    }

    #[test]
    fn from_env_rejects_empty_token() {
        let err = SlackClient::from_env_with(|key| match key {
            "SLACK_TOKEN" => Ok("".into()),
            _ => Err(std::env::VarError::NotPresent),
        }).unwrap_err();
        assert!(matches!(err, FetchError::TokenNotSet));
    }

    #[test]
    fn from_env_rejects_invalid_prefix() {
        let err = SlackClient::from_env_with(|key| match key {
            "SLACK_TOKEN" => Ok("bad-token".into()),
            _ => Err(std::env::VarError::NotPresent),
        }).unwrap_err();
        assert!(matches!(err, FetchError::InvalidToken));
    }

    #[test]
    fn from_env_prefers_slack_token() {
        let client = SlackClient::from_env_with(|key| match key {
            "SLACK_TOKEN" => Ok("xoxb-primary".into()),
            "SLACK_BOT_TOKEN" => Ok("xoxb-fallback".into()),
            _ => Err(std::env::VarError::NotPresent),
        }).unwrap();
        let debug = format!("{:?}", client);
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("xoxb-"));
    }

    #[test]
    fn from_env_falls_back_to_bot_token() {
        let client = SlackClient::from_env_with(|key| match key {
            "SLACK_BOT_TOKEN" => Ok("xoxb-fallback".into()),
            _ => Err(std::env::VarError::NotPresent),
        }).unwrap();
        let debug = format!("{:?}", client);
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn retry_wait_429_uses_retry_after_header() {
        let wait = retry_wait(429, Some("3"), 0);
        assert_eq!(wait, Some(Duration::from_secs(3)));
    }

    #[test]
    fn retry_wait_429_defaults_to_5s_without_header() {
        let wait = retry_wait(429, None, 0);
        assert_eq!(wait, Some(Duration::from_secs(5)));
    }

    #[test]
    fn retry_wait_429_defaults_to_5s_on_non_numeric() {
        let wait = retry_wait(429, Some("not-a-number"), 0);
        assert_eq!(wait, Some(Duration::from_secs(5)));
    }

    #[test]
    fn retry_wait_500_exponential_backoff() {
        assert_eq!(retry_wait(500, None, 0), Some(Duration::from_millis(500)));
        assert_eq!(retry_wait(502, None, 1), Some(Duration::from_millis(1000)));
        assert_eq!(retry_wait(503, None, 2), Some(Duration::from_millis(2000)));
    }

    #[test]
    fn retry_wait_non_retryable_returns_none() {
        assert!(retry_wait(400, None, 0).is_none());
        assert!(retry_wait(404, None, 0).is_none());
    }
}
