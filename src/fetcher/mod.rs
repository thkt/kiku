pub mod client;
pub mod sync;

pub use client::SlackClient;

#[derive(Debug, Clone)]
pub struct SlackMessage {
    pub ts: String,
    pub user: String,
    pub text: String,
    pub thread_ts: Option<String>,
    pub reply_count: u32,
}

#[derive(Debug)]
pub struct MessagePage {
    pub messages: Vec<SlackMessage>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("SLACK_TOKEN not set. Set SLACK_TOKEN=xoxb-... (bot token) or xoxp-... (user token)")]
    TokenNotSet,

    #[error("Invalid token: expected xoxb-* or xoxp-* prefix")]
    InvalidToken,

    #[error("Slack API error: {0}")]
    Api(String),

    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("Max retries ({0}) exceeded")]
    MaxRetries(u32),
}
