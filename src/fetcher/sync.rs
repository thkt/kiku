use std::time::Duration;

use tokio::sync::Mutex;
use tracing::info;

use crate::chunker;
use crate::storage::{self, Chunk, Db};

use super::{FetchError, SlackClient, SlackMessage};

#[derive(Debug, Clone, Copy, serde::Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum HarvestMode {
    Incremental,
    Backfill,
}

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub batch_limit: u32,
    pub max_age_days: u32,
    pub timeout_secs: u64,
}

#[derive(Debug, Default)]
pub struct HarvestResult {
    pub messages_fetched: u32,
    pub chunks_stored: u32,
    pub chunks_expired: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error(transparent)]
    Fetch(#[from] FetchError),

    #[error(transparent)]
    Storage(#[from] storage::StorageError),

    #[error("invalid sync state: {0}")]
    InvalidState(String),

    #[error("harvest timed out after {secs}s on channel {channel}")]
    Timeout { secs: u64, channel: String },
}

pub async fn harvest(
    client: &SlackClient,
    db: &Mutex<Db>,
    channel_id: &str,
    mode: HarvestMode,
    config: &SyncConfig,
) -> Result<HarvestResult, SyncError> {
    let timeout = Duration::from_secs(config.timeout_secs);
    tokio::time::timeout(timeout, harvest_inner(client, db, channel_id, mode, config))
        .await
        .map_err(|_| SyncError::Timeout {
            secs: config.timeout_secs,
            channel: channel_id.to_string(),
        })?
}

async fn harvest_inner(
    client: &SlackClient,
    db: &Mutex<Db>,
    channel_id: &str,
    mode: HarvestMode,
    config: &SyncConfig,
) -> Result<HarvestResult, SyncError> {
    let sync_state = {
        let db = db.lock().await;
        storage::get_sync_state(&db, channel_id)?
    };

    let cutoff = storage::cutoff_ts(config.max_age_days);

    if matches!(mode, HarvestMode::Backfill) {
        let should_exit_early = match sync_state.as_ref().and_then(|s| s.oldest_ts.as_deref()) {
            None => {
                info!("No oldest_ts in sync_state, run incremental first");
                true
            }
            Some(ots) if ots <= cutoff.as_str() => {
                info!(oldest_ts = %ots, cutoff = %cutoff, "Backfill complete");
                true
            }
            _ => false,
        };
        if should_exit_early {
            let conn = db.lock().await;
            let expired = storage::delete_expired_chunks(&conn, config.max_age_days)?;
            return Ok(HarvestResult { chunks_expired: expired, ..Default::default() });
        }
    }

    let channel_name = match sync_state.as_ref().and_then(|s| s.channel_name.clone()) {
        Some(cached) => cached,
        None => client.fetch_channel_name(channel_id).await?,
    };

    let (oldest, latest) = match mode {
        HarvestMode::Incremental => {
            (sync_state.as_ref().map(|s| s.latest_ts.clone()), None)
        }
        HarvestMode::Backfill => {
            let ots = sync_state.as_ref()
                .and_then(|s| s.oldest_ts.clone())
                .ok_or_else(|| SyncError::InvalidState(
                    "backfill requires oldest_ts in sync_state".into()
                ))?;
            (Some(cutoff), Some(ots))
        }
    };

    let mut result = HarvestResult::default();
    let mut cursor = None;

    loop {
        let page = client
            .fetch_history(channel_id, oldest.as_deref(), latest.as_deref(), cursor.as_deref())
            .await?;
        if page.messages.is_empty() {
            break;
        }

        let page_count = page.messages.len() as u32;
        let threads = page.messages.iter().filter(|m| m.reply_count > 0).count();
        info!(messages = page_count, threads, "Fetched page");

        let mut page_chunks = Vec::new();
        let mut page_stats = FetchStats::default();
        for msg in &page.messages {
            page_stats.update_ts(&msg.ts);
            if let Some(chunk) = make_chunk(client, msg, channel_id, &channel_name).await? {
                page_chunks.push(chunk);
            }
        }
        let (stored, expired) = {
            let conn = db.lock().await;
            store_chunks(&conn, channel_id, &channel_name, &page_chunks, &page_stats, config.max_age_days)?
        };

        result.messages_fetched += page_count;
        result.chunks_stored += stored;
        result.chunks_expired += expired;

        cursor = page.next_cursor;
        if cursor.is_none() || result.messages_fetched >= config.batch_limit {
            break;
        }
    }

    info!(
        messages = result.messages_fetched,
        stored = result.chunks_stored,
        expired = result.chunks_expired,
        "Harvest complete"
    );

    Ok(result)
}

#[derive(Default)]
struct FetchStats {
    max_ts: Option<String>,
    min_ts: Option<String>,
}

impl FetchStats {
    fn update_ts(&mut self, ts: &str) {
        if self.max_ts.as_ref().is_none_or(|c| ts > c.as_str()) {
            self.max_ts = Some(ts.to_string());
        }
        if self.min_ts.as_ref().is_none_or(|c| ts < c.as_str()) {
            self.min_ts = Some(ts.to_string());
        }
    }
}

async fn make_chunk(
    client: &SlackClient,
    msg: &SlackMessage,
    channel_id: &str,
    channel_name: &str,
) -> Result<Option<Chunk>, SyncError> {
    if msg.reply_count > 0 {
        let replies = client.fetch_replies(channel_id, &msg.ts).await?;
        Ok(chunker::chunk_thread(&replies, channel_id, channel_name))
    } else {
        Ok(Some(chunker::chunk_message(msg, channel_id, channel_name)))
    }
}

fn store_chunks(
    db: &Db,
    channel_id: &str,
    channel_name: &str,
    chunks: &[Chunk],
    stats: &FetchStats,
    max_age_days: u32,
) -> Result<(u32, u32), SyncError> {
    let tx = db.unchecked_transaction().map_err(storage::StorageError::from)?;
    let mut stored = 0u32;
    for chunk in chunks {
        if storage::upsert_chunk(&tx, chunk)?.is_some() {
            stored += 1;
        }
    }
    if let Some(ref max_ts) = stats.max_ts {
        storage::upsert_sync_state(&tx, channel_id, max_ts, stats.min_ts.as_deref(), Some(channel_name))?;
    }
    let expired = storage::delete_expired_chunks_inner(&tx, max_age_days)?;
    tx.commit().map_err(storage::StorageError::from)?;
    Ok((stored, expired))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::Arc;
    use wiremock::matchers::{method, path as url_path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_db() -> Arc<Mutex<Db>> {
        Arc::new(Mutex::new(storage::open_db(Path::new(":memory:")).unwrap()))
    }

    fn test_config() -> SyncConfig {
        SyncConfig { batch_limit: 5000, max_age_days: 90, timeout_secs: 300 }
    }

    async fn mock_channel_info(server: &MockServer) {
        Mock::given(method("GET"))
            .and(url_path("/conversations.info"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "channel": {"id": "C123", "name": "dev"}
            })))
            .mount(server)
            .await;
    }

    // Use realistic Slack timestamps (Unix seconds, same length for correct string comparison)
    const TS_OLD: &str = "1770000050.000000";
    const TS_BASE: &str = "1770000100.000000";
    const TS_MID: &str = "1770000150.000000";
    const TS_NEW: &str = "1770000200.000000";
    const TS_REPLY: &str = "1770000101.000000";

    #[tokio::test]
    async fn incremental_first_harvest_stores_chunks_and_sets_sync_state() {
        let server = MockServer::start().await;
        mock_channel_info(&server).await;

        Mock::given(method("GET"))
            .and(url_path("/conversations.history"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "messages": [
                    {"ts": TS_NEW, "user": "U1", "text": "newer", "reply_count": 0},
                    {"ts": TS_BASE, "user": "U2", "text": "older", "reply_count": 0}
                ],
                "has_more": false
            })))
            .mount(&server)
            .await;

        let client = SlackClient::with_base_url("xoxb-test".into(), server.uri()).unwrap();
        let db = test_db();

        let result = harvest(&client, &db, "C123", HarvestMode::Incremental, &test_config())
            .await
            .unwrap();
        assert_eq!(result.messages_fetched, 2);
        assert_eq!(result.chunks_stored, 2);

        let db = db.lock().await;
        let state = storage::get_sync_state(&db, "C123").unwrap().unwrap();
        assert_eq!(state.latest_ts, TS_NEW);
        assert_eq!(state.oldest_ts.as_deref(), Some(TS_BASE));
    }

    #[tokio::test]
    async fn incremental_subsequent_fetches_only_newer() {
        let server = MockServer::start().await;
        mock_channel_info(&server).await;

        let db = test_db();
        {
            let conn = db.lock().await;
            storage::upsert_sync_state(&conn, "C123", TS_BASE, Some(TS_OLD), Some("dev")).unwrap();
        }

        Mock::given(method("GET"))
            .and(url_path("/conversations.history"))
            .and(query_param("oldest", TS_BASE))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "messages": [
                    {"ts": TS_MID, "user": "U1", "text": "new msg", "reply_count": 0}
                ],
                "has_more": false
            })))
            .mount(&server)
            .await;

        let client = SlackClient::with_base_url("xoxb-test".into(), server.uri()).unwrap();
        let result = harvest(&client, &db, "C123", HarvestMode::Incremental, &test_config())
            .await
            .unwrap();

        assert_eq!(result.chunks_stored, 1);
        let conn = db.lock().await;
        let state = storage::get_sync_state(&conn, "C123").unwrap().unwrap();
        assert_eq!(state.latest_ts, TS_MID);
        assert_eq!(state.oldest_ts.as_deref(), Some(TS_OLD)); // unchanged
    }

    #[tokio::test]
    async fn backfill_without_sync_state_returns_early() {
        let server = MockServer::start().await;
        let client = SlackClient::with_base_url("xoxb-test".into(), server.uri()).unwrap();
        let db = test_db();

        let result = harvest(&client, &db, "C123", HarvestMode::Backfill, &test_config())
            .await
            .unwrap();
        assert_eq!(result.messages_fetched, 0);
        assert_eq!(result.chunks_stored, 0);
    }

    #[tokio::test]
    async fn backfill_fetches_older_messages() {
        let server = MockServer::start().await;
        mock_channel_info(&server).await;

        let db = test_db();
        {
            let conn = db.lock().await;
            storage::upsert_sync_state(&conn, "C123", TS_NEW, Some(TS_MID), Some("dev")).unwrap();
        }

        Mock::given(method("GET"))
            .and(url_path("/conversations.history"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "messages": [
                    {"ts": TS_BASE, "user": "U1", "text": "old msg", "reply_count": 0}
                ],
                "has_more": false
            })))
            .mount(&server)
            .await;

        let client = SlackClient::with_base_url("xoxb-test".into(), server.uri()).unwrap();
        let result = harvest(&client, &db, "C123", HarvestMode::Backfill, &test_config())
            .await
            .unwrap();

        assert_eq!(result.messages_fetched, 1);
        assert_eq!(result.chunks_stored, 1);
    }

    #[tokio::test]
    async fn incremental_handles_pagination() {
        let server = MockServer::start().await;
        mock_channel_info(&server).await;

        // Page 2 (more specific matcher — mounted first, matches cursor)
        Mock::given(method("GET"))
            .and(url_path("/conversations.history"))
            .and(query_param("cursor", "cursor123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "messages": [
                    {"ts": TS_BASE, "user": "U2", "text": "page 2", "reply_count": 0}
                ],
                "has_more": false
            })))
            .mount(&server)
            .await;

        // Page 1 (less specific — mounted second, consumed once)
        Mock::given(method("GET"))
            .and(url_path("/conversations.history"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "messages": [
                    {"ts": TS_MID, "user": "U1", "text": "page 1", "reply_count": 0}
                ],
                "has_more": true,
                "response_metadata": {"next_cursor": "cursor123"}
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        let client = SlackClient::with_base_url("xoxb-test".into(), server.uri()).unwrap();
        let db = test_db();

        let result = harvest(&client, &db, "C123", HarvestMode::Incremental, &test_config())
            .await
            .unwrap();

        assert_eq!(result.messages_fetched, 2);
        assert_eq!(result.chunks_stored, 2);
    }

    #[tokio::test]
    async fn harvest_processes_threads() {
        let server = MockServer::start().await;
        mock_channel_info(&server).await;

        Mock::given(method("GET"))
            .and(url_path("/conversations.history"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "messages": [
                    {"ts": TS_BASE, "user": "U1", "text": "parent", "reply_count": 2, "thread_ts": TS_BASE}
                ],
                "has_more": false
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(url_path("/conversations.replies"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "messages": [
                    {"ts": TS_BASE, "user": "U1", "text": "parent", "thread_ts": TS_BASE},
                    {"ts": TS_REPLY, "user": "U2", "text": "reply", "thread_ts": TS_BASE}
                ],
                "has_more": false
            })))
            .mount(&server)
            .await;

        let client = SlackClient::with_base_url("xoxb-test".into(), server.uri()).unwrap();
        let db = test_db();

        let result = harvest(&client, &db, "C123", HarvestMode::Incremental, &test_config())
            .await
            .unwrap();

        assert_eq!(result.messages_fetched, 1);
        assert_eq!(result.chunks_stored, 1);
        let conn = db.lock().await;
        assert_eq!(storage::get_stats(&conn).unwrap().total_chunks, 1);
    }
}
