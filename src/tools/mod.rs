use std::sync::Arc;

use tokio::sync::Mutex;

use crate::config::{self, Config, ConfigError};
use crate::embedder::{Embed, EmbedError, Embedder};
use crate::fetcher::SlackClient;
use crate::fetcher::sync::{self, HarvestMode, SyncConfig};
use crate::query;
use crate::storage;
use crate::storage::search::SearchResult;

#[derive(Debug, thiserror::Error)]
pub enum KikuError {
    #[error("config: {0}")]
    Config(#[from] ConfigError),

    #[error("storage: {0}")]
    Storage(#[from] storage::StorageError),

    #[error("embedder: {0}")]
    Embed(#[from] EmbedError),

    #[error("{0}")]
    InvalidInput(String),

    #[error("{0}")]
    Unavailable(String),

    #[error("sync: {0}")]
    Sync(#[from] sync::SyncError),

    #[error("query: {0}")]
    Query(#[from] query::QueryError),
}

pub struct Kiku {
    db: Arc<Mutex<storage::Db>>,
    config: Config,
    client: Option<SlackClient>,
    embedder: Option<Box<dyn Embed>>,
}

impl Kiku {
    #[cfg(any(test, feature = "test-support"))]
    pub fn with_deps(
        db: storage::Db,
        config: Config,
        client: Option<SlackClient>,
        embedder: Option<Box<dyn Embed>>,
    ) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
            config,
            client,
            embedder,
        }
    }

    pub fn new() -> Result<Self, KikuError> {
        let config = Config::load()?;
        let db_path = config::data_dir()?.join("kiku.db");
        let db = storage::open_db(&db_path).map_err(|e| {
            tracing::error!(path = %db_path.display(), error = %e, "Failed to open database");
            e
        })?;

        let client = match SlackClient::from_env() {
            Ok(c) => {
                tracing::info!("SLACK_TOKEN found, harvest available");
                Some(c)
            }
            Err(e) => {
                tracing::warn!(error = %e, "SLACK_TOKEN not set, harvest unavailable");
                None
            }
        };

        let embedder: Option<Box<dyn Embed>> = match Embedder::from_env(reqwest::Client::new()) {
            Ok(e) => {
                tracing::info!("GEMINI_API_KEY found, search available");
                Some(Box::new(e))
            }
            Err(e) => {
                tracing::warn!(error = %e, "GEMINI_API_KEY not set, search unavailable");
                None
            }
        };

        tracing::info!(
            db = %db_path.display(),
            channels = config.channels.len(),
            "kiku initialized"
        );

        Ok(Self {
            db: Arc::new(Mutex::new(db)),
            config,
            client,
            embedder,
        })
    }

    pub async fn harvest(&self, channel: &str, mode: HarvestMode) -> Result<String, KikuError> {
        let client = self.client.as_ref().ok_or_else(|| {
            KikuError::Unavailable("SLACK_TOKEN not set".into())
        })?;

        Config::validate_channel_id(channel)
            .map_err(|e| KikuError::InvalidInput(e.to_string()))?;

        if !self.config.is_channel_allowed(channel) {
            return Err(KikuError::InvalidInput(
                format!("Channel {} not in allowlist", channel),
            ));
        }

        let sync_config = SyncConfig {
            batch_limit: self.config.batch_limit,
            max_age_days: self.config.max_age_days,
            timeout_secs: 300,
        };
        let result = sync::harvest(client, &self.db, channel, mode, &sync_config)
            .await?;

        if result.messages_fetched == 0 && matches!(mode, HarvestMode::Backfill) {
            return Ok(format!(
                "Backfill: no new messages. Run incremental first, or backfill already complete. ({} expired removed)",
                result.chunks_expired,
            ));
        }

        Ok(format!(
            "Harvest complete: {} messages fetched, {} chunks stored, {} expired removed",
            result.messages_fetched, result.chunks_stored, result.chunks_expired,
        ))
    }

    pub async fn search(
        &self,
        query_str: &str,
        channel: Option<&str>,
        limit: u32,
    ) -> Result<String, KikuError> {
        let embedder = self.embedder.as_ref().ok_or_else(|| {
            KikuError::Unavailable("GEMINI_API_KEY not set".into())
        })?;

        let limit = limit.clamp(1, 100);

        if let Some(ch) = channel {
            Config::validate_channel_id(ch)
                .map_err(|e| KikuError::InvalidInput(e.to_string()))?;
        }

        let outcome = query::search(
            Arc::clone(&self.db),
            embedder.as_ref(),
            query_str,
            channel,
            limit,
            self.config.embed_budget,
        )
        .await?;

        if outcome.results.is_empty() {
            return if outcome.embedded_now > 0 {
                Ok(format!("No results found (embedded {} new chunks)", outcome.embedded_now))
            } else {
                Ok("No results found. Run 'harvest' first to index conversations.".into())
            };
        }

        let output = format_search_results(&outcome.results, outcome.embedded_now);
        Ok(output)
    }

    pub async fn status(&self) -> Result<String, KikuError> {
        let db = Arc::clone(&self.db);
        let stats = tokio::task::spawn_blocking(move || {
            let conn = db.blocking_lock();
            storage::get_stats(&conn)
        })
        .await
        .expect("tokio runtime active")?;

        let mut output = format!(
            "Channels: {}\nChunks: {} ({} embedded, {}%)\n",
            stats.total_channels,
            stats.total_chunks,
            stats.embedded_chunks,
            stats.embed_percentage(),
        );

        for ch in &stats.channels {
            output.push_str(&format!(
                "\n  #{} ({}): {} chunks, latest: {}",
                ch.channel_name, ch.channel_id, ch.chunk_count, ch.latest_ts,
            ));
            if let Some(ref oldest) = ch.oldest_ts {
                output.push_str(&format!(", oldest: {oldest}"));
            }
        }

        Ok(output)
    }
}

fn format_search_results(results: &[SearchResult], embedded_now: u32) -> String {
    use std::collections::HashMap;

    let mut groups: HashMap<&str, Vec<(usize, &SearchResult)>> = HashMap::new();
    for (i, r) in results.iter().enumerate() {
        groups.entry(&r.channel_name).or_default().push((i, r));
    }

    let mut sorted: Vec<_> = groups.into_iter().collect();
    sorted.sort_by(|a, b| {
        let best_dist = |items: &[(usize, &SearchResult)]| {
            items.iter().map(|(_, r)| r.distance).fold(f32::INFINITY, f32::min)
        };
        best_dist(&a.1)
            .partial_cmp(&best_dist(&b.1))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut output = String::new();
    for (channel_name, chunks) in &sorted {
        let channel_id = &chunks[0].1.channel_id;
        output.push_str(&format!("## #{channel_name} ({channel_id})\n\n"));
        for (rank, r) in chunks {
            let mut meta = format!(
                "{}. [{}] ts={} authors={} (distance: {:.3})",
                rank + 1,
                r.chunk_type.as_str(),
                r.timestamp,
                r.authors.join(", "),
                r.distance,
            );
            if let Some(ref tts) = r.thread_ts {
                meta.push_str(&format!(" thread={tts}"));
            }
            output.push_str(&meta);
            output.push('\n');
            output.push_str(&r.content);
            output.push_str("\n\n");
        }
    }

    if embedded_now > 0 {
        output.push_str(&format!("---\n{embedded_now} new chunks embedded\n"));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{self, Chunk, ChunkType};
    use std::path::Path;

    fn test_kiku(channels: Vec<String>) -> Kiku {
        let db = storage::open_db(Path::new(":memory:")).unwrap();
        let config = Config {
            channels,
            ..Default::default()
        };
        Kiku::with_deps(db, config, None, None)
    }

    fn dummy_client() -> SlackClient {
        SlackClient::with_base_url("xoxb-test".into(), "http://localhost:1".into()).unwrap()
    }

    fn test_kiku_with_client(channels: Vec<String>) -> Kiku {
        let db = storage::open_db(Path::new(":memory:")).unwrap();
        let config = Config {
            channels,
            ..Default::default()
        };
        Kiku::with_deps(db, config, Some(dummy_client()), None)
    }

    fn test_kiku_with_embedder(channels: Vec<String>) -> Kiku {
        use crate::embedder::MockEmbedder;
        let db = storage::open_db(Path::new(":memory:")).unwrap();
        let config = Config {
            channels,
            ..Default::default()
        };
        Kiku::with_deps(db, config, None, Some(Box::new(MockEmbedder)))
    }

    fn test_chunk(channel_id: &str, ts: &str, content: &str) -> Chunk {
        Chunk {
            channel_id: channel_id.into(),
            channel_name: "dev".into(),
            chunk_type: ChunkType::Message,
            content: content.into(),
            authors: vec!["U001".into()],
            timestamp: ts.into(),
            thread_ts: None,
            message_hash: format!("hash_{ts}"),
        }
    }

    #[tokio::test]
    async fn harvest_rejects_without_client() {
        let kiku = test_kiku(vec![]);
        let err = kiku.harvest("C123", HarvestMode::Incremental).await.unwrap_err();
        assert!(err.to_string().contains("SLACK_TOKEN not set"));
    }

    #[tokio::test]
    async fn harvest_rejects_invalid_channel_id() {
        let kiku = test_kiku_with_client(vec![]);
        let err = kiku.harvest("invalid", HarvestMode::Incremental).await.unwrap_err();
        assert!(err.to_string().contains("Invalid channel ID"));
    }

    #[tokio::test]
    async fn harvest_rejects_channel_not_in_allowlist() {
        let kiku = test_kiku_with_client(vec!["C111".into()]);
        let err = kiku.harvest("C999", HarvestMode::Incremental).await.unwrap_err();
        assert!(err.to_string().contains("not in allowlist"));
    }

    #[tokio::test]
    async fn search_rejects_without_embedder() {
        let kiku = test_kiku(vec![]);
        let err = kiku.search("query", None, 10).await.unwrap_err();
        assert!(err.to_string().contains("GEMINI_API_KEY not set"));
    }

    #[tokio::test]
    async fn search_rejects_invalid_channel_filter() {
        let kiku = test_kiku_with_embedder(vec![]);
        let err = kiku.search("query", Some("bad"), 10).await.unwrap_err();
        assert!(err.to_string().contains("Invalid channel ID"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_no_results_suggests_harvest() {
        let kiku = test_kiku_with_embedder(vec![]);
        let output = kiku.search("query", None, 10).await.unwrap();
        assert!(output.contains("Run 'harvest'"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_no_results_after_embedding() {
        use crate::embedder::MockEmbedder;
        let db = storage::open_db(Path::new(":memory:")).unwrap();
        storage::insert_chunk(&db, &test_chunk("C123", "100.0", "hello world")).unwrap();
        let config = Config {
            channels: vec![],
            embed_budget: 50,
            ..Default::default()
        };
        let kiku = Kiku::with_deps(db, config, None, Some(Box::new(MockEmbedder)));
        let output = kiku.search("test", None, 10).await.unwrap();
        assert!(output.contains("## #dev"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_clamps_limit() {
        let kiku = test_kiku_with_embedder(vec![]);
        let _ = kiku.search("q", None, 0).await;
        let _ = kiku.search("q", None, 999).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_empty_db() {
        let kiku = test_kiku(vec![]);
        let output = kiku.status().await.unwrap();
        assert!(output.contains("Channels: 0"));
        assert!(output.contains("Chunks: 0"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_with_data() {
        let db = storage::open_db(Path::new(":memory:")).unwrap();
        storage::insert_chunk(&db, &test_chunk("C123", "100.0", "hello")).unwrap();
        storage::insert_chunk(&db, &test_chunk("C123", "200.0", "world")).unwrap();
        let config = Config::default();
        let kiku = Kiku::with_deps(db, config, None, None);
        let output = kiku.status().await.unwrap();
        assert!(output.contains("Channels: 1"));
        assert!(output.contains("Chunks: 2"));
        assert!(output.contains("#dev (C123)"));
    }

    fn result(channel_id: &str, channel_name: &str, ts: &str, dist: f32) -> SearchResult {
        SearchResult {
            chunk_id: 1,
            channel_id: channel_id.into(),
            channel_name: channel_name.into(),
            thread_ts: None,
            chunk_type: ChunkType::Message,
            content: format!("content at {ts}"),
            authors: vec!["U001".into()],
            timestamp: ts.into(),
            distance: dist,
        }
    }

    #[test]
    fn format_empty_results() {
        let output = format_search_results(&[], 0);
        assert!(output.is_empty());
    }

    #[test]
    fn format_single_result() {
        let results = vec![result("C1", "dev", "100.0", 0.5)];
        let output = format_search_results(&results, 0);
        assert!(output.contains("## #dev (C1)"));
        assert!(output.contains("[message]"));
        assert!(output.contains("ts=100.0"));
        assert!(output.contains("content at 100.0"));
        assert!(!output.contains("---"));
    }

    #[test]
    fn format_groups_by_channel_sorted_by_distance() {
        let results = vec![
            result("C2", "general", "200.0", 0.8),
            result("C1", "dev", "100.0", 0.3),
        ];
        let output = format_search_results(&results, 0);
        let dev_pos = output.find("## #dev").unwrap();
        let gen_pos = output.find("## #general").unwrap();
        assert!(dev_pos < gen_pos, "dev (dist=0.3) should appear before general (dist=0.8)");
    }

    #[test]
    fn format_includes_thread_ts() {
        let mut r = result("C1", "dev", "100.0", 0.5);
        r.thread_ts = Some("99.0".into());
        r.chunk_type = ChunkType::Thread;
        let output = format_search_results(&[r], 0);
        assert!(output.contains("thread=99.0"));
        assert!(output.contains("[thread]"));
    }

    #[test]
    fn format_shows_embedded_count() {
        let results = vec![result("C1", "dev", "100.0", 0.5)];
        let output = format_search_results(&results, 42);
        assert!(output.contains("---\n42 new chunks embedded"));
    }
}
