use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::info;

use crate::embedder::{Embed, EmbedError};
use crate::storage::{self, Db, search::SearchResult};

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Embed(#[from] EmbedError),

    #[error(transparent)]
    Storage(#[from] storage::StorageError),

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug)]
pub struct SearchOutcome {
    pub results: Vec<SearchResult>,
    pub embedded_now: u32,
}

pub async fn search(
    db: Arc<Mutex<Db>>,
    embedder: &(dyn Embed + '_),
    query: &str,
    channel_id: Option<&str>,
    limit: u32,
    embed_budget: u32,
) -> Result<SearchOutcome, QueryError> {
    let embedded_now = match embed_pending(&db, embedder, embed_budget).await {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(error = %e, "embed_pending failed, searching existing embeddings");
            0
        }
    };

    let query_embedding = embedder.embed_query(query).await?;

    let channel_owned = channel_id.map(String::from);
    let results: Vec<SearchResult> = tokio::task::spawn_blocking(move || {
        let conn = db.blocking_lock();
        storage::search::search_similar(&conn, &query_embedding, channel_owned.as_deref(), limit)
    })
    .await
    .map_err(QueryError::Join)?
    .map_err(QueryError::Storage)?;

    info!(
        query,
        results = results.len(),
        embedded_now,
        "Search complete"
    );

    Ok(SearchOutcome {
        results,
        embedded_now,
    })
}

async fn embed_pending(
    db: &Arc<Mutex<Db>>,
    embedder: &(dyn Embed + '_),
    budget: u32,
) -> Result<u32, QueryError> {
    if budget == 0 {
        return Ok(0);
    }

    let unembedded = {
        let conn = db.lock().await;
        storage::get_unembedded_chunks(&conn, budget)?
    };

    if unembedded.is_empty() {
        return Ok(0);
    }

    let (ids, texts): (Vec<i64>, Vec<String>) = unembedded.into_iter().unzip();
    let vectors = embedder.embed_documents(&texts).await?;

    let pairs: Vec<(i64, Vec<f32>)> = ids.into_iter().zip(vectors).collect();

    let db = Arc::clone(db);
    let inserted = tokio::task::spawn_blocking(move || {
        let conn = db.blocking_lock();
        storage::add_embeddings(&conn, &pairs)
    })
    .await??;

    info!(embedded = inserted, "Embedded pending chunks");
    Ok(inserted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedder::MockEmbedder;
    use crate::storage::{self, Chunk, ChunkType};
    use std::path::Path;

    fn test_db() -> Arc<Mutex<Db>> {
        Arc::new(Mutex::new(storage::open_db(Path::new(":memory:")).unwrap()))
    }

    fn test_chunk(ts: &str, content: &str) -> Chunk {
        Chunk {
            channel_id: "C123".into(),
            channel_name: "dev".into(),
            chunk_type: ChunkType::Message,
            content: content.into(),
            authors: vec!["U001".into()],
            timestamp: ts.into(),
            thread_ts: None,
            message_hash: format!("hash_{ts}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_embeds_and_returns_results() {
        let db = test_db();
        {
            let conn = db.lock().await;
            storage::insert_chunk(&conn, &test_chunk("100.0", "hello world")).unwrap();
            storage::insert_chunk(&conn, &test_chunk("200.0", "goodbye world")).unwrap();
        }

        let outcome = search(db, &MockEmbedder, "test query", None, 10, 50)
            .await
            .unwrap();

        assert_eq!(outcome.embedded_now, 2);
        assert_eq!(outcome.results.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_filters_by_channel() {
        let db = test_db();
        {
            let conn = db.lock().await;
            storage::insert_chunk(&conn, &test_chunk("100.0", "in C123")).unwrap();
            let other = Chunk {
                channel_id: "C999".into(),
                message_hash: "hash_other".into(),
                ..test_chunk("200.0", "in C999")
            };
            storage::insert_chunk(&conn, &other).unwrap();
        }

        let outcome = search(db, &MockEmbedder, "query", Some("C123"), 10, 50)
            .await
            .unwrap();

        assert_eq!(outcome.results.len(), 1);
        assert_eq!(outcome.results[0].channel_id, "C123");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_respects_embed_budget() {
        let db = test_db();
        {
            let conn = db.lock().await;
            for i in 0..5 {
                storage::insert_chunk(
                    &conn,
                    &test_chunk(&format!("{i}00.0"), &format!("msg {i}")),
                )
                .unwrap();
            }
        }

        let outcome = search(db, &MockEmbedder, "query", None, 10, 2)
            .await
            .unwrap();

        assert_eq!(outcome.embedded_now, 2);
        assert_eq!(outcome.results.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_with_zero_budget_skips_embedding() {
        let db = test_db();
        {
            let conn = db.lock().await;
            storage::insert_chunk(&conn, &test_chunk("100.0", "not embedded")).unwrap();
        }

        let outcome = search(db, &MockEmbedder, "query", None, 10, 0)
            .await
            .unwrap();

        assert_eq!(outcome.embedded_now, 0);
        assert!(outcome.results.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_falls_back_when_embed_fails() {
        use crate::embedder::FailingEmbedder;

        let db = test_db();
        {
            let conn = db.lock().await;
            storage::insert_chunk(&conn, &test_chunk("100.0", "already here")).unwrap();
        }
        // Pre-embed with working embedder
        let pre = search(Arc::clone(&db), &MockEmbedder, "query", None, 10, 50)
            .await
            .unwrap();
        assert_eq!(pre.embedded_now, 1);

        // Search with failing embedder — should still return results from existing embeddings
        let outcome = search(db, &FailingEmbedder, "query", None, 10, 50)
            .await
            .unwrap();
        assert_eq!(outcome.embedded_now, 0);
        assert_eq!(outcome.results.len(), 1);
    }
}
