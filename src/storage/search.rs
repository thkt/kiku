use rusqlite::Connection;

use super::{ChunkType, StorageError, embed::check_dims, f32_as_bytes};

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub chunk_id: i64,
    pub channel_id: String,
    pub channel_name: String,
    pub thread_ts: Option<String>,
    pub chunk_type: ChunkType,
    pub content: String,
    pub authors: Vec<String>,
    pub timestamp: String,
    pub distance: f32,
}

const BASE_SQL: &str = "\
    SELECT v.chunk_id, v.distance, c.channel_id, c.channel_name,
           c.chunk_type, c.content, c.authors, c.timestamp, c.thread_ts
    FROM vec_chunks v
    JOIN chunks c ON c.id = v.chunk_id
    WHERE v.embedding MATCH ?1 AND k = ?2";

pub fn search_similar(
    conn: &Connection,
    query_embedding: &[f32],
    channel_id: Option<&str>,
    limit: u32,
) -> Result<Vec<SearchResult>, StorageError> {
    check_dims(query_embedding)?;

    let query_bytes = f32_as_bytes(query_embedding);

    let sql = match channel_id {
        Some(_) => format!("{BASE_SQL} AND c.channel_id = ?3 ORDER BY v.distance"),
        None => format!("{BASE_SQL} ORDER BY v.distance"),
    };

    let mut stmt = conn.prepare(&sql)?;
    let rows = match channel_id {
        Some(ch) => stmt.query_map(
            rusqlite::params![query_bytes, limit as i64, ch],
            map_search_row,
        )?,
        None => stmt.query_map(
            rusqlite::params![query_bytes, limit as i64],
            map_search_row,
        )?,
    };

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn map_search_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchResult> {
    let authors_json: String = row.get(6)?;
    let authors: Vec<String> = serde_json::from_str(&authors_json).unwrap_or_else(|e| {
        tracing::warn!(json = %authors_json, error = %e, "Malformed authors JSON, defaulting to empty");
        Vec::new()
    });

    Ok(SearchResult {
        chunk_id: row.get(0)?,
        distance: row.get(1)?,
        channel_id: row.get(2)?,
        channel_name: row.get(3)?,
        chunk_type: ChunkType::from_db(&row.get::<_, String>(4)?),
        content: row.get(5)?,
        authors,
        timestamp: row.get(7)?,
        thread_ts: row.get(8)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{self, Chunk, ChunkType};
    use std::path::Path;

    fn test_db() -> Connection {
        storage::open_db(Path::new(":memory:")).unwrap()
    }

    fn dummy_embedding(seed: f32) -> Vec<f32> {
        let mut v = vec![0.0f32; crate::storage::EMBEDDING_DIMS as usize];
        v[0] = seed;
        v
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

    #[test]
    fn search_returns_results_by_distance() {
        let conn = test_db();
        let id1 = storage::insert_chunk(&conn, &test_chunk("100.0", "hello")).unwrap();
        let id2 = storage::insert_chunk(&conn, &test_chunk("200.0", "world")).unwrap();

        storage::add_embeddings(
            &conn,
            &[(id1, dummy_embedding(1.0)), (id2, dummy_embedding(2.0))],
        )
        .unwrap();

        let query = dummy_embedding(1.1); // closer to id1
        let results = search_similar(&conn, &query, None, 10).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].chunk_id, id1);
        assert_eq!(results[1].chunk_id, id2);
    }

    #[test]
    fn search_filters_by_channel() {
        let conn = test_db();
        let chunk1 = Chunk {
            channel_id: "C111".into(),
            ..test_chunk("100.0", "in channel 1")
        };
        let chunk2 = Chunk {
            channel_id: "C222".into(),
            message_hash: "hash_200".into(),
            ..test_chunk("200.0", "in channel 2")
        };
        let id1 = storage::insert_chunk(&conn, &chunk1).unwrap();
        let id2 = storage::insert_chunk(&conn, &chunk2).unwrap();

        storage::add_embeddings(
            &conn,
            &[(id1, dummy_embedding(1.0)), (id2, dummy_embedding(2.0))],
        )
        .unwrap();

        let query = dummy_embedding(0.0);
        let results = search_similar(&conn, &query, Some("C111"), 10).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].channel_id, "C111");
    }

    #[test]
    fn search_rejects_wrong_dimensions() {
        let conn = test_db();
        let bad_embedding = vec![1.0f32; 10];
        let err = search_similar(&conn, &bad_embedding, None, 10).unwrap_err();
        assert!(matches!(err, StorageError::DimensionMismatch { .. }));
    }

    #[test]
    fn search_returns_empty_when_no_embeddings() {
        let conn = test_db();
        storage::insert_chunk(&conn, &test_chunk("100.0", "no embedding")).unwrap();

        let query = dummy_embedding(1.0);
        let results = search_similar(&conn, &query, None, 10).unwrap();
        assert!(results.is_empty());
    }
}
