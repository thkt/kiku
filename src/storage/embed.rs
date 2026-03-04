use rusqlite::Connection;

use super::{EMBEDDING_DIMS, StorageError, f32_as_bytes};

pub(super) fn check_dims(embedding: &[f32]) -> Result<(), StorageError> {
    if embedding.len() != EMBEDDING_DIMS as usize {
        return Err(StorageError::DimensionMismatch {
            expected: EMBEDDING_DIMS as usize,
            actual: embedding.len(),
        });
    }
    Ok(())
}

pub fn add_embeddings(
    conn: &Connection,
    embeddings: &[(i64, Vec<f32>)],
) -> Result<u32, StorageError> {
    let tx = conn.unchecked_transaction()?;
    let mut exists_stmt =
        tx.prepare("SELECT EXISTS(SELECT 1 FROM vec_chunks WHERE chunk_id = ?1)")?;
    let mut insert_stmt =
        tx.prepare("INSERT INTO vec_chunks (chunk_id, embedding) VALUES (?1, ?2)")?;
    let mut inserted = 0u32;

    for (chunk_id, embedding) in embeddings {
        check_dims(embedding)?;
        let exists: bool = exists_stmt.query_row([chunk_id], |row| row.get(0))?;
        if exists {
            continue;
        }
        let bytes = f32_as_bytes(embedding);
        insert_stmt.execute(rusqlite::params![chunk_id, bytes])?;
        inserted += 1;
    }

    drop(exists_stmt);
    drop(insert_stmt);
    tx.commit()?;
    Ok(inserted)
}

pub fn get_unembedded_chunks(
    conn: &Connection,
    limit: u32,
) -> Result<Vec<(i64, String)>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT c.id, c.content FROM chunks c
         LEFT JOIN vec_chunks v ON c.id = v.chunk_id
         WHERE v.chunk_id IS NULL
         LIMIT ?1",
    )?;
    let rows = stmt.query_map([limit], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{self, Chunk, ChunkType, EMBEDDING_DIMS};
    use std::path::Path;

    fn test_db() -> Connection {
        storage::open_db(Path::new(":memory:")).unwrap()
    }

    fn dummy_embedding(seed: f32) -> Vec<f32> {
        let mut v = vec![0.0f32; EMBEDDING_DIMS as usize];
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
    fn add_embeddings_inserts_new() {
        let conn = test_db();
        let id = storage::insert_chunk(&conn, &test_chunk("100.0", "hello")).unwrap();
        let inserted = add_embeddings(&conn, &[(id, dummy_embedding(1.0))]).unwrap();
        assert_eq!(inserted, 1);
    }

    #[test]
    fn add_embeddings_skips_existing() {
        let conn = test_db();
        let id = storage::insert_chunk(&conn, &test_chunk("100.0", "hello")).unwrap();
        add_embeddings(&conn, &[(id, dummy_embedding(1.0))]).unwrap();
        let inserted = add_embeddings(&conn, &[(id, dummy_embedding(2.0))]).unwrap();
        assert_eq!(inserted, 0);
    }

    #[test]
    fn add_embeddings_rejects_wrong_dims() {
        let conn = test_db();
        let id = storage::insert_chunk(&conn, &test_chunk("100.0", "hello")).unwrap();
        let bad = vec![1.0f32; 10];
        let err = add_embeddings(&conn, &[(id, bad)]).unwrap_err();
        assert!(matches!(err, StorageError::DimensionMismatch { .. }));
    }

    #[test]
    fn get_unembedded_returns_chunks_without_embeddings() {
        let conn = test_db();
        let id1 = storage::insert_chunk(&conn, &test_chunk("100.0", "embedded")).unwrap();
        storage::insert_chunk(&conn, &test_chunk("200.0", "not embedded")).unwrap();
        add_embeddings(&conn, &[(id1, dummy_embedding(1.0))]).unwrap();

        let unembedded = get_unembedded_chunks(&conn, 100).unwrap();
        assert_eq!(unembedded.len(), 1);
        assert_eq!(unembedded[0].1, "not embedded");
    }

    #[test]
    fn get_unembedded_respects_limit() {
        let conn = test_db();
        storage::insert_chunk(&conn, &test_chunk("100.0", "a")).unwrap();
        storage::insert_chunk(&conn, &test_chunk("200.0", "b")).unwrap();

        let unembedded = get_unembedded_chunks(&conn, 1).unwrap();
        assert_eq!(unembedded.len(), 1);
    }
}
