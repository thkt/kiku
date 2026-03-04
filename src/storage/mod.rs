pub mod embed;
pub mod search;
pub mod types;

pub use embed::{add_embeddings, get_unembedded_chunks};
pub use types::*;

use std::path::Path;

use rusqlite::Connection;
use rusqlite::ffi::sqlite3_auto_extension;
use sqlite_vec::sqlite3_vec_init;

pub type Db = Connection;

pub const EMBEDDING_DIMS: u32 = 768;

#[cfg(not(target_endian = "little"))]
compile_error!("kiku requires a little-endian target for f32↔u8 embedding storage");

pub(crate) fn f32_as_bytes(slice: &[f32]) -> &[u8] {
    bytemuck::cast_slice(slice)
}

pub fn open_db(path: &Path) -> Result<Db, StorageError> {
    ensure_sqlite_vec_registered()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }
    let conn = open_with_wal_recovery(path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA busy_timeout=5000;",
    )?;
    init_schema(&conn)?;
    Ok(conn)
}

fn ensure_sqlite_vec_registered() -> Result<(), StorageError> {
    static INIT: std::sync::OnceLock<Result<(), i32>> = std::sync::OnceLock::new();
    let init_result = INIT.get_or_init(|| {
        // SAFETY: sqlite3_vec_init is the auto-extension entry point exported by sqlite-vec.
        let rc = unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute::<
                unsafe extern "C" fn(),
                unsafe extern "C" fn(
                    *mut rusqlite::ffi::sqlite3,
                    *mut *mut std::os::raw::c_char,
                    *const rusqlite::ffi::sqlite3_api_routines,
                ) -> std::os::raw::c_int,
            >(sqlite3_vec_init)))
        };
        if rc == 0 { Ok(()) } else { Err(rc) }
    });
    if let Err(rc) = init_result {
        return Err(StorageError::Io(std::io::Error::other(format!(
            "sqlite-vec extension failed to register (sqlite3 rc={rc}). \
             This is a process-level initialization error that cannot be retried."
        ))));
    }
    Ok(())
}

fn open_with_wal_recovery(path: &Path) -> Result<Connection, StorageError> {
    match Connection::open(path) {
        Ok(c) => Ok(c),
        Err(ref e) if is_recoverable_open_error(e) => {
            tracing::warn!(error = %e, "DB open failed (recoverable), removing WAL/SHM and retrying");
            let path_str = path.to_string_lossy();
            let _ = std::fs::remove_file(format!("{path_str}-wal"));
            let _ = std::fs::remove_file(format!("{path_str}-shm"));
            Ok(Connection::open(path)?)
        }
        Err(e) => Err(e.into()),
    }
}

fn is_recoverable_open_error(err: &rusqlite::Error) -> bool {
    use rusqlite::ffi::ErrorCode;
    matches!(
        err,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: ErrorCode::DatabaseCorrupt
                    | ErrorCode::CannotOpen
                    | ErrorCode::NotADatabase,
                ..
            },
            _,
        )
    )
}

const SCHEMA_VERSION: &str = "2";

const DDL: &str = "
    CREATE TABLE IF NOT EXISTS chunks (
        id INTEGER PRIMARY KEY,
        channel_id TEXT NOT NULL,
        channel_name TEXT NOT NULL,
        chunk_type TEXT NOT NULL,
        content TEXT NOT NULL,
        authors TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        thread_ts TEXT,
        message_hash TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_chunks_channel ON chunks(channel_id);
    CREATE INDEX IF NOT EXISTS idx_chunks_hash ON chunks(message_hash);
    CREATE INDEX IF NOT EXISTS idx_chunks_ts ON chunks(timestamp);

    CREATE TABLE IF NOT EXISTS sync_state (
        channel_id TEXT PRIMARY KEY,
        latest_ts TEXT NOT NULL,
        oldest_ts TEXT,
        channel_name TEXT,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS index_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
";

fn init_schema(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(DDL)?;

    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE IF NOT EXISTS vec_chunks USING vec0(
            chunk_id INTEGER PRIMARY KEY,
            embedding FLOAT[{EMBEDDING_DIMS}]
        )"
    ))?;

    let stored: String = match conn.query_row(
        "SELECT value FROM index_meta WHERE key = 'schema_version'",
        [],
        |row| row.get(0),
    ) {
        Ok(v) => v,
        Err(rusqlite::Error::QueryReturnedNoRows) => "0".to_string(),
        Err(e) => return Err(e.into()),
    };

    if stored == "1" {
        match conn.execute_batch("ALTER TABLE sync_state ADD COLUMN channel_name TEXT") {
            Ok(()) => {}
            Err(e) if e.to_string().contains("duplicate column") => {}
            Err(e) => return Err(e.into()),
        }
    }

    if stored != SCHEMA_VERSION {
        conn.execute(
            "INSERT OR REPLACE INTO index_meta (key, value) VALUES ('schema_version', ?1)",
            [SCHEMA_VERSION],
        )?;
    }

    Ok(())
}

pub fn insert_chunk(
    conn: &Connection,
    chunk: &Chunk,
) -> Result<i64, StorageError> {
    let authors_json = serde_json::to_string(&chunk.authors)
        .expect("Vec<String> serialization cannot fail");
    conn.execute(
        "INSERT INTO chunks (channel_id, channel_name, chunk_type, content, authors, timestamp, thread_ts, message_hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            chunk.channel_id,
            chunk.channel_name,
            chunk.chunk_type.as_str(),
            chunk.content,
            authors_json,
            chunk.timestamp,
            chunk.thread_ts,
            chunk.message_hash,
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn upsert_chunk(
    conn: &Connection,
    chunk: &Chunk,
) -> Result<Option<i64>, StorageError> {
    let existing: Option<(i64, String)> = match conn.query_row(
        "SELECT id, message_hash FROM chunks WHERE channel_id = ?1 AND timestamp = ?2 AND thread_ts IS ?3",
        rusqlite::params![chunk.channel_id, chunk.timestamp, chunk.thread_ts],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
    ) {
        Ok(row) => Some(row),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e.into()),
    };

    match existing {
        Some((_, ref hash)) if hash == &chunk.message_hash => Ok(None), // unchanged
        Some((old_id, _)) => {
            conn.execute("DELETE FROM vec_chunks WHERE chunk_id = ?1", [old_id])?;
            conn.execute("DELETE FROM chunks WHERE id = ?1", [old_id])?;
            insert_chunk(conn, chunk).map(Some)
        }
        None => insert_chunk(conn, chunk).map(Some),
    }
}

pub fn get_sync_state(
    conn: &Connection,
    channel_id: &str,
) -> Result<Option<SyncState>, StorageError> {
    match conn.query_row(
        "SELECT channel_id, latest_ts, oldest_ts, channel_name, updated_at FROM sync_state WHERE channel_id = ?1",
        [channel_id],
        |row| {
            Ok(SyncState {
                channel_id: row.get(0)?,
                latest_ts: row.get(1)?,
                oldest_ts: row.get(2)?,
                channel_name: row.get(3)?,
                updated_at: row.get(4)?,
            })
        },
    ) {
        Ok(state) => Ok(Some(state)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn upsert_sync_state(
    conn: &Connection,
    channel_id: &str,
    latest_ts: &str,
    oldest_ts: Option<&str>,
    channel_name: Option<&str>,
) -> Result<(), StorageError> {
    conn.execute(
        "INSERT INTO sync_state (channel_id, latest_ts, oldest_ts, channel_name, updated_at)
         VALUES (?1, ?2, ?3, ?4, datetime('now'))
         ON CONFLICT(channel_id) DO UPDATE SET
           latest_ts = CASE WHEN ?2 > latest_ts THEN ?2 ELSE latest_ts END,
           oldest_ts = CASE WHEN ?3 IS NOT NULL AND (?3 < oldest_ts OR oldest_ts IS NULL) THEN ?3 ELSE oldest_ts END,
           channel_name = COALESCE(?4, channel_name),
           updated_at = datetime('now')",
        rusqlite::params![channel_id, latest_ts, oldest_ts, channel_name],
    )?;
    Ok(())
}

pub fn delete_expired_chunks(
    conn: &Connection,
    max_age_days: u32,
) -> Result<u32, StorageError> {
    let tx = conn.unchecked_transaction()?;
    let deleted = delete_expired_chunks_inner(&tx, max_age_days)?;
    tx.commit()?;
    Ok(deleted)
}

pub(crate) fn delete_expired_chunks_inner(
    conn: &Connection,
    max_age_days: u32,
) -> Result<u32, StorageError> {
    let cutoff_ts = cutoff_ts(max_age_days);
    conn.execute(
        "DELETE FROM vec_chunks WHERE chunk_id IN (SELECT id FROM chunks WHERE timestamp < ?1)",
        [&cutoff_ts],
    )?;
    let deleted = u32::try_from(conn.execute(
        "DELETE FROM chunks WHERE timestamp < ?1",
        [&cutoff_ts],
    )?).unwrap_or(u32::MAX);
    Ok(deleted)
}

pub(crate) fn cutoff_ts(max_age_days: u32) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs();
    let cutoff = now.saturating_sub(max_age_days as u64 * 86400);
    format!("{cutoff}.000000")
}

pub fn get_stats(conn: &Connection) -> Result<IndexStatus, StorageError> {
    let total_chunks: u32 = conn.query_row(
        "SELECT COUNT(*) FROM chunks",
        [],
        |row| row.get(0),
    )?;

    let total_channels: u32 = conn.query_row(
        "SELECT COUNT(DISTINCT channel_id) FROM chunks",
        [],
        |row| row.get(0),
    )?;

    let embedded_chunks: u32 = conn.query_row(
        "SELECT COUNT(*) FROM vec_chunks",
        [],
        |row| row.get(0),
    )?;

    let mut stmt = conn.prepare(
        "SELECT c.channel_id, c.channel_name, COUNT(*) as chunk_count,
                MAX(c.timestamp) as latest_ts,
                s.oldest_ts
         FROM chunks c
         LEFT JOIN sync_state s ON c.channel_id = s.channel_id
         GROUP BY c.channel_id",
    )?;
    let channels = stmt
        .query_map([], |row| {
            Ok(ChannelStatus {
                channel_id: row.get(0)?,
                channel_name: row.get(1)?,
                chunk_count: row.get(2)?,
                latest_ts: row.get(3)?,
                oldest_ts: row.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(IndexStatus {
        total_channels,
        total_chunks,
        embedded_chunks,
        channels,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> Connection {
        open_db(Path::new(":memory:")).unwrap()
    }

    #[test]
    fn open_db_creates_schema() {
        let conn = test_db();
        let count: u32 = conn
            .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn insert_and_query_chunk() {
        let conn = test_db();
        let chunk = Chunk {
            channel_id: "C123".to_string(),
            channel_name: "dev".to_string(),
            chunk_type: ChunkType::Message,
            content: "hello world".to_string(),
            authors: vec!["U001".to_string()],
            timestamp: "1709467200.000000".to_string(),
            thread_ts: None,
            message_hash: "abc123".to_string(),
        };
        let id = insert_chunk(&conn, &chunk).unwrap();
        assert!(id > 0);

        let content: String = conn
            .query_row("SELECT content FROM chunks WHERE id = ?1", [id], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(content, "hello world");
    }

    #[test]
    fn upsert_chunk_skips_unchanged() {
        let conn = test_db();
        let chunk = Chunk {
            channel_id: "C123".to_string(),
            channel_name: "dev".to_string(),
            chunk_type: ChunkType::Message,
            content: "hello".to_string(),
            authors: vec!["U001".to_string()],
            timestamp: "1709467200.000000".to_string(),
            thread_ts: None,
            message_hash: "hash1".to_string(),
        };
        let id1 = upsert_chunk(&conn, &chunk).unwrap();
        assert!(id1.is_some());

        // Same hash → skip
        let id2 = upsert_chunk(&conn, &chunk).unwrap();
        assert!(id2.is_none());
    }

    #[test]
    fn upsert_chunk_replaces_on_hash_change() {
        let conn = test_db();
        let chunk1 = Chunk {
            channel_id: "C123".to_string(),
            channel_name: "dev".to_string(),
            chunk_type: ChunkType::Message,
            content: "v1".to_string(),
            authors: vec!["U001".to_string()],
            timestamp: "1709467200.000000".to_string(),
            thread_ts: None,
            message_hash: "hash1".to_string(),
        };
        upsert_chunk(&conn, &chunk1).unwrap();

        let chunk2 = Chunk {
            message_hash: "hash2".to_string(),
            content: "v2".to_string(),
            ..chunk1
        };
        let id2 = upsert_chunk(&conn, &chunk2).unwrap();
        assert!(id2.is_some());

        let count: u32 = conn
            .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn sync_state_round_trip() {
        let conn = test_db();
        assert!(get_sync_state(&conn, "C123").unwrap().is_none());

        upsert_sync_state(&conn, "C123", "100.000000", None, None).unwrap();
        let state = get_sync_state(&conn, "C123").unwrap().unwrap();
        assert_eq!(state.latest_ts, "100.000000");
        assert!(state.oldest_ts.is_none());

        // Update with newer ts
        upsert_sync_state(&conn, "C123", "200.000000", Some("50.000000"), Some("dev")).unwrap();
        let state = get_sync_state(&conn, "C123").unwrap().unwrap();
        assert_eq!(state.latest_ts, "200.000000");
        assert_eq!(state.oldest_ts.as_deref(), Some("50.000000"));
    }

    #[test]
    fn get_stats_returns_correct_counts() {
        let conn = test_db();
        let chunk = Chunk {
            channel_id: "C111".to_string(),
            channel_name: "dev".to_string(),
            chunk_type: ChunkType::Message,
            content: "msg1".to_string(),
            authors: vec!["U001".to_string()],
            timestamp: "100.000000".to_string(),
            thread_ts: None,
            message_hash: "h1".to_string(),
        };
        insert_chunk(&conn, &chunk).unwrap();
        insert_chunk(
            &conn,
            &Chunk {
                timestamp: "200.000000".to_string(),
                message_hash: "h2".to_string(),
                ..chunk.clone()
            },
        )
        .unwrap();

        let stats = get_stats(&conn).unwrap();
        assert_eq!(stats.total_channels, 1);
        assert_eq!(stats.total_chunks, 2);
        assert_eq!(stats.embedded_chunks, 0);
        assert_eq!(stats.channels.len(), 1);
    }

    #[test]
    fn delete_expired_chunks_removes_old() {
        let conn = test_db();
        let old_chunk = Chunk {
            channel_id: "C123".to_string(),
            channel_name: "dev".to_string(),
            chunk_type: ChunkType::Message,
            content: "ancient".to_string(),
            authors: vec!["U001".to_string()],
            timestamp: "1500000000.000000".to_string(), // 2017-07-14 (10-digit, same as current ts)
            thread_ts: None,
            message_hash: "old_hash".to_string(),
        };
        let recent_ts = format!(
            "{}.000000",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        let new_chunk = Chunk {
            timestamp: recent_ts,
            message_hash: "new_hash".to_string(),
            content: "recent".to_string(),
            ..old_chunk.clone()
        };
        insert_chunk(&conn, &old_chunk).unwrap();
        insert_chunk(&conn, &new_chunk).unwrap();

        let deleted = delete_expired_chunks(&conn, 90).unwrap();
        assert_eq!(deleted, 1);

        let remaining: u32 = conn
            .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(remaining, 1);
    }
}
