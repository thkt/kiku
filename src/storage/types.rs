#[derive(Debug, Clone, PartialEq)]
pub enum ChunkType {
    /// reply_count > 0: parent + all replies as one chunk
    Thread,
    /// Standalone message not in any thread
    Message,
}

impl ChunkType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Thread => "thread",
            Self::Message => "message",
        }
    }

    pub fn from_db(s: &str) -> Self {
        match s {
            "thread" => Self::Thread,
            "message" => Self::Message,
            other => {
                tracing::warn!(chunk_type = other, "Unknown chunk_type in DB, defaulting to Message");
                Self::Message
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub channel_id: String,
    pub channel_name: String,
    pub chunk_type: ChunkType,
    pub content: String,
    pub authors: Vec<String>,
    pub timestamp: String,
    pub thread_ts: Option<String>,
    pub message_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SyncState {
    pub channel_id: String,
    pub latest_ts: String,
    pub oldest_ts: Option<String>,
    pub channel_name: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexStatus {
    pub total_channels: u32,
    pub total_chunks: u32,
    pub embedded_chunks: u32,
    pub channels: Vec<ChannelStatus>,
}

impl IndexStatus {
    pub fn embed_percentage(&self) -> u32 {
        if self.total_chunks > 0 {
            (self.embedded_chunks as f64 / self.total_chunks as f64 * 100.0).round() as u32
        } else {
            0
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelStatus {
    pub channel_id: String,
    pub channel_name: String,
    pub chunk_count: u32,
    pub latest_ts: String,
    pub oldest_ts: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("embedding dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
}
