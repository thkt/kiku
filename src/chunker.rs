use sha2::{Digest, Sha256};

use crate::fetcher::SlackMessage;
use crate::storage::{Chunk, ChunkType};

pub fn chunk_message(msg: &SlackMessage, channel_id: &str, channel_name: &str) -> Chunk {
    Chunk {
        channel_id: channel_id.to_string(),
        channel_name: channel_name.to_string(),
        chunk_type: ChunkType::Message,
        content: msg.text.clone(),
        authors: vec![msg.user.clone()],
        timestamp: msg.ts.clone(),
        thread_ts: None,
        message_hash: sha256(&msg.text),
    }
}

/// Returns `None` if `messages` is empty.
pub fn chunk_thread(messages: &[SlackMessage], channel_id: &str, channel_name: &str) -> Option<Chunk> {
    let first = messages.first()?;

    let content: String = messages
        .iter()
        .map(|m| m.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    let mut authors: Vec<String> = messages.iter().map(|m| m.user.clone()).collect();
    authors.sort();
    authors.dedup();

    let hash = sha256(&content);

    Some(Chunk {
        channel_id: channel_id.to_string(),
        channel_name: channel_name.to_string(),
        chunk_type: ChunkType::Thread,
        content,
        authors,
        timestamp: first.ts.clone(),
        thread_ts: Some(first.thread_ts.clone().unwrap_or_else(|| first.ts.clone())),
        message_hash: hash,
    })
}

fn sha256(input: &str) -> String {
    format!("{:x}", Sha256::digest(input.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(ts: &str, user: &str, text: &str) -> SlackMessage {
        SlackMessage {
            ts: ts.to_string(),
            user: user.to_string(),
            text: text.to_string(),
            thread_ts: None,
            reply_count: 0,
        }
    }

    fn thread_msg(ts: &str, user: &str, text: &str, thread_ts: &str) -> SlackMessage {
        SlackMessage {
            thread_ts: Some(thread_ts.to_string()),
            ..msg(ts, user, text)
        }
    }

    #[test]
    fn standalone_message_creates_message_chunk() {
        let m = msg("100.0", "U1", "hello world");
        let chunk = chunk_message(&m, "C123", "dev");

        assert_eq!(chunk.chunk_type, ChunkType::Message);
        assert_eq!(chunk.content, "hello world");
        assert_eq!(chunk.authors, vec!["U1"]);
        assert_eq!(chunk.timestamp, "100.0");
        assert!(chunk.thread_ts.is_none());
        assert!(!chunk.message_hash.is_empty());
    }

    #[test]
    fn thread_creates_thread_chunk_with_concatenated_content() {
        let messages = vec![
            thread_msg("100.0", "U1", "parent msg", "100.0"),
            thread_msg("101.0", "U2", "reply 1", "100.0"),
            thread_msg("102.0", "U1", "reply 2", "100.0"),
        ];
        let chunk = chunk_thread(&messages, "C123", "dev").unwrap();

        assert_eq!(chunk.chunk_type, ChunkType::Thread);
        assert_eq!(chunk.content, "parent msg\nreply 1\nreply 2");
        assert_eq!(chunk.timestamp, "100.0");
        assert_eq!(chunk.thread_ts, Some("100.0".to_string()));
    }

    #[test]
    fn thread_deduplicates_authors() {
        let messages = vec![
            thread_msg("1.0", "U1", "a", "1.0"),
            thread_msg("2.0", "U2", "b", "1.0"),
            thread_msg("3.0", "U1", "c", "1.0"),
        ];
        let chunk = chunk_thread(&messages, "C1", "ch").unwrap();

        assert_eq!(chunk.authors, vec!["U1", "U2"]);
    }

    #[test]
    fn hash_changes_when_content_changes() {
        let m1 = msg("1.0", "U1", "version 1");
        let m2 = msg("1.0", "U1", "version 2");

        let h1 = chunk_message(&m1, "C1", "ch").message_hash;
        let h2 = chunk_message(&m2, "C1", "ch").message_hash;

        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_stable_for_same_content() {
        let m = msg("1.0", "U1", "same text");

        let h1 = chunk_message(&m, "C1", "ch").message_hash;
        let h2 = chunk_message(&m, "C1", "ch").message_hash;

        assert_eq!(h1, h2);
    }

    #[test]
    fn chunk_thread_empty_messages_returns_none() {
        assert!(chunk_thread(&[], "C1", "ch").is_none());
    }
}
