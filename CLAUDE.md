# kiku - Slack Conversation Semantic Search CLI

## What is kiku

Slack チャンネルの会話からユビキタス言語を効率よく発見するための semantic
search インフラ。yomu (~/GitHub/yomu/) の兄弟ツール。

**コアユースケース:** キーワードを知らない状態で「このチャンネルで独特な使われ方を
している用語」を発見する。Slack search API はキーワード前提なので、embedding ベース
のセマンティック検索が必要。

## Architecture

yomu からコピーフォーク。Rust CLI (clap)。

```
kiku/src/
├── main.rs            ← CLI entry point (clap subcommands)
├── lib.rs             ← Module declarations
├── config.rs          ← Config + channel allowlist
├── fetcher/
│   ├── mod.rs         ← Types: SlackMessage, MessagePage, FetchError
│   ├── client.rs      ← Slack API client (rate limit + backoff)
│   └── sync.rs        ← Cursor-based incremental/backfill sync
├── chunker.rs         ← Thread-based chunking (SHA256 hash)
├── embedder.rs        ← Gemini embedding API (copy from yomu)
├── storage/
│   ├── mod.rs         ← SQLite + sqlite-vec, sync_state, expiry
│   ├── types.rs       ← Chunk, ChunkType, SyncState
│   ├── embed.rs       ← Embedding storage/retrieval
│   └── search.rs      ← Vector similarity search
├── query.rs           ← Semantic search (embed_pending → vec search)
├── tools/mod.rs       ← Kiku struct: harvest, search, status
└── redact.rs          ← Token redaction for error messages
```

## Design Decisions

| Decision             | Choice                | Rationale                                |
| -------------------- | --------------------- | ---------------------------------------- |
| 検索方式             | embedding-only        | 日本語に FTS5 は不適。概念発見がコア要件 |
| チャンク単位         | thread / message      | 時間窓は意味境界にならない               |
| embedding タイミング | search 時に遅延       | harvest 時間の肥大化を回避               |
| DB 配置              | $XDG_DATA_HOME/kiku/  | プロジェクト外でセキュリティ確保         |
| Token                | bot token (xoxb) 推奨 | スコープ制限可能。xoxp もフォールバック  |
| glossary 生成        | kiku 外（スキル）     | LLM 処理は CLI の責務外                  |
| 共有コード戦略       | コピーフォーク        | 独立進化を許容。workspace は時期尚早     |

## Implementation Status

全フェーズ実装完了。84 tests passing, 0 clippy warnings。
FR 詳細は spec 参照: `~/.claude/workspace/planning/2026-03-03-kiku/spec.md`

## Key References

- SOW: ~/.claude/workspace/planning/2026-03-03-kiku/sow.md
- Spec: ~/.claude/workspace/planning/2026-03-03-kiku/spec.md
- Plan: ~/.claude/workspace/planning/2026-03-03-kiku/plan.md
- ADR: ~/.claude/adr/0021-build-slack-semantic-search-mcp-kiku.md
- yomu (reference): ~/GitHub/yomu/

## Slack API Reference

| Endpoint              | Tier | Rate       | Purpose               |
| --------------------- | ---- | ---------- | --------------------- |
| conversations.history | 3    | 1 req/s    | Channel message fetch |
| conversations.replies | 2    | 20 req/min | Thread reply fetch    |
| conversations.info    | 3    | 1 req/s    | Channel name resolve  |

## Environment Variables

| Variable        | Required | Description                                    |
| --------------- | -------- | ---------------------------------------------- |
| SLACK_TOKEN     | Yes      | Bot token (xoxb-) or user token (xoxp-)        |
| GEMINI_API_KEY  | No       | For embedding (search won't embed without it)  |
| XDG_DATA_HOME   | No       | DB location override (default: ~/.local/share) |
| XDG_CONFIG_HOME | No       | Config location override (default: ~/.config)  |

## CLI Usage

```bash
kiku harvest C1234567890              # Incremental sync
kiku harvest C1234567890 --mode backfill  # Backfill older messages
kiku search "ドメイン用語"              # Semantic search
kiku search "認証" --channel C123 --limit 5
kiku status                            # Index stats
```

## Test Commands

```bash
cargo test           # Run all tests
cargo check          # Type check only
cargo test -- --nocapture  # With stdout
```

## Known Issues (audit-2026-03-04 #5)

40 findings (0H, 16M, 24L)。前回比 5 resolved, 15 new。
詳細: `~/.claude/workspace/history/audit-2026-03-04-101500.yaml`

### Priority High

| ID    | File         | Issue                                               |
| ----- | ------------ | --------------------------------------------------- |
| F-045 | tools/mod.rs | open_db エラーで StorageError variant 喪失          |
| F-059 | storage/mod  | schema migration `let _` が全エラーを握り潰し       |
| F-060 | tools/mod.rs | status() が async Mutex 保持したまま blocking query |

### Root Causes

| ID     | Status  | Description                                               |
| ------ | ------- | --------------------------------------------------------- |
| RC-001 | carried | 型レベル不変条件なし（raw string, public fields）         |
| RC-002 | carried | エラー情報の境界消失（F-045, F-059 含む）                 |
| RC-003 | carried | DI 不可の依存（cutoff_ts, jitter に SystemTime 直接使用） |
| RC-007 | new     | async/blocking 境界の不統一（F-004, F-060）               |
