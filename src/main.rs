use clap::{Parser, Subcommand};
use kiku::tools::Kiku;
use tracing::info;

use kiku::fetcher::sync::HarvestMode;

#[derive(Parser)]
#[command(name = "kiku", about = "Slack conversation semantic search")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Fetch and index Slack channel conversations
    Harvest {
        /// Slack channel ID (e.g., C1234567890)
        channel: String,
        /// Sync mode
        #[arg(long, default_value = "incremental")]
        mode: HarvestMode,
    },
    /// Semantic search over indexed conversations
    Search {
        /// Search query
        query: String,
        /// Filter by channel ID
        #[arg(long)]
        channel: Option<String>,
        /// Max results (1-100)
        #[arg(long, default_value = "10")]
        limit: u32,
    },
    /// Show index status
    Status,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("kiku=info".parse()?),
        )
        .init();

    let cli = Cli::parse();
    let kiku = Kiku::new()?;

    match cli.command {
        Command::Harvest { channel, mode } => {
            let output = kiku.harvest(&channel, mode).await?;
            println!("{output}");
        }
        Command::Search { query, channel, limit } => {
            let output = kiku.search(&query, channel.as_deref(), limit).await?;
            println!("{output}");
        }
        Command::Status => {
            let output = kiku.status().await?;
            println!("{output}");
        }
    }

    info!("done");
    Ok(())
}
