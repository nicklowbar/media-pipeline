use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use tracing::{info, warn};

mod config;
mod db;
mod library;
mod metadata;
mod plex;
mod pipeline;
mod policy;
mod rename;
mod sync;
mod transcode;

use crate::config::Config;
use crate::db::Database;

#[derive(Parser)]
#[command(name = "media-pipeline")]
#[command(about = "Automated media sync, rename, transcode, and ingest pipeline")]
struct Cli {
    #[arg(short, long, value_name = "FILE", default_value = "/etc/media-pipeline/config.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the full pipeline
    Run,
    /// Run only the sync phase
    #[command(name = "sync-only")]
    SyncOnly,
    /// Run only the process phase (rename + transcode + move)
    #[command(name = "process-only")]
    ProcessOnly,
    /// Show pipeline status
    Status,
    /// Seed the database from existing staging / library directories
    Seed,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing (journald-compatible if available)
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    info!("media-pipeline starting");

    let config = Config::load(&cli.config)
        .with_context(|| format!("failed to load config from {}", cli.config.display()))?;

    info!(config_path = %cli.config.display(), "config loaded");

    let db_path = &config.database.path;
    let db = Database::open(db_path)
        .with_context(|| format!("failed to open database at {}", db_path.display()))?;

    info!(db_path = %db_path.display(), "database opened");

    match cli.command {
        Commands::Run => {
            info!("running full pipeline");
            pipeline::run_full(&config, &db).await?;
        }
        Commands::SyncOnly => {
            info!("running sync phase only");
            pipeline::run_sync(&config, &db).await?;
        }
        Commands::ProcessOnly => {
            info!("running process phase only");
            pipeline::run_process(&config, &db).await?;
        }
        Commands::Status => {
            pipeline::print_status(&db)?;
        }
        Commands::Seed => {
            warn!("seed command not yet implemented — see Phase 2");
        }
    }

    info!("media-pipeline finished");
    Ok(())
}
