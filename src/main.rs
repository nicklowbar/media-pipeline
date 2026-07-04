use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use tracing::{info, warn};

mod config;
mod db;
mod layout;
mod library;
mod metadata;
mod plex;
mod pipeline;
mod policy;
mod rename;
mod sync;
mod worker_pool;

use crate::config::Config;
use crate::db::Database;

#[derive(Parser)]
#[command(name = "media-pipeline")]
#[command(about = "Automated media sync, rename, and ingest pipeline")]
struct Cli {
    #[arg(
        short, long, value_name = "FILE",
        default_value = "/etc/media-pipeline/config.toml",
        global = true,
    )]
    config: PathBuf,

    /// Run continuously on the given interval (e.g. "12h", "30m").
    /// Without this flag, runs once and exits.
    #[arg(long, value_name = "DURATION")]
    interval: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the full pipeline
    Run,
    /// Show pipeline status
    Status,
    /// Seed the database from existing staging / library directories
    Seed,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Parse the config first so we can pick up the log level. The
    // config is the single source of truth for log verbosity; the
    // env var MEDIA_PIPELINE_LOG_LEVEL (applied during
    // load_with_env) is the container-deploy override.
    //
    // Config-load errors are surfaced via anyhow's Display on the
    // returned Err — that prints to stderr with the `with_context`
    // message. The tracing subscriber is bootstrapped below, after
    // we know what level to use.
    let config = Config::load_with_env(&cli.config)
        .with_context(|| format!("failed to load config from {}", cli.config.display()))?;

    // Build the EnvFilter from the resolved level. Falls back to
    // "info" if the configured value is unparseable (typo guard,
    // same principle as apply_env_u16).
    let filter = tracing_subscriber::EnvFilter::try_new(config.log_level())
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter(filter)
        .init();

    info!(log_level = %config.log_level(), "media-pipeline starting");

    info!(config_path = %cli.config.display(), "config loaded");

    let db_path = &config.database.path;
    let db = Database::open(db_path)
        .with_context(|| format!("failed to open database at {}", db_path.display()))?;

    info!(db_path = %db_path.display(), "database opened");

    // `--interval` only applies to the Run command.
    let interval = cli.interval.as_deref();

    match &cli.command {
        Commands::Run => {
            loop {
                pipeline::run_full(&config, &db).await?;

                match interval {
                    Some(dur_str) => {
                        let dur = parse_duration(dur_str)
                            .with_context(|| format!("invalid interval duration: {dur_str}"))?;
                        info!(secs = dur.as_secs(), "sync complete, sleeping before next run");
                        tokio::time::sleep(dur).await;
                    }
                    None => break,
                }
            }
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

fn parse_duration(s: &str) -> anyhow::Result<std::time::Duration> {
    let s = s.trim();
    let secs = if s.ends_with('h') || s.ends_with('H') {
        let n: u64 = s[..s.len()-1]
            .parse()
            .with_context(|| format!("invalid hours in duration: {s}"))?;
        n * 3600
    } else if s.ends_with('m') || s.ends_with('M') {
        let n: u64 = s[..s.len()-1]
            .parse()
            .with_context(|| format!("invalid minutes in duration: {s}"))?;
        n * 60
    } else if s.ends_with('s') || s.ends_with('S') {
        let n: u64 = s[..s.len()-1]
            .parse()
            .with_context(|| format!("invalid seconds in duration: {s}"))?;
        n
    } else {
        anyhow::bail!("duration must end in h, m, or s: {s}");
    };
    Ok(std::time::Duration::from_secs(secs))
}
