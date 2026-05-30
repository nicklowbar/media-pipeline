use std::path::Path;

use anyhow::Context;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::db::{Database, DirectoryState};
use crate::library;
use crate::plex;
use crate::policy::DetectedPolicy;
use crate::rename;
use crate::sync;
use crate::transcode;

/// Run the full pipeline: sync → analyze → rename → transcode → move → plex scan
pub async fn run_full(config: &Config, db: &Database) -> anyhow::Result<()> {
    run_sync(config, db).await?;
    run_process(config, db).await?;
    Ok(())
}

/// Run only the sync phase
pub async fn run_sync(config: &Config, db: &Database) -> anyhow::Result<()> {
    info!("starting sync phase");

    let mut sync_engine = sync::SyncEngine::new(config).await
        .context("failed to initialize sync engine")?;

    for (category_name, _) in &config.categories {
        info!(category = %category_name, "syncing category");
        if let Err(e) = sync_engine.sync_category(category_name, db).await {
            error!(category = %category_name, error = %e, "category sync failed");
            // Continue with other categories rather than failing the whole pipeline
        }
    }

    info!("sync phase complete");
    Ok(())
}

/// Run only the process phase: analyze → rename → transcode → move → plex scan
pub async fn run_process(config: &Config, db: &Database) -> anyhow::Result<()> {
    info!("starting process phase");

    // 1. Analyze
    analyze_directories(config, db).await?;

    // 2. Rename
    rename_directories(config, db)?;

    // 3. Transcode
    transcode_directories(config, db).await?;

    // 4. Move to library
    move_to_library(config, db)?;

    // 5. Plex scan
    trigger_plex_scans(config, db).await?;

    info!("process phase complete");
    Ok(())
}

async fn analyze_directories(config: &Config, db: &Database) -> anyhow::Result<()> {
    let dirs = db.get_directories_in_state(DirectoryState::Synced)?;
    info!(count = dirs.len(), "directories to analyze");

    for dir in dirs {
        let staging_path = Path::new(&dir.staging_path);
        info!(dir_id = dir.id, path = %staging_path.display(), "analyzing directory");

        db.set_directory_state(dir.id, DirectoryState::Analyzing)?;

        match crate::policy::analyze_directory(staging_path, db, dir.id).await {
            Ok(detected_policy) => {
                let policy_str = detected_policy.as_str();
                db.set_directory_policy(dir.id, policy_str)?;
                db.set_directory_state(dir.id, DirectoryState::Analyzed)?;
                info!(dir_id = dir.id, policy = %policy_str, "analysis complete");
            }
            Err(e) => {
                db.set_directory_error(dir.id, &format!("analysis failed: {}", e))?;
                error!(dir_id = dir.id, error = %e, "analysis failed");
            }
        }
    }

    Ok(())
}

fn rename_directories(config: &Config, db: &Database) -> anyhow::Result<()> {
    let dirs = db.get_directories_in_state(DirectoryState::Analyzed)?;
    info!(count = dirs.len(), "directories to rename");

    for dir in dirs {
        let staging_path = Path::new(&dir.staging_path);
        info!(dir_id = dir.id, path = %staging_path.display(), "renaming directory");

        db.set_directory_state(dir.id, DirectoryState::Renaming)?;

        let detected_policy = dir.detected_policy.as_deref()
            .and_then(DetectedPolicy::from_str);

        match rename::rename_directory(staging_path, config, &dir.category, db, dir.id, detected_policy.as_ref()) {
            Ok(()) => {
                db.set_directory_state(dir.id, DirectoryState::Renamed)?;
                info!(dir_id = dir.id, "rename complete");
            }
            Err(e) => {
                db.set_directory_error(dir.id, &format!("rename failed: {}", e))?;
                error!(dir_id = dir.id, error = %e, "rename failed");
            }
        }
    }

    Ok(())
}

async fn transcode_directories(config: &Config, db: &Database) -> anyhow::Result<()> {
    let dirs = db.get_directories_in_state(DirectoryState::Renamed)?;
    info!(count = dirs.len(), "directories to transcode");

    for dir in dirs {
        let staging_path = Path::new(&dir.staging_path);
        info!(dir_id = dir.id, path = %staging_path.display(), "transcoding directory");

        let detected_policy = dir.detected_policy.as_deref()
            .and_then(DetectedPolicy::from_str)
            .unwrap_or(DetectedPolicy::None);

        if matches!(detected_policy, DetectedPolicy::None | DetectedPolicy::Manual) {
            info!(dir_id = dir.id, policy = %detected_policy.as_str(), "skipping transcode");
            db.set_directory_state(dir.id, DirectoryState::Transcoded)?;
            continue;
        }

        db.set_directory_state(dir.id, DirectoryState::Transcoding)?;

        match transcode::transcode_directory(staging_path, detected_policy, db, dir.id, config.group_name()).await {
            Ok(()) => {
                db.set_directory_state(dir.id, DirectoryState::Transcoded)?;
                info!(dir_id = dir.id, "transcode complete");
            }
            Err(e) => {
                db.set_directory_error(dir.id, &format!("transcode failed: {}", e))?;
                error!(dir_id = dir.id, error = %e, "transcode failed");
            }
        }
    }

    Ok(())
}

fn move_to_library(config: &Config, db: &Database) -> anyhow::Result<()> {
    let dirs = db.get_directories_in_state(DirectoryState::Transcoded)?;
    info!(count = dirs.len(), "directories to move");

    for dir in dirs {
        let staging_path = Path::new(&dir.staging_path);
        let library_dir = config.library_path(&dir.category);

        info!(dir_id = dir.id, src = %staging_path.display(), dst = %library_dir.display(), "moving to library");

        db.set_directory_state(dir.id, DirectoryState::Moving)?;

        match library::move_to_library(staging_path, &library_dir, db, dir.id) {
            Ok(library_path) => {
                db.set_directory_library_path(dir.id, &library_path.to_string_lossy())?;
                db.set_directory_state(dir.id, DirectoryState::InLibrary)?;
                info!(dir_id = dir.id, path = %library_path.display(), "move complete");
            }
            Err(e) => {
                db.set_directory_error(dir.id, &format!("move failed: {}", e))?;
                error!(dir_id = dir.id, error = %e, "move failed");
            }
        }
    }

    Ok(())
}

async fn trigger_plex_scans(config: &Config, db: &Database) -> anyhow::Result<()> {
    // Find directories that were just moved and haven't been scanned yet
    let mut scanned_categories = std::collections::HashSet::new();

    let dirs = db.get_directories_in_state(DirectoryState::InLibrary)?;
    for dir in dirs {
        if dir.plex_scan_at.is_some() {
            continue;
        }

        if let Some(section_key) = config
            .categories
            .get(&dir.category)
            .and_then(|c| c.plex_section)
        {
            if scanned_categories.insert((dir.category.clone(), section_key)) {
                info!(category = %dir.category, section = section_key, "triggering plex scan");
                match plex::trigger_scan(config, section_key).await {
                    Ok(()) => {
                        db.set_plex_scan_at(dir.id)?;
                        info!(category = %dir.category, "plex scan triggered");
                    }
                    Err(e) => {
                        warn!(category = %dir.category, error = %e, "plex scan failed");
                    }
                }
            }
        }
    }

    Ok(())
}

pub fn print_status(db: &Database) -> anyhow::Result<()> {
    let counts = db.count_directories_by_state()?;
    println!("Pipeline Status:");
    println!("{:-<30}", "");
    for (state, count) in counts {
        println!("  {:<20} {:>4}", state, count);
    }
    Ok(())
}
