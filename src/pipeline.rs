use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::db::{Database, DirectoryState};
use crate::library;
use crate::layout;
use crate::metadata::{clean_title_for_directory, CachedLookup, HttpLookup, MetadataLookup, NoopLookup};
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

/// Build the metadata-lookup impl from config. If TMDB credentials are
/// present, wraps an `HttpLookup` in an in-memory `CachedLookup` with
/// a SQLite-backed durable layer (`metadata_cache` table). Otherwise
/// returns `NoopLookup` so the pipeline runs with no API calls. The
/// result is wrapped in `Arc` so the lookup can be shared across
/// directory-rename calls in one process.
pub fn build_metadata_lookup(config: &Config, db: &Database) -> Arc<dyn MetadataLookup> {
    if config.metadata.has_tmdb_credentials() {
        let timeout = Duration::from_secs(
            config.metadata.request_timeout_secs.unwrap_or(5),
        );
        let http = HttpLookup::new(timeout);
        let ttl = Duration::from_secs(
            config.metadata.cache_ttl_days.unwrap_or(30) * 24 * 60 * 60,
        );
        info!("metadata lookup: HttpLookup + CachedLookup (tmdb configured, db-backed cache)");
        Arc::new(CachedLookup::with_db(Arc::new(http), ttl, Arc::new(db.clone())))
    } else {
        info!("metadata lookup: NoopLookup (no tmdb credentials configured)");
        Arc::new(NoopLookup)
    }
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

    // Build the metadata-lookup impl from config. The DB is passed
    // in so the durable layer of `CachedLookup` writes to the same
    // SQLite file the pipeline uses for everything else. On the
    // finfunnel deployment (no TMDB env var), this is `NoopLookup`
    // and the DB layer is unused.
    let lookup = build_metadata_lookup(config, db);

    // 1. Analyze
    analyze_directories(config, db).await?;

    // 2. Rename. Returns a map of `dir_id -> Option<CanonicalTitle>`
    //    so the move step can use the resolved title (if any) to
    //    pick the library layout path.
    let canonical_titles = rename_directories(config, db, lookup.as_ref()).await?;

    // 3. Transcode
    transcode_directories(config, db).await?;

    // 4. Move to library. The canonical map is consumed here; if
    //    a title isn't in the map, we fall back to the locally-
    //    parsed title.
    move_to_library(config, db, &canonical_titles)?;

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

async fn rename_directories(
    config: &Config,
    db: &Database,
    lookup: &dyn MetadataLookup,
) -> anyhow::Result<HashMap<i64, Option<crate::metadata::CanonicalTitle>>> {
    let dirs = db.get_directories_in_state(DirectoryState::Analyzed)?;
    info!(count = dirs.len(), "directories to rename");

    let mut canonicals: HashMap<i64, Option<crate::metadata::CanonicalTitle>> = HashMap::new();

    for dir in dirs {
        let staging_path = Path::new(&dir.staging_path);
        info!(dir_id = dir.id, path = %staging_path.display(), "renaming directory");

        db.set_directory_state(dir.id, DirectoryState::Renaming)?;

        let detected_policy = dir.detected_policy.as_deref()
            .and_then(DetectedPolicy::from_str);

        match rename::rename_directory(
            staging_path,
            config,
            &dir.category,
            db,
            dir.id,
            detected_policy.as_ref(),
            lookup,
        ).await {
            Ok(canonical) => {
                canonicals.insert(dir.id, canonical);
                db.set_directory_state(dir.id, DirectoryState::Renamed)?;
                info!(dir_id = dir.id, "rename complete");
            }
            Err(e) => {
                db.set_directory_error(dir.id, &format!("rename failed: {}", e))?;
                error!(dir_id = dir.id, error = %e, "rename failed");
            }
        }
    }

    Ok(canonicals)
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

fn move_to_library(
    config: &Config,
    db: &Database,
    canonical_titles: &HashMap<i64, Option<crate::metadata::CanonicalTitle>>,
) -> anyhow::Result<()> {
    let dirs = db.get_directories_in_state(DirectoryState::Transcoded)?;
    info!(count = dirs.len(), "directories to move");

    for dir in dirs {
        let staging_path = Path::new(&dir.staging_path);
        let staging_basename = staging_path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();

        // Resolve the final library path. The cleaned title comes
        // from the canonical lookup (TMDB) if available, otherwise
        // from re-parsing the primary video file. Year is from
        // TMDB only — we don't trust the filename.
        let canonical = canonical_titles.get(&dir.id).and_then(|c| c.as_ref());
        let (title, year) = match canonical {
            Some(c) => (c.title.clone(), c.year),
            None => {
                // Re-parse the primary video to derive a local title.
                // The parser is pure and the primary file is still
                // in the staging dir at this point.
                let primary = rename::primary_video_path(staging_path);
                match primary
                    .as_ref()
                    .and_then(|p| p.file_name())
                    .and_then(|n| n.to_str())
                    .map(rename::parse_release_metadata)
                {
                    Some(meta) => {
                        let cleaned = clean_title_for_directory(&meta, None);
                        (cleaned, None)
                    }
                    None => (staging_basename.clone(), None),
                }
            }
        };

        let final_library_path = layout::resolve_library_path(
            config,
            &dir.category,
            &staging_basename,
            &title,
            year,
        );

        info!(
            dir_id = dir.id,
            src = %staging_path.display(),
            dst = %final_library_path.display(),
            "moving to library"
        );

        db.set_directory_state(dir.id, DirectoryState::Moving)?;

        match library::move_to_library(staging_path, &final_library_path, db, dir.id) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CategoryConfig, Config, DatabaseConfig, MetadataConfig, PathsConfig, PlexConfig, SshConfig};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use tempfile::tempdir;

    /// Build a `Config` with a single movies category, a temp staging
    /// base, and a temp library base. Other categories are absent
    /// so `library_path("tvshows")` etc. would panic if called.
    fn make_test_config(staging: &PathBuf, library: &PathBuf) -> Config {
        let mut categories = HashMap::new();
        categories.insert(
            "movies".to_string(),
            CategoryConfig {
                remote_dir: "movies".to_string(),
                library_folder: "Movies".to_string(),
                transcode_policy: None,
                plex_section: None,
            },
        );
        Config {
            ssh: SshConfig {
                host: "test".to_string(),
                user: "test".to_string(),
                private_key_path: PathBuf::from("/dev/null"),
                remote_base_path: PathBuf::from("/tmp"),
            },
            database: DatabaseConfig {
                path: PathBuf::from(":memory:"),
            },
            paths: PathsConfig {
                staging: staging.clone(),
                library: library.clone(),
            },
            plex: PlexConfig {
                url: "http://test:32400".to_string(),
                sections: HashMap::new(),
            },
            logging: None,
            group_name: Some("REPACK".to_string()),
            categories,
            metadata: MetadataConfig::default(),
        }
    }

    /// End-to-end test of the layout wiring: a staging dir with a
    /// noisy release name gets renamed by `rename_directory`, and
    /// then `move_to_library` uses the layout resolver to pick a
    /// clean directory name (`<Title>/`) under the library.
    ///
    /// The NoopLookup is used, so the canonical title is None and
    /// the resolver falls back to the local-parse path. The
    /// expected outcome is the staging dir lands at
    /// `<library>/Movies/Shoresy/`.
    #[test]
    fn test_move_to_library_uses_layout_resolver() {
        let tmp = tempdir().unwrap();
        let staging = tmp.path().join("staging");
        let library = tmp.path().join("library");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::create_dir_all(library.join("Movies")).unwrap();

        // Drop a noisy release file in the staging dir.
        let video = staging.join("Shoresy.S05E03.1080p.HEVC.x265-MeGusta.mkv");
        std::fs::write(&video, b"fake video data").unwrap();

        // Register the directory as having been renamed (state =
        // Transcoded) so `move_to_library` picks it up.
        let db = Database::open(std::path::Path::new(":memory:")).unwrap();
        db.upsert_directory(
            "movies",
            staging.to_string_lossy().as_ref(),
            staging.to_string_lossy().as_ref(),
            "abc123",
        ).unwrap();
        let dir_id = db.get_directory_by_id(1).unwrap().unwrap().id;
        db.set_directory_state(dir_id, DirectoryState::Transcoded).unwrap();

        let config = make_test_config(&staging, &library);

        // No canonical titles — the local-parse path picks the dir name.
        let canonicals: HashMap<i64, Option<crate::metadata::CanonicalTitle>> = HashMap::new();
        move_to_library(&config, &db, &canonicals).unwrap();

        // The directory landed at <library>/Movies/Shoresy/, not at
        // <library>/Movies/Shoresy.S05E03.1080p.HEVC.x265-REPACK/.
        // The file inside is still the original release name — this
        // test only exercises the move step, not rename.
        let expected = library.join("Movies").join("Shoresy");
        assert!(
            expected.exists(),
            "expected dir at {:?} but it doesn't exist; library contents: {:?}",
            expected,
            std::fs::read_dir(library.join("Movies")).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>()
        );
        assert!(expected.join("Shoresy.S05E03.1080p.HEVC.x265-MeGusta.mkv").exists());
    }

    /// When the canonical title is present in the map, it overrides
    /// the local-parse fallback. The library dir uses the canonical
    /// title (and year for the collision chain).
    #[test]
    fn test_move_to_library_uses_canonical_title() {
        let tmp = tempdir().unwrap();
        let staging = tmp.path().join("staging");
        let library = tmp.path().join("library");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::create_dir_all(library.join("Movies")).unwrap();

        let video = staging.join("Some.Noisy.Release.Name.2024-GROUP.mkv");
        std::fs::write(&video, b"fake").unwrap();

        let db = Database::open(std::path::Path::new(":memory:")).unwrap();
        db.upsert_directory(
            "movies",
            staging.to_string_lossy().as_ref(),
            staging.to_string_lossy().as_ref(),
            "abc123",
        ).unwrap();
        let dir_id = db.get_directory_by_id(1).unwrap().unwrap().id;
        db.set_directory_state(dir_id, DirectoryState::Transcoded).unwrap();

        let config = make_test_config(&staging, &library);

        // Canonical title present — should win over the local parse.
        let mut canonicals: HashMap<i64, Option<crate::metadata::CanonicalTitle>> = HashMap::new();
        canonicals.insert(
            dir_id,
            Some(crate::metadata::CanonicalTitle {
                title: "The Actual Movie".to_string(),
                year: Some(2024),
                external_id: "tt12345".to_string(),
                season_count: None,
            }),
        );
        move_to_library(&config, &db, &canonicals).unwrap();

        let expected = library.join("Movies").join("The Actual Movie");
        assert!(
            expected.exists(),
            "expected dir at {:?} but it doesn't exist; library contents: {:?}",
            expected,
            std::fs::read_dir(library.join("Movies")).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>()
        );
    }
}
