//! Pipeline orchestration. As of 2026-06-15, the pipeline runs
//! **two** concurrent worker pools (downloader + analyze) plus a
//! sequential Plex scan at the end. The rename and move pools
//! are disabled pending a CIFS permission fix on physalis (the
//! container runs as uid=1000/gid=1000 but the mount is
//! uid=0/gid=1111,forcegid, so the move worker's `fs::rename`
//! cannot create new entries). See
//! `/home/nicklowbar/.claude/plans/concurrent-beaming-kurzweil.md`
//! for the disablement rationale and the re-enablement path.
//!
//! Both pools share the same `WorkerPool<W>` shape (see
//! `worker_pool.rs`); the only thing that varies is the per-slot
//! `W` type. The `RenameSlotWorker` and `MoveSlotWorker` types
//! still exist in `worker_pool.rs` and are still tested in this
//! file's `test_move_worker_*` tests — they're just not
//! constructed as pools here.
//!
//! ## Pool shape (concurrent + cascading drain)
//!
//! Both pools are spawned at startup and run concurrently
//! against the same SQLite DB. The cascade drain in `run_full`
//! is:
//!
//! 1. `sync.drain_pool().await?` — wait for the downloader
//!    pool to finish (all `Detected` rows are `Synced`).
//! 2. `analyze_pool.signal_drain(); analyze_join.await??;` —
//!    wait for analyze to finish (all `Synced` → `Analyzed`).
//! 3. `trigger_plex_scans(...).await?` — sequential post-process.
//!
//! Cascading is required because:
//! - The analyze pool is essentially free (a single SQL
//!   UPDATE per row). The cascade is just a join handle.
//!
//! ## Pool/worker instantiation
//!
//! The analyze pool is constructed here with a single slot. The
//! downloader pool is constructed inside `SyncEngine::new` (in
//! `sync.rs`) with 4 slots. The `DownloaderSlotWorker` (also in
//! `sync.rs`) implements the same `Worker` trait as
//! `AnalyzeSlotWorker` in `worker_pool.rs`.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::config::Config;
use crate::db::Database;
use crate::metadata::{CachedLookup, HttpLookup, MetadataLookup, NoopLookup};
use crate::plex;
use crate::sync;
use crate::worker_pool::{AnalyzeSlotWorker, Worker, WorkerPool};

/// Run the full pipeline: 2 concurrent worker pools
/// (downloader + analyze) + sequential Plex scan.
///
/// Rename and move pools are disabled pending the CIFS
/// permission fix on physalis — see the module-level docs.
pub async fn run_full(config: &Config, db: &Database) -> anyhow::Result<()> {
    // Wrap the config in an Arc so the per-slot workers can
    // hold a reference. The downloader pool's per-slot workers
    // (constructed inside `SyncEngine::new`) also hold an Arc
    // to this same value.
    let config = Arc::new(config.clone());

    // Sync engine. Its `new` connects to the remote, opens
    // the walker + N downloader SFTP channels, and spawns
    // the downloader pool in the background. By the time
    // `new` returns, the downloader pool is already running
    // and claiming `Detected` rows from the DB.
    let mut sync_engine = match sync::SyncEngine::new(&config, db).await {
        Ok(engine) => engine,
        Err(e) => {
            tracing::error!(error = %e, "failed to initialize SyncEngine; aborting pipeline");
            return Err(e);
        }
    };

    // Analyze pool: 1 slot. The work is a single SQL UPDATE
    // (`Synced` → `Analyzed`). Bumping slot count wouldn't
    // help — the bottleneck is the downloader, not analyze.
    //
    // Rename and move pools are intentionally not constructed
    // here. See the module-level docs.
    let analyze_pool = Arc::new(WorkerPool::new(
        vec![AnalyzeSlotWorker::new()],
        Duration::from_millis(200),
    ));

    // Spawn the aggregator task. `spawn` returns a
    // `JoinHandle` that resolves to the first error from
    // any slot, or `Ok(())` if all slots exited cleanly.
    let analyze_join = analyze_pool.spawn(db.clone());

    // Walk each category sequentially. The walker is per-
    // category (it reuses the same SFTP channel for the
    // duration of one walk), and parallel walks are not
    // safe today. The downloader pool runs concurrently
    // with the walker — it doesn't need the walker
    // channel.
    for (category_name, _) in &config.categories {
        info!(category = %category_name, "syncing category");
        if let Err(e) = sync_engine.sync_category(category_name, db).await {
            tracing::error!(category = %category_name, error = %e, "category sync failed");
            // Continue with other categories rather than
            // failing the whole pipeline; the DB is the
            // source of truth and a per-category failure
            // doesn't poison the rest.
        }
    }

    // Cascading drain. The downloader pool finishes first
    // (signal + await via `SyncEngine::drain_pool`); then
    // the analyze pool drains. Each pool's `signal_drain`
    // wakes any slot parked in `select!`; the `join.await`
    // resolves when the pool's slot count reaches zero and
    // all slots have exited.
    //
    // (The rename and move pools are gone — see the
    // module-level docs. When they're re-enabled, the
    // cascade will extend: analyze → rename → move.)
    sync_engine.drain_pool().await?;
    analyze_pool.signal_drain();
    let _ = analyze_join.await??;

    // Sequential post-process. The per-section dedup in
    // `trigger_plex_scans` makes it a poor fit for the
    // worker pool model.
    trigger_plex_scans(&config, db).await?;

    info!("pipeline complete");
    Ok(())
}

/// Build the metadata-lookup impl from config. If TMDB
/// credentials are present, wraps an `HttpLookup` in an
/// in-memory `CachedLookup` with a SQLite-backed durable layer
/// (`metadata_cache` table). Otherwise returns `NoopLookup` so
/// the pipeline runs with no API calls. The result is wrapped
/// in `Arc` so the lookup can be shared across per-slot rename
/// workers (today that's just one slot, but the trait shape
/// makes the slot count irrelevant to the lookup).
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

async fn trigger_plex_scans(config: &Config, db: &Database) -> anyhow::Result<()> {
    // Find directories that were just moved and haven't been scanned yet
    let mut scanned_categories = std::collections::HashSet::new();

    let dirs = db.get_directories_in_state(crate::db::DirectoryState::InLibrary)?;
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
    use crate::config::{CategoryConfig, Config, DatabaseConfig, MetadataConfig, PathsConfig, PlexConfig, SshConfig, SyncConfig};
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
                plex_section: None,
            },
        );
        Config {
            ssh: SshConfig {
                host: "test".to_string(),
                port: None,
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
            sync: SyncConfig::default(),
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
    ///
    /// With the move work now inside `MoveSlotWorker`, this test
    /// is rewritten to drive the worker directly.
    #[tokio::test]
    async fn test_move_worker_uses_layout_resolver() {
        use std::collections::HashMap as StdHashMap;
        use crate::db::DirectoryState;
        use crate::worker_pool::MoveSlotWorker;
        use crate::metadata::CanonicalTitle;

        let tmp = tempdir().unwrap();
        let staging = tmp.path().join("staging");
        let library = tmp.path().join("library");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::create_dir_all(library.join("Movies")).unwrap();

        // Drop a noisy release file in the staging dir.
        let video = staging.join("Shoresy.S05E03.1080p.HEVC.x265-MeGusta.mkv");
        std::fs::write(&video, b"fake video data").unwrap();

        // Register the directory as having been renamed so
        // the move pool would pick it up.
        let db = Database::open(std::path::Path::new(":memory:")).unwrap();
        let (id, _) = db.upsert_directory(
            "movies",
            staging.to_string_lossy().as_ref(),
            staging.to_string_lossy().as_ref(),
            "abc123",
        ).unwrap();
        db.set_directory_state(id, DirectoryState::Renamed).unwrap();

        let config = Arc::new(make_test_config(&staging, &library));

        // No canonical titles — the local-parse path picks the dir name.
        let canonical_titles: Arc<std::sync::RwLock<StdHashMap<i64, Option<CanonicalTitle>>>> =
            Arc::new(std::sync::RwLock::new(StdHashMap::new()));
        let w = MoveSlotWorker::new(config, canonical_titles);

        // Manually drive the claim → process → success loop.
        let row = db.claim_renamed_row().unwrap().unwrap();
        w.process(&db, row.clone()).await.unwrap();
        db.set_directory_state(row.id, DirectoryState::InLibrary).unwrap();

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
    #[tokio::test]
    async fn test_move_worker_uses_canonical_title() {
        use std::collections::HashMap as StdHashMap;
        use crate::db::DirectoryState;
        use crate::worker_pool::MoveSlotWorker;
        use crate::metadata::CanonicalTitle;

        let tmp = tempdir().unwrap();
        let staging = tmp.path().join("staging");
        let library = tmp.path().join("library");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::create_dir_all(library.join("Movies")).unwrap();

        let video = staging.join("Some.Noisy.Release.Name.2024-GROUP.mkv");
        std::fs::write(&video, b"fake").unwrap();

        let db = Database::open(std::path::Path::new(":memory:")).unwrap();
        let (id, _) = db.upsert_directory(
            "movies",
            staging.to_string_lossy().as_ref(),
            staging.to_string_lossy().as_ref(),
            "abc123",
        ).unwrap();
        db.set_directory_state(id, DirectoryState::Renamed).unwrap();

        let config = Arc::new(make_test_config(&staging, &library));

        // Canonical title present — should win over the local parse.
        let canonical_titles: Arc<std::sync::RwLock<StdHashMap<i64, Option<CanonicalTitle>>>> =
            Arc::new(std::sync::RwLock::new(StdHashMap::new()));
        canonical_titles.write().unwrap().insert(
            id,
            Some(CanonicalTitle {
                title: "The Actual Movie".to_string(),
                year: Some(2024),
                external_id: "tt12345".to_string(),
                season_count: None,
            }),
        );
        let w = MoveSlotWorker::new(config, canonical_titles);

        let row = db.claim_renamed_row().unwrap().unwrap();
        w.process(&db, row.clone()).await.unwrap();
        db.set_directory_state(row.id, DirectoryState::InLibrary).unwrap();

        let expected = library.join("Movies").join("The Actual Movie");
        assert!(
            expected.exists(),
            "expected dir at {:?} but it doesn't exist; library contents: {:?}",
            expected,
            std::fs::read_dir(library.join("Movies")).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>()
        );
    }
}
