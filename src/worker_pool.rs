//! Generic worker pool for the pipeline's per-phase dispatch.
//!
//! The pool shape is shared by all four pools (sync, analyze,
//! rename, move). Each pool is a `WorkerPool<W>` over some
//! per-slot worker type `W: Worker`. The downloader pool is
//! `WorkerPool<DownloaderSlotWorker>` (defined in `sync.rs` — the
//! SFTP-specific bits live with the SFTP types); the three
//! process pools are `WorkerPool<AnalyzeSlotWorker>`,
//! `WorkerPool<RenameSlotWorker>`, `WorkerPool<MoveSlotWorker>`.
//!
//! ## Why one shape for all four pools
//!
//! The pre-refactor downloader pool was ~150 lines of dispatch
//! loop (JoinSet + watch::channel drain signal + per-slot claim/
//! work loop). The three new process pools (analyze, rename,
//! move) would have re-implemented the same shape, three times.
//! Unifying them on a single generic `WorkerPool<W>` means the
//! dispatch loop exists in one place (`worker_loop` below) and
//! the per-pool differences are encoded as `W::claim` and
//! `W::process`.
//!
//! ## Per-slot state lives on the worker
//!
//! The pre-opened SFTP channel, the shared `Arc<Mutex<Handle>>`,
//! the `Arc<Config>`, the `Arc<dyn MetadataLookup>`, the
//! canonical-titles map — all of these are per-slot state, not
//! per-pool state. Putting them inside the `W` value means the
//! pool itself is fully generic: `WorkerPool<W>` doesn't know
//! about SFTP or metadata at all.
//!
//! ## Drain shape
//!
//! The pool exits when the caller calls `signal_drain` AND the
//! DB has no rows in the worker's claim state. The five
//! properties of the loop (preserved verbatim from
//! `downloader_loop`):
//!
//! 1. Drain check at top of loop. Exit only when
//!    `drain_signal && count_claim_state() == 0`. The "DB error
//!    during count → assume pending=1" guard.
//! 2. Claim with no-row fallback. On `Ok(None)`, do
//!    `tokio::select! { _ = drain_signal.changed() => continue, _ = tokio::time::sleep(poll_interval) => continue }`.
//! 3. Claim-failure backoff. On `Err`, sleep poll interval and
//!    `continue` (don't crash the slot).
//! 4. Success transitions state via `set_directory_state`.
//! 5. Failure transitions state AND returns `Err`. The
//!    aggregator surfaces the first error.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::db::{Database, DirectoryRow, DirectoryState};
use crate::metadata::MetadataLookup;

/// Per-slot worker trait. The pool is a `WorkerPool<W>` over
/// some `W: Worker`. The two methods that vary per pool are
/// `claim` (which row to claim) and `process` (what to do with
/// it); the rest are read-only metadata for the dispatch loop.
///
/// `Send + Sync + 'static` is required so the pool can spawn
/// one task per slot and share `&W` across the slot's lifetime.
/// `Sync` is required because the slot's `process` takes
/// `&self`; the SFTP `Handle` is `!Sync` but the per-slot
/// `DownloaderSlotWorker` wraps it in a `tokio::sync::Mutex<Handle>`
/// (which IS `Sync`), so the per-slot worker is `Sync`.
pub trait Worker: Send + Sync + 'static {
    /// Human-readable name for logging. The dispatch loop prefixes
    /// all per-row log lines with this so the operator can tell
    /// which pool claimed a row when multiple pools are active
    /// concurrently.
    fn name(&self) -> &'static str;

    /// The state a row must be in for this pool to claim it.
    /// Used as the `WHERE state = ?` filter in the claim UPDATE
    /// and in the drain-check `count(*)` query.
    fn claim_state(&self) -> &'static str;

    /// The state a row is set to by the atomic claim UPDATE
    /// (the `*ing` state — the pool's "in-flight" marker).
    fn in_flight_state(&self) -> &'static str;

    /// The state a row is set to on a successful process (the
    /// `*ed` state — the pool's "done" marker).
    fn success_state(&self) -> DirectoryState;

    /// The state a row is set to on a process failure (the
    /// `*Failed` state — the pool's "this row needs operator
    /// attention" marker).
    fn failure_state(&self) -> DirectoryState;

    /// Count rows remaining for this pool's drain check. Used by the
    /// generic worker_loop drain condition. The default uses
    /// `db.count_directories_in_state(self.claim_state())`.
    /// File-level workers override this with a custom count query.
    fn count_remaining(&self, db: &Database) -> anyhow::Result<i64> {
        db.count_directories_in_state(self.claim_state())
    }

    /// Returns true if this worker manages its own success/failure
    /// state transitions (e.g. the file-download worker marks
    /// individual files synced/failed rather than transitioning
    /// the directory row). When true, the worker_loop skips the
    /// automatic `set_directory_state` calls after `process()`.
    fn handles_own_completion(&self) -> bool { false }

    /// Atomically claim one row in `claim_state` and transition
    /// it to `in_flight_state`. Returns `Ok(None)` if no row is
    /// available, or `Err` on DB failure (the loop backs off and
    /// tries again).
    fn claim(&self, db: &Database) -> anyhow::Result<Option<DirectoryRow>>;

    /// The per-row work. The row is in `in_flight_state` for the
    /// duration. On success, the pool transitions to
    /// `success_state`. On `Err`, the pool transitions to
    /// `failure_state` and propagates the error (halting the
    /// slot).
    ///
    /// Async because every pool's per-row work is async:
    /// downloader pool's work is SFTP I/O, the process pools'
    /// work is filesystem I/O + (for rename) an async metadata
    /// lookup. Returned as a `Pin<Box<dyn Future>>` so the trait
    /// can be object-safe while still supporting async methods.
    fn process<'a>(
        &'a self,
        db: &'a Database,
        row: DirectoryRow,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;
}

/// Generic worker pool. The caller pre-builds one `W` per slot
/// (with whatever per-slot state the worker needs — pre-opened
/// SFTP channels, an `Arc<Config>`, an `Arc<dyn MetadataLookup>`,
/// etc.) and hands the `Vec<W>` to `WorkerPool::new`.
///
/// The workers are wrapped in `Arc<W>` internally so the
/// spawned slots can hold a `'static` reference. Callers can
/// either pass `vec![W::new(...)]` directly (each W is moved
/// into a fresh `Arc`) or pre-wrap with `Arc::new(W::new(...))`
/// if they need to share a worker handle elsewhere.
pub struct WorkerPool<W: Worker> {
    pub(crate) workers: Vec<Arc<W>>,
    drain_signal: Arc<watch::Sender<bool>>,
    poll_interval: Duration,
}

impl<W: Worker> From<Vec<W>> for WorkerPool<W> {
    fn from(workers: Vec<W>) -> Self {
        Self::new(workers, Duration::from_millis(200))
    }
}

impl<W: Worker> WorkerPool<W> {
    /// Construct a pool with one pre-built worker per slot. The
    /// caller is responsible for any per-slot state setup
    /// (e.g. opening SFTP channels for the downloader pool).
    pub fn new(workers: Vec<W>, poll_interval: Duration) -> Self {
        let (drain_tx, _drain_rx) = watch::channel(false);
        Self {
            workers: workers.into_iter().map(Arc::new).collect(),
            drain_signal: Arc::new(drain_tx),
            poll_interval,
        }
    }

    /// Construct a pool from pre-wrapped `Arc<W>` workers. Use
    /// this when the same worker handle must be shared with
    /// code outside the pool (e.g. the SyncEngine holds the
    /// downloader workers so the walker can inspect them).
    pub fn from_arcs(workers: Vec<Arc<W>>, poll_interval: Duration) -> Self {
        let (drain_tx, _drain_rx) = watch::channel(false);
        Self {
            workers,
            drain_signal: Arc::new(drain_tx),
            poll_interval,
        }
    }

    /// Signal the pool to drain. Each slot exits once the DB
    /// has no rows in the worker's claim state. The signal is
    /// idempotent.
    pub fn signal_drain(&self) {
        // `send` only updates if the value changed; ignore the
        // error (the only way it fails is if there are no
        // receivers, which means the pool is already gone).
        let _ = self.drain_signal.send(true);
    }

    /// Spawn one task per slot. Returns the `JoinHandle` of the
    /// aggregator task that awaits all slots; the caller awaits
    /// this handle after `signal_drain`. The first error from
    /// any slot propagates; successful slots are silently
    /// consumed.
    ///
    /// Each slot holds an `Arc<W>` for its lifetime, so the
    /// pool itself can be dropped after the aggregator handle
    /// resolves.
    pub fn spawn(&self, db: Database) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let workers: Vec<Arc<W>> = self.workers.iter().map(Arc::clone).collect();
        let drain_signal = Arc::clone(&self.drain_signal);
        let poll_interval = self.poll_interval;
        tokio::spawn(async move {
            let mut set = JoinSet::new();
            for (slot_id, w) in workers.into_iter().enumerate() {
                let drain_rx = drain_signal.subscribe();
                let db = db.clone();
                set.spawn(async move {
                    worker_loop(slot_id, w, db, drain_rx, poll_interval).await
                });
            }
            // Await all slot tasks. If any returns Err,
            // propagate the first one.
            let mut first_err: Option<anyhow::Error> = None;
            while let Some(res) = set.join_next().await {
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                    Err(join) => {
                        if first_err.is_none() {
                            first_err = Some(anyhow::anyhow!("worker task panicked: {}", join));
                        }
                    }
                }
            }
            match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    }
}

/// The per-slot dispatch loop. Mirrors `downloader_loop` (in
/// `sync.rs`, soon to be deleted) exactly. The five properties
/// at the top of this file are preserved verbatim.
///
/// The slot's `&W` is borrowed for the loop's lifetime. The
/// loop is async; the spawned `tokio::task` returns when the
/// slot exits.
async fn worker_loop<W: Worker>(
    slot_id: usize,
    w: Arc<W>,
    db: Database,
    mut drain_signal: watch::Receiver<bool>,
    poll_interval: Duration,
) -> anyhow::Result<()> {
    info!(worker = w.name(), slot = slot_id, "worker: starting");

    loop {
        // Check drain condition first. We exit only when the
        // orchestrator has signaled AND the DB has no more
        // claimable rows. Checking the count only when the
        // signal is set is the cheap case; the expensive case
        // (the count is non-zero) is rare and bounded by how
        // fast the upstream stage is producing rows.
        if *drain_signal.borrow() {
            let pending = w.count_remaining(&db).unwrap_or_else(|e| {
                // If the count itself fails, assume pending = 1
                // so we keep polling. A DB error here is very
                // unusual; logging + assuming pending is safer
                // than exiting on what might be a transient
                // error.
                warn!(worker = w.name(), slot = slot_id, error = %e,
                      "worker: count failed; assuming pending=1");
                1
            });
            if pending == 0 {
                info!(worker = w.name(), slot = slot_id, "worker: pool drained, exiting");
                return Ok(());
            }
            // Drain signaled but work is still pending — fall
            // through to claim.
        }

        // Try to claim a row. If no row is in the claim state,
        // wait for either a drain-signal change or a poll
        // interval, whichever comes first. The select keeps
        // the exit latency low (we don't have to wait out
        // the full poll interval after a drain signal).
        let claim = match w.claim(&db) {
            Ok(Some(row)) => row,
            Ok(None) => {
                tokio::select! {
                    _ = drain_signal.changed() => continue,
                    _ = tokio::time::sleep(poll_interval) => continue,
                }
            }
            Err(e) => {
                warn!(worker = w.name(), slot = slot_id, error = %e,
                      "worker: claim failed; backing off");
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        info!(
            worker = w.name(),
            slot = slot_id,
            dir_id = claim.id,
            "worker: starting row"
        );

        // The row is in `in_flight_state` (claim transitioned it).
        let result = w.process(&db, claim.clone()).await;

        // Skip automatic state transitions if the worker manages its own
        // completion (e.g. file-download worker transitions individual files).
        if !w.handles_own_completion() {
            match result {
                Ok(()) => {
                    db.set_directory_state(claim.id, w.success_state())?;
                    info!(worker = w.name(), slot = slot_id, dir_id = claim.id,
                          "worker: complete");
                }
                Err(e) => {
                    let msg = format!("{} failed: {}", w.name(), e);
                    let _ = db.set_directory_error(claim.id, w.failure_state(), &msg);
                    error!(worker = w.name(), slot = slot_id, dir_id = claim.id, error = %e,
                           "worker: failed");
                    // Halt: returning Err from this loop closes
                    // this slot. The aggregator collects errors
                    // from all slots and propagates the first one.
                    return Err(e);
                }
            }
        }
    }
}

// =========================================================================
// Per-slot worker impls
// =========================================================================
//
// The downloader pool's worker (`DownloaderSlotWorker`) lives
// in `sync.rs` because the SFTP types are there. The three
// process-pool workers live here.

// -------------------------------------------------------------------------
// AnalyzeSlotWorker
// -------------------------------------------------------------------------
//
// The analyze stage is a no-op (Tdarr owns library-side
// re-encoding; see memory/architecture-pipeline-vs-tdarr.md).
// The "work" is one SQL UPDATE: `set_directory_policy(id,
// "none")`. The state transitions are still observed (the row
// goes from `Synced` → `Analyzing` → `Analyzed`) so a future
// stage that needs an actual analyze step can be slotted in
// without changing the surrounding state machine or pool
// plumbing.

pub struct AnalyzeSlotWorker;

impl AnalyzeSlotWorker {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AnalyzeSlotWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl Worker for AnalyzeSlotWorker {
    fn name(&self) -> &'static str { "analyze" }
    fn claim_state(&self) -> &'static str { "synced" }
    fn in_flight_state(&self) -> &'static str { "analyzing" }
    fn success_state(&self) -> DirectoryState { DirectoryState::Analyzed }
    fn failure_state(&self) -> DirectoryState { DirectoryState::AnalyzeFailed }

    fn claim(&self, db: &Database) -> anyhow::Result<Option<DirectoryRow>> {
        db.claim_synced_row()
    }

    fn process<'a>(
        &'a self,
        db: &'a Database,
        row: DirectoryRow,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            // The actual transcode-detection work is now a no-op
            // (Tdarr owns library-side re-encoding). Recording
            // `detected_policy = "none"` is the one observable
            // effect; the worker_loop will transition the row
            // to `Analyzed` on Ok.
            db.set_directory_policy(row.id, "none")?;
            Ok(())
        })
    }
}

// -------------------------------------------------------------------------
// RenameSlotWorker
// -------------------------------------------------------------------------
//
// The rename stage runs `rename::rename_directory` per row. On
// success, the resolved canonical title (if any) is written to
// the shared `Arc<RwLock<HashMap<i64, Option<CanonicalTitle>>>>`
// for the move pool to read. The write happens BEFORE the
// `Renamed` state transition (which is the move pool's claim
// trigger) — see the cross-pool handoff discussion in the
// plan.

pub struct RenameSlotWorker {
    config: Arc<Config>,
    lookup: Arc<dyn MetadataLookup>,
    /// Shared with `MoveSlotWorker`. Written by rename, read by
    /// move. The `move` pool's claim is gated on
    /// `state = 'renamed'`, and the rename pool sets that state
    /// only after writing to the map — so the move pool is
    /// guaranteed to see the map entry by the time it claims
    /// the row.
    canonical_titles: Arc<std::sync::RwLock<std::collections::HashMap<i64, Option<crate::metadata::CanonicalTitle>>>>,
}

impl RenameSlotWorker {
    pub fn new(
        config: Arc<Config>,
        lookup: Arc<dyn MetadataLookup>,
        canonical_titles: Arc<std::sync::RwLock<std::collections::HashMap<i64, Option<crate::metadata::CanonicalTitle>>>>,
    ) -> Self {
        Self { config, lookup, canonical_titles }
    }
}

impl Worker for RenameSlotWorker {
    fn name(&self) -> &'static str { "rename" }
    fn claim_state(&self) -> &'static str { "analyzed" }
    fn in_flight_state(&self) -> &'static str { "renaming" }
    fn success_state(&self) -> DirectoryState { DirectoryState::Renamed }
    fn failure_state(&self) -> DirectoryState { DirectoryState::RenameFailed }

    fn claim(&self, db: &Database) -> anyhow::Result<Option<DirectoryRow>> {
        db.claim_analyzed_row()
    }

    fn process<'a>(
        &'a self,
        db: &'a Database,
        row: DirectoryRow,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        let config = Arc::clone(&self.config);
        let lookup = Arc::clone(&self.lookup);
        let canonical_titles = Arc::clone(&self.canonical_titles);
        Box::pin(async move {
            let staging_path = std::path::Path::new(&row.staging_path);
            let canonical = crate::rename::rename_directory(
                staging_path,
                &config,
                &row.category,
                db,
                row.id,
                lookup.as_ref(),
            ).await?;
            // Write the canonical title to the shared map
            // BEFORE the worker_loop transitions the row to
            // `Renamed`. The move pool's claim is gated on
            // `Renamed`, so it's guaranteed to see this write.
            canonical_titles.write().unwrap().insert(row.id, canonical);
            Ok(())
        })
    }
}

// -------------------------------------------------------------------------
// MoveSlotWorker
// -------------------------------------------------------------------------
//
// The move stage runs `library::move_to_library` per row. The
// final library path is resolved via `layout::resolve_library_path`,
// using the canonical title from the shared map if present (so
// the title TMDB returned wins over the locally-parsed one)
// and falling back to the local-parse path otherwise (today's
// `move_to_library` lines 260-280 behavior).
//
// **Concurrency assumption.** The move pool is 1-slot. This is
// important: `library::move_to_library` has a TOCTOU race in
// its duplicate-detection path (it checks for an existing dir
// at the target, then moves into it). Two concurrent moves
// against the same library root could double-move a row. The
// 1-slot design is what bounds that risk. If a future change
// bumps the slot count, this assumption breaks.

pub struct MoveSlotWorker {
    config: Arc<Config>,
    canonical_titles: Arc<std::sync::RwLock<std::collections::HashMap<i64, Option<crate::metadata::CanonicalTitle>>>>,
}

impl MoveSlotWorker {
    pub fn new(
        config: Arc<Config>,
        canonical_titles: Arc<std::sync::RwLock<std::collections::HashMap<i64, Option<crate::metadata::CanonicalTitle>>>>,
    ) -> Self {
        Self { config, canonical_titles }
    }
}

impl Worker for MoveSlotWorker {
    fn name(&self) -> &'static str { "move" }
    fn claim_state(&self) -> &'static str { "renamed" }
    fn in_flight_state(&self) -> &'static str { "moving" }
    fn success_state(&self) -> DirectoryState { DirectoryState::InLibrary }
    fn failure_state(&self) -> DirectoryState { DirectoryState::MoveFailed }

    fn claim(&self, db: &Database) -> anyhow::Result<Option<DirectoryRow>> {
        db.claim_renamed_row()
    }

    fn process<'a>(
        &'a self,
        db: &'a Database,
        row: DirectoryRow,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        let config = Arc::clone(&self.config);
        let canonical_titles = Arc::clone(&self.canonical_titles);
        let worker_name = self.name();
        Box::pin(async move {
            let staging_path = std::path::Path::new(&row.staging_path);
            let staging_basename = staging_path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();

            // Resolve the final library path. The cleaned title
            // comes from the canonical lookup (TMDB) if the
            // rename stage wrote one to the map; otherwise we
            // re-parse the primary video file. Year is from
            // TMDB only — we don't trust the filename.
            //
            // The map read is short — we grab the read lock,
            // clone the canonical (cheap `Option<CanonicalTitle>`),
            // drop the lock, and continue. The lock is held
            // only for the duration of the lookup, not the move.
            let canonical = canonical_titles
                .read()
                .unwrap()
                .get(&row.id)
                .and_then(|c| c.as_ref())
                .cloned();
            let (title, year) = match canonical {
                Some(c) => (c.title, c.year),
                None => {
                    // Re-parse the primary video to derive a
                    // local title. The parser is pure and the
                    // primary file is still in the staging dir
                    // at this point.
                    let primary = crate::rename::primary_video_path(staging_path);
                    match primary
                        .as_ref()
                        .and_then(|p| p.file_name())
                        .and_then(|n| n.to_str())
                        .map(crate::rename::parse_release_metadata)
                    {
                        Some(meta) => {
                            let cleaned = crate::metadata::clean_title_for_directory(&meta, None);
                            (cleaned, None)
                        }
                        None => (staging_basename.clone(), None),
                    }
                }
            };

            let final_library_path = crate::layout::resolve_library_path(
                &config,
                &row.category,
                &staging_basename,
                &title,
                year,
            );

            info!(
                worker = worker_name,
                dir_id = row.id,
                src = %staging_path.display(),
                dst = %final_library_path.display(),
                "moving to library"
            );

            let library_path = crate::library::move_to_library(
                staging_path,
                &final_library_path,
                db,
                row.id,
            )?;
            db.set_directory_library_path(row.id, &library_path.to_string_lossy())?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Sanity check: an empty DB's `count_directories_in_state`
    /// returns 0 for any state, so a pool that has nothing to do
    /// exits its drain-check immediately.
    #[test]
    fn test_count_by_state_on_empty_db() {
        let dir = tempdir().unwrap();
        let db = Database::open(&dir.path().join("test.db")).unwrap();
        assert_eq!(db.count_directories_in_state("detected").unwrap(), 0);
        assert_eq!(db.count_directories_in_state("synced").unwrap(), 0);
    }

    /// The claim helpers are structurally identical to
    /// `claim_detected_row`: they all use the same atomic
    /// `UPDATE ... WHERE state = ?` + `RETURNING` pattern. This
    /// test exercises the three new ones end-to-end to make
    /// sure the in-flight transition is correct.
    #[test]
    fn test_claim_synced_analyzed_renamed_round_trip() {
        let dir = tempdir().unwrap();
        let db = Database::open(&dir.path().join("test.db")).unwrap();

        // Insert a row and drive it to `Synced`.
        let id = db.upsert_directory(
            "movies",
            "/srv/movies/Foo",
            "/staging/Foo",
            "h1",
        ).unwrap().0;
        db.set_directory_state(id, DirectoryState::Synced).unwrap();

        // claim_synced_row → Analyzing.
        let row = db.claim_synced_row().unwrap().unwrap();
        assert_eq!(row.id, id);
        let after = db.get_directory_by_id(id).unwrap().unwrap();
        assert_eq!(after.state, DirectoryState::Analyzing);

        // Set to Analyzed and claim → Renaming.
        db.set_directory_state(id, DirectoryState::Analyzed).unwrap();
        let row = db.claim_analyzed_row().unwrap().unwrap();
        assert_eq!(row.id, id);
        let after = db.get_directory_by_id(id).unwrap().unwrap();
        assert_eq!(after.state, DirectoryState::Renaming);

        // Set to Renamed and claim → Moving.
        db.set_directory_state(id, DirectoryState::Renamed).unwrap();
        let row = db.claim_renamed_row().unwrap().unwrap();
        assert_eq!(row.id, id);
        let after = db.get_directory_by_id(id).unwrap().unwrap();
        assert_eq!(after.state, DirectoryState::Moving);
    }

    /// The `AnalyzeSlotWorker::process` is a one-line SQL
    /// UPDATE; we exercise it directly. The test is the
    /// minimal check that the trait `W::process` returns Ok
    /// for an empty work loop.
    #[tokio::test]
    async fn test_analyze_worker_process_sets_policy_none() {
        let dir = tempdir().unwrap();
        let db = Database::open(&dir.path().join("test.db")).unwrap();
        let id = db.upsert_directory(
            "movies", "/srv/movies/Foo", "/staging/Foo", "h1",
        ).unwrap().0;
        db.set_directory_state(id, DirectoryState::Synced).unwrap();

        let w = AnalyzeSlotWorker::new();
        let row = db.claim_synced_row().unwrap().unwrap();
        w.process(&db, row).await.unwrap();
        let after = db.get_directory_by_id(id).unwrap().unwrap();
        assert_eq!(after.detected_policy.as_deref(), Some("none"));
    }

    /// The canonical-titles map handoff between rename and
    /// move. The rename worker writes to the map; the move
    /// worker reads. The test simulates both ends to verify
    /// the contract.
    #[test]
    fn test_canonical_titles_map_handoff_rename_to_move() {
        use std::sync::RwLock;
        use std::collections::HashMap;
        use crate::metadata::CanonicalTitle;

        let map: Arc<RwLock<HashMap<i64, Option<CanonicalTitle>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Rename-side write: insert a canonical for dir_id = 42.
        let canonical = CanonicalTitle {
            title: "The Actual Movie".to_string(),
            year: Some(2024),
            external_id: "tt12345".to_string(),
            season_count: None,
        };
        map.write().unwrap().insert(42, Some(canonical));

        // Move-side read: look it up.
        let read = map.read().unwrap();
        let got = read.get(&42).and_then(|c| c.as_ref()).cloned();
        drop(read);
        let c = got.expect("map should have an entry for 42");
        assert_eq!(c.title, "The Actual Movie");
        assert_eq!(c.year, Some(2024));
    }
}
