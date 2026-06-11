use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use rusqlite::{params, Connection, OptionalExtension};
use tracing::{debug, info};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectoryState {
    Detected,
    Syncing,
    Synced,
    Analyzing,
    Analyzed,
    Renaming,
    Renamed,
    Transcoding,
    Transcoded,
    Moving,
    InLibrary,
    Failed,
}

impl DirectoryState {
    pub fn as_str(&self) -> &'static str {
        match self {
            DirectoryState::Detected => "detected",
            DirectoryState::Syncing => "syncing",
            DirectoryState::Synced => "synced",
            DirectoryState::Analyzing => "analyzing",
            DirectoryState::Analyzed => "analyzed",
            DirectoryState::Renaming => "renaming",
            DirectoryState::Renamed => "renamed",
            DirectoryState::Transcoding => "transcoding",
            DirectoryState::Transcoded => "transcoded",
            DirectoryState::Moving => "moving",
            DirectoryState::InLibrary => "in_library",
            DirectoryState::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "detected" => Some(DirectoryState::Detected),
            "syncing" => Some(DirectoryState::Syncing),
            "synced" => Some(DirectoryState::Synced),
            "analyzing" => Some(DirectoryState::Analyzing),
            "analyzed" => Some(DirectoryState::Analyzed),
            "renaming" => Some(DirectoryState::Renaming),
            "renamed" => Some(DirectoryState::Renamed),
            "transcoding" => Some(DirectoryState::Transcoding),
            "transcoded" => Some(DirectoryState::Transcoded),
            "moving" => Some(DirectoryState::Moving),
            "in_library" => Some(DirectoryState::InLibrary),
            "failed" => Some(DirectoryState::Failed),
            _ => None,
        }
    }
}

/// Handle to the pipeline's SQLite database. Cheap to clone — the
/// underlying `Connection` is shared via `Arc<Mutex<_>>` so multiple
/// consumers (the rename path, the metadata cache, future workers)
/// can hold a handle to the same on-disk file.
#[derive(Clone)]
pub struct Database {
    conn: Arc<std::sync::Mutex<Connection>>,
}

impl Database {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open SQLite database at {}", path.display()))?;

        let db = Database {
            conn: Arc::new(std::sync::Mutex::new(conn)),
        };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS directories (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                category        TEXT NOT NULL,
                remote_path     TEXT NOT NULL,
                staging_path    TEXT NOT NULL,
                library_path    TEXT,
                state           TEXT NOT NULL CHECK(state IN (
                    'detected','syncing','synced',
                    'analyzing','analyzed',
                    'renaming','renamed',
                    'transcoding','transcoded',
                    'moving','in_library','failed')),
                manifest_hash   TEXT,
                detected_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
                synced_at       DATETIME,
                analyzed_at     DATETIME,
                renamed_at      DATETIME,
                transcoded_at   DATETIME,
                moved_at        DATETIME,
                detected_policy TEXT,
                plex_scan_at    DATETIME,
                error_message   TEXT,
                UNIQUE(category, remote_path)
            );

            CREATE TABLE IF NOT EXISTS files (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                dir_id              INTEGER NOT NULL REFERENCES directories(id) ON DELETE CASCADE,
                original_name       TEXT NOT NULL,
                renamed_name        TEXT,
                transcode_policy    TEXT,
                transcode_status    TEXT CHECK(transcode_status IN ('pending','in_progress','done','failed')),
                needs_transcode     INTEGER NOT NULL DEFAULT 0,
                final_name          TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_dirs_state ON directories(state);
            CREATE INDEX IF NOT EXISTS idx_dirs_category ON directories(category);
            CREATE INDEX IF NOT EXISTS idx_files_dir ON files(dir_id);

            -- Metadata lookup cache (durable across restarts). Keyed on the
            -- SHA-256 of (category|without_group); the same key shape the
            -- in-memory CachedLookup uses. Negative results are NOT cached
            -- here — the schema only stores positive hits, so a later
            -- version of the data or a different parse may still succeed.
            CREATE TABLE IF NOT EXISTS metadata_cache (
                source_hash     TEXT PRIMARY KEY,
                category        TEXT NOT NULL,
                canonical_title TEXT NOT NULL,
                canonical_year  INTEGER,
                external_id     TEXT,
                raw_response    TEXT,
                fetched_at      TEXT NOT NULL,
                expires_at      TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_metadata_cache_expires
                ON metadata_cache(expires_at);
            "#,
        )?;
        Ok(())
    }

    /// Insert a newly-detected directory or update its manifest hash and reset state.
    pub fn upsert_directory(
        &self,
        category: &str,
        remote_path: &str,
        staging_path: &str,
        manifest_hash: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO directories (category, remote_path, staging_path, state, manifest_hash)
            VALUES (?1, ?2, ?3, 'detected', ?4)
            ON CONFLICT(category, remote_path) DO UPDATE SET
                manifest_hash = excluded.manifest_hash,
                state = CASE
                    WHEN directories.manifest_hash != excluded.manifest_hash THEN 'detected'
                    ELSE directories.state
                END,
                detected_at = CASE
                    WHEN directories.manifest_hash != excluded.manifest_hash THEN CURRENT_TIMESTAMP
                    ELSE directories.detected_at
                END,
                error_message = CASE
                    WHEN directories.manifest_hash != excluded.manifest_hash THEN NULL
                    ELSE directories.error_message
                END
            "#,
            params![category, remote_path, staging_path, manifest_hash],
        )?;
        Ok(())
    }

    /// Update the state of a directory.
    pub fn set_directory_state(&self, id: i64, state: DirectoryState) -> anyhow::Result<()> {
        let state_str = state.as_str();
        let timestamp_col = match state {
            DirectoryState::Synced => "synced_at",
            DirectoryState::Analyzed => "analyzed_at",
            DirectoryState::Renamed => "renamed_at",
            DirectoryState::Transcoded => "transcoded_at",
            DirectoryState::InLibrary => "moved_at",
            _ => "detected_at",
        };
        let sql = format!(
            "UPDATE directories SET state = ?1, {} = CURRENT_TIMESTAMP WHERE id = ?2",
            timestamp_col
        );
        self.conn.lock().unwrap().execute(&sql, params![state_str, id])?;
        Ok(())
    }

    pub fn set_directory_error(&self, id: i64, message: &str) -> anyhow::Result<()> {
        self.conn.lock().unwrap().execute(
            "UPDATE directories SET state = 'failed', error_message = ?1 WHERE id = ?2",
            params![message, id],
        )?;
        Ok(())
    }

    pub fn set_directory_library_path(
        &self,
        id: i64,
        path: &str,
    ) -> anyhow::Result<()> {
        self.conn.lock().unwrap().execute(
            "UPDATE directories SET library_path = ?1 WHERE id = ?2",
            params![path, id],
        )?;
        Ok(())
    }

    pub fn set_directory_policy(
        &self,
        id: i64,
        policy: &str,
    ) -> anyhow::Result<()> {
        self.conn.lock().unwrap().execute(
            "UPDATE directories SET detected_policy = ?1 WHERE id = ?2",
            params![policy, id],
        )?;
        Ok(())
    }

    pub fn set_plex_scan_at(&self, id: i64) -> anyhow::Result<()> {
        self.conn.lock().unwrap().execute(
            "UPDATE directories SET plex_scan_at = CURRENT_TIMESTAMP WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// Get directories in a given state.
    pub fn get_directories_in_state(
        &self,
        state: DirectoryState,
    ) -> anyhow::Result<Vec<DirectoryRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, category, remote_path, staging_path, library_path, state, manifest_hash, detected_at, synced_at, analyzed_at, renamed_at, transcoded_at, moved_at, detected_policy, plex_scan_at, error_message
             FROM directories WHERE state = ?1"
        )?;

        let rows = stmt.query_map(params![state.as_str()], |row| {
            Ok(DirectoryRecord {
                id: row.get(0)?,
                category: row.get(1)?,
                remote_path: row.get(2)?,
                staging_path: row.get(3)?,
                library_path: row.get(4)?,
                state: DirectoryState::from_str(&row.get::<_, String>(5)?).unwrap(),
                manifest_hash: row.get(6)?,
                detected_at: row.get(7)?,
                synced_at: row.get(8)?,
                analyzed_at: row.get(9)?,
                renamed_at: row.get(10)?,
                transcoded_at: row.get(11)?,
                moved_at: row.get(12)?,
                detected_policy: row.get(13)?,
                plex_scan_at: row.get(14)?,
                error_message: row.get(15)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn get_directory_by_id(&self, id: i64) -> anyhow::Result<Option<DirectoryRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, category, remote_path, staging_path, library_path, state, manifest_hash, detected_at, synced_at, analyzed_at, renamed_at, transcoded_at, moved_at, detected_policy, plex_scan_at, error_message
             FROM directories WHERE id = ?1"
        )?;

        let row = stmt.query_row(params![id], |row| {
            Ok(DirectoryRecord {
                id: row.get(0)?,
                category: row.get(1)?,
                remote_path: row.get(2)?,
                staging_path: row.get(3)?,
                library_path: row.get(4)?,
                state: DirectoryState::from_str(&row.get::<_, String>(5)?).unwrap(),
                manifest_hash: row.get(6)?,
                detected_at: row.get(7)?,
                synced_at: row.get(8)?,
                analyzed_at: row.get(9)?,
                renamed_at: row.get(10)?,
                transcoded_at: row.get(11)?,
                moved_at: row.get(12)?,
                detected_policy: row.get(13)?,
                plex_scan_at: row.get(14)?,
                error_message: row.get(15)?,
            })
        }).optional()?;
        Ok(row)
    }

    /// Insert a file record for a directory.
    pub fn insert_file(
        &self,
        dir_id: i64,
        original_name: &str,
        transcode_policy: &str,
        needs_transcode: bool,
    ) -> anyhow::Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO files (dir_id, original_name, transcode_policy, needs_transcode, transcode_status)
             VALUES (?1, ?2, ?3, ?4, 'pending')",
            params![dir_id, original_name, transcode_policy, needs_transcode as i32],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn update_file_renamed(&self, id: i64, renamed_name: &str) -> anyhow::Result<()> {
        self.conn.lock().unwrap().execute(
            "UPDATE files SET renamed_name = ?1 WHERE id = ?2",
            params![renamed_name, id],
        )?;
        Ok(())
    }

    pub fn update_file_final(&self, id: i64, final_name: &str, status: &str) -> anyhow::Result<()> {
        self.conn.lock().unwrap().execute(
            "UPDATE files SET final_name = ?1, transcode_status = ?2 WHERE id = ?3",
            params![final_name, status, id],
        )?;
        Ok(())
    }

    pub fn get_files_for_directory(&self, dir_id: i64) -> anyhow::Result<Vec<FileRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, dir_id, original_name, renamed_name, transcode_policy, transcode_status, needs_transcode, final_name
             FROM files WHERE dir_id = ?1"
        )?;

        let rows = stmt.query_map(params![dir_id], |row| {
            Ok(FileRecord {
                id: row.get(0)?,
                dir_id: row.get(1)?,
                original_name: row.get(2)?,
                renamed_name: row.get(3)?,
                transcode_policy: row.get(4)?,
                transcode_status: row.get(5)?,
                needs_transcode: row.get::<_, i32>(6)? != 0,
                final_name: row.get(7)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn count_directories_by_state(&self) -> anyhow::Result<Vec<(String, i64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT state, COUNT(*) FROM directories GROUP BY state"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    // ----------------------------------------------------------------------
    // Metadata cache (durable, SQLite-backed)
    //
    // The metadata-lookup step hits an external API (TMDB/IGDB/etc.) for a
    // canonical title. To survive process restarts — and to let a second
    // pipeline invocation skip a re-fetch — the positive results are
    // persisted here. The cache is *positive-only*: a miss in the API is
    // not stored, so a later version of the data, or a different parse,
    // can still succeed.
    //
    // The key (source_hash) is the SHA-256 of (category|without_group),
    // matching the in-memory `CachedLookup` key shape. Storing the same
    // shape in both layers means the two caches are interchangeable
    // from the caller's perspective.
    //
    // All helper failures are surfaced as `anyhow::Error`. Callers (in
    // `metadata.rs`) should treat DB errors as "cache miss" rather than
    // propagating them — the lookup must never block the pipeline.
    // ----------------------------------------------------------------------

    /// Fetch a cached canonical title. Returns `Ok(None)` on miss OR
    /// when the row is past its `expires_at` (expired entries are
    /// treated as misses, not deleted; a separate prune step can
    /// sweep them later). Returns `Err(_)` only on actual SQL failure
    /// — callers should log and fall back to the inner lookup.
    pub fn get_cached_metadata(
        &self,
        source_hash: &str,
    ) -> anyhow::Result<Option<crate::metadata::CanonicalTitle>> {
        let now = chrono::Utc::now().to_rfc3339();
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT canonical_title, canonical_year, external_id
                 FROM metadata_cache
                 WHERE source_hash = ?1 AND expires_at > ?2",
                params![source_hash, now],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;

        let (title, year_i64, external_id) = match row {
            Some(r) => r,
            None => return Ok(None),
        };
        let year = year_i64.and_then(|y| u32::try_from(y).ok());
        Ok(Some(crate::metadata::CanonicalTitle {
            title,
            year,
            external_id,
            season_count: None,
        }))
    }

    /// Persist a positive metadata result. Overwrites any prior row
    /// for the same `source_hash` (upsert via `ON CONFLICT`). `ttl` is
    /// applied to the current wall-clock time.
    pub fn store_cached_metadata(
        &self,
        source_hash: &str,
        category: &str,
        title: &crate::metadata::CanonicalTitle,
        ttl: std::time::Duration,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now();
        let expires = now + chrono::Duration::from_std(ttl)
            .unwrap_or_else(|_| chrono::Duration::days(30));
        let year_i64 = title.year.map(i64::from);

        self.conn.lock().unwrap().execute(
            r#"
            INSERT INTO metadata_cache (
                source_hash, category, canonical_title, canonical_year,
                external_id, raw_response, fetched_at, expires_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7)
            ON CONFLICT(source_hash) DO UPDATE SET
                category = excluded.category,
                canonical_title = excluded.canonical_title,
                canonical_year = excluded.canonical_year,
                external_id = excluded.external_id,
                fetched_at = excluded.fetched_at,
                expires_at = excluded.expires_at
            "#,
            params![
                source_hash,
                category,
                title.title,
                year_i64,
                title.external_id,
                now.to_rfc3339(),
                expires.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    /// Test-only: count of rows in the metadata_cache table. Used by
    /// `metadata.rs` tests to assert that negative results are not
    /// stored.
    #[cfg(test)]
    pub fn count_metadata_cache_rows(&self) -> anyhow::Result<i64> {
        let n: i64 = self.conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM metadata_cache", [], |r| r.get(0))?;
        Ok(n)
    }
}

#[derive(Debug, Clone)]
pub struct DirectoryRecord {
    pub id: i64,
    pub category: String,
    pub remote_path: String,
    pub staging_path: String,
    pub library_path: Option<String>,
    pub state: DirectoryState,
    pub manifest_hash: Option<String>,
    pub detected_at: Option<String>,
    pub synced_at: Option<String>,
    pub renamed_at: Option<String>,
    pub transcoded_at: Option<String>,
    pub moved_at: Option<String>,
    pub detected_policy: Option<String>,
    pub plex_scan_at: Option<String>,
    pub error_message: Option<String>,
    pub analyzed_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FileRecord {
    pub id: i64,
    pub dir_id: i64,
    pub original_name: String,
    pub renamed_name: Option<String>,
    pub transcode_policy: Option<String>,
    pub transcode_status: Option<String>,
    pub needs_transcode: bool,
    pub final_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn in_memory_db() -> Database {
        Database::open(Path::new(":memory:")).unwrap()
    }

    #[test]
    fn test_init_schema() {
        let db = in_memory_db();
        let counts = db.count_directories_by_state().unwrap();
        assert!(counts.is_empty());
    }

    #[test]
    fn test_upsert_and_get_directory() {
        let db = in_memory_db();
        db.upsert_directory("movies", "/srv/data/media/movies/Foo", "/staging/Movies/Foo", "hash123").unwrap();

        let detected = db.get_directories_in_state(DirectoryState::Detected).unwrap();
        assert_eq!(detected.len(), 1);
        assert_eq!(detected[0].category, "movies");
        assert_eq!(detected[0].remote_path, "/srv/data/media/movies/Foo");
    }

    #[test]
    fn test_upsert_same_path_does_not_duplicate() {
        let db = in_memory_db();
        db.upsert_directory("movies", "/srv/data/media/movies/Foo", "/staging/Movies/Foo", "hash123").unwrap();
        db.upsert_directory("movies", "/srv/data/media/movies/Foo", "/staging/Movies/Foo", "hash123").unwrap();

        let detected = db.get_directories_in_state(DirectoryState::Detected).unwrap();
        assert_eq!(detected.len(), 1);
    }

    #[test]
    fn test_upsert_different_hash_resets_to_detected() {
        let db = in_memory_db();
        db.upsert_directory("movies", "/srv/data/media/movies/Foo", "/staging/Movies/Foo", "hash123").unwrap();
        db.set_directory_state(1, DirectoryState::Synced).unwrap();

        // Now simulate a changed manifest on remote
        db.upsert_directory("movies", "/srv/data/media/movies/Foo", "/staging/Movies/Foo", "hash456").unwrap();

        let detected = db.get_directories_in_state(DirectoryState::Detected).unwrap();
        assert_eq!(detected.len(), 1);
    }

    #[test]
    fn test_state_transitions_and_timestamps() {
        let db = in_memory_db();
        db.upsert_directory("tvshows", "/srv/data/media/tv/Bar", "/staging/TvShows/Bar", "hash789").unwrap();

        let dir = db.get_directories_in_state(DirectoryState::Detected).unwrap().pop().unwrap();
        assert_eq!(dir.state, DirectoryState::Detected);

        db.set_directory_state(dir.id, DirectoryState::Syncing).unwrap();
        db.set_directory_state(dir.id, DirectoryState::Synced).unwrap();
        db.set_directory_state(dir.id, DirectoryState::Analyzed).unwrap();
        db.set_directory_state(dir.id, DirectoryState::Renamed).unwrap();
        db.set_directory_state(dir.id, DirectoryState::Transcoded).unwrap();
        db.set_directory_state(dir.id, DirectoryState::InLibrary).unwrap();

        let in_lib = db.get_directories_in_state(DirectoryState::InLibrary).unwrap();
        assert_eq!(in_lib.len(), 1);
        assert!(in_lib[0].synced_at.is_some());
        assert!(in_lib[0].renamed_at.is_some());
        assert!(in_lib[0].transcoded_at.is_some());
        assert!(in_lib[0].moved_at.is_some());
    }

    #[test]
    fn test_set_directory_error() {
        let db = in_memory_db();
        db.upsert_directory("movies", "/srv/data/media/movies/Bad", "/staging/Movies/Bad", "hash000").unwrap();
        db.set_directory_error(1, "something went wrong").unwrap();

        let failed = db.get_directories_in_state(DirectoryState::Failed).unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].error_message, Some("something went wrong".to_string()));
    }

    #[test]
    fn test_directory_policy() {
        let db = in_memory_db();
        db.upsert_directory("movies", "/srv/data/media/movies/Foo", "/staging/Movies/Foo", "hash123").unwrap();
        db.set_directory_policy(1, "x264_to_x265").unwrap();

        let dir = db.get_directory_by_id(1).unwrap().unwrap();
        assert_eq!(dir.detected_policy, Some("x264_to_x265".to_string()));
    }

    #[test]
    fn test_insert_and_get_files() {
        let db = in_memory_db();
        db.upsert_directory("movies", "/srv/data/media/movies/Foo", "/staging/Movies/Foo", "hash123").unwrap();

        let file_id = db.insert_file(1, "movie.mkv", "x264_to_x265", true).unwrap();
        assert_eq!(file_id, 1);

        let files = db.get_files_for_directory(1).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].original_name, "movie.mkv");
        assert_eq!(files[0].transcode_status, Some("pending".to_string()));
        assert!(files[0].needs_transcode);

        db.update_file_renamed(1, "movie-x265-NXELE.mkv").unwrap();
        db.update_file_final(1, "movie-x265-NXELE.mkv", "done").unwrap();

        let files = db.get_files_for_directory(1).unwrap();
        assert_eq!(files[0].renamed_name, Some("movie-x265-NXELE.mkv".to_string()));
        assert_eq!(files[0].final_name, Some("movie-x265-NXELE.mkv".to_string()));
        assert_eq!(files[0].transcode_status, Some("done".to_string()));
    }

    #[test]
    fn test_count_directories_by_state() {
        let db = in_memory_db();
        db.upsert_directory("movies", "/srv/data/media/movies/A", "/staging/Movies/A", "h1").unwrap();
        db.upsert_directory("tvshows", "/srv/data/media/tv/B", "/staging/TvShows/B", "h2").unwrap();
        db.set_directory_state(1, DirectoryState::Synced).unwrap();

        let counts = db.count_directories_by_state().unwrap();
        assert_eq!(counts.len(), 2); // detected + synced
    }

    // ----------------------------------------------------------------------
    // Sync dedup tests
    //
    // The pipeline's "don't re-download unchanged content" guarantee lives in
    // `upsert_directory` (see `ON CONFLICT` clause above): when an existing
    // (category, remote_path) row's manifest_hash matches the new one, the
    // state is preserved. When the hash changes, the state resets to
    // `detected` for reprocessing. These tests pin that contract.
    // ----------------------------------------------------------------------

    /// Helper: upsert a directory and return its id.
    fn make_dir(db: &Database, category: &str, remote: &str, hash: &str) -> i64 {
        db.upsert_directory(
            category,
            remote,
            &format!("/staging/{}", remote.rsplit('/').next().unwrap_or(remote)),
            hash,
        )
        .unwrap();
        // New rows are inserted in 'detected' state; there is exactly one
        // such row for this (category, remote_path) tuple, so fetching by
        // remote_path via a fresh query gives us its id.
        let rows = db.get_directories_in_state(DirectoryState::Detected).unwrap();
        rows.into_iter()
            .find(|r| r.category == category && r.remote_path == remote)
            .map(|r| r.id)
            .expect("upserted row not found in detected state")
    }

    #[test]
    fn test_dedup_unchanged_manifest_preserves_state() {
        let db = in_memory_db();
        let id = make_dir(&db, "tvshows", "/srv/data/media/tv/Show.A", "hash-A");
        db.set_directory_state(id, DirectoryState::Synced).unwrap();

        // Re-upsert with the same hash. The row must NOT regress to Detected.
        db.upsert_directory(
            "tvshows",
            "/srv/data/media/tv/Show.A",
            "/staging/Show.A",
            "hash-A",
        )
        .unwrap();

        let detected = db.get_directories_in_state(DirectoryState::Detected).unwrap();
        assert!(
            detected.iter().all(|r| r.id != id),
            "row with unchanged manifest regressed to Detected — would cause duplicate sync"
        );
        let synced = db.get_directories_in_state(DirectoryState::Synced).unwrap();
        assert_eq!(synced.iter().filter(|r| r.id == id).count(), 1);
    }

    #[test]
    fn test_dedup_changed_manifest_resets_to_detected() {
        let db = in_memory_db();
        let id = make_dir(&db, "movies", "/srv/data/media/movies/Foo", "hash-A");
        db.set_directory_state(id, DirectoryState::InLibrary).unwrap();

        let original_detected_at = db
            .get_directory_by_id(id)
            .unwrap()
            .and_then(|r| r.detected_at)
            .expect("detected_at should be set after initial upsert");

        // Manifest changed on the remote.
        db.upsert_directory(
            "movies",
            "/srv/data/media/movies/Foo",
            "/staging/Foo",
            "hash-B",
        )
        .unwrap();

        let detected = db.get_directories_in_state(DirectoryState::Detected).unwrap();
        assert_eq!(
            detected.iter().filter(|r| r.id == id).count(),
            1,
            "row with changed manifest should be queued for reprocessing"
        );
        let in_lib = db.get_directories_in_state(DirectoryState::InLibrary).unwrap();
        assert!(
            in_lib.iter().all(|r| r.id != id),
            "row with changed manifest should have left InLibrary"
        );

        let refreshed_detected_at = db
            .get_directory_by_id(id)
            .unwrap()
            .and_then(|r| r.detected_at)
            .expect("detected_at should be set after re-upsert");
        // Wall-clock may not advance between two close upserts; assert the
        // value is present and the state was reset. A strict comparison
        // would be flaky on fast machines.
        assert!(!refreshed_detected_at.is_empty());
        // And the error_message from any prior failure must have been cleared.
        let dir = db.get_directory_by_id(id).unwrap().unwrap();
        assert!(dir.error_message.is_none());
        let _ = original_detected_at; // referenced only to make the timestamp semantics explicit
    }

    #[test]
    fn test_dedup_per_category_independence() {
        let db = in_memory_db();
        // The UNIQUE(category, remote_path) constraint means the same path
        // can exist once per category. This is how the pipeline keeps
        // movies/ and tv/ namespaces from colliding.
        let movies_id = make_dir(&db, "movies", "/srv/data/media/shared/Foo", "m-hash");
        let tv_id = make_dir(&db, "tvshows", "/srv/data/media/shared/Foo", "t-hash");
        assert_ne!(movies_id, tv_id);

        let detected = db.get_directories_in_state(DirectoryState::Detected).unwrap();
        assert_eq!(detected.len(), 2);
    }

    #[test]
    fn test_dedup_idempotent_under_repeated_calls() {
        let db = in_memory_db();
        // The 1-hour sync timer will fire many times for the same remote
        // content. Each call must be a no-op once the manifest is stable.
        for _ in 0..5 {
            db.upsert_directory(
                "movies",
                "/srv/data/media/movies/Foo",
                "/staging/Foo",
                "stable-hash",
            )
            .unwrap();
        }

        let detected = db.get_directories_in_state(DirectoryState::Detected).unwrap();
        assert_eq!(
            detected.len(),
            1,
            "repeated upserts of the same (category, path, hash) should produce one row"
        );
    }

    #[test]
    fn test_dedup_failed_state_recovered_by_manifest_change() {
        let db = in_memory_db();
        let id = make_dir(&db, "tvshows", "/srv/data/media/tv/Broken", "hash-X");
        db.set_directory_error(id, "rsync connection timed out").unwrap();

        let failed = db.get_directories_in_state(DirectoryState::Failed).unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(
            failed[0].error_message.as_deref(),
            Some("rsync connection timed out")
        );

        // New manifest hash arrives — pipeline should reset to Detected
        // and clear the error so the next sync run picks it up.
        db.upsert_directory(
            "tvshows",
            "/srv/data/media/tv/Broken",
            "/staging/Broken",
            "hash-Y",
        )
        .unwrap();

        let after = db.get_directory_by_id(id).unwrap().unwrap();
        assert_eq!(after.state, DirectoryState::Detected);
        assert!(after.error_message.is_none());
    }

    #[test]
    fn test_dedup_state_machine_round_trip() {
        let db = in_memory_db();
        let id = make_dir(&db, "tvshows", "/srv/data/media/tv/Show.B", "hash-B");

        // Walk the full state machine.
        for s in [
            DirectoryState::Syncing,
            DirectoryState::Synced,
            DirectoryState::Analyzing,
            DirectoryState::Analyzed,
            DirectoryState::Renaming,
            DirectoryState::Renamed,
            DirectoryState::Transcoding,
            DirectoryState::Transcoded,
            DirectoryState::Moving,
            DirectoryState::InLibrary,
        ] {
            db.set_directory_state(id, s).unwrap();
        }

        // Subsequent timer fires with the same manifest must NOT re-process.
        for _ in 0..3 {
            db.upsert_directory(
                "tvshows",
                "/srv/data/media/tv/Show.B",
                "/staging/Show.B",
                "hash-B",
            )
            .unwrap();
            let after = db.get_directory_by_id(id).unwrap().unwrap();
            assert_eq!(
                after.state,
                DirectoryState::InLibrary,
                "completed row regressed under same-hash upsert"
            );
        }
    }

    // ----------------------------------------------------------------------
    // Metadata cache tests
    //
    // The metadata_cache table is the durable layer for canonical-
    // title lookups. These tests pin the SQL-level contract:
    // upsert, retrieval, expiry filter, key uniqueness.
    // ----------------------------------------------------------------------

    use crate::metadata::CanonicalTitle;

    fn make_title(title: &str, year: Option<u32>, ext_id: &str) -> CanonicalTitle {
        CanonicalTitle {
            title: title.to_string(),
            year,
            external_id: ext_id.to_string(),
            season_count: None,
        }
    }

    #[test]
    fn test_metadata_cache_miss_returns_none() {
        let db = in_memory_db();
        let result = db.get_cached_metadata("nonexistent-hash").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_metadata_cache_store_and_retrieve() {
        let db = in_memory_db();
        let title = make_title("Shoresy", Some(2022), "tt14058038");

        db.store_cached_metadata(
            "hash-1",
            "movies",
            &title,
            std::time::Duration::from_secs(60),
        )
        .unwrap();

        let result = db.get_cached_metadata("hash-1").unwrap();
        let cached = result.expect("row should be present");
        assert_eq!(cached.title, "Shoresy");
        assert_eq!(cached.year, Some(2022));
        assert_eq!(cached.external_id, "tt14058038");
    }

    #[test]
    fn test_metadata_cache_upsert_overwrites_prior_row() {
        let db = in_memory_db();
        let original = make_title("Original", Some(2020), "tt-orig");
        let updated = make_title("Updated", Some(2021), "tt-upd");

        db.store_cached_metadata(
            "hash-shared",
            "movies",
            &original,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        db.store_cached_metadata(
            "hash-shared",
            "movies",
            &updated,
            std::time::Duration::from_secs(60),
        )
        .unwrap();

        // Same key → same row (PRIMARY KEY), value updated.
        assert_eq!(db.count_metadata_cache_rows().unwrap(), 1);
        let cached = db.get_cached_metadata("hash-shared").unwrap().unwrap();
        assert_eq!(cached.title, "Updated");
        assert_eq!(cached.year, Some(2021));
    }

    #[test]
    fn test_metadata_cache_expired_entry_treated_as_miss() {
        // Store an entry that's already past its expires_at by writing
        // it with a 0-second TTL and sleeping briefly. The `WHERE
        // expires_at > now` filter in the read should drop it.
        let db = in_memory_db();
        let title = make_title("ExpireSoon", Some(2024), "tt-exp");

        // Use a TTL of 1s so the row exists but expires soon.
        db.store_cached_metadata(
            "hash-exp",
            "movies",
            &title,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        // Row is present immediately.
        let present = db.get_cached_metadata("hash-exp").unwrap();
        assert!(present.is_some(), "row should be cached just after write");

        // Wait past the TTL.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let after_expiry = db.get_cached_metadata("hash-exp").unwrap();
        assert!(after_expiry.is_none(), "expired row should be invisible to reads");
    }

    #[test]
    fn test_metadata_cache_handles_missing_year() {
        // year is Option<u32> in the struct, Option<i64> in SQLite
        // (NULL when None). Verify the round-trip preserves the None.
        let db = in_memory_db();
        let title = make_title("Yearless", None, "tt-y");

        db.store_cached_metadata(
            "hash-y",
            "movies",
            &title,
            std::time::Duration::from_secs(60),
        )
        .unwrap();

        let cached = db.get_cached_metadata("hash-y").unwrap().unwrap();
        assert_eq!(cached.title, "Yearless");
        assert_eq!(cached.year, None);
    }
}
