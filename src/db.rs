use std::path::Path;

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

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open SQLite database at {}", path.display()))?;

        let db = Database { conn };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> anyhow::Result<()> {
        self.conn.execute_batch(
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
        self.conn.execute(
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
        self.conn.execute(&sql, params![state_str, id])?;
        Ok(())
    }

    pub fn set_directory_error(&self, id: i64, message: &str) -> anyhow::Result<()> {
        self.conn.execute(
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
        self.conn.execute(
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
        self.conn.execute(
            "UPDATE directories SET detected_policy = ?1 WHERE id = ?2",
            params![policy, id],
        )?;
        Ok(())
    }

    pub fn set_plex_scan_at(&self, id: i64) -> anyhow::Result<()> {
        self.conn.execute(
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
        let mut stmt = self.conn.prepare(
            "SELECT id, category, remote_path, staging_path, library_path, state, manifest_hash, detected_at, synced_at, renamed_at, transcoded_at, moved_at, detected_policy, plex_scan_at, error_message
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
                renamed_at: row.get(9)?,
                transcoded_at: row.get(10)?,
                moved_at: row.get(11)?,
                detected_policy: row.get(12)?,
                plex_scan_at: row.get(13)?,
                error_message: row.get(14)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn get_directory_by_id(&self, id: i64) -> anyhow::Result<Option<DirectoryRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, category, remote_path, staging_path, library_path, state, manifest_hash, detected_at, synced_at, renamed_at, transcoded_at, moved_at, plex_scan_at, error_message
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
                renamed_at: row.get(9)?,
                transcoded_at: row.get(10)?,
                moved_at: row.get(11)?,
                detected_policy: row.get(12)?,
                plex_scan_at: row.get(13)?,
                error_message: row.get(14)?,
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
        self.conn.execute(
            "INSERT INTO files (dir_id, original_name, transcode_policy, needs_transcode, transcode_status)
             VALUES (?1, ?2, ?3, ?4, 'pending')",
            params![dir_id, original_name, transcode_policy, needs_transcode as i32],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn update_file_renamed(&self, id: i64, renamed_name: &str) -> anyhow::Result<()> {
        self.conn.execute(
            "UPDATE files SET renamed_name = ?1 WHERE id = ?2",
            params![renamed_name, id],
        )?;
        Ok(())
    }

    pub fn update_file_final(&self, id: i64, final_name: &str, status: &str) -> anyhow::Result<()> {
        self.conn.execute(
            "UPDATE files SET final_name = ?1, transcode_status = ?2 WHERE id = ?3",
            params![final_name, status, id],
        )?;
        Ok(())
    }

    pub fn get_files_for_directory(&self, dir_id: i64) -> anyhow::Result<Vec<FileRecord>> {
        let mut stmt = self.conn.prepare(
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
        let mut stmt = self.conn.prepare(
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
}
