//! Per-category library layout resolution.
//!
//! The pipeline moves fully-processed directories from staging into
//! the live library. The destination is computed by this module:
//!
//! - **Movies** and **TV** use the cleaned title (TMDB canonical or
//!   local parse) as the directory basename, with a collision chain
//!   that escalates to `<Title> (<Year>)/` then `<Title> N>/`.
//! - **Other categories** (Music, Games, Books, Comics, Erotic) keep
//!   the existing single-folder layout — the staging dir's basename
//!   is used as-is. (Per-artist/per-album for music needs fields the
//!   parser doesn't extract yet; deferred.)
//!
//! The collision-resolution check is a `Path::exists()` stat. The
//! deeper content-hash dedup (for the "same content under a different
//! name" case) lives in `library::move_to_library`.

use std::path::{Path, PathBuf};

use crate::config::Config;

/// Categories that get the cleaned-title + collision-chain layout.
/// Other categories fall through to today's behavior (staging dir's
/// basename becomes the library subdir).
const LAYOUT_AWARE_CATEGORIES: &[&str] = &["movies", "tvshows"];

/// Compute the final library path for a directory that has just
/// finished transcode. `library_base` is the per-category folder
/// inside the library (e.g. `/library/Movies`); the resolver
/// returns the full leaf path that the staging dir should be moved
/// into.
///
/// `staging_basename` is the basename of the staging directory — used
/// as the fallback leaf for categories that don't get the cleaned
/// layout. `title` is the cleaned directory name (TMDB canonical or
/// local parse). `year` is the release year from TMDB (or None for
/// the local-parse path).
pub fn resolve_library_path(
    config: &Config,
    category: &str,
    staging_basename: &str,
    title: &str,
    year: Option<u32>,
) -> PathBuf {
    let library_base = config.library_path(category);

    if LAYOUT_AWARE_CATEGORIES.contains(&category) {
        resolve_destination(&library_base, title, year)
    } else {
        // Today's behavior: the staging dir's basename is the leaf.
        library_base.join(staging_basename)
    }
}

/// Find a free path under `library_base` for a directory named
/// `title`. If `<title>/` already exists, escalate to
/// `<title> (<year>)/` (if a year is available), then to
/// `<title> 2/`, `<title> 3/`, … until a free name is found. The
/// number-suffix chain skips over existing `<title> N>/` so we
/// never overwrite something just because a new arrival had a
/// higher number.
///
/// The 999-iteration cap is a safety net for the pathological
/// "literally hundreds of `Matrix 2`, `Matrix 3`, … already exist"
/// case. Past that, fall back to a timestamp suffix.
pub fn resolve_destination(
    library_base: &Path,
    title: &str,
    year: Option<u32>,
) -> PathBuf {
    // Fast path: the cleaned title is free.
    let primary = library_base.join(title);
    if !primary.exists() {
        return primary;
    }

    // Collision. Try `<title> (<year>)/` if we have a year.
    if let Some(y) = year {
        let with_year = library_base.join(format!("{} ({})", title, y));
        if !with_year.exists() {
            return with_year;
        }
    }

    // Numeric suffix chain. We need to find the lowest free `N` —
    // so we walk from 2 upward, checking each.
    let mut n: u32 = 2;
    loop {
        let candidate = library_base.join(format!("{} {}", title, n));
        if !candidate.exists() {
            return candidate;
        }
        n += 1;
        if n > 999 {
            // Pathological case. Use a timestamp suffix so we still
            // make progress rather than failing the pipeline.
            return library_base.join(format!(
                "{} {}",
                title,
                chrono::Local::now().format("%Y%m%d%H%M%S")
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    /// Build a temp library base with optional pre-existing dirs.
    /// Returns the library base path; callers append to it.
    fn make_library_base() -> PathBuf {
        let tmp = tempfile::tempdir().unwrap();
        let lib = tmp.path().join("Movies");
        fs::create_dir_all(&lib).unwrap();
        // Hold the TempDir alive for the test by leaking the path —
        // the test cleanup happens via the OS reclaiming the dir.
        std::mem::forget(tmp);
        lib
    }

    /// Build a `Config` that has a `movies` category pointing at
    /// the given library base. Other categories are not configured.
    fn make_config(library_base: &Path) -> Config {
        use std::collections::HashMap;
        use std::path::PathBuf;
        Config {
            ssh: crate::config::SshConfig {
                host: "test".to_string(),
                user: "test".to_string(),
                private_key_path: PathBuf::from("/dev/null"),
                remote_base_path: PathBuf::from("/tmp"),
            },
            database: crate::config::DatabaseConfig {
                path: PathBuf::from(":memory:"),
            },
            paths: crate::config::PathsConfig {
                staging: PathBuf::from("/staging"),
                library: library_base.parent().unwrap().to_path_buf(),
            },
            plex: crate::config::PlexConfig {
                url: "http://test:32400".to_string(),
                sections: HashMap::new(),
            },
            logging: None,
            group_name: None,
            categories: {
                let mut m = HashMap::new();
                m.insert(
                    "movies".to_string(),
                    crate::config::CategoryConfig {
                        remote_dir: "movies".to_string(),
                        library_folder: library_base.file_name().unwrap().to_string_lossy().to_string(),
                        transcode_policy: None,
                        plex_section: None,
                    },
                );
                m.insert(
                    "tvshows".to_string(),
                    crate::config::CategoryConfig {
                        remote_dir: "tv".to_string(),
                        library_folder: "TvShows".to_string(),
                        transcode_policy: None,
                        plex_section: None,
                    },
                );
                m.insert(
                    "music".to_string(),
                    crate::config::CategoryConfig {
                        remote_dir: "music".to_string(),
                        library_folder: "Music".to_string(),
                        transcode_policy: None,
                        plex_section: None,
                    },
                );
                m
            },
            metadata: crate::config::MetadataConfig::default(),
        }
    }

    // ----------------------------------------------------------------------
    // resolve_destination tests
    // ----------------------------------------------------------------------

    #[test]
    fn test_resolve_destination_no_collision() {
        let lib = make_library_base();
        let result = resolve_destination(&lib, "Shoresy", None);
        assert_eq!(result, lib.join("Shoresy"));
    }

    #[test]
    fn test_resolve_destination_collision_with_year() {
        let lib = make_library_base();
        // Pre-existing "Shoresy" directory.
        fs::create_dir_all(lib.join("Shoresy")).unwrap();
        // No year passed in — falls through to numeric suffix.
        let result = resolve_destination(&lib, "Shoresy", Some(2025));
        assert_eq!(result, lib.join("Shoresy (2025)"));
    }

    #[test]
    fn test_resolve_destination_collision_no_year() {
        let lib = make_library_base();
        fs::create_dir_all(lib.join("Shoresy")).unwrap();
        let result = resolve_destination(&lib, "Shoresy", None);
        assert_eq!(result, lib.join("Shoresy 2"));
    }

    #[test]
    fn test_resolve_destination_collision_year_also_collides() {
        let lib = make_library_base();
        fs::create_dir_all(lib.join("Shoresy")).unwrap();
        fs::create_dir_all(lib.join("Shoresy (2025)")).unwrap();
        let result = resolve_destination(&lib, "Shoresy", Some(2025));
        assert_eq!(result, lib.join("Shoresy 2"));
    }

    #[test]
    fn test_resolve_destination_skips_existing_numeric_suffixes() {
        // If "Foo 2" already exists, jump to "Foo 3" — never reuse
        // an existing numeric suffix.
        let lib = make_library_base();
        fs::create_dir_all(lib.join("Foo")).unwrap();
        fs::create_dir_all(lib.join("Foo 2")).unwrap();
        let result = resolve_destination(&lib, "Foo", None);
        assert_eq!(result, lib.join("Foo 3"));
    }

    #[test]
    fn test_resolve_destination_finds_lowest_free_suffix() {
        // Gaps in the numeric chain (e.g. "Foo 5" exists but "Foo 4"
        // doesn't) — return the lowest free number.
        let lib = make_library_base();
        fs::create_dir_all(lib.join("Foo")).unwrap();
        fs::create_dir_all(lib.join("Foo 2")).unwrap();
        fs::create_dir_all(lib.join("Foo 3")).unwrap();
        fs::create_dir_all(lib.join("Foo 5")).unwrap();
        let result = resolve_destination(&lib, "Foo", None);
        assert_eq!(result, lib.join("Foo 4"));
    }

    // ----------------------------------------------------------------------
    // resolve_library_path tests
    // ----------------------------------------------------------------------

    #[test]
    fn test_resolve_library_path_movies() {
        let lib = make_library_base();
        let config = make_config(&lib);
        let result = resolve_library_path(&config, "movies", "TheMatrix.2024", "The Matrix", Some(1999));
        assert_eq!(result, lib.join("The Matrix"));
    }

    #[test]
    fn test_resolve_library_path_tv() {
        // Use a separate tempdir for TvShows to avoid conflict with the movies lib.
        let tmp = tempfile::tempdir().unwrap();
        let tv = tmp.path().join("TvShows");
        fs::create_dir_all(&tv).unwrap();
        let config = make_config(&tmp.path().join("Movies"));
        // Override paths.library manually to point at the parent of `tv`.
        let mut config = config;
        config.paths.library = tmp.path().to_path_buf();
        let result = resolve_library_path(&config, "tvshows", "remote-dir-name", "Shoresy", None);
        assert_eq!(result, tv.join("Shoresy"));
    }

    #[test]
    fn test_resolve_library_path_movies_with_collision() {
        let lib = make_library_base();
        let config = make_config(&lib);
        // Pre-existing "The Matrix" dir.
        fs::create_dir_all(lib.join("The Matrix")).unwrap();
        let result = resolve_library_path(&config, "movies", "TheMatrix.2024", "The Matrix", Some(1999));
        assert_eq!(result, lib.join("The Matrix (1999)"));
    }

    #[test]
    fn test_resolve_library_path_music_uses_staging_basename() {
        // Music is not layout-aware. The staging dir's basename
        // becomes the leaf, like today.
        let tmp = tempfile::tempdir().unwrap();
        let music = tmp.path().join("Music");
        fs::create_dir_all(&music).unwrap();
        let mut config = make_config(&tmp.path().join("Movies"));
        config.paths.library = tmp.path().to_path_buf();
        let result = resolve_library_path(&config, "music", "Pink.Floyd-Dark.Side.Of.The.Moon.1973.FLAC", "Pink Floyd", Some(1973));
        assert_eq!(result, music.join("Pink.Floyd-Dark.Side.Of.The.Moon.1973.FLAC"));
    }

    #[test]
    fn test_resolve_library_path_games_uses_staging_basename() {
        // Games is not in the layout-aware list — fall through to
        // staging-basename behavior, like today.
        let tmp = tempfile::tempdir().unwrap();
        let games = tmp.path().join("Games");
        fs::create_dir_all(&games).unwrap();
        let mut config = make_config(&tmp.path().join("Movies"));
        config.paths.library = tmp.path().to_path_buf();
        // Add the games category to the config so library_path() works.
        config.categories.insert(
            "games".to_string(),
            crate::config::CategoryConfig {
                remote_dir: "games".to_string(),
                library_folder: "Games".to_string(),
                transcode_policy: None,
                plex_section: None,
            },
        );
        let result = resolve_library_path(&config, "games", "rune-company.of.heroes.3", "Company of Heroes 3", None);
        assert_eq!(result, games.join("rune-company.of.heroes.3"));
    }
}
