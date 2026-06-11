use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub ssh: SshConfig,
    pub database: DatabaseConfig,
    pub paths: PathsConfig,
    pub plex: PlexConfig,
    pub logging: Option<LoggingConfig>,
    /// Release group name to replace original uploaders in filenames.
    /// Defaults to "REPACK" if not specified.
    pub group_name: Option<String>,
    pub categories: HashMap<String, CategoryConfig>,
    /// Metadata-lookup configuration. Optional — when omitted, the
    /// pipeline uses `NoopLookup` (no API calls).
    #[serde(default)]
    pub metadata: MetadataConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SshConfig {
    pub host: String,
    pub user: String,
    pub private_key_path: PathBuf,
    pub remote_base_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PathsConfig {
    pub staging: PathBuf,
    pub library: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlexConfig {
    pub url: String,
    #[serde(default)]
    pub sections: HashMap<String, i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    pub level: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CategoryConfig {
    pub remote_dir: String,
    pub library_folder: String,
    /// Optional default transcode policy for this category.
    /// If omitted, the pipeline will auto-detect per-title.
    pub transcode_policy: Option<TranscodePolicy>,
    pub plex_section: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscodePolicy {
    None,
    X264ToX265,
    Downscale1080p,
}

impl TranscodePolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            TranscodePolicy::None => "none",
            TranscodePolicy::X264ToX265 => "x264_to_x265",
            TranscodePolicy::Downscale1080p => "downscale_1080p",
        }
    }
}

/// Configuration for the metadata-lookup step. When `tmdb_api_key_env`
/// is unset (or the corresponding env var is missing), the pipeline
/// selects `NoopLookup` and the lookup step is a no-op. The actual
/// key is read from the environment at runtime — never written to
/// disk or to the config file.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MetadataConfig {
    /// Name of the env var that holds the TMDB API key. If unset or the
    /// env var is missing, `NoopLookup` is used.
    #[serde(default)]
    pub tmdb_api_key_env: Option<String>,
    /// Categories that should hit the API. Defaults to all of them.
    /// Names must match keys in `categories`.
    #[serde(default)]
    pub enabled_categories: Option<Vec<String>>,
    /// Per-request timeout in seconds. Defaults to 5s.
    #[serde(default)]
    pub request_timeout_secs: Option<u64>,
    /// In-memory cache TTL in days. Defaults to 30.
    #[serde(default)]
    pub cache_ttl_days: Option<u64>,
}

impl MetadataConfig {
    /// Returns true if a TMDB API key is configured AND the env var
    /// is set in the current process. Used to decide between
    /// `NoopLookup` and `HttpLookup` at startup.
    pub fn has_tmdb_credentials(&self) -> bool {
        match &self.tmdb_api_key_env {
            Some(env_var) => std::env::var(env_var).is_ok(),
            None => false,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;

        let mut config: Self = toml::from_str(&contents)
            .with_context(|| format!("failed to parse config file: {}", path.display()))?;

        // Load Plex token from env if present
        if let Ok(token) = std::env::var("PLEX_TOKEN") {
            // We could store this in the config, but it's better to pass it around separately.
            // For now, the plex module reads PLEX_TOKEN from env directly.
        }

        // Validate category configs
        for (_name, cat) in &config.categories {
            // transcode_policy is optional and auto-detected per-title when omitted.
            // If provided, it must be one of the valid variants (enforced by serde).
            // plex_section is optional (null allowed).
            let _ = cat;
        }

        Ok(config)
    }

    pub fn staging_path(&self, category: &str) -> PathBuf {
        self.paths.staging.join(&self.categories[category].library_folder)
    }

    pub fn library_path(&self, category: &str) -> PathBuf {
        self.paths.library.join(&self.categories[category].library_folder)
    }

    pub fn remote_path(&self, category: &str) -> PathBuf {
        self.ssh.remote_base_path.join(&self.categories[category].remote_dir)
    }

    pub fn group_name(&self) -> &str {
        self.group_name.as_deref().unwrap_or("REPACK")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn create_temp_config(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_load_valid_config() {
        let toml = r#"
[ssh]
host = "downloads.example.com"
user = "mediapipe"
private_key_path = "/root/.ssh/id_rsa"
remote_base_path = "/srv/data/media"

[database]
path = "/data/pipeline.db"

[paths]
staging = "/staging"
library = "/library"

group_name = "REPACK"

[plex]
url = "http://plex:32400"

[plex.sections]
movies = 1

[categories.movies]
remote_dir = "movies"
library_folder = "Movies"
"#;
        let temp = create_temp_config(toml);
        let config = Config::load(temp.path()).unwrap();

        assert_eq!(config.ssh.host, "downloads.example.com");
        assert_eq!(config.paths.staging, Path::new("/staging"));
        assert_eq!(config.plex.sections.get("movies"), Some(&1));
        assert_eq!(config.categories.get("movies").unwrap().remote_dir, "movies");
        assert_eq!(config.group_name(), "REPACK");
    }

    #[test]
    fn test_load_missing_file() {
        let result = Config::load(Path::new("/nonexistent/path/config.toml"));
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("failed to read config file"));
    }

    #[test]
    fn test_load_invalid_toml() {
        let toml = "this is not valid toml {{";
        let temp = create_temp_config(toml);
        let result = Config::load(temp.path());
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("failed to parse config file"));
    }

    #[test]
    fn test_staging_path() {
        let toml = r#"
[ssh]
host = "downloads.example.com"
user = "mediapipe"
private_key_path = "/root/.ssh/id_rsa"
remote_base_path = "/srv/data/media"

[database]
path = "/data/pipeline.db"

[paths]
staging = "/staging"
library = "/library"

[plex]
url = "http://plex:32400"

[categories.tvshows]
remote_dir = "tv"
library_folder = "TvShows"
"#;
        let temp = create_temp_config(toml);
        let config = Config::load(temp.path()).unwrap();

        assert_eq!(
            config.staging_path("tvshows"),
            Path::new("/staging/TvShows")
        );
        assert_eq!(
            config.library_path("tvshows"),
            Path::new("/library/TvShows")
        );
        assert_eq!(
            config.remote_path("tvshows"),
            Path::new("/srv/data/media/tv")
        );
    }

    #[test]
    fn test_transcode_policy_as_str() {
        assert_eq!(TranscodePolicy::None.as_str(), "none");
        assert_eq!(TranscodePolicy::X264ToX265.as_str(), "x264_to_x265");
        assert_eq!(TranscodePolicy::Downscale1080p.as_str(), "downscale_1080p");
    }

    #[test]
    fn test_default_group_name() {
        let toml = r#"
[ssh]
host = "downloads.example.com"
user = "mediapipe"
private_key_path = "/root/.ssh/id_rsa"
remote_base_path = "/srv/data/media"

[database]
path = "/data/pipeline.db"

[paths]
staging = "/staging"
library = "/library"

[plex]
url = "http://plex:32400"

[categories.movies]
remote_dir = "movies"
library_folder = "Movies"
"#;
        let temp = create_temp_config(toml);
        let config = Config::load(temp.path()).unwrap();
        assert_eq!(config.group_name(), "REPACK");
    }

    #[test]
    fn test_metadata_config_defaults_to_noop() {
        // No `[metadata]` section at all — should default to noop.
        let toml = r#"
[ssh]
host = "downloads.example.com"
user = "mediapipe"
private_key_path = "/root/.ssh/id_rsa"
remote_base_path = "/srv/data/media"

[database]
path = "/data/pipeline.db"

[paths]
staging = "/staging"
library = "/library"

[plex]
url = "http://plex:32400"

[categories.movies]
remote_dir = "movies"
library_folder = "Movies"
"#;
        let temp = create_temp_config(toml);
        let config = Config::load(temp.path()).unwrap();
        assert!(!config.metadata.has_tmdb_credentials());
    }

    #[test]
    fn test_metadata_config_tmdb_key_env_var_present() {
        // Config names the env var; the env var is set; should report
        // credentials present.
        let toml = r#"
[ssh]
host = "downloads.example.com"
user = "mediapipe"
private_key_path = "/root/.ssh/id_rsa"
remote_base_path = "/srv/data/media"

[database]
path = "/data/pipeline.db"

[paths]
staging = "/staging"
library = "/library"

[plex]
url = "http://plex:32400"

[metadata]
tmdb_api_key_env = "TEST_TMDB_KEY_VAR_FOR_CONFIG"

[categories.movies]
remote_dir = "movies"
library_folder = "Movies"
"#;
        let temp = create_temp_config(toml);
        let config = Config::load(temp.path()).unwrap();

        // The env var isn't set; should report no credentials.
        std::env::remove_var("TEST_TMDB_KEY_VAR_FOR_CONFIG");
        assert!(!config.metadata.has_tmdb_credentials());

        // Set it; should report credentials present.
        std::env::set_var("TEST_TMDB_KEY_VAR_FOR_CONFIG", "fake-key");
        assert!(config.metadata.has_tmdb_credentials());
        std::env::remove_var("TEST_TMDB_KEY_VAR_FOR_CONFIG");
    }
}
