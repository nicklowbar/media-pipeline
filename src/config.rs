use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Deserialize;

/// Environment variable names that override specific config fields
/// at startup. The names are stable and documented; the TOML fields
/// stay in the file as fallbacks so the binary works on a host
/// without any env vars set.
///
/// Precedence, for every overridable field:
///   1. Environment variable (if set and non-empty)
///   2. TOML file value
///   3. In-code default (where applicable)
pub mod env {
    pub const SSH_HOST: &str = "MEDIA_PIPELINE_SSH_HOST";
    pub const SSH_USER: &str = "MEDIA_PIPELINE_SSH_USER";
    pub const SSH_KEY: &str = "MEDIA_PIPELINE_SSH_KEY";
    pub const SSH_REMOTE_BASE: &str = "MEDIA_PIPELINE_SSH_REMOTE_BASE";
    pub const DATABASE_PATH: &str = "MEDIA_PIPELINE_DATABASE_PATH";
    pub const STAGING: &str = "MEDIA_PIPELINE_STAGING";
    pub const LIBRARY: &str = "MEDIA_PIPELINE_LIBRARY";
    pub const PLEX_URL: &str = "MEDIA_PIPELINE_PLEX_URL";
    pub const TMDB_API_KEY: &str = "MEDIA_PIPELINE_TMDB_API_KEY";
}

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

/// Configuration for the metadata-lookup step. The TMDB API key
/// follows the same env-var-overrides-TOML pattern as every other
/// overridable field. The TOML field (`tmdb_api_key`) defaults to
/// the empty string; the `MEDIA_PIPELINE_TMDB_API_KEY` env var
/// overrides it at load time. Native deployments typically set the
/// TOML value; container deployments typically set the env var.
/// Either path is supported and the precedence is the same as for
/// paths, SSH, and Plex URL.
///
/// When the resolved value is empty (TOML empty AND env var unset
/// or empty), the pipeline selects `NoopLookup` and the lookup step
/// is a no-op.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MetadataConfig {
    /// TMDB API key. Defaults to empty. Overridden at load time by
    /// the `MEDIA_PIPELINE_TMDB_API_KEY` env var. Set this in the
    /// TOML for native deployments, or leave empty and use the env
    /// var for container deployments.
    #[serde(default)]
    pub tmdb_api_key: String,
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
    /// Returns true if a TMDB API key is set, considering both the
    /// TOML value (set at parse time) and the env var (read at
    /// `apply_env_overrides` time). The env var wins if both are
    /// set, because env-var resolution happens after TOML parse.
    pub fn has_tmdb_credentials(&self) -> bool {
        !self.tmdb_api_key.is_empty()
    }
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;

        let config: Self = toml::from_str(&contents)
            .with_context(|| format!("failed to parse config file: {}", path.display()))?;

        Ok(config)
    }

    /// Apply env-var overrides to a freshly-loaded config. Precedence
    /// is env > TOML > in-code default. The env-var names are stable
    /// and documented in the `env` module above. An empty env-var
    /// value is treated as unset (it never overrides the TOML value).
    ///
    /// This is a separate step from `load` so that callers (and tests)
    /// can load the file in isolation without env-var interference.
    /// In production, `run_process` calls `load` and then
    /// `apply_env_overrides`; native and container deployments get
    /// the same precedence either way.
    pub fn apply_env_overrides(&mut self) {
        apply_env_string(&mut self.ssh.host, env::SSH_HOST);
        apply_env_string(&mut self.ssh.user, env::SSH_USER);
        apply_env_path(&mut self.ssh.private_key_path, env::SSH_KEY);
        apply_env_path(&mut self.ssh.remote_base_path, env::SSH_REMOTE_BASE);
        apply_env_path(&mut self.database.path, env::DATABASE_PATH);
        apply_env_path(&mut self.paths.staging, env::STAGING);
        apply_env_path(&mut self.paths.library, env::LIBRARY);
        apply_env_string(&mut self.plex.url, env::PLEX_URL);
        apply_env_string(&mut self.metadata.tmdb_api_key, env::TMDB_API_KEY);
    }

    /// Convenience: load the config file and apply env-var overrides
    /// in one call. Equivalent to `load(...).map(|mut c| { c.apply_env_overrides(); c })`.
    pub fn load_with_env(path: &Path) -> anyhow::Result<Self> {
        let mut config = Self::load(path)?;
        config.apply_env_overrides();
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

/// If the named env var is set and non-empty, replace `target` with
/// its value. An empty value is treated as "not set" — empty env vars
/// are usually deployment mistakes (a shell that exports a blank, an
/// un-set Compose var) and the TOML value is a safer fallback.
fn apply_env_string(target: &mut String, var: &str) {
    if let Ok(value) = std::env::var(var) {
        if !value.is_empty() {
            *target = value;
        }
    }
}

/// Path-flavored version of `apply_env_string`. Same empty-string
/// guard.
fn apply_env_path(target: &mut PathBuf, var: &str) {
    if let Ok(value) = std::env::var(var) {
        if !value.is_empty() {
            *target = PathBuf::from(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Env-var manipulation is process-global. Each test that reads
    /// or writes the `MEDIA_PIPELINE_*` env vars must hold this lock
    /// to keep parallel test runs from racing on the same names.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
        let _env = ENV_LOCK.lock().unwrap();
        // The TMDB key follows the same pattern as every other
        // overridable field: TOML has a `tmdb_api_key = ""` slot
        // (defaulting to empty), and the `MEDIA_PIPELINE_TMDB_API_KEY`
        // env var overrides it. `has_tmdb_credentials` checks the
        // resolved value, not the env var directly.
        let var = env::TMDB_API_KEY;
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
tmdb_api_key = ""

[categories.movies]
remote_dir = "movies"
library_folder = "Movies"
"#;
        let temp = create_temp_config(toml);

        // Env var unset -> TOML value (empty) wins -> no credentials.
        std::env::remove_var(var);
        let mut config = Config::load(temp.path()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.metadata.tmdb_api_key, "");
        assert!(!config.metadata.has_tmdb_credentials());

        // Env var set -> override wins -> credentials present.
        std::env::set_var(var, "fake-key");
        let mut config = Config::load(temp.path()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.metadata.tmdb_api_key, "fake-key");
        assert!(config.metadata.has_tmdb_credentials());

        // Empty env var -> TOML value stands (empty-string guard).
        std::env::set_var(var, "");
        let mut config = Config::load(temp.path()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.metadata.tmdb_api_key, "");
        assert!(!config.metadata.has_tmdb_credentials());

        std::env::remove_var(var);
    }

    #[test]
    fn test_metadata_config_tmdb_key_toml_value() {
        let _env = ENV_LOCK.lock().unwrap();
        // Native deployment use case: a single TOML file is the
        // entire config surface, with the TMDB key in the file.
        // No env var is set, but `has_tmdb_credentials` returns true
        // because the TOML value is non-empty.
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
tmdb_api_key = "baked-in-key"

[categories.movies]
remote_dir = "movies"
library_folder = "Movies"
"#;
        let temp = create_temp_config(toml);
        std::env::remove_var(env::TMDB_API_KEY);
        let mut config = Config::load(temp.path()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.metadata.tmdb_api_key, "baked-in-key");
        assert!(config.metadata.has_tmdb_credentials());
    }

    #[test]
    fn test_metadata_config_tmdb_env_overrides_toml() {
        let _env = ENV_LOCK.lock().unwrap();
        // The env var wins over a non-empty TOML value, matching
        // the precedence for every other overridable field.
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
tmdb_api_key = "toml-key"

[categories.movies]
remote_dir = "movies"
library_folder = "Movies"
"#;
        let temp = create_temp_config(toml);
        std::env::set_var(env::TMDB_API_KEY, "env-key");
        let mut config = Config::load(temp.path()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.metadata.tmdb_api_key, "env-key");
        std::env::remove_var(env::TMDB_API_KEY);
    }

    #[test]
    fn test_paths_env_override_staging() {
        let _env = ENV_LOCK.lock().unwrap();
        // The stable env var `MEDIA_PIPELINE_STAGING` wins over the
        // literal `paths.staging` from the config file.
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

        std::env::remove_var(env::STAGING);
        let mut config = Config::load(temp.path()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.paths.staging, Path::new("/staging"));

        std::env::set_var(env::STAGING, "/var/lib/pipeline/staging");
        let mut config = Config::load(temp.path()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.paths.staging, Path::new("/var/lib/pipeline/staging"));
        std::env::remove_var(env::STAGING);
    }

    #[test]
    fn test_paths_env_override_library() {
        let _env = ENV_LOCK.lock().unwrap();
        // Same pattern for the library path.
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

        std::env::remove_var(env::LIBRARY);
        let mut config = Config::load(temp.path()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.paths.library, Path::new("/library"));

        std::env::set_var(env::LIBRARY, "/mnt/mediaserver");
        let mut config = Config::load(temp.path()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.paths.library, Path::new("/mnt/mediaserver"));
        std::env::remove_var(env::LIBRARY);
    }

    #[test]
    fn test_env_override_empty_string_falls_back() {
        let _env = ENV_LOCK.lock().unwrap();
        // An env var that's set to the empty string should NOT
        // override - empty paths are a deployment mistake (un-set env
        // var in compose, a shell that exports a blank value, etc.)
        // and the config-file value is a safer fallback.
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
        std::env::set_var(env::STAGING, "");
        std::env::set_var(env::LIBRARY, "");
        std::env::set_var(env::SSH_HOST, "");
        let mut config = Config::load(temp.path()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.paths.staging, Path::new("/staging"));
        assert_eq!(config.paths.library, Path::new("/library"));
        assert_eq!(config.ssh.host, "downloads.example.com");
        std::env::remove_var(env::STAGING);
        std::env::remove_var(env::LIBRARY);
        std::env::remove_var(env::SSH_HOST);
    }

    #[test]
    fn test_ssh_env_overrides() {
        let _env = ENV_LOCK.lock().unwrap();
        // All four SSH fields have env-var overrides.
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

        std::env::set_var(env::SSH_HOST, "seedbox.example.net");
        std::env::set_var(env::SSH_USER, "altuser");
        std::env::set_var(env::SSH_KEY, "/home/pipeline/.ssh/id_ed25519");
        std::env::set_var(env::SSH_REMOTE_BASE, "/data/incoming");
        let mut config = Config::load(temp.path()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.ssh.host, "seedbox.example.net");
        assert_eq!(config.ssh.user, "altuser");
        assert_eq!(config.ssh.private_key_path, Path::new("/home/pipeline/.ssh/id_ed25519"));
        assert_eq!(config.ssh.remote_base_path, Path::new("/data/incoming"));
        std::env::remove_var(env::SSH_HOST);
        std::env::remove_var(env::SSH_USER);
        std::env::remove_var(env::SSH_KEY);
        std::env::remove_var(env::SSH_REMOTE_BASE);
    }
}
