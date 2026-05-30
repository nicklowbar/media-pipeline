use anyhow::Context;
use tracing::{debug, info, warn};

use crate::config::Config;

/// Resolve Plex token from environment or file.
fn resolve_token() -> anyhow::Result<String> {
    std::env::var("PLEX_TOKEN")
        .or_else(|_| {
            std::env::var("PLEX_TOKEN_FILE")
                .ok()
                .and_then(|path| std::fs::read_to_string(path).ok())
                .map(|s| s.trim().to_string())
                .ok_or_else(|| anyhow::anyhow!("PLEX_TOKEN not set"))
        })
        .context("Plex token not found in environment or file")
}

/// Build the Plex refresh URL.
fn build_refresh_url(base_url: &str, section_key: i64) -> String {
    format!("{}/library/sections/{}/refresh", base_url.trim_end_matches('/'), section_key)
}

/// Trigger a Plex library section scan.
pub async fn trigger_scan(config: &Config, section_key: i64) -> anyhow::Result<()> {
    let token = resolve_token()?;
    let url = build_refresh_url(&config.plex.url, section_key);

    debug!(url = %url, "sending plex scan request");

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("X-Plex-Token", token)
        .send()
        .await
        .with_context(|| format!("failed to send Plex scan request to {}", url))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Plex scan failed with status {}: {}", status, body);
    }

    info!(section = section_key, "plex scan triggered successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_token_missing() {
        // Ensure env vars are unset
        std::env::remove_var("PLEX_TOKEN");
        std::env::remove_var("PLEX_TOKEN_FILE");
        let result = resolve_token();
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_token_from_env() {
        std::env::set_var("PLEX_TOKEN", "my-secret-token");
        std::env::remove_var("PLEX_TOKEN_FILE");
        let token = resolve_token().unwrap();
        assert_eq!(token, "my-secret-token");
        std::env::remove_var("PLEX_TOKEN");
    }

    #[test]
    fn test_resolve_token_from_file() {
        std::env::remove_var("PLEX_TOKEN");
        let temp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temp.path(), "file-token\n").unwrap();
        std::env::set_var("PLEX_TOKEN_FILE", temp.path().to_str().unwrap());
        let token = resolve_token().unwrap();
        assert_eq!(token, "file-token");
        std::env::remove_var("PLEX_TOKEN_FILE");
    }

    #[test]
    fn test_build_refresh_url() {
        assert_eq!(
            build_refresh_url("http://plex:32400", 1),
            "http://plex:32400/library/sections/1/refresh"
        );
        assert_eq!(
            build_refresh_url("http://plex:32400/", 2),
            "http://plex:32400/library/sections/2/refresh"
        );
    }
}
