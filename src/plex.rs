use anyhow::Context;
use tracing::{debug, info, warn};

use crate::config::Config;

/// Resolve Plex token from the MEDIA_PIPELINE_PLEX_TOKEN env var.
fn resolve_token() -> anyhow::Result<String> {
    let token = std::env::var(crate::config::env::PLEX_TOKEN)
        .context("MEDIA_PIPELINE_PLEX_TOKEN not set")?;
    Ok(token.trim().to_string())
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
    use injectorpp::interface::injector::*;
    use std::cell::RefCell;
    use std::env::VarError;

    thread_local! {
        /// Fake env state: the (key, value) pair that `std::env::var` should
        /// return; any other key returns `NotPresent`. This lives in a
        /// RefCell because the value contains a `String` (not `Copy`) and
        /// the closure we hand to injectorpp must be a non-capturing fn
        /// pointer, so we share state across that boundary via the
        /// thread-local instead of borrowing from the test scope.
        static FAKE_ENV: RefCell<Option<(&'static str, String)>> = const { RefCell::new(None) };
    }

    /// Non-capturing fn pointer that reads the thread-local fake state.
    /// Matches the signature of the monomorphized `std::env::var::<&str>`.
    fn fake_env_var(key: &str) -> Result<String, VarError> {
        FAKE_ENV.with(|cell| match &*cell.borrow() {
            Some((k, v)) if *k == key => Ok(v.clone()),
            _ => Err(VarError::NotPresent),
        })
    }

    /// Install a fake `std::env::var` for the duration of the test. The
    /// returned binding must be held until the test finishes; dropping it
    /// uninstalls the hook.
    fn hook_env_var(key: &'static str, value: String) -> InjectorPP {
        FAKE_ENV.with(|cell| *cell.borrow_mut() = Some((key, value)));
        let mut injector = InjectorPP::new();
        injector
            .when_called(injectorpp::func!(fn(std::env::var)(&'static str) -> Result<String, VarError>))
            .will_execute_raw(injectorpp::func!(fn(fake_env_var)(&'static str) -> Result<String, VarError>));
        injector
    }

    /// Drop guard that clears the fake env state when the test ends, so
    /// one test's state can't leak into the next.
    struct ClearGuard;
    impl Drop for ClearGuard {
        fn drop(&mut self) {
            FAKE_ENV.with(|cell| *cell.borrow_mut() = None);
        }
    }

    #[test]
    fn test_resolve_token_missing() {
        // Env var absent -> error.
        let _injector = hook_env_var("UNUSED_KEY", "unused".to_string());
        let _clear = ClearGuard;

        let result = resolve_token();
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_token_from_env() {
        // MEDIA_PIPELINE_PLEX_TOKEN set -> token comes from env.
        let _injector = hook_env_var("MEDIA_PIPELINE_PLEX_TOKEN", "my-secret-token\n".to_string());
        let _clear = ClearGuard;

        let token = resolve_token().unwrap();
        assert_eq!(token, "my-secret-token");
    }

    #[test]
    fn test_resolve_token_trims_whitespace() {
        // Newlines / spaces around the value are stripped.
        let _injector = hook_env_var("MEDIA_PIPELINE_PLEX_TOKEN", "  my-token  \n".to_string());
        let _clear = ClearGuard;

        let token = resolve_token().unwrap();
        assert_eq!(token, "my-token");
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
