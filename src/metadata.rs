//! Metadata lookup for canonical title naming.
//!
//! The pipeline does a *best-effort* lookup of a release's canonical
//! title (e.g. "Shoresy (2022)") against free metadata APIs. The
//! lookup is cached, never blocks the pipeline, and never fails the
//! rename step: a transport-level failure is logged and the pipeline
//! falls back to the locally-parsed title from the filename.
//!
//! When no API credentials are configured, the [`NoopLookup`] impl is
//! selected and the metadata-lookup step is a no-op. The rest of the
//! pipeline runs identically — directory/file renaming and codec
//! swap use the locally-parsed title.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::db::Database;
use crate::rename::ReleaseMetadata;

/// Canonical-title information returned by a successful metadata
/// lookup. Fields beyond `title` are advisory — the rename path
/// currently uses only `title` and `year`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalTitle {
    /// Canonical display title, e.g. "Shoresy" or "Serial Experiments Lain".
    pub title: String,
    /// Release year if known.
    pub year: Option<u32>,
    /// External ID from the upstream API (e.g. TMDB ID, MusicBrainz
    /// release-group UUID). Useful for downstream tools that want to
    /// look up the same release across services.
    pub external_id: String,
    /// For TV: number of seasons the upstream API knows about.
    pub season_count: Option<u32>,
}

/// Trait for canonical-title lookups. Implementations:
/// - [`NoopLookup`]: always returns `Ok(None)`. The default.
/// - [`MockLookup`]: returns canned data from an in-memory map. Tests.
/// - [`CachedLookup`]: wraps another lookup with an in-memory cache.
/// - [`HttpLookup`]: hits TMDB / MusicBrainz / IGDB. (future work)
#[async_trait]
pub trait MetadataLookup: Send + Sync {
    /// Look up a canonical title for a parsed release.
    ///
    /// Returns:
    /// - `Ok(Some(canonical))` if the API returned a positive match.
    /// - `Ok(None)` if the API returned no match (404 / empty result).
    /// - `Err(_)` only for transport-level failures (timeout, network,
    ///   rate-limit). The caller should treat these as "no match this
    ///   time" and use the local parse.
    async fn lookup(
        &self,
        meta: &ReleaseMetadata,
        category: &str,
    ) -> anyhow::Result<Option<CanonicalTitle>>;
}

/// Lookup that always returns no match. The default when no API
/// credentials are configured. The pipeline continues to work using
/// the locally-parsed title.
pub struct NoopLookup;

#[async_trait]
impl MetadataLookup for NoopLookup {
    async fn lookup(
        &self,
        _meta: &ReleaseMetadata,
        _category: &str,
    ) -> anyhow::Result<Option<CanonicalTitle>> {
        Ok(None)
    }
}

/// Lookup backed by an in-memory `HashMap`. Used in tests to verify
/// that the directory-rename path picks up canonical names correctly
/// without needing a live HTTP server. Production code should use
/// [`NoopLookup`] (no API configured) or `HttpLookup` (API configured).
pub struct MockLookup {
    entries: HashMap<String, CanonicalTitle>,
}

impl MockLookup {
    /// Create a new `MockLookup` from a map of `key -> CanonicalTitle`.
    /// The key is `(category, source_hash)` or whatever the caller's
    /// keying function produces — see `mock_key` for the default shape.
    pub fn new(entries: HashMap<String, CanonicalTitle>) -> Self {
        Self { entries }
    }

    /// Build a mock lookup with no entries; `lookup` always returns
    /// `Ok(None)`.
    pub fn empty() -> Self {
        Self::new(HashMap::new())
    }
}

#[async_trait]
impl MetadataLookup for MockLookup {
    async fn lookup(
        &self,
        meta: &ReleaseMetadata,
        category: &str,
    ) -> anyhow::Result<Option<CanonicalTitle>> {
        // Key on (category, without_group) so the same title hits the
        // same entry across multiple files of a series.
        let key = format!("{}|{}", category, meta.without_group);
        Ok(self.entries.get(&key).cloned())
    }
}

/// In-memory cache wrapping another [`MetadataLookup`], with an
/// optional SQLite-backed layer for durability across process
/// restarts. Caches positive results only (no negative caching — a
/// later re-parse or a different lookup may still succeed). Cache
/// entries expire after `ttl`.
///
/// Lookup order: in-memory → DB → inner lookup. A hit at any layer
/// short-circuits; on a miss in the inner lookup, the result is
/// written to BOTH the in-memory and DB layers. DB errors degrade
/// gracefully: a failure to read the DB falls through to the inner
/// lookup, a failure to write the DB is logged but does not propagate
/// — the metadata step must never block the pipeline.
pub struct CachedLookup<L: MetadataLookup + ?Sized> {
    inner: Arc<L>,
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    ttl: Duration,
    /// Optional durable cache. When `Some`, positive results are
    /// persisted to SQLite and read back on subsequent lookups.
    db: Option<Arc<Database>>,
}

struct CacheEntry {
    title: CanonicalTitle,
    cached_at: DateTime<Utc>,
}

impl<L: MetadataLookup + ?Sized> CachedLookup<L> {
    /// Construct a `CachedLookup` with in-memory caching only.
    pub fn new(inner: Arc<L>, ttl: Duration) -> Self {
        Self {
            inner,
            cache: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            db: None,
        }
    }

    /// Construct a `CachedLookup` that also persists positive results
    /// to the given `Database`. The DB is consulted on every lookup;
    /// a hit in the DB short-circuits the inner lookup. The DB TTL
    /// matches the in-memory TTL.
    pub fn with_db(inner: Arc<L>, ttl: Duration, db: Arc<Database>) -> Self {
        Self {
            inner,
            cache: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            db: Some(db),
        }
    }

    fn cache_key(meta: &ReleaseMetadata, category: &str) -> String {
        format!("{}|{}", category, meta.without_group)
    }

    /// Hash the (category, without_group) key into the SHA-256 used
    /// as the SQLite primary key. The in-memory key keeps the human-
    /// readable form for log readability; the DB key is the hash so
    /// the schema doesn't store unbounded user-derived strings as PKs
    /// and so a future migration to a content-addressable store can
    /// reuse the same identifier.
    fn db_key(cache_key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(cache_key.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

#[async_trait]
impl<L: MetadataLookup + ?Sized + Send + Sync> MetadataLookup for CachedLookup<L> {
    async fn lookup(
        &self,
        meta: &ReleaseMetadata,
        category: &str,
    ) -> anyhow::Result<Option<CanonicalTitle>> {
        let key = Self::cache_key(meta, category);
        let now = Utc::now();

        // 1. In-memory cache.
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(&key) {
                let age = now.signed_duration_since(entry.cached_at);
                if age.to_std().unwrap_or(Duration::MAX) < self.ttl {
                    return Ok(Some(entry.title.clone()));
                }
            }
        }

        // 2. Durable (DB) cache. Failures here are logged and treated
        //    as a miss — we don't want a corrupt/locked DB to block
        //    the pipeline.
        if let Some(db) = &self.db {
            let db_key = Self::db_key(&key);
            match db.get_cached_metadata(&db_key) {
                Ok(Some(title)) => {
                    // Repopulate the in-memory layer so subsequent
                    // calls in this process don't re-hit SQLite.
                    let mut cache = self.cache.lock().await;
                    cache.insert(
                        key,
                        CacheEntry {
                            title: title.clone(),
                            cached_at: now,
                        },
                    );
                    return Ok(Some(title));
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(error = %e, "metadata cache read failed, falling through");
                }
            }
        }

        // 3. Inner lookup.
        let result = self.inner.lookup(meta, category).await?;

        // 4. Persist positive results to in-memory and DB.
        if let Some(ref title) = result {
            {
                let mut cache = self.cache.lock().await;
                cache.insert(
                    key.clone(),
                    CacheEntry {
                        title: title.clone(),
                        cached_at: now,
                    },
                );
            }
            if let Some(db) = &self.db {
                let db_key = Self::db_key(&key);
                if let Err(e) = db.store_cached_metadata(&db_key, category, title, self.ttl) {
                    warn!(error = %e, "metadata cache write failed");
                }
            }
        }

        Ok(result)
    }
}

/// HTTP-based lookup against free metadata APIs. The actual API
/// plumbing is future work — this stub returns `Ok(None)` so the
/// pipeline stays runnable in dev. The wiring is in place
/// (constructor takes a `reqwest::Client` and per-category dispatch
/// function pointers) so the implementations can drop in without
/// changing the trait surface.
pub struct HttpLookup {
    /// Per-category HTTP fetch function. Each takes
    /// `(client, api_key, title, year)` and returns the canonical
    /// match or `None`. The `Err(_)` arm is reserved for transport
    /// failures (timeout, network).
    _client: reqwest::Client,
    _timeout: Duration,
}

impl HttpLookup {
    /// Construct a new `HttpLookup` with the given timeout. The HTTP
    /// client is initialized but not yet used — the per-category
    /// fetch functions are wired in a follow-up.
    pub fn new(timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            _client: client,
            _timeout: timeout,
        }
    }
}

#[async_trait]
impl MetadataLookup for HttpLookup {
    async fn lookup(
        &self,
        _meta: &ReleaseMetadata,
        _category: &str,
    ) -> anyhow::Result<Option<CanonicalTitle>> {
        // Stub: real implementation in a follow-up. Returning Ok(None)
        // here means the pipeline falls back to the locally-parsed
        // title, which is the documented graceful-degradation behavior.
        Ok(None)
    }
}

// ----------------------------------------------------------------------
// Title cleaning for directory layout
//
// The pipeline places processed directories at
// `library/<CategoryFolder>/<Title>/`. The title here is a
// human-readable name with all the release noise stripped — no
// resolution tags, no codec tags, no season markers, no release
// group. When a `CanonicalTitle` is available (from the metadata
// lookup), it wins; otherwise we derive the title from the
// locally-parsed `ReleaseMetadata`.
//
// The cleaner is deliberately forgiving: a few leftover tokens in
// the title (Plex's own matcher can usually still resolve them) are
// better than dropping the title entirely. The opposite failure —
// returning an empty string — is guarded against with a fallback
// to the original basename.
// ----------------------------------------------------------------------

/// Build a directory-name-safe title from a parsed release and an
/// optional canonical title. Always returns a non-empty, non-path-
/// component string. Whichever input is richer is preferred:
/// `canonical` (TMDB-cleaned) > local parse of `meta.without_group`.
pub fn clean_title_for_directory(
    meta: &ReleaseMetadata,
    canonical: Option<&CanonicalTitle>,
) -> String {
    // 1. If we have a canonical title from the metadata lookup, that
    //    is the cleanest possible input. Use it directly (still
    //    sanitize, since the API could in principle return garbage).
    if let Some(c) = canonical {
        let s = sanitize_for_directory(&c.title);
        if !s.is_empty() {
            return s;
        }
    }

    // 2. Fall back to the locally-parsed stem. Split on the common
    //    release-name separators and strip release-noise tokens.
    let cleaned = clean_local_title(&meta.without_group);
    if !cleaned.is_empty() {
        return cleaned;
    }

    // 3. Pathological input — every token got stripped. Fall back to
    //    the raw basename. The directory may carry release noise, but
    //    at least it's a non-empty identifier and Plex can usually
    //    still parse it.
    let raw = meta
        .raw
        .rsplit_once('.')
        .map(|(stem, _ext)| stem)
        .unwrap_or(&meta.raw);
    let s = sanitize_for_directory(raw);
    if s.is_empty() {
        // Truly empty input. Use a literal placeholder so the move
        // step still produces a valid path.
        "untitled".to_string()
    } else {
        s
    }
}

/// Strip release-noise tokens from a locally-parsed stem and return
/// the first 1-3 surviving tokens joined with spaces. Empty string if
/// nothing survives the filter.
fn clean_local_title(stem: &str) -> String {
    // Replace the common release-name separators with spaces so we
    // can split on a single delimiter.
    let normalized = stem.replace(['.', '_', '-'], " ");
    let tokens: Vec<&str> = normalized.split_whitespace().collect();

    let mut kept: Vec<&str> = Vec::new();
    for tok in tokens {
        if kept.len() >= 3 {
            break;
        }
        if is_release_noise_token(tok) {
            continue;
        }
        kept.push(tok);
    }

    let joined = kept.join(" ");
    sanitize_for_directory(&joined)
}

/// True if a token is release-metadata noise rather than title text:
/// season markers, resolution tags, source/codec tags, pure track
/// numbers, or pure 4-digit years (which we don't want to use in the
/// local-parse path — the year comes from TMDB on collision only).
fn is_release_noise_token(tok: &str) -> bool {
    let lower = tok.to_ascii_lowercase();
    let stripped = lower.replace(['.', '-', '_'], "");

    // Season / episode markers: S05, S05E03, S00E01v2.
    if looks_like_season_episode(&lower) {
        return true;
    }
    // Pure track numbers: 01, 02, 1, 23, 123.
    if !stripped.is_empty() && stripped.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    // Resolution: 1080p, 720p, 2160p, 4k, 8k, uhd, hd, sd.
    if matches!(
        stripped.as_str(),
        "1080p" | "720p" | "2160p" | "4320p" | "4k" | "8k" | "uhd" | "hd" | "sd"
    ) {
        return true;
    }
    // Source: bluray, brrip, bdrip, webdl, webrip, hdtv, pdtv, dvdrip, etc.
    if matches!(
        stripped.as_str(),
        "bluray"
            | "brrip"
            | "bdrip"
            | "webdl"
            | "webrip"
            | "hdtv"
            | "pdtv"
            | "dvdrip"
            | "dvdscr"
            | "dvd"
            | "remux"
            | "dsnp"
            | "atvp"
            | "pmtp"
            | "web"
            | "dvb"
            | "dsr"
            | "sdtv"
            | "ppv"
            | "cam"
            | "ts"
            | "tc"
    ) {
        return true;
    }
    // Codecs (video + audio).
    if matches!(
        stripped.as_str(),
        "x264" | "x265"
            | "h264" | "h265"
            | "hevc" | "avc" | "av1" | "vp9"
            | "flac" | "mp3" | "aac" | "dts" | "dtshd" | "dtshdma"
            | "truehd" | "atmos" | "opus" | "vorbis"
            | "ac3" | "eac3" | "lpcm" | "pcm"
            | "10bit" | "60fps" | "hdr" | "hdr10" | "hdr10plus" | "dovi" | "dv"
            | "dd" | "dd51" | "dd71" | "lossless" | "320" | "v0" | "v2"
    ) {
        return true;
    }
    // Quality flags and release types that sometimes leak into the stem.
    if matches!(
        stripped.as_str(),
        "proper" | "repack" | "remastered" | "regraded"
            | "hybrid" | "imax" | "extended" | "theatrical"
            | "criterion" | "complete" | "internal" | "limited"
            | "dc" | "dual" | "audio" | "multi"
    ) {
        return true;
    }
    false
}

/// Match `s05`, `s05e03`, `s00e01v2` (case-insensitive). Words like
/// "shoresy" don't match because they don't start with `s` followed
/// by a digit.
fn looks_like_season_episode(lower: &str) -> bool {
    let bytes = lower.as_bytes();
    if bytes.len() < 2 {
        return false;
    }
    // First char must be 's' or 'S'.
    if bytes[0] != b's' {
        return false;
    }
    // Second char must be a digit.
    if !bytes[1].is_ascii_digit() {
        return false;
    }
    // Pattern: s + 1-2 digits + optional (e + 1-3 digits) + optional (v + digits)
    let rest = &lower[1..];
    let mut chars = rest.chars().peekable();
    let mut season_digits = 0;
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() && season_digits < 2 {
            chars.next();
            season_digits += 1;
        } else {
            break;
        }
    }
    if season_digits == 0 {
        return false;
    }
    if chars.peek() == Some(&'e') {
        chars.next();
        let mut ep_digits = 0;
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() && ep_digits < 3 {
                chars.next();
                ep_digits += 1;
            } else {
                break;
            }
        }
        if ep_digits == 0 {
            return false;
        }
    }
    if chars.peek() == Some(&'v') {
        chars.next();
        let mut v_digits = 0;
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                chars.next();
                v_digits += 1;
            } else {
                break;
            }
        }
        if v_digits == 0 {
            return false;
        }
    }
    // Must have consumed the whole token — otherwise something like
    // "show" or "shoresy" might match by accident. Use a stricter
    // check: did we consume at least the season marker and nothing
    // remains?
    chars.next().is_none()
}

/// Make a string safe to use as a single path component: replace
/// path separators and control characters with `_`, collapse runs
/// of whitespace, trim. Always returns a non-empty string (caller
/// is responsible for choosing a fallback if the result is empty).
fn sanitize_for_directory(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_was_space = false;
    for c in s.chars() {
        if c == '/' || c == '\\' || c == '\0' || c.is_control() {
            out.push('_');
            last_was_space = false;
        } else if c.is_whitespace() {
            if !last_was_space && !out.is_empty() {
                out.push(' ');
                last_was_space = true;
            }
        } else {
            out.push(c);
            last_was_space = false;
        }
    }
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn make_meta(stem: &str) -> ReleaseMetadata {
        ReleaseMetadata {
            raw: format!("{}.mkv", stem),
            group: None,
            ext: "mkv".to_string(),
            without_group: stem.to_string(),
            season_episode: None,
            resolution: None,
            source: None,
            codec: None,
            track: None,
        }
    }

    #[tokio::test]
    async fn test_noop_lookup_always_returns_none() {
        let lookup = NoopLookup;
        let meta = make_meta("Movie.2024");
        let result = lookup.lookup(&meta, "movies").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_mock_lookup_returns_canonical_for_known_title() {
        let mut entries = HashMap::new();
        entries.insert(
            "movies|Shoresy".to_string(),
            CanonicalTitle {
                title: "Shoresy".to_string(),
                year: Some(2022),
                external_id: "tt14058038".to_string(),
                season_count: None,
            },
        );
        let lookup = MockLookup::new(entries);
        let meta = make_meta("Shoresy");
        let result = lookup.lookup(&meta, "movies").await.unwrap();
        assert!(result.is_some());
        let title = result.unwrap();
        assert_eq!(title.title, "Shoresy");
        assert_eq!(title.year, Some(2022));
    }

    #[tokio::test]
    async fn test_mock_lookup_returns_none_for_unknown_title() {
        let lookup = MockLookup::empty();
        let meta = make_meta("Unknown");
        let result = lookup.lookup(&meta, "movies").await.unwrap();
        assert!(result.is_none());
    }

    /// A counting mock: each call to `lookup` increments a counter.
    /// Used to verify the cache only calls the inner lookup once.
    struct CountingLookup {
        counter: Arc<AtomicUsize>,
        response: Option<CanonicalTitle>,
    }

    #[async_trait]
    impl MetadataLookup for CountingLookup {
        async fn lookup(
            &self,
            _meta: &ReleaseMetadata,
            _category: &str,
        ) -> anyhow::Result<Option<CanonicalTitle>> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(self.response.clone())
        }
    }

    #[tokio::test]
    async fn test_cache_returns_cached_entry_without_calling_lookup() {
        let counter = Arc::new(AtomicUsize::new(0));
        let response = Some(CanonicalTitle {
            title: "Test".to_string(),
            year: Some(2024),
            external_id: "x".to_string(),
            season_count: None,
        });
        let inner = Arc::new(CountingLookup {
            counter: counter.clone(),
            response,
        });
        let cached = CachedLookup::new(inner.clone(), Duration::from_secs(60));
        let meta = make_meta("Test");

        // First call: counter goes from 0 to 1, result cached.
        let r1 = cached.lookup(&meta, "movies").await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert!(r1.is_some());

        // Second call: counter stays at 1 (cache hit).
        let r2 = cached.lookup(&meta, "movies").await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert!(r2.is_some());
    }

    #[tokio::test]
    async fn test_cache_does_not_store_negative_results() {
        let counter = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(CountingLookup {
            counter: counter.clone(),
            response: None,
        });
        let cached = CachedLookup::new(inner.clone(), Duration::from_secs(60));
        let meta = make_meta("Unknown");

        // First call: no result, not cached.
        let r1 = cached.lookup(&meta, "movies").await.unwrap();
        assert!(r1.is_none());
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Second call: counter goes to 2 (negative not cached).
        let r2 = cached.lookup(&meta, "movies").await.unwrap();
        assert!(r2.is_none());
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_cache_ttl_expires_old_entries() {
        let counter = Arc::new(AtomicUsize::new(0));
        let response = Some(CanonicalTitle {
            title: "Test".to_string(),
            year: Some(2024),
            external_id: "x".to_string(),
            season_count: None,
        });
        let inner = Arc::new(CountingLookup {
            counter: counter.clone(),
            response,
        });
        // TTL of 0 means everything is expired.
        let cached = CachedLookup::new(inner.clone(), Duration::from_millis(0));
        let meta = make_meta("Test");

        cached.lookup(&meta, "movies").await.unwrap();
        // Sleep just enough that the entry is past TTL.
        tokio::time::sleep(Duration::from_millis(2)).await;
        cached.lookup(&meta, "movies").await.unwrap();

        // Both calls hit the inner lookup because the TTL is effectively zero.
        assert!(counter.load(Ordering::SeqCst) >= 2);
    }

    // ----------------------------------------------------------------------
    // DB-backed (durable) cache tests
    //
    // These tests exercise the SQLite-backed layer added on top of
    // the in-memory `CachedLookup`. The DB layer is what survives a
    // process restart — the in-memory layer is just memoization for
    // a single invocation.
    // ----------------------------------------------------------------------

    use crate::db::Database;
    use std::path::Path;

    fn in_memory_db() -> Database {
        Database::open(Path::new(":memory:")).unwrap()
    }

    #[tokio::test]
    async fn test_db_cache_writes_positive_result() {
        let db = in_memory_db();
        let counter = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(CountingLookup {
            counter: counter.clone(),
            response: Some(CanonicalTitle {
                title: "Shoresy".to_string(),
                year: Some(2022),
                external_id: "tt14058038".to_string(),
                season_count: None,
            }),
        });
        let cached = CachedLookup::with_db(inner.clone(), Duration::from_secs(60), Arc::new(db.clone()));
        let meta = make_meta("Shoresy");

        let r1 = cached.lookup(&meta, "movies").await.unwrap();
        assert!(r1.is_some());
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // The DB now has one row.
        assert_eq!(db.count_metadata_cache_rows().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_db_cache_does_not_write_negative_result() {
        let db = in_memory_db();
        let counter = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(CountingLookup {
            counter: counter.clone(),
            response: None,
        });
        let cached = CachedLookup::with_db(inner.clone(), Duration::from_secs(60), Arc::new(db.clone()));
        let meta = make_meta("Unknown");

        let r1 = cached.lookup(&meta, "movies").await.unwrap();
        assert!(r1.is_none());
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Negative results are NOT persisted: a later re-parse or a
        // different parse may still succeed.
        assert_eq!(
            db.count_metadata_cache_rows().unwrap(),
            0,
            "negative results must not be cached in the DB"
        );
    }

    #[tokio::test]
    async fn test_db_cache_survives_new_cached_instance() {
        // The whole point of the DB layer: a second CachedLookup
        // built against the same DB should hit the persisted row
        // without calling its inner lookup.
        let db = in_memory_db();

        // First instance: writes to DB.
        let counter_1 = Arc::new(AtomicUsize::new(0));
        let inner_1 = Arc::new(CountingLookup {
            counter: counter_1.clone(),
            response: Some(CanonicalTitle {
                title: "Lain".to_string(),
                year: Some(1998),
                external_id: "tt0119831".to_string(),
                season_count: None,
            }),
        });
        let cached_1 = CachedLookup::with_db(
            inner_1.clone(),
            Duration::from_secs(60),
            Arc::new(db.clone()),
        );
        let meta = make_meta("Lain");
        let r1 = cached_1.lookup(&meta, "tvshows").await.unwrap();
        assert!(r1.is_some());
        assert_eq!(counter_1.load(Ordering::SeqCst), 1);
        assert_eq!(db.count_metadata_cache_rows().unwrap(), 1);

        // Second instance: a fresh in-memory cache, same DB. Its
        // inner counter starts at 0 — if the DB layer is doing its
        // job, the inner lookup is never called.
        let counter_2 = Arc::new(AtomicUsize::new(0));
        let inner_2 = Arc::new(CountingLookup {
            counter: counter_2.clone(),
            response: Some(CanonicalTitle {
                title: "WRONG".to_string(), // would override if called
                year: Some(1999),
                external_id: "tt-wrong".to_string(),
                season_count: None,
            }),
        });
        let cached_2 = CachedLookup::with_db(
            inner_2.clone(),
            Duration::from_secs(60),
            Arc::new(db.clone()),
        );
        let r2 = cached_2.lookup(&meta, "tvshows").await.unwrap();
        assert_eq!(counter_2.load(Ordering::SeqCst), 0, "DB hit should not call inner");
        let title = r2.expect("expected DB hit");
        assert_eq!(title.title, "Lain", "DB hit should return the original value, not WRONG");
        assert_eq!(title.year, Some(1998));
    }

    #[tokio::test]
    async fn test_db_cache_keys_separated_by_category() {
        // The same `without_group` in different categories should not
        // collide — they're separate canonical-title records.
        let db = in_memory_db();
        let inner = Arc::new(CountingLookup {
            counter: Arc::new(AtomicUsize::new(0)),
            response: Some(CanonicalTitle {
                title: "X".to_string(),
                year: Some(2020),
                external_id: "tt-x".to_string(),
                season_count: None,
            }),
        });
        let cached = CachedLookup::with_db(inner.clone(), Duration::from_secs(60), Arc::new(db.clone()));
        let meta = make_meta("X");

        cached.lookup(&meta, "movies").await.unwrap();
        cached.lookup(&meta, "tvshows").await.unwrap();

        // Two distinct rows, one per category.
        assert_eq!(db.count_metadata_cache_rows().unwrap(), 2);
    }

    #[tokio::test]
    async fn test_db_cache_expired_entry_treated_as_miss() {
        // Store an entry with a 1-second TTL, wait long enough that
        // it expires, then verify a second lookup misses the DB and
        // calls the inner lookup again.
        let db = in_memory_db();
        let counter = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(CountingLookup {
            counter: counter.clone(),
            response: Some(CanonicalTitle {
                title: "ExpireTest".to_string(),
                year: Some(2024),
                external_id: "tt-expire".to_string(),
                season_count: None,
            }),
        });
        let cached = CachedLookup::with_db(inner.clone(), Duration::from_secs(1), Arc::new(db.clone()));
        let meta = make_meta("ExpireTest");

        cached.lookup(&meta, "movies").await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Override the response so a re-fetch would change the value
        // — this lets us distinguish "DB miss → re-fetch" from
        // "DB hit → returns old value".
        let counter2 = Arc::new(AtomicUsize::new(0));
        let inner2 = Arc::new(CountingLookup {
            counter: counter2.clone(),
            response: Some(CanonicalTitle {
                title: "Updated".to_string(),
                year: Some(2025),
                external_id: "tt-updated".to_string(),
                season_count: None,
            }),
        });
        // Wait past the TTL.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let cached2 = CachedLookup::with_db(inner2.clone(), Duration::from_secs(60), Arc::new(db.clone()));
        let r = cached2.lookup(&meta, "movies").await.unwrap();
        assert_eq!(counter2.load(Ordering::SeqCst), 1, "expired entry should not be returned");
        let title = r.expect("expected re-fetched result");
        assert_eq!(title.title, "Updated");
    }

    #[test]
    fn test_db_key_is_stable_hash() {
        // The same key always hashes to the same value.
        let k1 = CachedLookup::<NoopLookup>::db_key("movies|Shoresy");
        let k2 = CachedLookup::<NoopLookup>::db_key("movies|Shoresy");
        assert_eq!(k1, k2);
        // Different inputs hash to different values.
        let k3 = CachedLookup::<NoopLookup>::db_key("tvshows|Shoresy");
        assert_ne!(k1, k3);
        // SHA-256 hex is 64 chars.
        assert_eq!(k1.len(), 64);
    }

    // ----------------------------------------------------------------------
    // clean_title_for_directory tests
    //
    // The cleaner is what produces the `library/<Category>/<Title>/`
    // basename. The contract is:
    //   1. Canonical title (TMDB) wins when present.
    //   2. Otherwise, the first 1-3 surviving tokens of the local parse
    //      are the title.
    //   3. Release-noise tokens (season, resolution, source, codec, track
    //      numbers) are stripped.
    //   4. Path-unsafe characters are replaced.
    //   5. The function never returns an empty string.
    // ----------------------------------------------------------------------

    /// Build a ReleaseMetadata whose `without_group` is the given
    /// string. Mirrors the production parse for a stem like
    /// `Foo.1080p.x264` (i.e. the group is already stripped).
    fn make_meta_with_stem(stem: &str) -> ReleaseMetadata {
        let raw = format!("{}.mkv", stem);
        let group = crate::rename::parse_release_metadata(&raw).group;
        let without_group = stem.to_string();
        let season_episode = crate::rename::parse_release_metadata(&raw).season_episode;
        let resolution = crate::rename::parse_release_metadata(&raw).resolution;
        let source = crate::rename::parse_release_metadata(&raw).source;
        let codec = crate::rename::parse_release_metadata(&raw).codec;
        ReleaseMetadata {
            raw,
            group,
            ext: "mkv".to_string(),
            without_group,
            season_episode,
            resolution,
            source,
            codec,
            track: None,
        }
    }

    #[test]
    fn test_clean_title_canonical_wins() {
        // The local parse yields a noisy stem; the canonical title
        // from TMDB is the cleaner "The Matrix" — the canonical wins.
        let meta = make_meta_with_stem("The.Matrix.1999.1080p.BluRay.x264");
        let canonical = CanonicalTitle {
            title: "The Matrix".to_string(),
            year: Some(1999),
            external_id: "tt0133093".to_string(),
            season_count: None,
        };
        assert_eq!(
            clean_title_for_directory(&meta, Some(&canonical)),
            "The Matrix"
        );
    }

    #[test]
    fn test_clean_title_local_parse_tv() {
        // TV release: strip season, resolution, codec, group remnants.
        let meta = make_meta_with_stem("Shoresy.S05E03.1080p.HEVC.x265");
        assert_eq!(clean_title_for_directory(&meta, None), "Shoresy");
    }

    #[test]
    fn test_clean_title_local_parse_movie() {
        // Movie release with a year in the filename. The parser
        // treats a 4-digit numeric token after a `.` as a candidate
        // group; if it's not in the denylist, the year is stripped
        // from `without_group`. So the cleaner sees a stem without
        // the year and produces just "The Matrix". (Year-on-collision
        // is the TMDB-driven path; the local-parse path does not
        // synthesize a year from the filename.)
        let meta = make_meta_with_stem("The.Matrix.1999.1080p.BluRay.x264");
        assert_eq!(clean_title_for_directory(&meta, None), "The Matrix");
    }

    #[test]
    fn test_clean_title_strips_season_and_resolution() {
        // Multiple noise tokens in different positions.
        let meta = make_meta_with_stem("Foo.S05E03.1080p.x265");
        assert_eq!(clean_title_for_directory(&meta, None), "Foo");
    }

    #[test]
    fn test_clean_title_handles_empty_remaining_tokens() {
        // Every token is noise. The cleaner falls back to the raw
        // stem — better an empty-safe identifier than nothing.
        let meta = make_meta_with_stem("1080p.BluRay.x264");
        // The raw stem is "1080p.BluRay.x264" which sanitizes to
        // "1080p BluRay x264". Acceptable: Plex can usually still
        // match, and we never return empty.
        let result = clean_title_for_directory(&meta, None);
        assert!(!result.is_empty(), "cleaner must never return empty");
    }

    #[test]
    fn test_clean_title_sanitizes_path_unsafe_chars() {
        // Canonical title with slashes / backslashes / control chars
        // must be sanitized to `_`. (Defensive: TMDB shouldn't return
        // these, but a noop canonical or a weird API response might.)
        // The sanitizer does NOT touch `:` or `?` (those are valid on
        // Linux/macOS) — only path separators and control chars.
        let meta = make_meta_with_stem("foo");
        let canonical = CanonicalTitle {
            title: "Weird/Title\\With:bad?chars\there".to_string(),
            year: None,
            external_id: "x".to_string(),
            season_count: None,
        };
        let result = clean_title_for_directory(&meta, Some(&canonical));
        assert!(!result.contains('/'), "got: {}", result);
        assert!(!result.contains('\\'), "got: {}", result);
        assert!(!result.contains('\t'), "got: {}", result);
    }

    #[test]
    fn test_clean_title_tracks_dont_count_as_title() {
        // A leading track number is not part of the title. After
        // stripping, the song title "Heroes" survives.
        let meta = make_meta_with_stem("01 - Heroes");
        // Note: the parser's track field captures `01` separately,
        // but the cleaner only sees `without_group` and strips the
        // token via the pure-number check.
        assert_eq!(clean_title_for_directory(&meta, None), "Heroes");
    }

    #[test]
    fn test_clean_title_caps_at_three_tokens() {
        // Long titles: take the first 3 surviving tokens. Plex's
        // own matcher fills in the rest from metadata.
        let meta = make_meta_with_stem("The.Long.Walk.To.Freedom.2024.1080p");
        let result = clean_title_for_directory(&meta, None);
        assert_eq!(result, "The Long Walk");
    }

    #[test]
    fn test_clean_title_preserves_alphanumeric_inside_words() {
        // A token that contains a digit but isn't a pure number (and
        // isn't a season/resolution) should be kept.
        let meta = make_meta_with_stem("Se7en.1995.1080p.BluRay");
        // As with the Matrix case, the parser eats the 1995 year
        // token as a group candidate; the cleaner sees "Se7en" only.
        let result = clean_title_for_directory(&meta, None);
        assert_eq!(result, "Se7en");
    }
}
