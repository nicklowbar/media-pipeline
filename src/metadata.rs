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
}
