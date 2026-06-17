//! Transcode policy detection. With library-side re-encoding owned
//! by Tdarr (see `memory/architecture-pipeline-vs-tdarr.md`), the
//! pipeline no longer auto-detects per-title transcode decisions.
//! The analyze stage is a stub: it records `detected_policy =
//! "none"` and bumps the row to `Analyzed`. Tdarr does its own
//! probe-based re-encoding decision on its own schedule.

use std::path::Path;

use crate::db::Database;

/// Transcode policy determined for a single title (top-level
/// directory). The pipeline no longer runs transcode detection;
/// this enum is reduced to its `None` variant to keep the column /
/// serialization shape stable. A future stage that needs to make
/// per-title transcode decisions would add variants back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedPolicy {
    None,
}

impl DetectedPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            DetectedPolicy::None => "none",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "none" => Some(DetectedPolicy::None),
            _ => None,
        }
    }

    /// Whether the filename should have its codec tag updated
    /// during rename. Always false now — the rename path doesn't
    /// insert codec tags.
    pub fn changes_codec(&self) -> bool {
        false
    }
}

/// Stub analyze step. Records `detected_policy = "none"` for the
/// row and returns the constant. Kept as a free function (not
/// inlined into the worker) so the call shape matches the
/// pre-stub code: a single `analyze_directory(staging_path, db,
/// dir_id)` call per `Synced` row.
pub async fn analyze_directory(
    _staging_path: &Path,
    db: &Database,
    dir_id: i64,
) -> anyhow::Result<DetectedPolicy> {
    db.set_directory_policy(dir_id, DetectedPolicy::None.as_str())?;
    Ok(DetectedPolicy::None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detected_policy_as_str() {
        assert_eq!(DetectedPolicy::None.as_str(), "none");
    }

    #[test]
    fn test_detected_policy_from_str() {
        assert_eq!(DetectedPolicy::from_str("none"), Some(DetectedPolicy::None));
        assert_eq!(DetectedPolicy::from_str("unknown"), None);
    }

    #[test]
    fn test_detected_policy_changes_codec() {
        assert!(!DetectedPolicy::None.changes_codec());
    }
}
