use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use lazy_static::lazy_static;
use regex::Regex;
use tracing::{debug, info, trace, warn};
use walkdir::WalkDir;

use crate::config::Config;
use crate::db::Database;
use crate::policy::DetectedPolicy;

/// Regex to extract the group name from a release filename.
/// Matches the last segment before the extension that follows a hyphen or dot.
/// Examples:
///   Movie.Title.2024.1080p.BluRay.x264-GROUP.mkv -> group = "GROUP"
///   Show.Name.S01E02.1080p.WEB-DL-GROUP2.mkv -> group = "GROUP2"
lazy_static::lazy_static! {
    static ref GROUP_REGEX: Regex = Regex::new(
        r"^(?P<prefix>.*)[\.\-](?P<group>[^\.\-]+)(?P<ext>\.[a-zA-Z0-9]+)$"
    ).unwrap();

    static ref CODEC_REGEX: Regex = Regex::new(
        r"(?i)(?P<codec>x264|h264|H\.264|X264|H264)"
    ).unwrap();
}

/// Rename all files in a staging directory to Plex-friendly names.
/// - Replace release group with the configured group name.
/// - Update codec tag in filename if transcode policy changes codec.
/// - Rename non-video files to match the primary video file.
pub fn rename_directory(
    staging_path: &Path,
    config: &Config,
    _category: &str,
    db: &Database,
    dir_id: i64,
    detected_policy: Option<&DetectedPolicy>,
) -> anyhow::Result<()> {
    let group_name = config.group_name();
    let will_change_codec = detected_policy.map(|p| p.changes_codec()).unwrap_or(false);

    // Walk directory and collect files
    let mut files = Vec::new();
    for entry in WalkDir::new(staging_path).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let relative = entry.path().strip_prefix(staging_path)
            .unwrap_or(entry.path());
        files.push((entry.path().to_path_buf(), relative.to_path_buf()));
    }

    // Identify the primary video file (largest video file in the root directory)
    let primary_video = files.iter().find(|(full, rel)| {
        rel.parent() == Some(Path::new("")) && is_video_file(full)
    });

    let primary_base = primary_video.map(|(full, _)| {
        let new_name = rename_file(full, will_change_codec, group_name)?;
        Ok::<_, anyhow::Error>(PathBuf::from(&new_name).file_stem().unwrap().to_string_lossy().to_string())
    }).transpose()?;

    // Rename each file
    let mut rename_map: HashMap<PathBuf, PathBuf> = HashMap::new();

    for (full_path, relative_path) in &files {
        let parent = full_path.parent().unwrap_or(Path::new(""));
        let original_name = full_path.file_name().unwrap().to_string_lossy();

        let new_name = if is_video_file(full_path) {
            rename_file(full_path, will_change_codec, group_name)?
        } else {
            // Non-video file: if in root directory and we have a primary video, match its base name
            if relative_path.parent() == Some(Path::new("")) {
                if let Some(ref base) = primary_base {
                    rename_non_video_to_match(&original_name, &full_path, base)?
                } else {
                    rename_file(full_path, false, group_name)?
                }
            } else {
                // Subdirectory files: keep as-is or apply group replacement
                rename_file(full_path, false, group_name)?
            }
        };

        let new_path = parent.join(&new_name);
        if full_path != &new_path {
            rename_map.insert(full_path.clone(), new_path);
        }
    }

    // Perform actual renames
    for (old_path, new_path) in &rename_map {
        trace!(old = %old_path.display(), new = %new_path.display(), "renaming file");

        // Ensure parent exists
        if let Some(parent) = new_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::rename(old_path, new_path)
            .with_context(|| format!("failed to rename {} to {}", old_path.display(), new_path.display()))?;

        // Record in DB
        let original = old_path.file_name().unwrap().to_string_lossy();
        let renamed = new_path.file_name().unwrap().to_string_lossy();
        let policy_str = detected_policy.map(|p| p.as_str()).unwrap_or("none");
        db.insert_file(
            dir_id,
            &original,
            policy_str,
            is_video_file(new_path),
        )?;
    }

    // Update file records with renamed names
    for (old_path, new_path) in &rename_map {
        let renamed = new_path.file_name().unwrap().to_string_lossy();
        // Find the file record and update it
        // For now, we'll just track via the insert above
    }

    info!(dir_id, count = rename_map.len(), "directory renamed");
    Ok(())
}

/// Rename a single file.
fn rename_file(path: &Path, update_codec: bool, group_name: &str) -> anyhow::Result<String> {
    let name = path.file_name().unwrap().to_string_lossy();

    // Skip if already has the group name
    if name.contains(group_name) {
        return Ok(name.to_string());
    }

    let mut new_name = name.to_string();

    // Replace group with configured group name
    if let Some(caps) = GROUP_REGEX.captures(&name) {
        let prefix = &caps["prefix"];
        let ext = &caps["ext"];
        new_name = format!("{}-{}{}", prefix, group_name, ext);
    } else {
        // No group found: append group name before extension
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or(&name);
        new_name = if ext.is_empty() {
            format!("{}-{}", stem, group_name)
        } else {
            format!("{}-{}.{}", stem, group_name, ext)
        };
    }

    // Update codec tag if applicable
    if update_codec {
        new_name = CODEC_REGEX.replace_all(&new_name, "x265").to_string();
    }

    Ok(new_name)
}

/// Rename a non-video file to match the primary video file's base name.
fn rename_non_video_to_match(
    original_name: &str,
    full_path: &Path,
    primary_base: &str,
) -> anyhow::Result<String> {
    let ext = full_path.extension().and_then(|e| e.to_str()).unwrap_or("");
    let new_name = if ext.is_empty() {
        primary_base.to_string()
    } else {
        format!("{}.{}", primary_base, ext)
    };
    Ok(new_name)
}

fn is_video_file(path: &Path) -> bool {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let ext_lower = ext.to_lowercase();
        matches!(
            ext_lower.as_str(),
            "mp4" | "mkv" | "avi" | "mov" | "m4v" | "ts" | "m2ts" | "wmv"
        )
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_rename_file_replaces_group_with_custom_name() {
        let path = Path::new("Movie.Title.2024.1080p.BluRay.x264-GROUP.mkv");
        let result = rename_file(path, false, "REPACK").unwrap();
        assert_eq!(result, "Movie.Title.2024.1080p.BluRay.x264-REPACK.mkv");
    }

    #[test]
    fn test_rename_file_with_hyphen_separator() {
        let path = Path::new("Show.Name.S01E02.1080p.WEB-DL-GROUP2.mkv");
        let result = rename_file(path, false, "REPACK").unwrap();
        assert_eq!(result, "Show.Name.S01E02.1080p.WEB-DL-REPACK.mkv");
    }

    #[test]
    fn test_rename_file_updates_codec_tag() {
        let path = Path::new("Movie.Title.2024.1080p.BluRay.x264-GROUP.mkv");
        let result = rename_file(path, true, "REPACK").unwrap();
        assert_eq!(result, "Movie.Title.2024.1080p.BluRay.x265-REPACK.mkv");
    }

    #[test]
    fn test_rename_file_updates_h264_codec_tag() {
        let path = Path::new("Movie.Title.2024.1080p.BluRay.h264-GROUP.mkv");
        let result = rename_file(path, true, "REPACK").unwrap();
        assert_eq!(result, "Movie.Title.2024.1080p.BluRay.x265-REPACK.mkv");
    }

    #[test]
    fn test_rename_file_skips_already_repack() {
        let path = Path::new("Movie.Title.2024.1080p.BluRay.x265-REPACK.mkv");
        let result = rename_file(path, true, "REPACK").unwrap();
        assert_eq!(result, "Movie.Title.2024.1080p.BluRay.x265-REPACK.mkv");
    }

    #[test]
    fn test_rename_file_no_group_appends_repack() {
        let path = Path::new("SomeFile.mkv");
        let result = rename_file(path, false, "REPACK").unwrap();
        assert_eq!(result, "SomeFile-REPACK.mkv");
    }

    #[test]
    fn test_rename_file_no_extension() {
        let path = Path::new("README");
        let result = rename_file(path, false, "REPACK").unwrap();
        assert_eq!(result, "README-REPACK");
    }

    #[test]
    fn test_rename_file_hevc_preserves_codec() {
        let path = Path::new("Some.Movie.2160p.BluRay.HEVC-GROUP.mkv");
        let result = rename_file(path, true, "REPACK").unwrap();
        // HEVC is not in CODEC_REGEX, so it should stay HEVC
        assert_eq!(result, "Some.Movie.2160p.BluRay.HEVC-REPACK.mkv");
    }

    #[test]
    fn test_rename_non_video_to_match() {
        let path = PathBuf::from("subtitle.srt");
        let result = rename_non_video_to_match("subtitle.srt", &path, "Movie.Title.2024.1080p.BluRay.x265-NXELE").unwrap();
        assert_eq!(result, "Movie.Title.2024.1080p.BluRay.x265-NXELE.srt");
    }

    #[test]
    fn test_is_video_file_recognizes_extensions() {
        assert!(is_video_file(Path::new("movie.mkv")));
        assert!(is_video_file(Path::new("movie.mp4")));
        assert!(is_video_file(Path::new("movie.MKV")));
        assert!(!is_video_file(Path::new("subtitle.srt")));
        assert!(!is_video_file(Path::new("movie.nfo")));
    }
}
