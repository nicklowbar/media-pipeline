use std::path::{Path, PathBuf};

use anyhow::Context;
use tokio::process::Command;
use tracing::{debug, info, trace, warn};
use walkdir::WalkDir;

use crate::db::{Database, DirectoryRecord};

/// Policy determined for a single title (top-level directory).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedPolicy {
    /// No transcoding needed (already x265 or no video files).
    None,
    /// At least one video file is H.264 and should be transcoded to x265.
    X264ToX265,
    /// At least one video file is 4K and should be downscaled to 1080p.
    Downscale1080p,
    /// Contains unrecognizable formats (DVD ISO, etc.) that cannot be auto-transcoded.
    Manual,
}

impl DetectedPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            DetectedPolicy::None => "none",
            DetectedPolicy::X264ToX265 => "x264_to_x265",
            DetectedPolicy::Downscale1080p => "downscale_1080p",
            DetectedPolicy::Manual => "manual",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "none" => Some(DetectedPolicy::None),
            "x264_to_x265" => Some(DetectedPolicy::X264ToX265),
            "downscale_1080p" => Some(DetectedPolicy::Downscale1080p),
            "manual" => Some(DetectedPolicy::Manual),
            _ => None,
        }
    }

    /// Whether the filename should have its codec tag updated during rename.
    pub fn changes_codec(&self) -> bool {
        matches!(self, DetectedPolicy::X264ToX265 | DetectedPolicy::Downscale1080p)
    }
}

/// Analyzes all video files in a staging directory and determines the transcode policy.
pub async fn analyze_directory(
    staging_path: &Path,
    _db: &Database,
    dir_id: i64,
) -> anyhow::Result<DetectedPolicy> {
    let mut video_files = Vec::new();

    for entry in WalkDir::new(staging_path).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if !entry.file_type().is_file() {
            continue;
        }
        if is_video_file(path) {
            video_files.push(path.to_path_buf());
        }
    }

    if video_files.is_empty() {
        trace!(dir_id, "no video files found, policy = none");
        return Ok(DetectedPolicy::None);
    }

    info!(
        dir_id,
        count = video_files.len(),
        "analyzing video files for transcode policy"
    );

    let mut needs_x264_to_x265 = false;
    let mut needs_downscale = false;
    let mut has_unrecognizable = false;

    for video_path in video_files {
        let file_name = video_path.file_name().unwrap().to_string_lossy();

        match probe_video(&video_path).await {
            Ok(info) => {
                trace!(
                    file = %file_name,
                    codec = %info.codec,
                    width = info.width,
                    height = info.height,
                    "probed video"
                );

                if info.codec == "h264" || info.codec == "mpeg4" || info.codec == "avc" {
                    needs_x264_to_x265 = true;
                }

                if info.width > 1920 || info.height > 1080 {
                    // 4K or larger — could be candidate for downscale
                    // For now, we don't auto-downscale unless explicitly configured.
                    // If we wanted auto-downscale, uncomment:
                    // needs_downscale = true;
                }
            }
            Err(e) => {
                warn!(
                    file = %file_name,
                    error = %e,
                    "failed to probe video — marking as manual"
                );
                has_unrecognizable = true;
            }
        }
    }

    Ok(select_policy_from_probe_results(needs_x264_to_x265, needs_downscale, has_unrecognizable, dir_id))
}

fn select_policy_from_probe_results(
    needs_x264_to_x265: bool,
    needs_downscale: bool,
    has_unrecognizable: bool,
    dir_id: i64,
) -> DetectedPolicy {
    let policy = if has_unrecognizable {
        info!(dir_id, "policy = manual (unrecognizable formats detected)");
        DetectedPolicy::Manual
    } else if needs_downscale {
        info!(dir_id, "policy = downscale_1080p");
        DetectedPolicy::Downscale1080p
    } else if needs_x264_to_x265 {
        info!(dir_id, "policy = x264_to_x265");
        DetectedPolicy::X264ToX265
    } else {
        info!(dir_id, "policy = none (all files already in target format)");
        DetectedPolicy::None
    };
    policy
}

#[derive(Debug)]
struct VideoInfo {
    codec: String,
    width: u32,
    height: u32,
}

async fn probe_video(path: &Path) -> anyhow::Result<VideoInfo> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("v:0")
        .arg("-show_entries")
        .arg("stream=codec_name,width,height")
        .arg("-of")
        .arg("json")
        .arg(path)
        .output()
        .await
        .context("failed to spawn ffprobe")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("ffprobe failed: {}", stderr);
    }

    let json: serde_json::Value = serde_json::from_slice(&output.stdout)
        .context("failed to parse ffprobe JSON output")?;

    let stream = json
        .get("streams")
        .and_then(|s| s.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| anyhow::anyhow!("no video stream found in ffprobe output"))?;

    let codec = stream
        .get("codec_name")
        .and_then(|c| c.as_str())
        .unwrap_or("unknown")
        .to_lowercase();

    let width = stream
        .get("width")
        .and_then(|w| w.as_u64())
        .unwrap_or(0) as u32;

    let height = stream
        .get("height")
        .and_then(|h| h.as_u64())
        .unwrap_or(0) as u32;

    Ok(VideoInfo {
        codec,
        width,
        height,
    })
}

fn is_video_file(path: &Path) -> bool {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let ext_lower = ext.to_lowercase();
        matches!(
            ext_lower.as_str(),
            "mp4" | "mkv" | "avi" | "mov" | "m4v" | "ts" | "m2ts" | "wmv" | "flv"
        )
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detected_policy_as_str() {
        assert_eq!(DetectedPolicy::None.as_str(), "none");
        assert_eq!(DetectedPolicy::X264ToX265.as_str(), "x264_to_x265");
        assert_eq!(DetectedPolicy::Downscale1080p.as_str(), "downscale_1080p");
        assert_eq!(DetectedPolicy::Manual.as_str(), "manual");
    }

    #[test]
    fn test_detected_policy_from_str() {
        assert_eq!(DetectedPolicy::from_str("none"), Some(DetectedPolicy::None));
        assert_eq!(DetectedPolicy::from_str("x264_to_x265"), Some(DetectedPolicy::X264ToX265));
        assert_eq!(DetectedPolicy::from_str("downscale_1080p"), Some(DetectedPolicy::Downscale1080p));
        assert_eq!(DetectedPolicy::from_str("manual"), Some(DetectedPolicy::Manual));
        assert_eq!(DetectedPolicy::from_str("unknown"), None);
    }

    #[test]
    fn test_detected_policy_changes_codec() {
        assert!(!DetectedPolicy::None.changes_codec());
        assert!(!DetectedPolicy::Manual.changes_codec());
        assert!(DetectedPolicy::X264ToX265.changes_codec());
        assert!(DetectedPolicy::Downscale1080p.changes_codec());
    }

    #[test]
    fn test_select_policy_unrecognizable_wins() {
        // Even if x264 is also true, unrecognizable takes precedence
        let policy = select_policy_from_probe_results(true, false, true, 1);
        assert_eq!(policy, DetectedPolicy::Manual);
    }

    #[test]
    fn test_select_policy_downscale_before_x264() {
        let policy = select_policy_from_probe_results(true, true, false, 1);
        assert_eq!(policy, DetectedPolicy::Downscale1080p);
    }

    #[test]
    fn test_select_policy_x264() {
        let policy = select_policy_from_probe_results(true, false, false, 1);
        assert_eq!(policy, DetectedPolicy::X264ToX265);
    }

    #[test]
    fn test_select_policy_none() {
        let policy = select_policy_from_probe_results(false, false, false, 1);
        assert_eq!(policy, DetectedPolicy::None);
    }

    #[test]
    fn test_is_video_file() {
        assert!(is_video_file(Path::new("movie.mkv")));
        assert!(is_video_file(Path::new("movie.mp4")));
        assert!(!is_video_file(Path::new("movie.srt")));
        assert!(!is_video_file(Path::new("movie.nfo")));
    }
}
