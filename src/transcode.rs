use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{anyhow, Context};
use lazy_static::lazy_static;
use regex::Regex;
use tokio::fs;
use tokio::process::Command;
use tokio::io::AsyncBufReadExt;
use tracing::{debug, error, info, trace, warn};

use crate::db::Database;
use crate::policy::DetectedPolicy;

lazy_static::lazy_static! {
    static ref TIME_REGEX: Regex = Regex::new(r"time=\s*(\d+:\d+:\d+\.\d+)").unwrap();
    static ref FRAME_REGEX: Regex = Regex::new(r"frame=\s*(\d+)").unwrap();
}

/// Transcode all video files in a staging directory according to the detected policy.
///
/// Not invoked from the normal `run` path — library-side re-encoding
/// is owned by Tdarr per the architecture decision
/// (memory/architecture-pipeline-vs-tdarr.md). Kept here as a
/// callable for the rare case where a future operator wants to
/// force-re-encode a batch.
#[allow(dead_code)]
pub async fn transcode_directory(
    staging_path: &Path,
    detected_policy: DetectedPolicy,
    _db: &Database,
    dir_id: i64,
    group_name: &str,
) -> anyhow::Result<()> {
    if matches!(detected_policy, DetectedPolicy::None | DetectedPolicy::Manual) {
        debug!(dir_id, policy = %detected_policy.as_str(), "no transcoding needed, skipping");
        return Ok(());
    }

    // Walk directory and find video files
    let mut video_files = Vec::new();
    for entry in walkdir::WalkDir::new(staging_path).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if !entry.file_type().is_file() {
            continue;
        }
        if !is_video_file(path) {
            continue;
        }

        // Skip already-processed files (e.g., files that already have our group name)
        if has_group_name(path, group_name) {
            trace!(file = %path.display(), "skipping already-processed file");
            continue;
        }

        video_files.push(path.to_path_buf());
    }

    info!(dir_id, count = video_files.len(), "video files to transcode");

    for video_path in video_files {
        let file_name = video_path.file_name().unwrap().to_string_lossy();

        // Check if this file should be transcoded by probing its codec
        let should_transcode = match probe_video_codec(&video_path).await {
            Ok(codec) => should_transcode_for_policy(&detected_policy, &codec),
            Err(e) => {
                warn!(file = %file_name, error = %e, "failed to probe codec, assuming transcode needed");
                true
            }
        };

        if !should_transcode {
            debug!(file = %file_name, codec = ?probe_video_codec(&video_path).await.ok(), "skipping transcode — codec already matches target");
            continue;
        }

        // Create temp file in same directory
        let tmp_name = format!("{}.tmp.{}", file_name, group_name.to_lowercase());
        let tmp_path = video_path.with_file_name(&tmp_name);

        info!(file = %file_name, tmp = %tmp_path.display(), "starting transcode");

        let result = match detected_policy {
            DetectedPolicy::X264ToX265 => {
                transcode_x264_to_x265(&video_path, &tmp_path).await
            }
            DetectedPolicy::Downscale1080p => {
                transcode_downscale_1080p(&video_path, &tmp_path).await
            }
            DetectedPolicy::None | DetectedPolicy::Manual => unreachable!(),
        };

        match result {
            Ok(()) => {
                // Atomic replace
                trace!(old = %video_path.display(), new = %tmp_path.display(), "atomic rename");
                fs::rename(&tmp_path, &video_path).await
                    .with_context(|| format!(
                        "failed to rename temp file {} to {}",
                        tmp_path.display(),
                        video_path.display()
                    ))?;

                info!(file = %file_name, "transcode complete");
            }
            Err(e) => {
                // Clean up temp file
                let _ = fs::remove_file(&tmp_path).await;
                error!(file = %file_name, error = %e, "transcode failed");
                return Err(e);
            }
        }
    }

    Ok(())
}

async fn transcode_x264_to_x265(
    input: &Path,
    output: &Path,
) -> anyhow::Result<()> {
    let mut child = Command::new("ffmpeg")
        .arg("-y")
        .arg("-nostdin")
        .arg("-i")
        .arg(input)
        .arg("-map")
        .arg("0")
        .arg("-c:v")
        .arg("libx265")
        .arg("-vtag")
        .arg("hvc1")
        .arg("-preset")
        .arg("slow")
        .arg("-crf")
        .arg("23")
        .arg("-c:a")
        .arg("aac")
        .arg("-c:s")
        .arg("copy")
        .arg(output)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn ffmpeg for x264→x265")?;

    let stderr = child.stderr.take().expect("stderr was piped");
    let mut reader = tokio::io::BufReader::new(stderr).lines();

    while let Some(line) = reader.next_line().await? {
        if let Some(frame) = FRAME_REGEX.captures(&line).and_then(|c| c.get(1)) {
            trace!(frame = frame.as_str(), "ffmpeg progress");
        }
        if let Some(time) = TIME_REGEX.captures(&line).and_then(|c| c.get(1)) {
            trace!(time = time.as_str(), "ffmpeg progress");
        }
    }

    let status = child.wait().await.context("ffmpeg process failed")?;
    if !status.success() {
        anyhow::bail!("ffmpeg exited with non-zero status: {:?}", status.code());
    }

    Ok(())
}

async fn transcode_downscale_1080p(
    input: &Path,
    output: &Path,
) -> anyhow::Result<()> {
    let mut child = Command::new("ffmpeg")
        .arg("-y")
        .arg("-nostdin")
        .arg("-i")
        .arg(input)
        .arg("-c:v")
        .arg("libx265")
        .arg("-vtag")
        .arg("hvc1")
        .arg("-preset")
        .arg("slow")
        .arg("-crf")
        .arg("23")
        .arg("-vf")
        .arg("scale=1920:-2:flags=lanczos")
        .arg("-c:a")
        .arg("copy")
        .arg("-c:s")
        .arg("mov_text")
        .arg("-metadata:s:s:0")
        .arg("language=eng")
        .arg("-metadata:s:s:1")
        .arg("language=spa")
        .arg(output)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn ffmpeg for 1080p downscale")?;

    let stderr = child.stderr.take().expect("stderr was piped");
    let mut reader = tokio::io::BufReader::new(stderr).lines();

    while let Some(line) = reader.next_line().await? {
        if let Some(frame) = FRAME_REGEX.captures(&line).and_then(|c| c.get(1)) {
            trace!(frame = frame.as_str(), "ffmpeg progress");
        }
        if let Some(time) = TIME_REGEX.captures(&line).and_then(|c| c.get(1)) {
            trace!(time = time.as_str(), "ffmpeg progress");
        }
    }

    let status = child.wait().await.context("ffmpeg process failed")?;
    if !status.success() {
        anyhow::bail!("ffmpeg exited with non-zero status: {:?}", status.code());
    }

    Ok(())
}

/// Probe the video codec using ffprobe
async fn probe_video_codec(path: &Path) -> anyhow::Result<String> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("v:0")
        .arg("-show_entries")
        .arg("stream=codec_name")
        .arg("-of")
        .arg("default=nw=1:nk=1")
        .arg(path)
        .output()
        .await
        .context("failed to spawn ffprobe")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("ffprobe failed: {}", stderr);
    }

    let codec = String::from_utf8(output.stdout)?
        .trim()
        .to_lowercase();

    Ok(codec)
}

fn should_transcode_for_policy(policy: &DetectedPolicy, codec: &str) -> bool {
    match policy {
        DetectedPolicy::X264ToX265 | DetectedPolicy::Downscale1080p => {
            codec == "h264" || codec == "mpeg4" || codec == "avc"
        }
        DetectedPolicy::None | DetectedPolicy::Manual => false,
    }
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

fn has_group_name(path: &Path, group_name: &str) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.contains(group_name))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_transcode_for_policy_x264_sources() {
        let policy = DetectedPolicy::X264ToX265;
        assert!(should_transcode_for_policy(&policy, "h264"));
        assert!(should_transcode_for_policy(&policy, "mpeg4"));
        assert!(should_transcode_for_policy(&policy, "avc"));
        assert!(!should_transcode_for_policy(&policy, "hevc"));
        assert!(!should_transcode_for_policy(&policy, "x265"));
    }

    #[test]
    fn test_should_transcode_for_policy_downscale() {
        let policy = DetectedPolicy::Downscale1080p;
        assert!(should_transcode_for_policy(&policy, "h264"));
        assert!(!should_transcode_for_policy(&policy, "hevc"));
    }

    #[test]
    fn test_should_transcode_for_policy_none() {
        let policy = DetectedPolicy::None;
        assert!(!should_transcode_for_policy(&policy, "h264"));
        assert!(!should_transcode_for_policy(&policy, "hevc"));
    }

    #[test]
    fn test_should_transcode_for_policy_manual() {
        let policy = DetectedPolicy::Manual;
        assert!(!should_transcode_for_policy(&policy, "h264"));
        assert!(!should_transcode_for_policy(&policy, "hevc"));
    }

    #[test]
    fn test_is_video_file_extensions() {
        assert!(is_video_file(Path::new("movie.mkv")));
        assert!(is_video_file(Path::new("movie.mp4")));
        assert!(is_video_file(Path::new("movie.flv")));
        assert!(!is_video_file(Path::new("movie.srt")));
        assert!(!is_video_file(Path::new("movie.nfo")));
    }

    #[test]
    fn test_has_group_name() {
        assert!(has_group_name(Path::new("Movie-x265-REPACK.mkv"), "REPACK"));
        assert!(!has_group_name(Path::new("Movie-x264-GROUP.mkv"), "REPACK"));
        assert!(!has_group_name(Path::new("Movie.mkv"), "REPACK"));
    }
}
