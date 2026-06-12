use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use lazy_static::lazy_static;
use regex::Regex;
use tracing::{debug, info, trace, warn};
use walkdir::WalkDir;

use crate::config::Config;
use crate::db::Database;
use crate::metadata::MetadataLookup;
use crate::policy::DetectedPolicy;

lazy_static! {
    /// Inner-codec swap, used after the group is swapped. Catches codec
    /// tags that live inside a quality block, e.g. the `x264` inside
    /// `[FuniDub 1080p x264 AAC]`. Per-tunable: only matches `x264` /
    /// `h264` / `H.264` since those are the cases the transcode policy
    /// flips to `x265`. `hevc` / `x265` / `h265` are preserved.
    static ref CODEC_REGEX: Regex = Regex::new(
        r"(?i)(?P<codec>x264|h264|H\.264|X264|H264)"
    ).unwrap();
}

/// Parsed metadata for a single release filename. Captures everything
/// the pipeline needs to do a group swap on the basename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseMetadata {
    /// Original basename as supplied (sans directory).
    pub raw: String,
    /// Detected release group, if any. None means "no group found;
    /// append `-REPACK` to the stem."
    pub group: Option<DetectedGroup>,
    /// The file extension lowercased, no leading dot (e.g. `mkv`).
    pub ext: String,
    /// The portion of the basename BEFORE the group, with the group
    /// token spliced out and the surrounding separators preserved.
    /// Used as the "title" hint for the metadata lookup.
    pub without_group: String,
    /// For TV: detected season/episode (e.g. `S01E02`, `S00E01v2`).
    pub season_episode: Option<SeasonEpisode>,
    /// Resolution tag, normalized (e.g. `1080p`, `2160p`).
    pub resolution: Option<String>,
    /// Source tag, normalized (e.g. `BluRay`, `WEB-DL`).
    pub source: Option<String>,
    /// Codec tag, normalized (e.g. `x264`, `x265`, `HEVC`).
    pub codec: Option<String>,
    /// Leading track number for music files, e.g. `01` in `01 - Heroes.mp3`.
    /// Stored separately from `group` because it's not a release group.
    pub track: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedGroup {
    pub name: String,
    pub style: GroupStyle,
    /// Byte offset range into `without_group` covering the *group token
    /// only* (not the preceding separator). Used to splice the new
    /// group name into the right place.
    pub span: (usize, usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupStyle {
    /// `[Group]_title` or `[Group] title.ext`
    LeadingBracket,
    /// `title-GROUP.ext` (separator was `-`)
    TrailingHyphen,
    /// `title.GROUP.ext` (separator was `.`)
    TrailingDot,
    /// `group-title.ext` (no brackets, leading)
    LeadingUnbracketed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeasonEpisode {
    pub season: Option<u32>,
    pub episode: Option<u32>,
    pub version: Option<String>, // e.g. "v2", "v3" for repacks
}

/// False-positive denylist: tokens that look like release groups but
/// actually mean something else. Comparison is case-insensitive after
/// stripping non-alphanumeric chars. Populated from the finfunnel
/// exclude lists and the broader release-naming conventions.
const FALSE_POSITIVE_GROUPS: &[&str] = &[
    // Codecs
    "x264", "x265", "h264", "h265", "hevc", "avc", "av1", "vp9",
    // Audio formats
    "flac", "mp3", "aac", "dts", "dtshd", "dtshdma", "truehd", "atmos",
    "opus", "vorbis", "ac3", "eac3", "dd", "dd51", "dd71",
    "lpcm", "pcm", "320", "v0", "v2", "lossless",
    // Quality tags
    "1080p", "720p", "2160p", "4k", "uhd", "hd", "sd", "8k",
    "10bit", "60fps", "hdr", "hdr10", "hdr10plus", "dovi", "dv",
    // Sources / extraction methods. These are *not* release groups
    // even though they look like one — they describe how the file was
    // ripped/captured, not who uploaded it. The denylist-gate prevents
    // the trailing-form parser from mistaking e.g. `Movie.DVDrip.mkv`
    // for `Movie.[DVDrip].mkv` (group=DVDr).
    "bluray", "brrip", "bdrip", "webdl", "webrip", "hdtv",
    "pdtv", "dvdrip", "dvdscr", "dvd", "cam", "ts", "tc", "r5",
    "remux", "bd", "dsnp", "atvp", "pmtp", "web", "dvb", "dsr", "sdtv", "ppv",
    // Release types
    "proper", "repack", "remastered", "regraded", "hybrid", "imax",
    "extended", "theatrical", "criterion", "complete", "internal",
    "limited", "dc",
    // Episode/show tags (sample is dropped separately; listed here so
    // the trailing-form parser doesn't match it as a group)
    "sample", "trailer", "extras", "featurettes", "nfo", "readme", "txt",
    // Other non-group tokens
    "dual", "audio", "multi", "esub", "subs", "hindi", "english",
    "japanese",
];

/// Parse a release filename (basename only — no directory) into
/// structured metadata. Tries three group-detection rules in priority
/// order:
/// 1. Leading `[Group]` (anime/fansub convention)
/// 2. Trailing `-GROUP.ext` or `.GROUP.ext` (classic form, denylist-gated)
/// 3. Leading unbracketed `group-` (games split files like `rune-...`)
///
/// Pure numeric candidates and tokens in the denylist are rejected as
/// groups; the file is then treated as having no group.
pub fn parse_release_metadata(name: &str) -> ReleaseMetadata {
    let name = name.trim();
    let (stem, ext) = split_basename(name);
    let ext = ext.to_lowercase();

    // The .sample suffix is dropped on the *stem* before any group
    // detection runs. This means the trailing-form parser doesn't
    // accidentally identify `sample` as a group, and the rest of the
    // pipeline doesn't need to know the file was originally a sample.
    let (effective_stem, sample_dropped) = match strip_sample_suffix(&stem) {
        Some(s) => (s.to_string(), true),
        None => (stem.clone(), false),
    };

    let group = detect_leading_bracket(&effective_stem)
        .or_else(|| detect_trailing(&effective_stem))
        .or_else(|| detect_leading_unbracketed(&effective_stem));

    let without_group = match &group {
        Some(g) => {
            let mut s = effective_stem.clone();
            // Span covers the group token (and brackets, for LeadingBracket).
            // We splice in the new group name, so for the *without_group*
            // view we replace the whole span with an empty string.
            s.replace_range(g.span.0..g.span.1, "");
            s
        }
        None => effective_stem.clone(),
    };

    let season_episode = parse_season_episode(&without_group);
    let resolution = parse_resolution(&without_group);
    let source = parse_source(&without_group);
    let codec = parse_codec(&without_group);
    let track = parse_track(&without_group);

    // The `raw` field stores the basename as supplied (with .sample
    // dropped if it was present). This is what the rename step operates
    // on; the caller doesn't have to re-derive it.
    let raw = if sample_dropped {
        format!("{}.{}", effective_stem, ext)
    } else {
        name.to_string()
    };

    ReleaseMetadata {
        raw,
        group,
        ext,
        without_group,
        season_episode,
        resolution,
        source,
        codec,
        track,
    }
}

/// Split a basename into (stem, ext). The extension is empty if the
/// basename has no recognized extension.
fn split_basename(name: &str) -> (String, String) {
    if let Some(dot) = name.rfind('.') {
        let stem = &name[..dot];
        let ext = &name[dot + 1..];
        // Don't treat a leading dot (hidden files) as an extension.
        if !stem.is_empty() {
            return (stem.to_string(), ext.to_string());
        }
    }
    (name.to_string(), String::new())
}

/// If the stem ends in `.sample`, drop that suffix.
fn strip_sample_suffix(stem: &str) -> Option<String> {
    let lower = stem.to_lowercase();
    if lower.ends_with(".sample") {
        Some(stem[..stem.len() - ".sample".len()].to_string())
    } else {
        None
    }
}

fn detect_leading_bracket(name: &str) -> Option<DetectedGroup> {
    // Pattern: `[<token>]_<rest>` or `[<token>] <rest>` or `[<token>].<rest>`.
    let bytes = name.as_bytes();
    if bytes.first() != Some(&b'[') {
        return None;
    }
    let close = name.find(']')?;
    if close <= 1 {
        return None;
    }
    let token = &name[1..close];
    let after = name[close + 1..].chars().next()?;
    if !matches!(after, '_' | ' ' | '.') {
        return None;
    }
    if is_false_positive_group(token) || is_pure_number(token) {
        return None;
    }
    Some(DetectedGroup {
        name: token.to_string(),
        style: GroupStyle::LeadingBracket,
        // Span covers the whole `[<token>]` (with brackets) so the
        // replacement step restores the brackets around the new name.
        span: (0, close + 1),
    })
}

fn detect_trailing(name: &str) -> Option<DetectedGroup> {
    // Look for the last `\.` or `-` in `name`. The candidate is what
    // follows. Reject denylist hits and pure-number candidates.
    let mut last_sep: Option<(usize, char)> = None;
    for (i, c) in name.char_indices() {
        if c == '.' || c == '-' {
            last_sep = Some((i, c));
        }
    }
    let (sep_idx, sep_char) = last_sep?;
    let candidate = &name[sep_idx + 1..];
    if candidate.is_empty() {
        return None;
    }
    if is_false_positive_group(candidate) || is_pure_number(candidate) {
        return None;
    }
    // The candidate must be a single token — no `.`, space, or
    // bracket characters inside. A `.` would mean we matched something
    // mid-path; a space is invalid in a release group; brackets/
    // parens are typical of title metadata (e.g. `(ANIME)`, `[FLAC]`)
    // and shouldn't be parsed as a group.
    if candidate.contains('.')
        || candidate.contains(' ')
        || candidate.contains('(')
        || candidate.contains(')')
        || candidate.contains('[')
        || candidate.contains(']')
    {
        return None;
    }
    let style = if sep_char == '-' {
        GroupStyle::TrailingHyphen
    } else {
        GroupStyle::TrailingDot
    };
    let start = sep_idx + 1;
    let end = name.len();
    if end <= start {
        return None;
    }
    Some(DetectedGroup {
        name: candidate.to_string(),
        style,
        span: (start, end),
    })
}

fn detect_leading_unbracketed(name: &str) -> Option<DetectedGroup> {
    // Pattern: `<word>-<rest>...` where word is short, all-lowercase,
    // and the rest has at least one `.` (game-split signature).
    let dash = name.find('-')?;
    let candidate = &name[..dash];
    let rest = &name[dash + 1..];
    if candidate.is_empty() || candidate.len() > 8 {
        return None;
    }
    if !candidate.chars().all(|c| c.is_ascii_lowercase()) {
        return None;
    }
    if candidate.contains('.') || candidate.contains('[') {
        return None;
    }
    if is_false_positive_group(candidate) || is_pure_number(candidate) {
        return None;
    }
    // The "rest" must look like a games split (multiple tokens separated
    // by `.` or `_`). This prevents false matches on words like
    // `coalgirls-Lain` — though the leading-bracket detector usually
    // catches those first.
    if !rest.contains('.') {
        return None;
    }
    if rest.starts_with(' ') {
        return None;
    }
    Some(DetectedGroup {
        name: candidate.to_string(),
        style: GroupStyle::LeadingUnbracketed,
        // Span covers just the word (no leading hyphen — we want to
        // replace the word and keep the hyphen).
        span: (0, dash),
    })
}

fn is_false_positive_group(candidate: &str) -> bool {
    let normalized: String = candidate
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect();
    if normalized.is_empty() {
        return true;
    }
    FALSE_POSITIVE_GROUPS
        .iter()
        .any(|entry| *entry == normalized.as_str())
}

fn is_pure_number(candidate: &str) -> bool {
    !candidate.is_empty() && candidate.chars().all(|c| c.is_ascii_digit())
}

fn parse_season_episode(s: &str) -> Option<SeasonEpisode> {
    // S01E02, S01E02v3, S01, S00E01 (specials). Word-bounded so we
    // don't pick up `S01` inside a longer token.
    lazy_static! {
        static ref RE: Regex = Regex::new(
            r"(?i)\bS(?P<season>\d{1,2})(?:E(?P<episode>\d{1,3}))?(?P<ver>v\d+)?\b"
        ).unwrap();
    }
    let caps = RE.captures(s)?;
    let season = caps.name("season").and_then(|m| m.as_str().parse().ok());
    let episode = caps.name("episode").and_then(|m| m.as_str().parse().ok());
    let version = caps.name("ver").map(|m| m.as_str().to_string());
    Some(SeasonEpisode { season, episode, version })
}

fn parse_resolution(s: &str) -> Option<String> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"(?i)\b(\d{3,4}p|4k|8k|uhd)\b").unwrap();
    }
    RE.find(s).map(|m| m.as_str().to_lowercase())
}

fn parse_source(s: &str) -> Option<String> {
    const SOURCES: &[&str] = &[
        "BluRay", "Blu-Ray", "WEB-DL", "WEB-DLRip", "WEBRip",
        "HDTV", "DVDRip", "BDRip", "Remux",
    ];
    let lower = s.to_lowercase();
    for src in SOURCES {
        if lower.contains(&src.to_lowercase()) {
            return Some(src.to_string());
        }
    }
    None
}

fn parse_codec(s: &str) -> Option<String> {
    // The codec field captures both video codecs (x264, hevc, etc.)
    // and audio formats (FLAC, AAC, etc.) — anything that's a
    // signal of the file's encoding rather than a group/source/quality
    // tag. This is informational; the rename path only swaps the
    // video codecs that the transcode policy changes.
    const CODECS: &[&str] = &[
        "x264", "x265", "h264", "h265", "HEVC", "AVC", "AV1", "VP9",
        "FLAC", "AAC", "DTS", "TrueHD", "Atmos", "Opus", "Vorbis",
        "AC3", "EAC3", "MP3", "LPCM", "PCM",
    ];
    let bytes = s.as_bytes();
    let lower = s.to_lowercase();
    for c in CODECS {
        let needle = c.to_lowercase();
        if let Some(idx) = lower.find(&needle) {
            let before_ok = idx == 0
                || !bytes[idx - 1].is_ascii_alphanumeric();
            let after_idx = idx + needle.len();
            let after_ok = after_idx >= s.len()
                || !bytes[after_idx].is_ascii_alphanumeric();
            if before_ok && after_ok {
                return Some(c.to_string());
            }
        }
    }
    None
}

fn parse_track(s: &str) -> Option<u32> {
    // Leading `NN - ` or `NN. ` for music tracks.
    lazy_static! {
        static ref RE: Regex = Regex::new(r"^\s*(\d{1,3})[\s.\-]").unwrap();
    }
    let caps = RE.captures(s)?;
    caps.get(1).and_then(|m| m.as_str().parse().ok())
}

/// Rename all files in a staging directory to Plex-friendly names.
/// - Replace release group with the configured group name.
/// - Update codec tag in filename if transcode policy changes codec.
/// - Rename non-video files to match the primary video file.
/// - Best-effort: call the metadata lookup for the primary video to
///   warm the canonical-title cache. Failures are logged, not propagated.
///
/// This function is async (not mixed sync/async) so the metadata
/// lookup's `.await` runs directly on the caller's runtime. The
/// filesystem operations are still blocking — they're fast enough
/// that the cost of moving them to a thread pool isn't worth it,
/// and a single thread's worth of blocking I/O on a directory of
/// typical media files is negligible.
pub async fn rename_directory(
    staging_path: &Path,
    config: &Config,
    category: &str,
    db: &Database,
    dir_id: i64,
    detected_policy: Option<&DetectedPolicy>,
    lookup: &dyn MetadataLookup,
) -> anyhow::Result<Option<crate::metadata::CanonicalTitle>> {
    let group_name = config.group_name();
    let will_change_codec = detected_policy.map(|p| p.changes_codec()).unwrap_or(false);

    // Walk directory and collect files.
    let mut files = Vec::new();
    for entry in WalkDir::new(staging_path).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let relative = entry.path().strip_prefix(staging_path)
            .unwrap_or(entry.path());
        files.push((entry.path().to_path_buf(), relative.to_path_buf()));
    }

    // Identify the primary video file (first video file in the root
    // directory — the rename path is order-preserving, so this matches
    // the old behavior).
    let primary_video = files.iter().find(|(full, rel)| {
        rel.parent() == Some(Path::new("")) && is_video_file(full)
    });

    // Best-effort metadata lookup for the primary video. A failure
    // here does not fail the rename; the directory-level rename
    // continues using the locally-parsed title. The resolved
    // canonical title (if any) is returned so the move step can use
    // it to choose the library layout path.
    let mut canonical_title: Option<crate::metadata::CanonicalTitle> = None;
    if let Some((primary_full, _)) = primary_video {
        if let Some(basename) = primary_full.file_name().and_then(|n| n.to_str()) {
            let meta = parse_release_metadata(basename);
            match lookup.lookup(&meta, category).await {
                Ok(Some(canonical)) => {
                    debug!(
                        title = %canonical.title,
                        year = ?canonical.year,
                        external_id = %canonical.external_id,
                        "resolved canonical title"
                    );
                    canonical_title = Some(canonical);
                }
                Ok(None) => {
                    debug!(basename, "no canonical title found");
                }
                Err(e) => {
                    warn!(error = %e, basename, "metadata lookup failed, using local parse");
                }
            }
        }
    }

    let primary_base = primary_video.map(|(full, _)| {
        let new_name = rename_file(full, will_change_codec, group_name)?;
        Ok::<_, anyhow::Error>(
            PathBuf::from(&new_name)
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .to_string(),
        )
    }).transpose()?;

    // Rename each file.
    let mut rename_map: HashMap<PathBuf, PathBuf> = HashMap::new();

    for (full_path, relative_path) in &files {
        let parent = full_path.parent().unwrap_or(Path::new(""));
        let original_name = full_path.file_name().unwrap().to_string_lossy();

        let new_name = if is_video_file(full_path) {
            rename_file(full_path, will_change_codec, group_name)?
        } else if relative_path.parent() == Some(Path::new("")) {
            // Non-video file in the root directory: match the primary
            // video's base name if available, otherwise apply the
            // standard group-swap rename.
            if let Some(ref base) = primary_base {
                rename_non_video_to_match(&original_name, full_path, base)?
            } else {
                rename_file(full_path, false, group_name)?
            }
        } else {
            // Subdirectory files: apply the standard group-swap rename.
            rename_file(full_path, false, group_name)?
        };

        let new_path = parent.join(&new_name);
        if full_path != &new_path {
            rename_map.insert(full_path.clone(), new_path);
        }
    }

    // Perform the actual renames.
    for (old_path, new_path) in &rename_map {
        trace!(old = %old_path.display(), new = %new_path.display(), "renaming file");

        if let Some(parent) = new_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::rename(old_path, new_path)
            .with_context(|| format!("failed to rename {} to {}", old_path.display(), new_path.display()))?;

        let original = old_path.file_name().unwrap().to_string_lossy();
        let policy_str = detected_policy.map(|p| p.as_str()).unwrap_or("none");
        db.insert_file(
            dir_id,
            &original,
            policy_str,
            is_video_file(new_path),
        )?;
    }

    info!(dir_id, count = rename_map.len(), "directory renamed");
    Ok(canonical_title)
}

/// Rename a single file. Uses the structured parser to find the group,
/// then splices the configured group name in. If the file already
/// contains the target group name, it is returned unchanged. The full
/// path (parent directory) is preserved; only the file name is changed.
fn rename_file(path: &Path, update_codec: bool, group_name: &str) -> anyhow::Result<String> {
    // Operate on just the file name; the caller is responsible for
    // joining the result back to the parent. The test/expected outputs
    // are basenames, so the function returns a basename.
    let name = path.file_name().unwrap().to_string_lossy();

    if name.contains(group_name) {
        return Ok(name.to_string());
    }

    let new_name = apply_group_swap(&name, group_name);

    let new_name = if update_codec {
        CODEC_REGEX.replace_all(&new_name, "x265").to_string()
    } else {
        new_name
    };

    Ok(new_name)
}

/// Apply the group swap to a basename. Drops `.sample` suffix; if no
/// group is detected, appends `-<group_name>` to the stem.
fn apply_group_swap(name: &str, group_name: &str) -> String {
    // Parse the *full* name (with extension) so the parser's span math
    // is correct relative to the whole basename. The extension is
    // preserved at the end.
    let meta = parse_release_metadata(name);

    // The parser may have dropped a `.sample` suffix from `raw`; in
    // that case we want to operate on the trimmed form for the splice.
    let base = if meta.raw != name { &meta.raw } else { name };

    let new_stem = match &meta.group {
        Some(g) => {
            // Operate on the stem-without-extension, since `g.span` is
            // relative to that. We rejoin the extension at the end.
            let (stem, ext_with_dot) = {
                let p = std::path::Path::new(base);
                let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or(base);
                let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
                if ext.is_empty() {
                    (stem.to_string(), String::new())
                } else {
                    (stem.to_string(), format!(".{}", ext))
                }
            };
            match g.style {
                GroupStyle::LeadingBracket => {
                    let mut s = stem.clone();
                    s.replace_range(g.span.0..g.span.1, &format!("[{}]", group_name));
                    format!("{}{}", s, ext_with_dot)
                }
                GroupStyle::TrailingHyphen | GroupStyle::TrailingDot => {
                    let mut s = stem.clone();
                    s.replace_range(g.span.0..g.span.1, group_name);
                    format!("{}{}", s, ext_with_dot)
                }
                GroupStyle::LeadingUnbracketed => {
                    let mut s = stem.clone();
                    s.replace_range(g.span.0..g.span.1, group_name);
                    format!("{}{}", s, ext_with_dot)
                }
            }
        }
        None => {
            // No group: append `-REPACK` before the extension.
            let (stem, ext_with_dot) = {
                let p = std::path::Path::new(base);
                let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or(base);
                let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
                if ext.is_empty() {
                    (stem.to_string(), String::new())
                } else {
                    (stem.to_string(), format!(".{}", ext))
                }
            };
            format!("{}-{}{}", stem, group_name, ext_with_dot)
        }
    };

    new_stem
}

/// Rename a non-video file to match the primary video file's base name.
fn rename_non_video_to_match(
    original_name: &str,
    full_path: &Path,
    primary_base: &str,
) -> anyhow::Result<String> {
    let ext = full_path.extension().and_then(|e| e.to_str()).unwrap_or("");
    let _ = original_name; // kept for API parity; not used
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

/// Find the primary video file in a staging directory. Returns the
/// first video file sitting at the root of the directory — the same
/// heuristic `rename_directory` uses. This is exposed so the move
/// step can re-parse the filename to derive a local title for the
/// library layout resolver when no TMDB canonical is available.
pub fn primary_video_path(staging_path: &Path) -> Option<PathBuf> {
    use walkdir::WalkDir;
    for entry in WalkDir::new(staging_path).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry
            .path()
            .strip_prefix(staging_path)
            .unwrap_or(entry.path());
        // Root-level only — first hit wins (directory iteration order).
        if rel.parent() == Some(Path::new("")) && is_video_file(entry.path()) {
            return Some(entry.path().to_path_buf());
        }
    }
    None
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

    // ----------------------------------------------------------------------
    // Real-world Plex-naming test cases
    //
    // These cases are drawn from the live title releases and the user's
    // description of the intended Plex schema (title + (year) for movies;
    // Show (Year)/Season NN/<episode> for TV; Season 00 for specials).
    // The file rename itself is a group+swap transform — Plex handles the
    // directory structure via its own metadata parser, so the rename
    // layer only normalizes the filename.
    //
    // Each case expresses the *expected* (correct) output. Cases against
    // the current parser that produce a wrong group extraction are flagged
    // with `expected_to_pass = false` so we can iterate through them as
    // the parser is improved.
    // ----------------------------------------------------------------------

    struct RenameCase {
        input: &'static str,
        group_only: &'static str,            // expected output with update_codec=false
        codec_swap: Option<&'static str>,    // expected output with update_codec=true
        note: &'static str,
        /// True if the current rename logic is expected to produce this
        /// result; false means the test encodes desired behavior that the
        /// parser does not yet implement (TDD-style).
        expected_to_pass: bool,
    }

    const RENAME_CASES: &[RenameCase] = &[
        // ---- TV / Movies: standard release-group pattern ----
        RenameCase {
            input: "Shoresy.S05E03.1080p.HEVC.x265-MeGusta.mkv",
            group_only: "Shoresy.S05E03.1080p.HEVC.x265-REPACK.mkv",
            codec_swap: Some("Shoresy.S05E03.1080p.HEVC.x265-REPACK.mkv"),
            note: "HEVC release, group swap only (no codec change)",
            expected_to_pass: true,
        },
        RenameCase {
            input: "The Expanse S01-S06 1080p BluRay x265-KONTRAST.mkv",
            group_only: "The Expanse S01-S06 1080p BluRay x265-REPACK.mkv",
            codec_swap: Some("The Expanse S01-S06 1080p BluRay x265-REPACK.mkv"),
            note: "Multi-season pack, x265, group swap",
            expected_to_pass: true,
        },
        RenameCase {
            input: "A Certain Magical Index -The Movie-[2013].x264.DVDrip(ANIME).mp4",
            group_only: "A Certain Magical Index -The Movie-[2013].x264.DVDrip(ANIME)-REPACK.mp4",
            codec_swap: Some("A Certain Magical Index -The Movie-[2013].x265.DVDrip(ANIME)-REPACK.mp4"),
            note: "Anime movie. The trailing `DVDrip(ANIME)` contains parens, so the new parser correctly rejects it as a group (it's title metadata) and falls back to appending -REPACK at the end. The codec swap still flips the inner x264 tag.",
            expected_to_pass: true,
        },
        RenameCase {
            input: "Some.Movie.2160p.BluRay.HEVC-GROUP.mkv",
            group_only: "Some.Movie.2160p.BluRay.HEVC-REPACK.mkv",
            codec_swap: Some("Some.Movie.2160p.BluRay.HEVC-REPACK.mkv"),
            note: "HEVC preserved (regex only matches x264/h264/H.264)",
            expected_to_pass: true,
        },

        // ---- TV: live bugs in the current parser ----
        // The trailing-form parser walks the whole string for the last
        // separator, so it picks the LAST `[\.\-]` and the candidate
        // can be a codec/source tag. These cases capture the *correct*
        // behavior and will fail until the new priority-ordered parser
        // is in place.
        RenameCase {
            input: "Pantheon S01-S02 web 10bit hevc-d3g.mkv",
            group_only: "Pantheon S01-S02 web 10bit hevc-REPACK.mkv",
            codec_swap: Some("Pantheon S01-S02 web 10bit hevc-REPACK.mkv"),
            note: "Multi-word codec tag followed by hyphen-group: parser correctly extracts `d3g`",
            expected_to_pass: true,
        },
        RenameCase {
            input: "[Coalgirls]_Serial_Experiments_Lain_(1520x1080_Blu-Ray_FLAC).mkv",
            group_only: "[REPACK]_Serial_Experiments_Lain_(1520x1080_Blu-Ray_FLAC).mkv",
            codec_swap: Some("[REPACK]_Serial_Experiments_Lain_(1520x1080_Blu-Ray_FLAC).mkv"),
            note: "Leading-bracket release group `[Coalgirls]` should be swapped for REPACK. The trailing `(1520x1080_Blu-Ray_FLAC)` is title metadata (resolution/source/audio), not a group.",
            expected_to_pass: true,
        },
        RenameCase {
            input: "[Golumpa] A Certain Magical Index S3 - 01 (Toaru Majutsu no Index III) [FuniDub 1080p x264 AAC] [9443F0FB].mkv",
            group_only: "[REPACK] A Certain Magical Index S3 - 01 (Toaru Majutsu no Index III) [FuniDub 1080p x264 AAC] [9443F0FB].mkv",
            codec_swap: Some("[REPACK] A Certain Magical Index S3 - 01 (Toaru Majutsu no Index III) [FuniDub 1080p x265 AAC] [9443F0FB].mkv"),
            note: "Leading-bracket release group `[Golumpa]` should be swapped for REPACK. The trailing `[9443F0FB]` is a CRC hash. The `x264` inside `[FuniDub 1080p x264 AAC]` should also swap to `x265` on codec update.",
            expected_to_pass: true,
        },

        // ---- Music ----
        RenameCase {
            input: "[THC@HongFire.com].GITS.SAC.2nd.GIG.OST.[FLAC].flac",
            group_only: "[REPACK].GITS.SAC.2nd.GIG.OST.[FLAC].flac",
            codec_swap: Some("[REPACK].GITS.SAC.2nd.GIG.OST.[FLAC].flac"),
            note: "Leading-bracket release group `[THC@HongFire.com]` should be swapped for REPACK. The trailing `[FLAC]` is an audio format tag, not a group.",
            expected_to_pass: true,
        },
        RenameCase {
            input: "01 - Heroes.mp3",
            group_only: "01 - Heroes-REPACK.mp3",
            codec_swap: Some("01 - Heroes-REPACK.mp3"),
            note: "Leading `01 -` is the track number, not a group; song title is preserved and `-REPACK` is appended before the extension. The directory part of the path is the caller's concern; `rename_file` operates on basenames.",
            expected_to_pass: true,
        },

        // ---- Games ----
        // The `RUNE` uploader is the leading release group in the same
        // style as `[Coalgirls]`, `[Golumpa]`, etc. — unbracketed, leading.
        RenameCase {
            input: "rune-company.of.heroes.3.r00",
            group_only: "REPACK-company.of.heroes.3.r00",
            codec_swap: Some("REPACK-company.of.heroes.3.r00"),
            note: "Leading `rune-` is the release group and should swap to REPACK. The split-file form `.r00` is part of the title, not a group.",
            expected_to_pass: true,
        },

        // ---- No-group / edge cases ----
        RenameCase {
            input: "SomeFile.mkv",
            group_only: "SomeFile-REPACK.mkv",
            codec_swap: Some("SomeFile-REPACK.mkv"),
            note: "No release group, REPACK appended before extension",
            expected_to_pass: true,
        },
        RenameCase {
            input: "README",
            group_only: "README-REPACK",
            codec_swap: None,
            note: "No extension, REPACK appended at end",
            expected_to_pass: true,
        },
        RenameCase {
            input: "Movie.Title.2024.1080p.BluRay.x265-REPACK.mkv",
            group_only: "Movie.Title.2024.1080p.BluRay.x265-REPACK.mkv",
            codec_swap: Some("Movie.Title.2024.1080p.BluRay.x265-REPACK.mkv"),
            note: "Already REPACK, unchanged",
            expected_to_pass: true,
        },
        RenameCase {
            input: "movie.x264.mkv",
            group_only: "movie.x264-REPACK.mkv",
            codec_swap: Some("movie.x265-REPACK.mkv"),
            note: "`x264` is a codec tag (denylist hit) so it is not extracted as a group; -REPACK is appended after it. Codec swap turns x264→x265.",
            expected_to_pass: true,
        },
        RenameCase {
            input: "Movie.Title.2024.1080p.BluRay.h264-GROUP.mkv",
            group_only: "Movie.Title.2024.1080p.BluRay.h264-REPACK.mkv",
            codec_swap: Some("Movie.Title.2024.1080p.BluRay.x265-REPACK.mkv"),
            note: "h264 codec tag, both group swap and codec swap on update",
            expected_to_pass: true,
        },
    ];

    #[test]
    fn test_rename_cases() {
        let mut failures: Vec<String> = Vec::new();

        for (idx, case) in RENAME_CASES.iter().enumerate() {
            // Test group-only (no codec update).
            let result = rename_file(Path::new(case.input), false, "REPACK").unwrap();
            if result != case.group_only {
                failures.push(format!(
                    "[{idx}] {note}\n  input:  {input}\n  expected (group): {expected}\n  got:             {got}\n  expected_to_pass: {pass}",
                    idx = idx,
                    note = case.note,
                    input = case.input,
                    expected = case.group_only,
                    got = result,
                    pass = case.expected_to_pass,
                ));
            }

            // Test codec-swap path.
            if let Some(expected_codec) = case.codec_swap {
                let result = rename_file(Path::new(case.input), true, "REPACK").unwrap();
                if result != expected_codec {
                    failures.push(format!(
                        "[{idx}] {note}\n  input:  {input}\n  expected (codec): {expected}\n  got:             {got}\n  expected_to_pass: {pass}",
                        idx = idx,
                        note = case.note,
                        input = case.input,
                        expected = expected_codec,
                        got = result,
                        pass = case.expected_to_pass,
                    ));
                }
            }
        }

        assert!(
            failures.is_empty(),
            "\n\n{} rename case(s) failed:\n\n{}\n",
            failures.len(),
            failures.join("\n\n")
        );
    }

    // ---- Named tests for the most important edge cases ----

    #[test]
    fn test_rename_pantheon_extracts_real_group_not_codec_word() {
        // The release group is `d3g`. The new trailing-form parser walks
        // the whole string for the last separator, finds the `-` before
        // `d3g`, and the denylist rejects `hevc` as a codec so we don't
        // accidentally extract it. The codec `hevc` stays in the title.
        let result = rename_file(
            Path::new("Pantheon S01-S02 web 10bit hevc-d3g.mkv"),
            false,
            "REPACK",
        )
        .unwrap();
        assert_eq!(
            result, "Pantheon S01-S02 web 10bit hevc-REPACK.mkv",
            "parser should extract `d3g` as the group, not `hevc`"
        );
    }

    #[test]
    fn test_rename_lain_preserves_compound_group() {
        // `[Coalgirls]_Serial_Experiments_Lain_(1520x1080_Blu-Ray_FLAC)`
        // has a leading-bracket release group `[Coalgirls]`. The trailing
        // `(1520x1080_Blu-Ray_FLAC)` is title metadata. `FLAC` is the
        // audio codec inside the release, not a group.
        let result = rename_file(
            Path::new("[Coalgirls]_Serial_Experiments_Lain_(1520x1080_Blu-Ray_FLAC).mkv"),
            false,
            "REPACK",
        )
        .unwrap();
        assert_eq!(
            result,
            "[REPACK]_Serial_Experiments_Lain_(1520x1080_Blu-Ray_FLAC).mkv",
            "parser should recognize the leading-bracket `[Coalgirls]` as the release group and swap it for REPACK"
        );
    }

    #[test]
    fn test_rename_golumpa_preserves_uploader_and_swaps_inner_codec() {
        // The `[Golumpa]` uploader tag appears at the start of the title
        // and is the actual release group. The trailing `[9443F0FB]` is
        // a CRC hash. The codec tag `x264` lives inside the middle
        // `[FuniDub 1080p x264 AAC]` block and should be swapped to
        // `x265` on codec update.
        let result = rename_file(
            Path::new(
                "[Golumpa] A Certain Magical Index S3 - 01 (Toaru Majutsu no Index III) [FuniDub 1080p x264 AAC] [9443F0FB].mkv",
            ),
            true,
            "REPACK",
        )
        .unwrap();
        assert_eq!(
            result,
            "[REPACK] A Certain Magical Index S3 - 01 (Toaru Majutsu no Index III) [FuniDub 1080p x265 AAC] [9443F0FB].mkv",
            "parser should extract [Golumpa] as the group and swap x264→x265 inside the codec tag"
        );
    }

    #[test]
    fn test_rename_music_preserves_track_title() {
        // For a track file `01 - Heroes.mp3`, the song title is part of
        // the filename. The track-number detector puts `01` in `track`,
        // the group detector finds no group, and `-REPACK` is appended
        // before the extension. The directory part of the path is the
        // caller's concern (handled in `rename_directory`).
        let result = rename_file(
            Path::new("01 - Heroes.mp3"),
            false,
            "REPACK",
        )
        .unwrap();
        assert_eq!(
            result, "01 - Heroes-REPACK.mp3",
            "parser should preserve the track title and only append -REPACK before .mp3"
        );
    }

    #[test]
    fn test_rename_non_video_to_match_with_spaces() {
        let subtitle_path = Path::new("Show.Name.S01E02.1080p.WEB-DL.srt");
        let result = rename_non_video_to_match(
            "Show.Name.S01E02.1080p.WEB-DL.srt",
            subtitle_path,
            "Show.Name.S01E02.1080p.WEB-DL-REPACK",
        )
        .unwrap();
        assert_eq!(result, "Show.Name.S01E02.1080p.WEB-DL-REPACK.srt");
    }

    // ----------------------------------------------------------------------
    // Parser unit tests
    // ----------------------------------------------------------------------

    #[test]
    fn test_parse_release_metadata_trailing_basic() {
        let meta = parse_release_metadata("Movie.x264-GROUP.mkv");
        assert_eq!(meta.group.as_ref().unwrap().name, "GROUP");
        assert_eq!(meta.group.as_ref().unwrap().style, GroupStyle::TrailingHyphen);
    }

    #[test]
    fn test_parse_release_metadata_trailing_dot() {
        let meta = parse_release_metadata("Movie.GROUP.mkv");
        assert_eq!(meta.group.as_ref().unwrap().name, "GROUP");
        assert_eq!(meta.group.as_ref().unwrap().style, GroupStyle::TrailingDot);
    }

    #[test]
    fn test_parse_release_metadata_leading_bracket() {
        let meta = parse_release_metadata("[Coalgirls]_Lain.mkv");
        let g = meta.group.as_ref().unwrap();
        assert_eq!(g.name, "Coalgirls");
        assert_eq!(g.style, GroupStyle::LeadingBracket);
    }

    #[test]
    fn test_parse_release_metadata_leading_unbracketed() {
        let meta = parse_release_metadata("rune-company.of.heroes.3.r00");
        let g = meta.group.as_ref().unwrap();
        assert_eq!(g.name, "rune");
        assert_eq!(g.style, GroupStyle::LeadingUnbracketed);
    }

    #[test]
    fn test_parse_release_metadata_no_group() {
        let meta = parse_release_metadata("SomeFile.mkv");
        assert!(meta.group.is_none());
        assert_eq!(meta.without_group, "SomeFile");
        assert_eq!(meta.ext, "mkv");
    }

    #[test]
    fn test_parse_release_metadata_codec_tag_not_group() {
        let meta = parse_release_metadata("movie.x264.mkv");
        // x264 is in the denylist — it must not be extracted as a group.
        assert!(meta.group.is_none());
        // The codec field captures it for the pipeline to use.
        assert_eq!(meta.codec, Some("x264".to_string()));
    }

    #[test]
    fn test_parse_release_metadata_track_number_not_group() {
        let meta = parse_release_metadata("01 - Heroes.mp3");
        assert!(meta.group.is_none());
        assert_eq!(meta.track, Some(1));
    }

    #[test]
    fn test_parse_release_metadata_season_episode() {
        let meta = parse_release_metadata("Show.S01E02.1080p.mkv");
        let se = meta.season_episode.as_ref().unwrap();
        assert_eq!(se.season, Some(1));
        assert_eq!(se.episode, Some(2));
    }

    #[test]
    fn test_parse_release_metadata_resolution() {
        let meta = parse_release_metadata("Show.S01E02.1080p.mkv");
        assert_eq!(meta.resolution, Some("1080p".to_string()));
    }

    #[test]
    fn test_parse_release_metadata_source_bluray() {
        let meta = parse_release_metadata("Movie.2024.BluRay.x264-GROUP.mkv");
        assert_eq!(meta.source, Some("BluRay".to_string()));
    }

    #[test]
    fn test_parse_release_metadata_codec_x264() {
        let meta = parse_release_metadata("Movie.2024.BluRay.x264-GROUP.mkv");
        assert_eq!(meta.codec, Some("x264".to_string()));
    }

    #[test]
    fn test_rename_drops_sample_suffix() {
        let meta = parse_release_metadata("Star.Wars-EPSiLON.sample.mkv");
        // Sample is dropped from the stem; the rest is parsed normally.
        assert_eq!(meta.ext, "mkv");
        // The stem is "Star.Wars-EPSiLON" (sample dropped) and the
        // group is detected on that stem.
        let g = meta.group.as_ref().unwrap();
        assert_eq!(g.name, "EPSiLON");

        // And the rename actually produces the right output.
        let result = rename_file(
            Path::new("Star.Wars-EPSiLON.sample.mkv"),
            false,
            "REPACK",
        )
        .unwrap();
        assert_eq!(result, "Star.Wars-REPACK.mkv");
    }

    #[test]
    fn test_denylist_catches_x264() {
        let meta = parse_release_metadata("movie.x264.mkv");
        assert!(meta.group.is_none());
    }

    #[test]
    fn test_denylist_catches_bracketed_flac() {
        // The bracketed form should not be confused with a group; the
        // brackets are preserved and the audio tag stays in the title.
        let meta = parse_release_metadata("[SomeArtist].Album.[FLAC].flac");
        // The leading-bracket detector would normally grab `SomeArtist`,
        // but `FLAC` is in the denylist so the trailing form correctly
        // sees no group at the end either.
        if let Some(g) = &meta.group {
            assert_ne!(g.name.to_lowercase(), "flac");
        }
        // The codec field captures the FLAC tag for the audio-pipeline use.
        assert_eq!(meta.codec, Some("FLAC".to_string()));
    }

    #[test]
    fn test_is_false_positive_group() {
        // Codecs
        assert!(is_false_positive_group("x264"));
        assert!(is_false_positive_group("X264"));
        assert!(is_false_positive_group("H265"));
        assert!(is_false_positive_group("hevc"));
        // Audio
        assert!(is_false_positive_group("flac"));
        assert!(is_false_positive_group("FLAC"));
        assert!(is_false_positive_group("DTS-HD.MA"));
        // Quality
        assert!(is_false_positive_group("1080p"));
        assert!(is_false_positive_group("4k"));
        // Sources
        assert!(is_false_positive_group("BluRay"));
        assert!(is_false_positive_group("web-dl"));
        // Numbers
        assert!(is_pure_number("01"));
        assert!(is_pure_number("1234"));
        assert!(!is_pure_number("abc"));
        // Real groups (should NOT be flagged)
        assert!(!is_false_positive_group("KONTRAST"));
        assert!(!is_false_positive_group("MeGusta"));
        assert!(!is_false_positive_group("d3g"));
        assert!(!is_false_positive_group("rune"));
    }

    #[test]
    fn test_split_basename() {
        let (stem, ext) = split_basename("Movie.2024.mkv");
        assert_eq!(stem, "Movie.2024");
        assert_eq!(ext, "mkv");

        let (stem, ext) = split_basename("noext");
        assert_eq!(stem, "noext");
        assert_eq!(ext, "");

        let (stem, ext) = split_basename(".hidden");
        assert_eq!(stem, ".hidden");
        assert_eq!(ext, "");
    }

    #[test]
    fn test_strip_sample_suffix() {
        assert_eq!(
            strip_sample_suffix("Star.Wars-EPSiLON.sample"),
            Some("Star.Wars-EPSiLON".to_string())
        );
        assert_eq!(
            strip_sample_suffix("Star.Wars-EPSiLON.SAMPLE"),
            Some("Star.Wars-EPSiLON".to_string())
        );
        assert_eq!(strip_sample_suffix("Star.Wars.mkv"), None);
    }

    // ----------------------------------------------------------------------
    // End-to-end rename_directory test
    //
    // Verifies the directory-rename path runs end-to-end with the
    // metadata-lookup trait wired in. The function is async, so these
    // tests use `#[tokio::test]`. The MockLookup is used here to
    // satisfy the trait; its own behavior is tested in
    // `metadata::tests`.
    // ----------------------------------------------------------------------

    use crate::metadata::MockLookup;

    fn make_test_config(tmp: &tempfile::TempDir, db_path: &PathBuf) -> Config {
        Config {
            ssh: crate::config::SshConfig {
                host: "test".to_string(),
                port: None,
                user: "test".to_string(),
                private_key_path: PathBuf::from("/dev/null"),
                remote_base_path: PathBuf::from("/tmp"),
            },
            database: crate::config::DatabaseConfig {
                path: db_path.clone(),
            },
            paths: crate::config::PathsConfig {
                staging: tmp.path().to_path_buf(),
                library: tmp.path().to_path_buf(),
            },
            plex: crate::config::PlexConfig {
                url: "http://test:32400".to_string(),
                sections: std::collections::HashMap::new(),
            },
            logging: None,
            group_name: Some("REPACK".to_string()),
            categories: std::collections::HashMap::new(),
            metadata: crate::config::MetadataConfig::default(),
        }
    }

    #[tokio::test]
    async fn test_rename_directory_with_mock_lookup_renames_files() {
        // Build a temp staging dir with a real video file. The file
        // name parses to `MeGusta` as the group; the metadata lookup
        // is a noop for this test (the lookup invocation path is
        // exercised by the metadata module's own tests). The point of
        // this test is to verify the directory rename succeeds when
        // the lookup is wired in.
        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().to_path_buf();
        let video = staging.join("Shoresy.S05E03.1080p.HEVC.x265-MeGusta.mkv");
        std::fs::write(&video, b"fake video data").unwrap();

        // Use an in-memory DB so the test doesn't depend on the
        // tempdir's lifecycle (and avoid the SQLite "file has moved"
        // error that comes from tempdir cleanup racing with the
        // connection's WAL checkpoint).
        let db = Database::open(std::path::Path::new(":memory:")).unwrap();
        db.upsert_directory(
            "movies",
            staging.to_string_lossy().as_ref(),
            "staging-original",
            "abc123",
        ).unwrap();
        let dir = db.get_directory_by_id(1).unwrap().unwrap();
        let dir_id = dir.id;

        let config = make_test_config(&tmp, &PathBuf::from(":memory:"));

        // The MockLookup is used here. With no tokio runtime in
        // scope, the inner `Handle::try_current()` returns `Err` and
        // the lookup step is skipped — the rename still succeeds.
        let lookup = MockLookup::empty();
        let result = rename_directory(
            &staging,
            &config,
            "movies",
            &db,
            dir_id,
            None,
            &lookup,
        ).await;
        assert!(result.is_ok(), "rename_directory failed: {:?}", result.err());
        // The MockLookup is empty, so no canonical title is returned.
        assert!(result.unwrap().is_none());

        // The video file was actually renamed.
        let renamed = staging.join("Shoresy.S05E03.1080p.HEVC.x265-REPACK.mkv");
        assert!(renamed.exists(), "renamed file should exist at {:?}", renamed);
        assert!(!video.exists(), "original file should have been moved");
    }

    #[tokio::test]
    async fn test_rename_directory_noop_lookup_still_renames() {
        // The noop lookup is the default. Verify rename_directory
        // works with it and produces the same file output.
        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().to_path_buf();
        let video = staging.join("Shoresy.S05E03.1080p.HEVC.x265-MeGusta.mkv");
        std::fs::write(&video, b"fake").unwrap();

        let db = Database::open(std::path::Path::new(":memory:")).unwrap();
        db.upsert_directory(
            "movies",
            staging.to_string_lossy().as_ref(),
            "staging-original",
            "abc123",
        ).unwrap();
        let dir = db.get_directory_by_id(1).unwrap().unwrap();
        let dir_id = dir.id;

        let config = make_test_config(&tmp, &PathBuf::from(":memory:"));

        let noop = crate::metadata::NoopLookup;
        let result = rename_directory(
            &staging,
            &config,
            "movies",
            &db,
            dir_id,
            None,
            &noop,
        ).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none(), "noop lookup returns no canonical");

        let renamed = staging.join("Shoresy.S05E03.1080p.HEVC.x265-REPACK.mkv");
        assert!(renamed.exists());
    }

    #[tokio::test]
    async fn test_rename_directory_returns_canonical_when_lookup_hits() {
        // MockLookup returns a CanonicalTitle for the parsed key;
        // rename_directory should surface it so the move step can
        // use it for the library layout.
        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().to_path_buf();
        let video = staging.join("Shoresy.S05E03.1080p.HEVC.x265-MeGusta.mkv");
        std::fs::write(&video, b"fake").unwrap();

        let db = Database::open(std::path::Path::new(":memory:")).unwrap();
        db.upsert_directory(
            "tvshows",
            staging.to_string_lossy().as_ref(),
            "staging-original",
            "abc123",
        ).unwrap();
        let dir = db.get_directory_by_id(1).unwrap().unwrap();
        let dir_id = dir.id;

        let config = make_test_config(&tmp, &PathBuf::from(":memory:"));

        // MockLookup returns a canonical for the without_group key.
        // After the parser strips the group `MeGusta`, the without_group
        // is `Shoresy.S05E03.1080p.HEVC.x265-` (the trailing separator
        // before the group is preserved as part of the stem). We
        // register against that exact key.
        let mut entries = std::collections::HashMap::new();
        entries.insert(
            "tvshows|Shoresy.S05E03.1080p.HEVC.x265-".to_string(),
            crate::metadata::CanonicalTitle {
                title: "Shoresy".to_string(),
                year: Some(2022),
                external_id: "tt14058038".to_string(),
                season_count: Some(5),
            },
        );
        let lookup = MockLookup::new(entries);
        let result = rename_directory(
            &staging,
            &config,
            "tvshows",
            &db,
            dir_id,
            None,
            &lookup,
        ).await;
        let canonical = result.expect("rename should succeed");
        let canonical = canonical.expect("mock lookup returns a canonical");
        assert_eq!(canonical.title, "Shoresy");
        assert_eq!(canonical.year, Some(2022));
        assert_eq!(canonical.external_id, "tt14058038");
    }
}
