# media-pipeline

Automated media sync, rename, and ingest pipeline.

Downloads media from a remote host via SSH/SFTP, renames files to a consistent format, and moves the result into a Plex/Jellyfin library. Library-side re-encoding (x264 → HEVC, target resolution ladder, format normalization) is handled by [Tdarr](https://home.tdarr.info/) — see `memory/architecture-pipeline-vs-tdarr.md` in the project vault for the split rationale.

## Features

- **SSH/SFTP sync** with manifest-based change detection (no more brittle rsync exclude lists)
- **Per-title auto-detection** of transcode needs via `ffprobe`
- **Configurable release group renaming** (replaces original uploader group names)
- **Atomic operations** throughout: temp files during transcode, atomic moves into the library
- **SQLite-backed state machine** tracks every title from `detected` → `in_library`
- **Plex library scan trigger** after ingest
- **Docker-ready** with multi-stage build

## Architecture

```
Remote host (downloads)
    │
    │  SFTP list + SHA-256 manifest hash
    ▼
Docker container (or local)
    ├── Staging volume
    ├── ffprobe analysis → DetectedPolicy
    ├── Rename files (group replacement + codec tag update)
    └── Atomic move to library mount
    │
    ▼
Library storage (TrueNAS / NAS / local)
    ├── TvShows
    ├── Movies
    ├── Music
    └── ...
```

The transcode step is deliberately omitted — Tdarr owns library-side re-encoding. See "Library stewardship" below.

## Quick Start

### 1. Build

```bash
cd media-pipeline
cargo build --release
```

Or build the Docker image:

```bash
docker build -t media-pipeline:latest .
```

### 2. Configure

Copy the example config and edit to match your environment:

```bash
cp config/media-pipeline.toml.example config/media-pipeline.toml
```

Key settings:

| Section | Purpose |
|---------|---------|
| `[ssh]` | Remote host to sync from |
| `[paths]` | Local staging and library directories |
| `group_name` | Release group name for renamed files (default: `REPACK`) |
| `[plex]` | Plex URL and section keys for scan triggers |
| `[categories.*]` | Maps remote directories to local library folders |

### 3. Run

```bash
# Full pipeline: sync → analyze → rename → transcode → move → plex scan
./target/release/media-pipeline run --config config/media-pipeline.toml

# Or individual phases
./target/release/media-pipeline sync-only --config config/media-pipeline.toml
./target/release/media-pipeline process-only --config config/media-pipeline.toml

# Check pipeline status
./target/release/media-pipeline status --config config/media-pipeline.toml
```

### 4. Docker (recommended for transcoding workloads)

```bash
docker run --rm \
  -v /mnt/mediaserver:/library \
  -v /opt/media-pipeline/staging:/staging \
  -v /opt/media-pipeline/config:/etc/media-pipeline:ro \
  -v /opt/media-pipeline/ssh:/root/.ssh:ro \
  -v /opt/media-pipeline/data:/data \
  -e MEDIA_PIPELINE_PLEX_TOKEN=your-token-here \
  media-pipeline:latest
```

## Detected Policies

After syncing, each title is analyzed to determine the appropriate post-processing:

| Policy | Trigger | Action |
|--------|---------|--------|
| `none` | Already HEVC/x265 or no video files | Skip transcoding |
| `x264_to_x265` | Source is H.264 | Re-encode to x265 (HEVC), AAC audio, copy subtitles |
| `downscale_1080p` | Source is 4K/H.264 (if enabled) | Re-encode to 1080p x265 with lanczos scaling |
| `manual` | Unrecognizable format (DVD ISO, etc.) | Skip; requires manual intervention |

Policies are detected per-title, not per-category.

## State Machine

Every top-level directory is tracked in SQLite:

```
detected → syncing → synced → analyzing → analyzed → renaming → renamed
    → moving → in_library
```

The `transcoding` and `transcoded` states are part of the schema for historical reasons but are no longer entered on a normal run — see "Library stewardship" below.

Failed states are recoverable: if the remote manifest changes, the record resets to `detected` for reprocessing.

## Library stewardship (Tdarr)

This pipeline drops files into the library as-is. Re-encoding to a target spec (HEVC/x265, the 4K → 1080p → 720p → 480p quality ladder, support for x264 / AVI / DVD-ISO inputs) is handled by [Tdarr](https://home.tdarr.info/), which walks the library periodically and re-encodes anything that doesn't match its configured health check.

Integration is filesystem-only: Tdarr watches the same `/library/` mount the pipeline writes to. No API coupling, no shared DB. The pipeline's `transcode` module and the `Transcoding`/`Transcoded` state variants are kept in the codebase for the rare case where a future operator wants to force-re-encode a batch, but they are not invoked by the normal `run` command.

## Testing

```bash
cargo test
```

Tests cover config parsing, DB state transitions, rename regex logic, policy selection, library move semantics, and Plex URL construction.

## Requirements

- Rust 1.78+ (for building from source)
- `ffprobe` (runtime, for ffprobe analysis)
- SSH private key for remote host access
- Plex token (optional, for library scan triggers)

## License

MIT — see [LICENSE](LICENSE).
