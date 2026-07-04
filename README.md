# media-pipeline

Automated media sync, rename, and ingest pipeline.

Downloads media from a remote host via SSH/SFTP, renames files to a consistent format, and moves the result into a Plex/Jellyfin library. Library-side re-encoding (x264 → HEVC, target resolution ladder, format normalization) is handled by [Tdarr](https://home.tdarr.info/) — see `TechnicalNotes/Projects/MediaPipeline/` in the Obsidian vault for the split rationale.

## Features

- **SSH/SFTP sync** with manifest-based change detection (no more brittle rsync exclude lists)
- **Per-file concurrent downloads** — multiple files from the same directory can download simultaneously; a stalled file no longer blocks other files in the same directory
- **SQLite-backed two-tier state machine** — directory-level states (`detected → in_library`) with a per-file `file_downloads` table (`detected → downloading → synced | failed`) underneath
- **Configurable release group renaming** (replaces original uploader group names)
- **Atomic operations** throughout: temp files during rename, atomic moves into the library
- **Plex library scan trigger** after ingest
- **Daemon mode** — runs continuously with a configurable sleep interval between syncs
- **Docker-ready** with multi-stage build

## Architecture

```
Remote host (downloads)
    │
    │  SFTP list + SHA-256 manifest hash
    ▼
Docker container (or local)
    ├── Staging volume
    ├── Walk: collect per-file manifests, claim files
    ├── Download: 4 concurrent file slots, per-file SHA-256 verification
    ├── Rename files (group replacement + codec tag update)  [disabled]
    └── Atomic move to library mount                          [disabled]
    │
    ▼
Library storage (TrueNAS / NAS / local)
    ├── TvShows
    ├── Movies
    ├── Music
    └── ...
```

Rename and move pools are disabled pending a CIFS permission fix (container runs as `uid=1000/gid=1111` on physalis). Files land in staging after download; a separate move step is required to get them into the library. See `pipeline.rs` module docs for the re-enablement path.

The transcode step is deliberately omitted — Tdarr owns library-side re-encoding. See "Library stewardship" below.

## Quick Start

### 1. Build

```bash
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
# One-shot: sync all categories once and exit
./target/release/media-pipeline run --config config/media-pipeline.toml

# Daemon mode: run continuously, sleeping 12h between syncs
./target/release/media-pipeline run --interval 12h --config config/media-pipeline.toml

# Check pipeline status
./target/release/media-pipeline status --config config/media-pipeline.toml
```

### 4. Docker

```bash
docker run --rm \
  -v /mnt/mediaserver:/library \
  -v /mnt/mediaserver/Staging:/staging \
  -v /opt/media-pipeline/config:/etc/media-pipeline:ro \
  -v /opt/media-pipeline/ssh:/ssh:ro \
  -v /opt/media-pipeline/data:/data \
  -e MEDIA_PIPELINE_PLEX_TOKEN=your-token-here \
  media-pipeline:latest run --interval 12h
```

Note: the container must run with the CIFS mount's gid (typically `1111`) as a secondary group, e.g. `--user 1000:1111`, so that files written to the TrueNAS share pass the mount's `forcegid` check.

## State Machine

The pipeline tracks state at two levels:

### Directory-level states

Every top-level directory is tracked in SQLite:

```
detected → syncing → synced → analyzing → analyzed → renaming → renamed
    → moving → in_library
```

Failed states are recoverable: if the remote manifest changes, the record resets to `detected` for reprocessing.

### Per-file states (file_downloads table)

Under the directory level, each file in a directory being downloaded has its own state machine:

```
detected → downloading → synced
                       → failed (retryable; resets to detected on next walk)
```

The file-level state allows multiple files from the same directory to download concurrently. A stalled or failed file does not block other files in the same directory. When a directory's files are all `synced`, the directory transitions to `syncing_done` (internal) and then `synced`.

### Stale file recovery

At pipeline startup, a stale-file sweep recovers any files stuck in `downloading` state older than 6 hours — these are reset to `detected` so they can be re-claimed and re-downloaded.

## Library stewardship (Tdarr)

This pipeline drops files into the library as-is. Re-encoding to a target spec (HEVC/x265, the 4K → 1080p → 720p → 480p quality ladder, support for x264 / AVI / DVD-ISO inputs) is handled by [Tdarr](https://home.tdarr.info/), which walks the library periodically and re-encodes anything that doesn't match its configured health check.

Integration is filesystem-only: Tdarr watches the same `/library/` mount the pipeline writes to. No API coupling, no shared DB.

## Testing

```bash
cargo test
```

Tests cover config parsing, DB state transitions, rename regex logic, policy selection, library move semantics, Plex URL construction, and the file-download state machine.

## Requirements

- Rust 1.78+
- `ffprobe` (runtime, for ffprobe analysis)
- SSH private key for remote host access
- Plex token (optional, for library scan triggers)

## License

MIT — see [LICENSE](LICENSE).
