#!/usr/bin/env bash
# One-time migration: reset stuck file_downloads rows and directories.
#
# Problem: file_downloads rows can get stuck in 'failed' or 'downloading'
# states, and directories can be stuck in 'syncing' — especially after
# interrupted pipeline runs. This script resets them all to a clean state
# so the next pipeline run processes them normally.
#
# Usage:
#   ./migrate-stuck-files.sh [--remote HOST] [--db-path PATH]
#
# Defaults:
#   HOST     physalis (via SSH key ~/.ssh/nicklow-physalis/nicklowbar-nicklow-physalis)
#   DB_PATH  /opt/media-pipeline/data/pipeline.db
#
# Example:
#   ./migrate-stuck-files.sh
#   ./migrate-stuck-files.sh --remote 192.168.0.188 --db-path /tmp/test.db

set -euo pipefail

SSH_HOST="${SSH_HOST:-physalis}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/nicklow-physalis/nicklowbar-nicklow-physalis}"
DB_PATH="${DB_PATH:-/opt/media-pipeline/data/pipeline.db}"

# ---- Argument parsing ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --remote)
            SSH_HOST="$2"; shift 2 ;;
        --db-path)
            DB_PATH="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,20p' "$0"; exit 0 ;;
        *)
            echo "error: unknown flag '$1'" >&2; exit 64 ;;
    esac
done

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ---- 1. Backup ----
BACKUP="${DB_PATH}.bak.$(date +%Y%m%dT%H%M%S)"
log "backing up DB to $BACKUP ..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" \
    "cp '$DB_PATH' '$BACKUP'"

# ---- 2. Before counts ----
log "before migration:"
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" \
    "sqlite3 '$DB_PATH' 'SELECT state, COUNT(*) FROM file_downloads GROUP BY state ORDER BY state;'"
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" \
    "sqlite3 '$DB_PATH' 'SELECT state, COUNT(*) FROM directories GROUP BY state ORDER BY state;'"

# ---- 3. Migration SQL ----
log "running migration..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" \
    "sqlite3 -batch '$DB_PATH'" <<'SQL'

-- Reset failed files to synced
UPDATE file_downloads SET state = 'synced' WHERE state = 'failed';
SELECT 'file_downloads: failed → synced, rows: ' || changes();

-- Reset in-progress files in active directories back to detected (will retry).
-- These were interrupted mid-download; resetting them to detected lets the
-- pool reclaim them on the next run.
UPDATE file_downloads SET state = 'detected'
  WHERE state = 'downloading'
    AND dir_id IN (
        SELECT id FROM directories WHERE state IN ('detected', 'syncing')
    );
SELECT 'file_downloads: downloading in active dirs → detected, rows: ' || changes();

-- Reset in-progress files in completed directories to synced.
-- The directory moved past download while these were in-flight; they
-- either completed or were superseded — mark them synced to reflect reality.
UPDATE file_downloads SET state = 'synced'
  WHERE state = 'downloading'
    AND dir_id IN (
        SELECT id FROM directories
         WHERE state IN ('analyzing', 'analyzed', 'renamed',
                         'moving', 'in_library')
    );
SELECT 'file_downloads: downloading in completed dirs → synced, rows: ' || changes();

-- Reset stuck syncing directories back to detected
UPDATE directories SET state = 'detected', syncing_at = NULL WHERE state = 'syncing';
SELECT 'directories: syncing → detected, rows: ' || changes();

-- Mark files in post-download directories as synced: the pipeline has
-- already moved past download for these, so their file_downloads rows
-- are a historical record that should reflect completion.
UPDATE file_downloads SET state = 'synced'
  WHERE dir_id IN (
      SELECT id FROM directories
       WHERE state IN ('synced', 'analyzing', 'analyzed', 'in_library')
  )
    AND state != 'synced';
SELECT 'file_downloads: post-download dirs → synced, rows: ' || changes();
SQL

# ---- 4. After counts ----
log "after migration:"
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" \
    "sqlite3 '$DB_PATH' 'SELECT state, COUNT(*) FROM file_downloads GROUP BY state ORDER BY state;'"
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" \
    "sqlite3 '$DB_PATH' 'SELECT state, COUNT(*) FROM directories GROUP BY state ORDER BY state;'"

log "migration complete. Backup at: $BACKUP"
