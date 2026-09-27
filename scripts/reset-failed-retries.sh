#!/usr/bin/env bash
# Reset failed file_downloads rows to 'detected' in directories that are
# still queued ('detected'), so the next pipeline run re-downloads them.
#
# Scope: only file rows in directories whose state is 'detected'. Files
# that failed in directories which already moved past download (synced,
# in_library, ...) are historical records and are left untouched.
#
# Usage:
#   ./reset-failed-retries.sh [--remote HOST] [--db-path PATH] [--dir-id ID]
#
# Defaults:
#   HOST     physalis (via SSH key ~/.ssh/nicklow-physalis/nicklowbar-nicklow-physalis)
#   DB_PATH  /opt/media-pipeline/data/pipeline.db
#
# --dir-id ID: targeted recovery of one directory. Resets the directory
# row to 'detected' and its failed/downloading file rows to 'detected',
# so the next run re-downloads them (existing bytes are sha-verified and
# deleted if corrupt). Use this for transient transport failures whose
# directory already moved past download.
#
# The script takes a database backup first and shows before/after counts.

set -euo pipefail

log() { echo "[$(date +%H:%M:%S)] $*"; }

SSH_HOST="${SSH_HOST:-physalis}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/nicklow-physalis/nicklowbar-nicklow-physalis}"
DB_PATH="${DB_PATH:-/opt/media-pipeline/data/pipeline.db}"
DIR_ID=""

# ---- Argument parsing ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --remote)
            SSH_HOST="$2"; shift 2 ;;
        --db-path)
            DB_PATH="$2"; shift 2 ;;
        --dir-id)
            DIR_ID="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,24p' "$0"; exit 0 ;;
        *)
            echo "error: unknown flag '$1'" >&2; exit 64 ;;
    esac
done

if [[ -n "$DIR_ID" ]]; then
    log "targeted recovery for dir_id $DIR_ID:"
    log "before:"
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" \
        "sqlite3 -header -column '$DB_PATH' \"SELECT id, state, SUBSTR(staging_path,-55) AS dir FROM directories WHERE id=$DIR_ID; SELECT rel_path, state FROM file_downloads WHERE dir_id=$DIR_ID AND state IN ('failed','downloading');\""
    BACKUP="${DB_PATH}.bak.$(date +%Y%m%dT%H%M%S)"
    log "backing up DB to $BACKUP ..."
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" "cp '$DB_PATH' '$BACKUP'"
    SQL="UPDATE directories SET state = 'detected', error_message = NULL WHERE id = $DIR_ID;
SELECT 'directories → detected, rows: ' || changes();
UPDATE file_downloads SET state = 'detected', error_message = NULL
 WHERE dir_id = $DIR_ID AND state IN ('failed', 'downloading');
SELECT 'file_downloads failed/downloading → detected, rows: ' || changes();"
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" \
        "sqlite3 -batch '$DB_PATH' \"$SQL\""
    log "after:"
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" \
        "sqlite3 -header -column '$DB_PATH' \"SELECT id, state FROM directories WHERE id=$DIR_ID; SELECT state, COUNT(*) FROM file_downloads WHERE dir_id=$DIR_ID GROUP BY state;\""
    log "targeted reset complete. Backup at: $BACKUP"
    exit 0
fi

ssh_cmd=(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST")

# ---- 1. Before counts ----
log "pending work in 'detected' directories before reset:"
"${ssh_cmd[@]}" "sqlite3 -header -column '$DB_PATH' \
    \"SELECT fd.dir_id, SUBSTR(d.staging_path, -60) AS dir, fd.state, COUNT(*) AS n \
      FROM file_downloads fd JOIN directories d ON d.id = fd.dir_id \
      WHERE d.state = 'detected' GROUP BY fd.dir_id, fd.state ORDER BY fd.dir_id, fd.state;\""

# ---- 2. Backup ----
BACKUP="${DB_PATH}.bak.$(date +%Y%m%dT%H%M%S)"
log "backing up DB to $BACKUP ..."
"${ssh_cmd[@]}" "cp '$DB_PATH' '$BACKUP'"

# ---- 3. Reset ----
log "resetting failed rows in detected directories..."
"${ssh_cmd[@]}" "sqlite3 -batch '$DB_PATH'" <<'SQL'
UPDATE file_downloads
   SET state = 'detected',
       error_message = NULL
 WHERE state = 'failed'
   AND dir_id IN (SELECT id FROM directories WHERE state = 'detected');
SELECT 'file_downloads: failed → detected, rows: ' || changes();
SQL

# ---- 4. After counts ----
log "pending work in 'detected' directories after reset:"
"${ssh_cmd[@]}" "sqlite3 -header -column '$DB_PATH' \
    \"SELECT fd.dir_id, SUBSTR(d.staging_path, -60) AS dir, fd.state, COUNT(*) AS n \
      FROM file_downloads fd JOIN directories d ON d.id = fd.dir_id \
      WHERE d.state = 'detected' GROUP BY fd.dir_id, fd.state ORDER BY fd.dir_id, fd.state;\""

log "reset complete. Backup at: $BACKUP"