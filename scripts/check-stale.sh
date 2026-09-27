#!/bin/bash
# Show stale in-flight rows: directories stuck in syncing and files stuck
# downloading past the pipeline's 6-hour sweep threshold.
#
# The next pipeline run recovers these automatically; this shows them
# before that, e.g. to judge whether a run was interrupted.
#
# Usage:
#   ./check-stale.sh [hostname]
#
# Defaults: hostname = physalis

set -e

HOST="${1:-physalis}"
DB_PATH="/data/pipeline.db"

sql() {
    ssh "$HOST" "docker exec media-pipeline sqlite3 -readonly -header -column \"$DB_PATH\" \"$1\""
}

echo "=== Stale syncing directories on $HOST (> 6h) ==="
sql "SELECT id, category, staging_path, syncing_at
  FROM directories
 WHERE state = 'syncing'
   AND syncing_at < datetime('now', '-6 hours');"

echo ""
echo "=== Stale downloading files (> 6h) ==="
sql "SELECT d.category, d.staging_path, fd.rel_path, fd.downloading_at, fd.error_message
  FROM file_downloads fd
  JOIN directories d ON d.id = fd.dir_id
 WHERE fd.state = 'downloading'
   AND fd.downloading_at < datetime('now', '-6 hours');"

echo ""
echo "=== Any in-flight rows (any age) ==="
sql "SELECT
       (SELECT COUNT(*) FROM directories    WHERE state = 'syncing')    AS syncing_dirs,
       (SELECT COUNT(*) FROM file_downloads WHERE state = 'downloading') AS downloading_files;"