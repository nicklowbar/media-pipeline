#!/bin/bash
# Show failed directories and failed file downloads, with error messages.
#
# The populated timestamp columns on a failed directory show how far the
# row got before failing (e.g. moved_at NULL + renamed_at set = failed
# during the move phase).
#
# Usage:
#   ./check-failed.sh [hostname]
#
# Defaults: hostname = physalis

set -e

HOST="${1:-physalis}"
DB_PATH="/data/pipeline.db"

sql() {
    ssh "$HOST" "docker exec media-pipeline sqlite3 -readonly -header -column \"$DB_PATH\" \"$1\""
}

echo "=== Failed directories on $HOST ==="
sql "SELECT id, category, state, staging_path, error_message,
       detected_at, synced_at, analyzed_at, renamed_at, moved_at
  FROM directories
 WHERE state LIKE '%_failed'
 ORDER BY detected_at DESC;"

echo ""
echo "=== Failed file downloads ==="
sql "SELECT d.category, d.staging_path, fd.rel_path, fd.error_message, fd.downloading_at
  FROM file_downloads fd
  JOIN directories d ON d.id = fd.dir_id
 WHERE fd.state = 'failed'
 ORDER BY d.category, d.staging_path;"

echo ""
echo "=== Summary ==="
sql "SELECT (SELECT COUNT(*) FROM directories WHERE state LIKE '%_failed') AS failed_dirs,
       (SELECT COUNT(*) FROM file_downloads WHERE state = 'failed')       AS failed_files;"