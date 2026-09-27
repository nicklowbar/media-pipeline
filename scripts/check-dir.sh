#!/bin/bash
# Drill into one directory: its pipeline row, per-file download states,
# and hash coverage.
#
# Matches directories by a substring of staging_path (LIKE %fragment%).
#
# Usage:
#   ./check-dir.sh <name-fragment> [hostname]
#
# Defaults: hostname = physalis

set -e

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <name-fragment> [hostname]" >&2
    exit 64
fi
FRAGMENT="${1//\'/\'\'}"
HOST="${2:-physalis}"
DB_PATH="/data/pipeline.db"

sql() {
    ssh "$HOST" "docker exec media-pipeline sqlite3 -readonly -header -column \"$DB_PATH\" \"$1\""
}

echo "=== Matching directories on $HOST ==="
DIR_IDS=$(sql "SELECT id FROM directories WHERE staging_path LIKE '%$FRAGMENT%';")

if [[ -z "$DIR_IDS" ]]; then
    echo "No directories match '$1'."
    exit 1
fi

ID_LIST=$(echo "$DIR_IDS" | tr '\n' ',' | sed 's/,$//')

sql "SELECT id, category, state, remote_path, staging_path, library_path,
       manifest_hash, detected_at, synced_at, analyzed_at, renamed_at, moved_at,
       error_message
  FROM directories
 WHERE id IN ($ID_LIST);"

echo ""
echo "=== Files (original, renamed, download state) ==="
sql "SELECT f.dir_id, f.original_name, COALESCE(f.renamed_name, '-') AS renamed_name,
       COALESCE(fd.state, 'no-row') AS download_state, fd.error_message
  FROM files f
  LEFT JOIN file_downloads fd
    ON fd.dir_id = f.dir_id AND fd.rel_path = f.original_name
 WHERE f.dir_id IN ($ID_LIST)
 ORDER BY f.dir_id, f.original_name;"

echo ""
echo "=== Hash coverage ==="
sql "SELECT d.id, d.staging_path,
       (SELECT COUNT(*) FROM file_hashes    WHERE dir_id = d.id) AS hashed,
       (SELECT COUNT(*) FROM file_downloads WHERE dir_id = d.id) AS tracked,
       (SELECT COUNT(*) FROM file_downloads WHERE dir_id = d.id AND state = 'synced') AS synced
  FROM directories d
 WHERE d.id IN ($ID_LIST);"