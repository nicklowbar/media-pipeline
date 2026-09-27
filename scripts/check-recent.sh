#!/bin/bash
# Show recently completed directories: moved into the library, newest first.
#
# Usage:
#   ./check-recent.sh [hostname] [limit]
#
# Defaults: hostname = physalis, limit = 20

set -e

HOST="${1:-physalis}"
LIMIT="${2:-20}"
DB_PATH="/data/pipeline.db"

sql() {
    ssh "$HOST" "docker exec media-pipeline sqlite3 -readonly -header -column \"$DB_PATH\" \"$1\""
}

echo "=== Recently moved to library on $HOST (max $LIMIT) ==="
sql "SELECT category, staging_path, library_path, moved_at
  FROM directories
 WHERE state = 'in_library'
 ORDER BY moved_at DESC
 LIMIT $LIMIT;"

echo ""
echo "=== Library inventory by category ==="
sql "SELECT category, COUNT(*) AS titles
  FROM directories
 WHERE state = 'in_library'
 GROUP BY category
 ORDER BY titles DESC;"