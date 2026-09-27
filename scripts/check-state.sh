#!/bin/bash
# Show the overall pipeline state distribution on a host.
#
# Sections:
#   1. Directory state counts (all pipeline phases)
#   2. Per-category breakdown of directory states
#   3. Per-file download state counts (file_downloads)
#   4. Metadata cache size / expired count
#
# Usage:
#   ./check-state.sh [hostname]
#
# Defaults: hostname = physalis

set -e

HOST="${1:-physalis}"
DB_PATH="/data/pipeline.db"

sql() {
    ssh "$HOST" "docker exec media-pipeline sqlite3 -readonly -header -column \"$DB_PATH\" \"$1\""
}

echo "=== Directory states on $HOST ==="
sql "SELECT state, COUNT(*) AS count FROM directories GROUP BY state ORDER BY count DESC;"

echo ""
echo "=== Per-category breakdown ==="
sql "SELECT category, state, COUNT(*) AS count FROM directories GROUP BY category, state ORDER BY category, state;"

echo ""
echo "=== Per-file download states ==="
sql "SELECT state, COUNT(*) AS count FROM file_downloads GROUP BY state ORDER BY count DESC;"

echo ""
echo "=== Metadata cache ==="
sql "SELECT COUNT(*) AS total, COALESCE(SUM(expires_at < datetime('now')), 0) AS expired FROM metadata_cache;"