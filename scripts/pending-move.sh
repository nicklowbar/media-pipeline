#!/bin/bash
# Query the media-pipeline database for directories that are synced or analyzed
# but not yet moved to the library (not in 'in_library' state).
#
# Usage:
#   ./pending-move.sh [hostname]
#
# Defaults: hostname = physalis

set -e

HOST="${1:-physalis}"
DB_PATH="/data/pipeline.db"

echo "=== Pending-move query on $HOST ==="
echo ""

ssh "$HOST" "docker exec media-pipeline sqlite3 -header -column \\
    \"$DB_PATH\" \\
    \"SELECT
        d.id,
        d.category,
        d.state,
        d.staging_path,
        COUNT(fh.rel_path) AS file_count,
        COALESCE(d.library_path, '(none)') AS library_path
     FROM directories d
     LEFT JOIN file_hashes fh ON fh.dir_id = d.id
     WHERE d.state IN ('synced', 'analyzed')
     GROUP BY d.id
     ORDER BY d.category, d.state, d.staging_path;
\"" 2>&1

echo ""
echo "=== Summary by state ==="
ssh "$HOST" "docker exec media-pipeline sqlite3 -csv \\
    \"$DB_PATH\" \\
    \"SELECT state, COUNT(*) AS count FROM directories WHERE state IN ('synced', 'analyzed') GROUP BY state ORDER BY state;\"" 2>&1

echo ""
echo "Total not-in-library:"
ssh "$HOST" "docker exec media-pipeline sqlite3 -csv \\
    \"$DB_PATH\" \\
    \"SELECT COUNT(*) FROM directories WHERE state NOT IN ('in_library', 'moving');\"" 2>&1
