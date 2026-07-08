#!/bin/bash
# Query the media-pipeline database for all titles in 'detected' state
# (synced or analyzed but not moved to library).
#
# Usage:
#   ./list-detected.sh [hostname]
#
# Defaults: hostname = physalis

set -e

HOST="${1:-physalis}"
DB_PATH="/data/pipeline.db"

echo "=== Detected titles on $HOST ==="
echo ""

ssh "$HOST" "docker exec media-pipeline sqlite3 -header -column \\
    \"$DB_PATH\" \\
    \"SELECT
        d.id,
        d.category,
        d.state,
        d.staging_path,
        d.detected_at,
        COUNT(fh.rel_path) AS file_count,
        d.manifest_hash
     FROM directories d
     LEFT JOIN file_hashes fh ON fh.dir_id = d.id
     WHERE d.state = 'detected'
     GROUP BY d.id
     ORDER BY d.category, d.detected_at DESC, d.staging_path;
\"" 2>&1

echo ""
echo "=== Summary by category ==="
ssh "$HOST" "docker exec media-pipeline sqlite3 -csv \\
    \"$DB_PATH\" \\
    \"SELECT category, COUNT(*) AS count FROM directories WHERE state = 'detected' GROUP BY category ORDER BY category;\"" 2>&1

echo ""
echo "Total detected:"
ssh "$HOST" "docker exec media-pipeline sqlite3 -csv \\
    \"$DB_PATH\" \\
    \"SELECT COUNT(*) FROM directories WHERE state = 'detected';\"" 2>&1
