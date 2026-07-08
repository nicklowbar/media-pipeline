#!/bin/bash
# Query the media-pipeline database for titles with files currently downloading.
# Uses the file_downloads table (per-file state) rather than directory state.
#
# Usage:
#   ./list-downloading.sh [hostname]
#
# Defaults: hostname = physalis

set -e

HOST="${1:-physalis}"
DB_PATH="/data/pipeline.db"

echo "=== Downloading titles on $HOST ==="
echo ""

ssh "$HOST" "docker exec media-pipeline sqlite3 -header -column \\
    \"$DB_PATH\" \\
    \"SELECT
        d.id,
        d.category,
        d.state AS dir_state,
        d.staging_path,
        COUNT(fd.id) AS total_files,
        SUM(CASE WHEN fd.state = 'downloading' THEN 1 ELSE 0 END) AS downloading,
        SUM(CASE WHEN fd.state = 'detected' THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN fd.state = 'synced' THEN 1 ELSE 0 END) AS synced,
        SUM(CASE WHEN fd.state = 'failed' THEN 1 ELSE 0 END) AS failed
     FROM directories d
     LEFT JOIN file_downloads fd ON fd.dir_id = d.id
     GROUP BY d.id
     HAVING downloading > 0 OR failed > 0
     ORDER BY d.category, d.staging_path;
\"" 2>&1

echo ""
echo "=== Summary ==="
echo "Files currently downloading:"
ssh "$HOST" "docker exec media-pipeline sqlite3 -csv \\
    \"$DB_PATH\" \\
    \"SELECT COUNT(*) FROM file_downloads WHERE state = 'downloading';\"" 2>&1

echo "Files in 'failed' state:"
ssh "$HOST" "docker exec media-pipeline sqlite3 -csv \\
    \"$DB_PATH\" \\
    \"SELECT COUNT(*) FROM file_downloads WHERE state = 'failed';\"" 2>&1

echo "Directories with active downloads:"
ssh "$HOST" "docker exec media-pipeline sqlite3 -csv \\
    \"$DB_PATH\" \\
    \"SELECT COUNT(DISTINCT dir_id) FROM file_downloads WHERE state IN ('downloading', 'failed');\"" 2>&1
